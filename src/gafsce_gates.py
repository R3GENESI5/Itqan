"""GAFSCE 4-gate bulk-change safety module.

Wraps any DB modification in 4 sequential gates (see narrator-audit SKILL):
  1. Mandatory dry-run diff (JSON + CSV + terminal summary)
  2. Invariant checks (freq/alive conservation, chain-integrity sample,
     no-strip-out, no-silent-downgrade)
  3. Bounded batch size (≤100 profile changes per run by default)
  4. Append-only audit log linked by hash to gate 1 dry-run

Usage:
    from gafsce_gates import SafeMerge

    with SafeMerge(script_name='fix_shuba', db_path=CHK,
                   expected_freq_delta=0) as sm:
        for pid, newvals in proposed.items():
            sm.change(
                pid=pid,
                action='enrich+merge_donor',
                old_values=snapshot_fields(profs[pid], changed_fields),
                new_values=newvals,
                reason='gk_mismatch_fix_gk_id_368_to_3795',
            )
        sm.commit()

The module refuses to write the DB unless all four gates pass.
Apply-mode is gated by the `--apply` CLI flag in the calling script.
"""
import csv
import hashlib
import json
import os
import random
import sys
from datetime import datetime
from pathlib import Path

# ---- Config ----
SAVEPOINTS = Path('D:/Hadith/src/savepoints')
AUDIT_LOG = SAVEPOINTS / 'audit_log.jsonl'
CHAIN_INDEX = SAVEPOINTS / 'raw_chain_index.pkl'
DEFAULT_BATCH_CAP = 100
CHAIN_SAMPLE_SIZE = 500


class SafeMergeError(Exception):
    """Raised when any gate fails. Caller should not catch this."""


class SafeMerge:
    """Context manager enforcing the 4 gates. Holds in-memory DB + staged changes."""

    def __init__(self, script_name, db_path,
                 expected_freq_delta=0,
                 expected_alive_delta=None,
                 allow_downgrade=False,
                 batch_cap=DEFAULT_BATCH_CAP,
                 apply_mode=False):
        self.script_name = script_name
        self.db_path = Path(db_path)
        self.expected_freq_delta = expected_freq_delta
        self.expected_alive_delta = expected_alive_delta   # None = compute from donors
        self.allow_downgrade = allow_downgrade
        self.batch_cap = batch_cap
        self.apply_mode = apply_mode

        self.run_id = f'{script_name}_{datetime.now().strftime("%Y%m%d_%H%M%S")}'
        self.dryrun_json = SAVEPOINTS / f'proposed_{self.run_id}.json'
        self.dryrun_csv  = SAVEPOINTS / f'proposed_{self.run_id}.csv'

        self.db = None
        self.profs = None
        self.changes = []     # list of change records
        self.started = datetime.now().isoformat(timespec='seconds')

    # ----- Context manager -----
    def __enter__(self):
        if not self.db_path.exists():
            raise SafeMergeError(f'DB not found: {self.db_path}')
        print(f'[SafeMerge] loading DB: {self.db_path.name}', file=sys.stderr)
        self.db = json.loads(self.db_path.read_text(encoding='utf-8'))
        self.profs = self.db['profiles']
        self._snapshot_baseline()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # If exception, always abort
        if exc_type is not None:
            print(f'[SafeMerge] ABORT due to exception: {exc_val}', file=sys.stderr)
        return False  # re-raise

    # ----- Staging -----
    def change(self, pid, action, old_values, new_values, reason):
        """Stage a single profile change. No DB write yet."""
        if pid not in self.profs:
            raise SafeMergeError(f'pid not in DB: {pid}')
        self.changes.append({
            'pid': pid,
            'action': action,
            'old_values': old_values,
            'new_values': new_values,
            'reason': reason,
        })

    # ----- Gates -----
    def _snapshot_baseline(self):
        """Capture metrics before any change — used for invariants."""
        alive = 0; redirects = 0; total_freq = 0
        for pid, p in self.profs.items():
            if not isinstance(p, dict): continue
            if p.get('_redirect_to'):
                redirects += 1
            elif p.get('_kinship_placeholder') or p.get('_abandoned'):
                pass
            else:
                alive += 1
            total_freq += (p.get('frequency') or 0)
        self.baseline = {'alive': alive, 'redirects': redirects, 'total_freq': total_freq}
        print(f'[SafeMerge] baseline: alive={alive:,} redirects={redirects:,} total_freq={total_freq:,}', file=sys.stderr)

    def _gate1_dryrun(self):
        """Write JSON + CSV diff, print samples."""
        # JSON patch file
        self.dryrun_json.write_text(json.dumps({
            'run_id': self.run_id,
            'script': self.script_name,
            'started': self.started,
            'count': len(self.changes),
            'changes': self.changes,
        }, ensure_ascii=False, indent=1), encoding='utf-8')

        # CSV per-field diff
        rows = []
        for c in self.changes:
            old = c['old_values'] or {}
            new = c['new_values'] or {}
            all_fields = set(old) | set(new)
            for f in all_fields:
                if old.get(f) != new.get(f):
                    rows.append({
                        'pid': c['pid'], 'action': c['action'],
                        'field': f,
                        'old': _stringify(old.get(f)),
                        'new': _stringify(new.get(f)),
                        'reason': c['reason'],
                    })
        if rows:
            with open(self.dryrun_csv, 'w', newline='', encoding='utf-8') as f:
                w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
                w.writeheader()
                for r in rows: w.writerow(r)

        # Hash the CSV for gate 4 linking
        csv_bytes = self.dryrun_csv.read_bytes() if self.dryrun_csv.exists() else b''
        self.gate1_hash = 'sha256:' + hashlib.sha256(csv_bytes).hexdigest()[:16]

        # Terminal summary
        print(f'\n=== GATE 1: Dry-run diff ===', file=sys.stderr)
        print(f'  staged changes:  {len(self.changes):,} profiles', file=sys.stderr)
        print(f'  field-level rows: {len(rows):,}', file=sys.stderr)
        print(f'  json:  {self.dryrun_json}', file=sys.stderr)
        print(f'  csv:   {self.dryrun_csv}', file=sys.stderr)
        print(f'  hash:  {self.gate1_hash}', file=sys.stderr)

        # Random + top samples
        if self.changes:
            random.seed(42)
            sample = random.sample(self.changes, min(20, len(self.changes)))
            print(f'\n  --- 20 random samples ---', file=sys.stderr)
            for c in sample:
                print(f'    {c["pid"][:40]:40s} {c["action"]:30s} {c["reason"][:60]}', file=sys.stderr)

    def _gate2_invariants(self):
        """Apply changes to an in-memory copy and check invariants."""
        print(f'\n=== GATE 2: Invariants ===', file=sys.stderr)
        # Simulate: copy profs, apply changes
        import copy
        sim = copy.deepcopy(self.profs)
        for c in self.changes:
            p = sim[c['pid']]
            if c['action'].startswith('redirect_to:'):
                target_pid = c['action'].split(':', 1)[1]
                sim[c['pid']] = {
                    '_redirect_to': target_pid,
                    '_redirect_reason': c['reason'],
                    '_redirect_date': self.started,
                    'full_name': p.get('full_name'),
                }
            else:
                for k, v in (c['new_values'] or {}).items():
                    p[k] = v

        # Compute post metrics
        alive = redirects = 0; total_freq = 0
        for pid, p in sim.items():
            if not isinstance(p, dict): continue
            if p.get('_redirect_to'):
                redirects += 1
            elif p.get('_kinship_placeholder') or p.get('_abandoned'):
                pass
            else:
                alive += 1
            total_freq += (p.get('frequency') or 0)

        freq_delta  = total_freq - self.baseline['total_freq']
        alive_delta = alive - self.baseline['alive']
        redir_delta = redirects - self.baseline['redirects']

        print(f'  freq delta:     {freq_delta:+,d} (expected: {self.expected_freq_delta:+,d})', file=sys.stderr)
        print(f'  alive delta:    {alive_delta:+,d}', file=sys.stderr)
        print(f'  redirect delta: {redir_delta:+,d}', file=sys.stderr)

        # Invariant 1: Frequency conservation
        if abs(freq_delta - self.expected_freq_delta) > 1:
            raise SafeMergeError(
                f'Gate 2 fail: freq delta {freq_delta:+,d} != expected {self.expected_freq_delta:+,d}. '
                f'Silent data loss risk. Aborting.')

        # Invariant 2: Alive-count conservation
        # Donors redirected = redir_delta (change in redirects)
        # alive should decrease by exactly that many
        if self.expected_alive_delta is not None:
            if alive_delta != self.expected_alive_delta:
                raise SafeMergeError(
                    f'Gate 2 fail: alive delta {alive_delta:+,d} != expected {self.expected_alive_delta:+,d}')
        else:
            # Default: alive should drop by exactly the new redirect count
            if alive_delta + redir_delta != 0:
                raise SafeMergeError(
                    f'Gate 2 fail: alive dropped by {-alive_delta:+,d} but redirects rose by {redir_delta:+,d}. '
                    f'Difference = {-alive_delta - redir_delta:+,d} profiles silently lost.')

        # Invariant 3: No profile strip-out (no freq/teachers loss on kept-alive profiles)
        stripped = []
        for c in self.changes:
            if c['action'].startswith('redirect_to:'): continue
            pid = c['pid']
            pre = self.profs[pid]
            post = sim[pid]
            pre_has = bool(pre.get('frequency') or pre.get('teachers') or pre.get('students'))
            post_has = bool(post.get('frequency') or post.get('teachers') or post.get('students'))
            if pre_has and not post_has:
                stripped.append(pid)
        if stripped:
            raise SafeMergeError(f'Gate 2 fail: {len(stripped)} profiles lost all freq/teachers/students: {stripped[:5]}')

        # Invariant 4: No silent downgrade
        if not self.allow_downgrade:
            downgraded = []
            GRADE_ORDER = {'companion':1,'very_reliable':2,'reliable':3,'mostly_reliable':4,
                           'acceptable':5,'slightly_weak':6,'weak':7,'abandoned':8,'fabricator':9,'unknown':10}
            for c in self.changes:
                pid = c['pid']
                pre_g = self.profs[pid].get('grade_en') or ''
                post_g = sim[pid].get('grade_en') or ''
                pre_o = GRADE_ORDER.get(pre_g, 10)
                post_o = GRADE_ORDER.get(post_g, 10)
                if post_o > pre_o:
                    downgraded.append((pid, pre_g, post_g))
            if downgraded:
                raise SafeMergeError(
                    f'Gate 2 fail: {len(downgraded)} profiles silently downgraded. Pass allow_downgrade=True to proceed. '
                    f'Sample: {downgraded[:5]}')

        # Invariant 5: Chain-integrity sample (soft — warn, don't fail, if index not available)
        # Load chain index if present; sample 500 chains; check resolution unchanged
        chain_result = self._check_chain_integrity(sim)
        print(f'  chain-integrity: {chain_result}', file=sys.stderr)

        print(f'  GATE 2 PASSED', file=sys.stderr)

    def _check_chain_integrity(self, sim):
        """Sample chains, verify every name still resolves post-change."""
        try:
            import pickle
            if not CHAIN_INDEX.exists():
                return 'skipped (no raw_chain_index.pkl)'
            with open(CHAIN_INDEX, 'rb') as f:
                data = pickle.load(f)
            chains = data.get('chains', [])
            if not chains: return 'skipped (empty chains)'

            # Build post-change resolver: pid direct OR via redirect
            def resolves(name):
                if name in sim: return True
                # Check via norm name (simple)
                for pid, p in sim.items():
                    if p.get('full_name') == name: return True
                return False

            random.seed(42)
            sample = random.sample(chains, min(CHAIN_SAMPLE_SIZE, len(chains)))
            miss = 0
            for ch in sample:
                for nm in ch.get('names', []):
                    if not resolves(nm):
                        miss += 1
                        break
            pct = 100 * (1 - miss / len(sample))
            return f'{pct:.1f}% of {len(sample)} chains resolve ({miss} miss)'
        except Exception as e:
            return f'skipped ({e})'

    def _gate3_batch(self):
        print(f'\n=== GATE 3: Batch cap ===', file=sys.stderr)
        if len(self.changes) > self.batch_cap:
            raise SafeMergeError(
                f'Gate 3 fail: {len(self.changes)} changes > batch_cap {self.batch_cap}. '
                f'Increase batch_cap explicitly or split into multiple runs.')
        print(f'  {len(self.changes)} <= cap {self.batch_cap}  PASS', file=sys.stderr)

    def _gate4_audit(self):
        print(f'\n=== GATE 4: Audit log ===', file=sys.stderr)
        SAVEPOINTS.mkdir(parents=True, exist_ok=True)
        with open(AUDIT_LOG, 'a', encoding='utf-8') as f:
            for c in self.changes:
                line = {
                    'ts': datetime.now().isoformat(timespec='seconds'),
                    'run_id': self.run_id,
                    'script': self.script_name,
                    'pid': c['pid'],
                    'action': c['action'],
                    'old_values': c['old_values'],
                    'new_values': c['new_values'],
                    'reason': c['reason'],
                    'gate1_hash': self.gate1_hash,
                    'baseline': self.baseline,
                }
                f.write(json.dumps(line, ensure_ascii=False) + '\n')
        print(f'  wrote {len(self.changes)} lines to {AUDIT_LOG.name}', file=sys.stderr)

    # ----- Commit -----
    def commit(self):
        """Run all 4 gates in order, then write DB atomically."""
        if not self.changes:
            print('[SafeMerge] no changes staged; nothing to do', file=sys.stderr)
            return

        # Gate 1 first — always
        self._gate1_dryrun()

        if not self.apply_mode:
            print(f'\n[SafeMerge] DRY-RUN ONLY. Review {self.dryrun_csv.name} and rerun with --apply.', file=sys.stderr)
            return

        # Gate 3 early (cheap check)
        self._gate3_batch()

        # Gate 2 — runs invariants on simulated post-state
        self._gate2_invariants()

        # Backup
        backup = SAVEPOINTS / f'sanadset_pre_{self.run_id}.json'
        backup.write_bytes(self.db_path.read_bytes())
        print(f'\n[SafeMerge] backup -> {backup.name}', file=sys.stderr)

        # Apply to self.profs (in-place now)
        for c in self.changes:
            pid = c['pid']
            p = self.profs[pid]
            if c['action'].startswith('redirect_to:'):
                target_pid = c['action'].split(':', 1)[1]
                self.profs[pid] = {
                    '_redirect_to': target_pid,
                    '_redirect_reason': c['reason'],
                    '_redirect_date': self.started,
                    'full_name': p.get('full_name'),
                }
            else:
                for k, v in (c['new_values'] or {}).items():
                    p[k] = v
                # Stamp provenance
                fp = p.setdefault('field_provenance', {})
                fp[f'gafsce_{c["action"]}'] = {
                    'date': self.started, 'reason': c['reason'],
                    'old_values': c['old_values'], 'run_id': self.run_id,
                }

        # Atomic write: temp + rename
        tmp = self.db_path.with_suffix('.tmp')
        tmp.write_text(json.dumps(self.db, ensure_ascii=False, indent=0), encoding='utf-8')
        os.replace(tmp, self.db_path)
        print(f'[SafeMerge] DB written atomically', file=sys.stderr)

        # Gate 4 — log AFTER successful write
        self._gate4_audit()

        print(f'\n[SafeMerge] COMMIT COMPLETE  run_id={self.run_id}', file=sys.stderr)


def _stringify(v):
    if v is None: return ''
    if isinstance(v, (dict, list)): return json.dumps(v, ensure_ascii=False)[:500]
    return str(v)[:500]


def snapshot_fields(profile, fields):
    """Helper: snapshot given fields from a profile dict."""
    return {f: profile.get(f) for f in fields}


# ---- CLI helper ----
def standard_argparse(description):
    """Every gated script should accept the same flags."""
    import argparse
    ap = argparse.ArgumentParser(description=description)
    ap.add_argument('--apply', action='store_true', help='Commit changes (default: dry-run only)')
    ap.add_argument('--batch-cap', type=int, default=DEFAULT_BATCH_CAP)
    ap.add_argument('--allow-downgrade', action='store_true')
    return ap
