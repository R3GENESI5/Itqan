"""Fix terminal-dominant companion profiles using GAFSCE 4-Gate protocol.

Source queue: chain_position_vs_grade.csv (terminal_pct >= 80% + non-companion).
Per Rule 1 (chain-start = companion 9/10): these are companions misgraded.

Three action types in one batch:
  1. enrich_to_companion  - 8 profiles: upgrade grade + GK enrichment
  2. redirect_to:<pid>    - 3 particle-prefix variants -> canonical companion
  3. flag_kinship         - 1 profile (أباها = "her father")

Skipped: وائل + لقيط (need manual investigation).

DRY-RUN by default. Pass --apply to commit (after user reviews CSV).
"""
import argparse, json, sys
from pathlib import Path

sys.path.insert(0, 'D:/Hadith/src')
from gafsce_gates import SafeMerge, snapshot_fields

CHK = 'D:/Hadith/src/savepoints/sanadset_final_20260416_213020.json'
GK_PATH = 'D:/Hadith/src/external_hadith/gk_json/gk_narrators.json'

ap = argparse.ArgumentParser()
ap.add_argument('--apply', action='store_true')
args = ap.parse_args()

# Load GK for enrichment data
gk = json.loads(Path(GK_PATH).read_text(encoding='utf-8'))['narrators']

# Action 1: Upgrades to companion + GK enrichment (8 profiles)
# Format: pid -> gk_id (verified manually 2026-04-21 against GK + classical conventions)
COMPANION_UPGRADES = {
    'أم سلمة':   8101,   # هند بنت حذيفة المخزومية, زوج النبي, d.63
    'أبو ذر':    2187,   # جندب بن عبد الله الغفاري, d.32
    'أبو أمامة': 3929,   # صدي بن عجلان الباهلي, d.86
    'خباب':      2698,   # خباب بن الأرت التميمي, d.37
    'سبرة':      3205,   # سبرة بن أبي سبرة الجعفي
    'طلق':       4030,   # طلق بن علي الحنفي
    'الشريد':    1463,   # الشريد بن سويد الثقفي
    'أم شريك':   6361,   # غزيلة بنت دودان (Umm Sharik)
}

# Action 2: Particle-prefix redirects to canonical companion
PREFIX_REDIRECTS = {
    'وأم سلمة':           'أم سلمة',
    'لأبي ذر':            'أبو ذر',
    'وأبي سعيد الخدري':   'أبو سعيد الخدري',
}

# Action 3: Kinship placeholder flag
KINSHIP_FLAGS = ['أباها']

# Identity fields that we OVERWRITE when GK supplies a value (current may be
# wrong from a prior bad GK match — Abu-Hurayra-style — so don't preserve).
# full_name is NOT in this list: chain-derived, keep canonical short form.
GK_OVERWRITE_FIELDS = ['alt_name','ism','kunya','nisba','full_nasab','laqab']

# Compute expected freq delta from redirects (their freq leaves alive count;
# chain data preserved via redirect resolution at viewer load time).
import json as _j
_db_peek = _j.load(open(CHK, encoding='utf-8'))['profiles']
_redirect_freq = sum((_db_peek.get(src, {}).get('frequency') or 0) for src in PREFIX_REDIRECTS)
print(f'[fix] expected freq delta from {len(PREFIX_REDIRECTS)} redirects: -{_redirect_freq}', file=sys.stderr)

# Expected alive-count change:
#   - each redirect: -1 (donor leaves alive)
#   - each kinship-flag: -1 (placeholder leaves alive)
#   - each upgrade: 0 (stays alive)
_expected_alive_delta = -(len(PREFIX_REDIRECTS) + len(KINSHIP_FLAGS))

with SafeMerge(
    script_name='fix_terminal_dominant_companions',
    db_path=CHK,
    expected_freq_delta=-_redirect_freq,
    expected_alive_delta=_expected_alive_delta,
    apply_mode=args.apply,
    batch_cap=20,
) as sm:
    profs = sm.profs

    # ---- Action 1: companion upgrades ----
    for pid, gk_id in COMPANION_UPGRADES.items():
        if pid not in profs:
            print(f'  WARN: {pid} not in DB, skipping', file=sys.stderr)
            continue
        p = profs[pid]
        e = gk.get(str(gk_id), {})
        if not e:
            print(f'  WARN: GK[{gk_id}] not found for {pid}, skipping', file=sys.stderr)
            continue

        # Build new_values first, then snapshot only the fields we'll actually write
        new = {
            'grade_en':    'companion',
            'grade_ar':    e.get('grade_ar') or 'صحابي',
            'tabaqat_num': '1',
            'gk_rawy_id':  gk_id,
        }
        # Overwrite identity fields when GK supplies them — current values may be
        # from a prior wrong GK match (Abu-Hurayra pattern). Map GK 'name' -> ism.
        gk_to_field = {
            'alt_name':   e.get('alt_name'),
            'ism':        e.get('name'),
            'kunya':      e.get('kunya'),
            'nisba':      e.get('nisba'),
            'full_nasab': e.get('full_nasab'),
        }
        for f, v in gk_to_field.items():
            if v: new[f] = v
        if e.get('death_year'):
            new['death_range'] = f'{e["death_year"]} هـ'

        # Snapshot ONLY fields present in new (no ghost-diff rows)
        old = snapshot_fields(p, list(new.keys()))

        sm.change(
            pid=pid, action='enrich_to_companion',
            old_values=old, new_values=new,
            reason=f'rule1_chain_start_companion+gk_{gk_id}_enrichment',
        )

    # ---- Action 2: particle-prefix redirects ----
    # Note: redirect freq is moved to target. Update expected_freq_delta accordingly.
    redirect_freq_total = 0
    for src, target in PREFIX_REDIRECTS.items():
        if src not in profs or target not in profs:
            print(f'  WARN: redirect {src}->{target} skipped (missing profile)', file=sys.stderr)
            continue
        # Redirects are a special action — for SafeMerge to compute invariants
        # correctly, the new_values is just _redirect_to; the gate logic does the
        # transform itself when action starts with 'redirect_to:'.
        sm.change(
            pid=src, action=f'redirect_to:{target}',
            old_values={'frequency': profs[src].get('frequency'),
                        'grade_en': profs[src].get('grade_en')},
            new_values={'_redirect_to': target},
            reason='particle_prefix_variant_of_canonical_companion',
        )
        redirect_freq_total += (profs[src].get('frequency') or 0)

    # ---- Action 3: kinship placeholder flags ----
    for pid in KINSHIP_FLAGS:
        if pid not in profs: continue
        sm.change(
            pid=pid, action='flag_kinship_placeholder',
            old_values={'_kinship_placeholder': profs[pid].get('_kinship_placeholder'),
                        'grade_en': profs[pid].get('grade_en')},
            new_values={'_kinship_placeholder': True},
            reason='full_name_is_kinship_term_not_proper_identity',
        )

    sm.commit()
