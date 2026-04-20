"""Scan-only audit: flag profile issues, produce CSV review queues. NO WRITES to DB.

Produces:
  out/gk_mismatch.csv        - profile.grade vs gk.tabaqah inconsistent
  out/missing_ism_famous.csv - high-freq profiles with no ism/full_nasab/laqab
  out/duplicate_fullnames.csv - same-norm duplicate alive profiles
  out/impossible_death.csv   - death_year > 500 AH on early-era narrator
  out/parser_artifacts.csv   - chain verbs in kunya/nisba/full_nasab/laqab
  out/chain_position_vs_grade.csv - terminal-position dominant but not companion
  out/summary.md             - counts + sample per category

Apply fixes AFTER human review.
"""
import json, re, os, sys, csv
from pathlib import Path
from collections import defaultdict, Counter

CHK = Path('D:/Hadith/src/savepoints/sanadset_final_20260416_213020.json')
GK  = Path('D:/Hadith/src/external_hadith/gk_json/gk_narrators.json')
OUT = Path('D:/Hadith/src/savepoints/scan_profile_issues_20260420')
OUT.mkdir(parents=True, exist_ok=True)

DIACR = re.compile(r'[\u064b-\u0652]')
def norm(s):
    if not s: return ''
    s = DIACR.sub('', s); s = re.sub(r'[أإآا]','ا',s)
    return s.replace('ة','ه').replace('\u0640','').replace('ى','ي').strip()

# Chain-verb / possessive patterns that shouldn't appear in kunya/nisba/laqab
CHAIN_VERBS = ['يقول','حدثنا','حدثني','أخبرنا','سمعت','قال ','قال:','عن فلان',
               'عنه','عنها','رضي الله']

def has_chain_artifact(s):
    if not isinstance(s, str): return False
    return any(v in s for v in CHAIN_VERBS)

# Grade -> expected tabaqah range
# tabaqah 1 = sahaba, 2 = senior-tabi'i, 3-5 = tabi'in, 6-9 = tabi-tabi'in, 10+ later
GRADE_TABAQAH = {
    'companion':          (1, 1),
    'very_reliable':      (2, 10),
    'reliable':           (2, 11),
    'mostly_reliable':    (2, 12),
    'acceptable':         (2, 12),
    'slightly_weak':      (2, 12),
    'weak':               (2, 12),
    'fabricator':         (2, 12),
    'abandoned':          (2, 12),
}
# Grade -> max plausible death_year (AH)
GRADE_MAX_DEATH = {
    'companion':          110,  # longest-lived was ~103
    'very_reliable':      300,  # mostly early tabaqah — most died by 300 AH
    'reliable':           400,
    'mostly_reliable':    400,
    'acceptable':         400,
    # No hard cap for later grades
}

def parse_year(s):
    """Extract year number from 'نن هـ' or 'xxx-yyy هـ' — return max year."""
    if not s: return None
    m = re.findall(r'\d{2,4}', str(s))
    if not m: return None
    try: return max(int(x) for x in m)
    except: return None

print('Loading DB...', file=sys.stderr)
db = json.loads(CHK.read_text(encoding='utf-8'))
profs = db['profiles']
print(f'  {len(profs):,} profiles', file=sys.stderr)

print('Loading GK...', file=sys.stderr)
gk = json.loads(GK.read_text(encoding='utf-8'))['narrators']
print(f'  {len(gk):,} GK entries', file=sys.stderr)

# alive = not redirect / kinship_placeholder / abandoned
alive = {}
for pid, p in profs.items():
    if not isinstance(p, dict) or p.get('_redirect_to'): continue
    if p.get('_kinship_placeholder') or p.get('_abandoned'): continue
    alive[pid] = p
print(f'  {len(alive):,} alive', file=sys.stderr)

# === SCAN 1: GK mismatch via tabaqah vs grade ===
print('\nScan 1: GK mismatch (tabaqah vs grade)...', file=sys.stderr)
mismatch = []
for pid, p in alive.items():
    gk_id = p.get('gk_rawy_id')
    if not gk_id: continue
    g_entry = gk.get(str(gk_id))
    if not g_entry: continue
    tab = g_entry.get('tabaqah')
    if not tab: continue
    grade = p.get('grade_en') or ''
    lo, hi = GRADE_TABAQAH.get(grade, (None, None))
    if lo is None: continue
    if tab < lo or tab > hi:
        mismatch.append({
            'pid': pid, 'full_name': p.get('full_name') or pid,
            'grade_en': grade, 'freq': p.get('frequency') or 0,
            'gk_rawy_id': gk_id, 'gk_tabaqah': tab,
            'gk_name': g_entry.get('name',''), 'gk_alt_name': g_entry.get('alt_name',''),
            'gk_grade': g_entry.get('grade_ar',''),
            'tabaqah_expected': f'{lo}-{hi}',
        })
mismatch.sort(key=lambda x: -x['freq'])
print(f'  found {len(mismatch):,}', file=sys.stderr)

# === SCAN 2: missing ism/nasab/laqab for high-freq profiles ===
print('Scan 2: missing ism/nasab/laqab (freq >= 1000)...', file=sys.stderr)
missing_ism = []
for pid, p in alive.items():
    fr = p.get('frequency') or 0
    if fr < 1000: continue
    if p.get('ism') or p.get('full_nasab') or p.get('laqab'): continue
    fn = p.get('full_name') or pid
    # If full_name already has "بن" then it's a nasab — not really missing
    if ' بن ' in fn: continue
    missing_ism.append({
        'pid': pid, 'full_name': fn, 'grade_en': p.get('grade_en') or '',
        'freq': fr, 'gk_rawy_id': p.get('gk_rawy_id') or '',
        'kunya': p.get('kunya') or '', 'nisba': p.get('nisba') or '',
        'death_range': p.get('death_range') or '',
    })
missing_ism.sort(key=lambda x: -x['freq'])
print(f'  found {len(missing_ism):,}', file=sys.stderr)

# === SCAN 3: duplicate full_names (same normalized form, both alive) ===
print('Scan 3: duplicate full_names...', file=sys.stderr)
by_norm = defaultdict(list)
for pid, p in alive.items():
    n = norm(p.get('full_name') or pid)
    if n: by_norm[n].append(pid)
dupes = []
for n, pids in by_norm.items():
    if len(pids) < 2: continue
    # Sort pids by freq desc
    ranked = sorted(pids, key=lambda x: -(alive[x].get('frequency') or 0))
    dupes.append({
        'norm_name': n, 'n_copies': len(pids),
        'pids': '|'.join(ranked),
        'total_freq': sum(alive[x].get('frequency') or 0 for x in pids),
        'top_pid_freq': alive[ranked[0]].get('frequency') or 0,
        'top_pid_grade': alive[ranked[0]].get('grade_en') or '',
        'full_name': alive[ranked[0]].get('full_name') or ranked[0],
    })
dupes.sort(key=lambda x: -x['total_freq'])
print(f'  found {len(dupes):,} duplicate-name clusters', file=sys.stderr)

# === SCAN 4: impossible death years ===
print('Scan 4: impossible death_year for grade...', file=sys.stderr)
impossible = []
for pid, p in alive.items():
    grade = p.get('grade_en') or ''
    max_d = GRADE_MAX_DEATH.get(grade)
    if max_d is None: continue
    dy = parse_year(p.get('death_range') or p.get('death') or p.get('death_year'))
    if not dy: continue
    if dy > max_d:
        impossible.append({
            'pid': pid, 'full_name': p.get('full_name') or pid,
            'grade_en': grade, 'freq': p.get('frequency') or 0,
            'death_value': p.get('death_range') or p.get('death') or '',
            'parsed_year': dy, 'max_plausible': max_d,
            'gk_rawy_id': p.get('gk_rawy_id') or '',
        })
impossible.sort(key=lambda x: -x['freq'])
print(f'  found {len(impossible):,}', file=sys.stderr)

# === SCAN 5: parser artifacts in identity fields ===
print('Scan 5: chain-verbs in kunya/nisba/laqab/full_nasab...', file=sys.stderr)
artifacts = []
for pid, p in alive.items():
    bad = {}
    for f in ['kunya','nisba','laqab','full_nasab','alt_name']:
        v = p.get(f)
        if has_chain_artifact(v):
            bad[f] = v
    if bad:
        artifacts.append({
            'pid': pid, 'full_name': p.get('full_name') or pid,
            'freq': p.get('frequency') or 0,
            'grade_en': p.get('grade_en') or '',
            **{f'bad_{k}': v for k, v in bad.items()},
        })
artifacts.sort(key=lambda x: -x['freq'])
print(f'  found {len(artifacts):,}', file=sys.stderr)

# === SCAN 6: chain-position-terminal-dominant but not companion ===
# NEVER treat mononym/famous as low-confidence — rule from user.
# Instead: flag likely-companion profiles currently unmarked.
print('Scan 6: terminal-position >= 80% but grade != companion...', file=sys.stderr)
terminal = []
for pid, p in alive.items():
    cp = p.get('chain_position') or {}
    if not isinstance(cp, dict): continue
    f, m, l = cp.get('first',0) or 0, cp.get('middle',0) or 0, cp.get('last',0) or 0
    total = f + m + l
    if total < 50: continue  # too small sample
    last_pct = 100 * l / total
    if last_pct >= 80 and p.get('grade_en') != 'companion':
        terminal.append({
            'pid': pid, 'full_name': p.get('full_name') or pid,
            'grade_en': p.get('grade_en') or '',
            'freq': p.get('frequency') or 0,
            'terminal_pct': round(last_pct, 1),
            'chain_total': total,
        })
terminal.sort(key=lambda x: -x['freq'])
print(f'  found {len(terminal):,}', file=sys.stderr)

# === Write CSVs ===
def write_csv(name, rows):
    if not rows: return
    path = OUT / f'{name}.csv'
    # Union of all keys across all rows — rows may have varying bad_* columns
    fields = []
    seen = set()
    for r in rows:
        for k in r.keys():
            if k not in seen: seen.add(k); fields.append(k)
    with open(path, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction='ignore')
        w.writeheader()
        for r in rows: w.writerow(r)
    print(f'  wrote {path}  ({len(rows):,} rows)', file=sys.stderr)

print('\nWriting CSVs...', file=sys.stderr)
write_csv('gk_mismatch', mismatch)
write_csv('missing_ism_famous', missing_ism)
write_csv('duplicate_fullnames', dupes)
write_csv('impossible_death', impossible)
write_csv('parser_artifacts', artifacts)
write_csv('chain_position_vs_grade', terminal)

# === Summary ===
summary = f"""# Profile issues scan — 2026-04-20

DB: 369,731 profiles, {len(alive):,} alive.

## Counts per issue type

| Issue | Count | Severity | Fix-confidence |
|---|---|---|---|
| GK mismatch (tabaqah vs grade) | {len(mismatch):,} | high — causes wrong kunya/nisba/full_nasab | manual review, GK re-match |
| Missing ism for high-freq (≥1000) | {len(missing_ism):,} | medium — cosmetic but impacts utility | GK lookup by kunya + tabaqah, high confidence |
| Duplicate full_name (alive) | {len(dupes):,} | high — same person, two entries | union-merge, high confidence |
| Impossible death year for grade | {len(impossible):,} | high — data error | manual check vs GK/web |
| Parser artifact in kunya/nisba | {len(artifacts):,} | high — chain text leaked into field | strip + re-enrich from GK |
| Terminal-dominant non-companion | {len(terminal):,} | medium — likely undermarked companions | re-grade via Rule 1 |

## Top 5 per category

### GK mismatch (highest-freq first)
{chr(10).join(f"- **{r['full_name']}** (pid={r['pid']}, freq={r['freq']:,}, grade={r['grade_en']}): linked to GK {r['gk_rawy_id']} tabaqah={r['gk_tabaqah']} (expected {r['tabaqah_expected']}). GK says: {r['gk_name']} / {r['gk_alt_name']}" for r in mismatch[:5])}

### Missing ism
{chr(10).join(f"- **{r['full_name']}** (freq={r['freq']:,}, grade={r['grade_en']}): kunya={r['kunya']!r}, nisba={r['nisba']!r}" for r in missing_ism[:5])}

### Duplicates
{chr(10).join(f"- **{r['full_name']}** ({r['n_copies']} copies, total freq={r['total_freq']:,}): pids={r['pids'][:200]}" for r in dupes[:5])}

### Impossible deaths
{chr(10).join(f"- **{r['full_name']}** (freq={r['freq']:,}, grade={r['grade_en']}): death={r['death_value']!r} parsed as {r['parsed_year']} AH (max plausible {r['max_plausible']})" for r in impossible[:5])}

### Parser artifacts
{chr(10).join(f"- **{r['full_name']}** (freq={r['freq']:,}): " + ', '.join(f"{k}={v!r}" for k,v in r.items() if k.startswith('bad_')) for r in artifacts[:5])}

### Likely-companion undermarked (terminal ≥ 80%)
{chr(10).join(f"- **{r['full_name']}** (freq={r['freq']:,}, grade={r['grade_en']}): terminal={r['terminal_pct']}% of {r['chain_total']} chains" for r in terminal[:5])}
"""
(OUT / 'summary.md').write_text(summary, encoding='utf-8')
print(f'\nSummary -> {OUT/"summary.md"}', file=sys.stderr)
print('\nAll CSVs + summary written. No DB changes made.', file=sys.stderr)
