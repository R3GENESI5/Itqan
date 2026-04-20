"""Build narrator -> hadith reverse index for the viewer.

Source: extended_hadiths.json (411K hadiths, 939 books, chain narrators).
Target: viewer/data/narrator_hadiths/shard_NNN.json — grouped in same 5000-
profile shards as the existing index.json, keyed by pid.

Per-pid record: [[book_id, hadith_idx_in_book, pos_in_chain], ...]
  - book_id          : int, matches extended_hadiths book_id
  - hadith_idx       : int, 0-based index within that book's hadiths
  - pos_in_chain     : int, 0 = compiler-end (source), len-1 = prophet-end

Names matched via normalized Arabic form + existing redirect map.
"""
import json, os, re, sys
from pathlib import Path
from collections import defaultdict

DB = Path('D:/Hadith/src/savepoints/sanadset_final_20260416_213020.json')
HAD = Path('D:/Hadith/app/data/extended_hadiths.json')
OUT_DIR = Path('D:/Hadith/viewer/data/narrator_hadiths')
OUT_DIR.mkdir(parents=True, exist_ok=True)
INDEX = Path('D:/Hadith/viewer/data/index.json')

DIACR = re.compile(r'[\u064b-\u0652]')
def norm(s):
    s = DIACR.sub('', s or '')
    s = re.sub(r'[أإآا]','ا',s)
    return s.replace('ة','ه').replace('\u0640','').replace('ى','ي').strip()

print('Loading DB...', file=sys.stderr)
db = json.loads(DB.read_text(encoding='utf-8'))
profs = db['profiles']

# pid -> shard_num (same as index.json ordering)
print('Loading viewer index.json for pid->shard mapping...', file=sys.stderr)
idx = json.loads(INDEX.read_text(encoding='utf-8'))
# Row cols: [pid, name, grade, freq, books, tc, sc, shard, flagmask]
pid_to_shard = {row[0]: row[7] for row in idx}
print(f'  {len(pid_to_shard):,} alive-index pids mapped to shards', file=sys.stderr)

# Norm-name -> pid (alive + redirects resolved to final target)
print('Building norm-name lookup...', file=sys.stderr)
name_to_pid = {}
for pid, p in profs.items():
    if not isinstance(p, dict): continue
    # Resolve redirects to final target
    final_pid = pid
    seen = set()
    while isinstance(profs.get(final_pid), dict) and profs[final_pid].get('_redirect_to'):
        if final_pid in seen: break
        seen.add(final_pid)
        final_pid = profs[final_pid]['_redirect_to']
    if final_pid not in pid_to_shard: continue  # only map to shard-indexed pids
    target = profs.get(final_pid, {})
    if not isinstance(target, dict) or target.get('_kinship_placeholder') or target.get('_abandoned'):
        continue
    # Multiple norm-forms per profile
    for candidate in [p.get('full_name'), p.get('norm_name'),
                      target.get('full_name'), target.get('norm_name'), pid]:
        n = norm(candidate)
        if n and n not in name_to_pid:
            name_to_pid[n] = final_pid
print(f'  {len(name_to_pid):,} norm-names -> alive pids', file=sys.stderr)

print('Loading extended_hadiths.json (255 MB)...', file=sys.stderr)
hadith_db = json.loads(HAD.read_text(encoding='utf-8'))
hadiths = hadith_db['hadiths']
print(f'  {len(hadiths):,} hadiths loaded', file=sys.stderr)

# Group hadiths by book_id, compute hadith_idx within book
print('Indexing hadiths by book_id + building reverse index...', file=sys.stderr)
book_idx = defaultdict(int)  # book_id -> running counter
pid_hadiths = defaultdict(list)  # pid -> [[book_id, hadith_idx, pos], ...]

matched = unmatched_names = 0
hadiths_with_at_least_one_match = 0

for h in hadiths:
    bid = h.get('book_id')
    if bid is None: continue
    idx_in_book = book_idx[bid]
    book_idx[bid] += 1
    chain = h.get('narrators') or []
    matched_any = False
    for pos, nm in enumerate(chain):
        pid = name_to_pid.get(norm(nm))
        if pid:
            pid_hadiths[pid].append([bid, idx_in_book, pos])
            matched += 1
            matched_any = True
        else:
            unmatched_names += 1
    if matched_any:
        hadiths_with_at_least_one_match += 1

print(f'  matched narrator-appearances:   {matched:,}', file=sys.stderr)
print(f'  unmatched narrator-appearances: {unmatched_names:,}', file=sys.stderr)
print(f'  hadiths w/ >=1 matched narrator: {hadiths_with_at_least_one_match:,} / {len(hadiths):,}', file=sys.stderr)
print(f'  unique pids with hadiths:        {len(pid_hadiths):,}', file=sys.stderr)

# Cap huge narrators at 2000 entries (Abu Hurayra has ~50K appearances)
CAP = 2000
capped = 0
for pid, lst in pid_hadiths.items():
    if len(lst) > CAP:
        pid_hadiths[pid] = lst[:CAP]
        capped += 1
print(f'  capped (>2000 -> 2000):          {capped:,}', file=sys.stderr)

# Shard by shard_num (same as narrator profile shards)
print('Sharding output...', file=sys.stderr)
by_shard = defaultdict(dict)
for pid, lst in pid_hadiths.items():
    shard = pid_to_shard.get(pid)
    if shard is None: continue
    by_shard[shard][pid] = lst

for shard, data in sorted(by_shard.items()):
    out = OUT_DIR / f'shard_{shard:03d}.json'
    out.write_text(json.dumps(data, ensure_ascii=False, separators=(',',':')),
                   encoding='utf-8')

total_sz = sum(os.path.getsize(OUT_DIR/f) for f in os.listdir(OUT_DIR))
print(f'\nWrote {len(by_shard)} shards, total {total_sz/1024/1024:.1f} MB', file=sys.stderr)

# Also write a meta + book lookup so viewer can map book_id -> name
book_meta = {b['id']: b['name_ar'] for b in hadith_db['books']}
(Path('D:/Hadith/viewer/data') / 'hadith_books.json').write_text(
    json.dumps(book_meta, ensure_ascii=False, separators=(',',':')), encoding='utf-8')
print(f'Wrote hadith_books.json ({len(book_meta)} books)', file=sys.stderr)
