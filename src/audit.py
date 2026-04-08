"""Full concordance and bridge audit."""
import json, glob, os
from pathlib import Path
from collections import defaultdict

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / 'app' / 'data'
SRC  = ROOT / 'src'

conc   = json.load(open(DATA / 'concordance.json'))
wd     = json.load(open(DATA / 'word_defs_v2.json'))
bridge = json.load(open(DATA / 'quran_hadith_bridge.json'))
fam    = json.load(open(DATA / 'family_corpus.json'))
alias  = json.load(open(SRC / 'root_alias_map.json'))

# ── 1. Per-book concordance coverage ─────────────────────────────────────────
book_counts      = defaultdict(int)
book_unique_words = defaultdict(set)
for word, ids in conc.items():
    for hid in ids:
        book = hid.split(':')[0]
        book_counts[book] += 1
        book_unique_words[book].add(word)

book_hadith_counts = {}
for book_dir in glob.glob(str(DATA / 'sunni' / '*') + '/'):
    book_id = os.path.basename(os.path.normpath(book_dir))
    idx_path = os.path.join(book_dir, 'index.json')
    if not os.path.exists(idx_path):
        continue
    idx = json.load(open(idx_path))
    total = 0
    for ch in idx:
        ch_file = os.path.join(book_dir, ch['file'])
        if os.path.exists(ch_file):
            total += len(json.load(open(ch_file)))
    book_hadith_counts[book_id] = total

print('=== PER-BOOK CONCORDANCE COVERAGE ===')
print(f'  {"Book":35s} {"Hadiths":>10} {"Conc entries":>13} {"Uniq words":>11} {"Entries/H":>10}')
total_hadiths = 0
total_entries = 0
for book in sorted(book_hadith_counts.keys()):
    actual  = book_hadith_counts[book]
    entries = book_counts.get(book, 0)
    uwords  = len(book_unique_words.get(book, set()))
    ratio   = entries / actual if actual else 0
    flag    = '  *** LOW COVERAGE' if ratio < 5 and actual > 100 else ''
    print(f'  {book:35s} {actual:>10,} {entries:>13,} {uwords:>11,} {ratio:>9.1f}x{flag}')
    total_hadiths += actual
    total_entries += entries
print(f'  {"TOTAL":35s} {total_hadiths:>10,} {total_entries:>13,}')

# ── 2. Capped words (500 limit) ───────────────────────────────────────────────
capped = [(w, wd.get(w, {}).get('r','?'), wd.get(w, {}).get('g','')[:50])
          for w, ids in conc.items() if len(ids) == 500]
print(f'\n=== WORDS CAPPED AT 500 (potentially lossy): {len(capped)} ===')
print('  These words appear in 500+ hadiths but we only store 500. May undercount.')
for w, root, gloss in sorted(capped, key=lambda x: x[0])[:20]:
    print(f'  {w:15s}  root:{root:8s}  | {gloss}')

# ── 3. Bridge root coverage ───────────────────────────────────────────────────
zero   = [(r, d['ayah_count'], d['definitions'].get('quran_meaning','')[:60])
           for r, d in bridge.items() if d['hadith_count'] == 0]
zero.sort(key=lambda x: -x[1])

print(f'\n=== BRIDGE: ROOTS WITH 0 HADITHS ({len(zero)} roots) ===')
print('  Roots present in Quran but no hadith match found (either rare or gap):')
print(f'  {"Root":8s} {"Ayahs":>6}  Meaning')
for root, ayahs, meaning in zero[:30]:
    print(f'  {root:8s} {ayahs:>6}  {meaning}')

# ── 4. Suspicious high-ratio families (noise check) ─────────────────────────
print('\n=== FAMILY CORPUS AUDIT ===')
print(f'  {"Family":30s} {"Ayahs":>7} {"Hadiths":>9} {"Roots":>6} {"H/A":>7}  Notes')
for fk, fd in sorted(fam.items(), key=lambda x: -x[1]['hadith_count']):
    ratio = fd['hadith_count'] / fd['ayah_count'] if fd['ayah_count'] else 0
    note = ''
    if ratio > 15:  note = 'HIGH RATIO — verify no noise roots'
    if fd['hadith_count'] < 1000: note = 'LOW COVERAGE'
    print(f'  {fk:30s} {fd["ayah_count"]:>7,} {fd["hadith_count"]:>9,} {fd["root_count"]:>6}  {ratio:>5.1f}x  {note}')

# ── 5. Root quality spot-checks ───────────────────────────────────────────────
print('\n=== SPOT CHECK: KEY ISLAMIC ROOTS ===')
spot_roots = {
    'صلو': 'prayer (salah)',
    'زكو': 'zakat (almsgiving)',
    'صوم': 'fasting',
    'حجج': 'hajj (pilgrimage)',
    'رحم': 'mercy',
    'علم': 'knowledge',
    'جهد': 'jihad/striving',
    'طلق': 'divorce',
    'ورث': 'inheritance',
    'ملك': 'sovereignty/kingship',
    'خلف': 'caliphate/succession',
    'فتن': 'fitnah/trials',
    'قبر': 'grave',
    'بعث': 'resurrection',
    'نفخ': 'trumpet (blow)',
}
print(f'  {"Root":8s} {"Topic":25s} {"Ayahs":>6} {"Hadiths":>9}  Top books')
for root, topic in spot_roots.items():
    if root in bridge:
        d = bridge[root]
        books = sorted(d['book_breakdown'].items(), key=lambda x: -x[1])[:3]
        book_str = ', '.join(f'{b}({n})' for b,n in books)
        print(f'  {root:8s} {topic:25s} {d["ayah_count"]:>6} {d["hadith_count"]:>9,}  {book_str}')
    else:
        print(f'  {root:8s} {topic:25s}  NOT IN BRIDGE')

# ── 6. Narrator index spot-check ─────────────────────────────────────────────
narr = json.load(open(DATA / 'narrator_index.json'))
print(f'\n=== NARRATOR INDEX ===')
print(f'  Total narrators indexed: {len(narr):,}')
top_n = sorted(narr.items(), key=lambda x: -x[1].get('count',0))[:15]
print(f'  {"Narrator":30s} {"Count":>7}  Books')
for name, data in top_n:
    books = list(data.get('books', {}).keys())[:4]
    print(f'  {name:30s} {data.get("count",0):>7,}  {", ".join(books)}')

# ── 7. Hadith connections integrity ──────────────────────────────────────────
hconn = json.load(open(DATA / 'hadith_connections.json'))
print(f'\n=== HADITH CONNECTIONS ===')
print(f'  Total connected hadith pairs: {len(hconn):,}')
sample = list(hconn.items())[:3]
for hid, connections in sample:
    print(f'  {hid}: {len(connections)} connections → {str(connections[:2])[:100]}')

# ── 8. Data files cross-check ─────────────────────────────────────────────────
print('\n=== DATA FILES SUMMARY ===')
files = {
    'concordance.json':         str(DATA / 'concordance.json'),
    'word_defs_v2.json':        str(DATA / 'word_defs_v2.json'),
    'quran_hadith_bridge.json': str(DATA / 'quran_hadith_bridge.json'),
    'family_corpus.json':       str(DATA / 'family_corpus.json'),
    'narrator_index.json':      str(DATA / 'narrator_index.json'),
    'hadith_connections.json':  str(DATA / 'hadith_connections.json'),
    'roots_lexicon.json':       str(DATA / 'roots_lexicon.json'),
    'root_alias_map.json':      str(SRC / 'root_alias_map.json'),
    'bridge_analysis.json':     str(SRC / 'bridge_analysis.json'),
}
for name, path in files.items():
    if os.path.exists(path):
        sz = os.path.getsize(path) / 1024 / 1024
        print(f'  {name:35s}  {sz:6.1f} MB  ✓')
    else:
        print(f'  {name:35s}  MISSING ✗')
