"""Split extended_hadiths.json into per-book files under viewer/data/hadiths/.

Each book file: ordered list of hadiths in same idx order used by the
reverse index (so hadith_idx from narrator_hadiths shards maps 1:1).

Per-hadith kept fields: number, narrators, matn_preview.
(book+book_id are redundant at the file level, so omitted inside.)
"""
import json, os, sys
from pathlib import Path
from collections import defaultdict

HAD = Path('D:/Hadith/app/data/extended_hadiths.json')
OUT_DIR = Path('D:/Hadith/viewer/data/hadiths')
OUT_DIR.mkdir(parents=True, exist_ok=True)

print('Loading extended_hadiths.json...', file=sys.stderr)
d = json.loads(HAD.read_text(encoding='utf-8'))
hadiths = d['hadiths']
books = {b['id']: b['name_ar'] for b in d['books']}

print(f'Grouping {len(hadiths):,} hadiths by book_id...', file=sys.stderr)
by_book = defaultdict(list)
for h in hadiths:
    bid = h.get('book_id')
    if bid is None: continue
    by_book[bid].append({
        'n': h.get('number'),
        'c': h.get('narrators') or [],
        'm': h.get('matn_preview') or '',
    })

print(f'Writing {len(by_book)} per-book files...', file=sys.stderr)
total_sz = 0
for bid, lst in by_book.items():
    out = OUT_DIR / f'book_{bid}.json'
    out.write_text(json.dumps({
        'book_id': bid,
        'name_ar': books.get(bid, ''),
        'count': len(lst),
        'hadiths': lst,
    }, ensure_ascii=False, separators=(',',':')), encoding='utf-8')
    total_sz += os.path.getsize(out)

print(f'\nWrote {len(by_book)} book files, total {total_sz/1024/1024:.1f} MB', file=sys.stderr)
print(f'Sample largest books:', file=sys.stderr)
for bid, lst in sorted(by_book.items(), key=lambda x: -len(x[1]))[:5]:
    sz = os.path.getsize(OUT_DIR/f'book_{bid}.json')/1024/1024
    print(f'  book_{bid:>4d}  {books.get(bid,"")[:30]:30s}  {len(lst):>6,} hadiths  {sz:.1f} MB', file=sys.stderr)
