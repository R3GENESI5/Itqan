"""
build_narrator_grades.py
========================
Merges KASHAF narrator grades (18,940 entries from Taqrib al-Tahdhib)
with our narrator_index.json → narrator_grades.json

Grade system (classical Arabic → English category → UI color):
  ثقة / وثقه         → reliable      → #2ecc71 green
  صدوق              → mostly_reliable → #f39c12 amber
  متروك / ضعيف       → weak          → #e74c3c red
  مجهول             → unknown        → #95a5a6 grey
  صحابي / صحابية     → companion     → #9b59b6 purple (highest trust)
  موضوع / كذاب       → fabricator    → #8b0000 dark red
"""

import csv, json, re
from pathlib import Path
from collections import defaultdict

ROOT = Path(__file__).resolve().parent.parent
SRC  = ROOT / 'src'
DATA = ROOT / 'app' / 'data'

# ── Grade normalization ───────────────────────────────────────────────────────
GRADE_MAP = {
    # Companions — highest trust, unquestioned
    'صحابي':       ('companion',      '#9b59b6'),
    'صحابية':      ('companion',      '#9b59b6'),
    'صحابي مشهور': ('companion',      '#9b59b6'),
    'صحابية مشهورة':('companion',     '#9b59b6'),
    'له صحبة':     ('companion',      '#9b59b6'),

    # Highly reliable
    'ثقة':         ('reliable',       '#2ecc71'),
    'ثقة ثبت':     ('reliable',       '#2ecc71'),
    'ثقة حافظ':    ('reliable',       '#2ecc71'),
    'ثقة عابد':    ('reliable',       '#2ecc71'),
    'ثقة فاضل':    ('reliable',       '#2ecc71'),
    'ثقة إمام':    ('reliable',       '#2ecc71'),
    'ثقة ثقة':     ('reliable',       '#2ecc71'),
    'وثقه الأئمة': ('reliable',       '#2ecc71'),
    'ثقة مأمون':   ('reliable',       '#2ecc71'),

    # Mostly reliable
    'صدوق':        ('mostly_reliable','#f39c12'),
    'صدوق حسن الحديث':('mostly_reliable','#f39c12'),
    'صدوق يهم':    ('mostly_reliable','#f39c12'),
    'لا بأس به':   ('mostly_reliable','#f39c12'),
    'صالح الحديث': ('mostly_reliable','#f39c12'),
    'حسن الحديث':  ('mostly_reliable','#f39c12'),

    # Weak — accepted with caution
    'ضعيف':        ('weak',          '#e74c3c'),
    'ضعيف الحديث': ('weak',          '#e74c3c'),
    'فيه ضعف':     ('weak',          '#e74c3c'),
    'فيه لين':     ('weak',          '#e74c3c'),
    'لين الحديث':  ('weak',          '#e74c3c'),
    'ضعفه البعض':  ('weak',          '#e74c3c'),

    # Abandoned / rejected
    'متروك':       ('abandoned',     '#c0392b'),
    'متروك الحديث':('abandoned',     '#c0392b'),
    'منكر الحديث': ('abandoned',     '#c0392b'),
    'رافضي':       ('abandoned',     '#c0392b'),

    # Fabricator
    'كذاب':        ('fabricator',    '#8b0000'),
    'يضع الحديث':  ('fabricator',    '#8b0000'),
    'وضاع':        ('fabricator',    '#8b0000'),
    'موضوع':       ('fabricator',    '#8b0000'),

    # Unknown
    'مجهول':       ('unknown',       '#95a5a6'),
    'مجهول الحال': ('unknown',       '#95a5a6'),
    'مستور':       ('unknown',       '#95a5a6'),
}

def normalize_name(name: str) -> str:
    """Strip diacritics and extra info after colon for matching."""
    name = re.sub(r'[\u0610-\u061A\u064B-\u065F\u0670]', '', name)
    name = name.split('،')[0].split(':')[0].strip()
    name = re.sub(r'\s+', ' ', name)
    return name

def classify_grade(grade_text: str) -> tuple:
    """Return (category, color) for a grade string."""
    if not grade_text:
        return ('unknown', '#95a5a6')
    grade_clean = grade_text.strip().split('،')[0].split(',')[0].strip()
    # Direct match
    if grade_clean in GRADE_MAP:
        return GRADE_MAP[grade_clean]
    # Keyword scan
    for key, val in GRADE_MAP.items():
        if key in grade_text:
            return val
    return ('unknown', '#95a5a6')

# ── Load KASHAF narrator CSV ──────────────────────────────────────────────────
print('Loading KASHAF narrator data...')
kashaf = {}  # normalized_name → {grade_ar, grade_en, color, places, birth, death}

with open(SRC / '_kashaf_narrators.csv', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        name_raw = row.get('name', '').strip()
        grade_ar = row.get('grade', '').strip()
        places   = row.get('places', '').strip()
        death    = row.get('death', '').strip()
        birth    = row.get('birth', '').strip()
        if not name_raw:
            continue
        cat, color = classify_grade(grade_ar)
        norm = normalize_name(name_raw)
        kashaf[norm] = {
            'name_ar':  name_raw,
            'grade_ar': grade_ar,
            'grade_en': cat,
            'color':    color,
            'places':   places,
            'birth':    birth,
            'death':    death,
        }

print(f'  KASHAF: {len(kashaf):,} narrators loaded')

# Grade distribution
grade_dist = defaultdict(int)
for v in kashaf.values():
    grade_dist[v['grade_en']] += 1
for g, cnt in sorted(grade_dist.items(), key=lambda x: -x[1]):
    print(f'  {g:20s}: {cnt:,}')

# ── Load our narrator_index.json ──────────────────────────────────────────────
print('\nLoading narrator_index.json...')
ni = json.load(open(DATA / 'narrator_index.json', encoding='utf-8'))
print(f'  Our narrator_index: {len(ni):,} entries')

# ── Merge ─────────────────────────────────────────────────────────────────────
print('\nMerging...')
merged = {}
matched = 0

for name_en, data in ni.items():
    entry = {
        'name_en':  name_en,
        'total':    data.get('total', 0),
        'books':    data.get('books', {}),
        'topics':   data.get('topics', {}),
        'grade_en': 'unknown',
        'grade_ar': '',
        'color':    '#95a5a6',
        'name_ar':  '',
        'places':   '',
        'birth':    '',
        'death':    '',
    }
    # Try to match to KASHAF by normalized name
    norm_en = name_en.strip()
    # Many of our names are transliterated; try direct key lookup first
    if norm_en in kashaf:
        g = kashaf[norm_en]
        entry.update({k: g[k] for k in ('grade_ar','grade_en','color','name_ar','places','birth','death')})
        matched += 1

    merged[name_en] = entry

print(f'  Matched {matched}/{len(ni)} narrators to KASHAF grades')

# ── Add ALL KASHAF narrators (not just matched ones) ─────────────────────────
# This gives us the full grade dictionary for isnad parsing
all_grades = {}
for norm, data in kashaf.items():
    all_grades[data['name_ar']] = {
        'grade_en': data['grade_en'],
        'color':    data['color'],
        'grade_ar': data['grade_ar'],
        'places':   data['places'],
        'birth':    data['birth'],
        'death':    data['death'],
    }

# ── Save ─────────────────────────────────────────────────────────────────────
print('\nSaving...')
out = {
    'narrator_profiles': merged,
    'grade_lookup':      all_grades,
    'grade_colors': {
        'companion':      '#9b59b6',
        'reliable':       '#2ecc71',
        'mostly_reliable':'#f39c12',
        'weak':           '#e74c3c',
        'abandoned':      '#c0392b',
        'fabricator':     '#8b0000',
        'unknown':        '#95a5a6',
    }
}
path = DATA / 'narrator_grades.json'
with open(path, 'w', encoding='utf-8') as f:
    json.dump(out, f, ensure_ascii=False, separators=(',',':'))
sz = path.stat().st_size // 1024
print(f'  Saved narrator_grades.json ({sz} KB)')
print(f'  {len(all_grades):,} graded narrators')
print(f'  {len(merged):,} narrator profiles')
