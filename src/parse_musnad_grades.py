"""
parse_musnad_grades.py
======================
Extracts Shu'ayb al-Arnaut's hadith grades from the DjVu OCR text of
Musnad Ahmad (Internet Archive: musnad_ahmad_arabic/musnad_djvu.txt).

Each hadith is graded in the pattern:
    تعليق شعيب الأرنؤوط : إسناده صحيح على شرط الشيخين

Outputs:
    app/data/sunni/ahmed/arnaut_grades.json  — {hadith_number: grade_info}
    (also prints stats)

Usage:
    python src/parse_musnad_grades.py
"""

import json, re
from pathlib import Path
from collections import Counter
from zipfile import ZipFile
from xml.etree import ElementTree as ET

ROOT = Path(__file__).resolve().parent.parent
INPUT_DOCX = ROOT / "src" / "rijal_raw" / "musnad_ahmad.docx"
INPUT_DJVU = ROOT / "src" / "rijal_raw" / "musnad_djvu.txt"
OUT = ROOT / "app" / "data" / "sunni" / "ahmed" / "arnaut_grades.json"

# ── OCR correction map ──────────────────────────────────────────────
OCR_FIXES = {
    'صحبح': 'صحيح',       # common OCR misread: ب instead of يـ
    'صحبيح': 'صحيح',
    'صحييح': 'صحيح',
    'إجناده': 'إسناده',     # ج instead of س
    'إسنا ده': 'إسناده',
    'إسنادة': 'إسناده',
    'الأرنوؤط': 'الأرنؤوط',
    'الأرنؤط': 'الأرنؤوط',
    'الأرنأوط': 'الأرنؤوط',
}

def fix_ocr(text):
    for bad, good in OCR_FIXES.items():
        text = text.replace(bad, good)
    return text


# ── Grade normalization ─────────────────────────────────────────────
# Priority order: first match wins
GRADE_PATTERNS = [
    # Sahih (authentic chain)
    (r'إسناده صحيح', 'sahih', 'إسناده صحيح'),
    (r'حديث صحيح', 'sahih', 'حديث صحيح'),
    (r'إسناده قوي', 'sahih', 'إسناده قوي'),
    # Sahih li-ghayrihi (authentic by corroboration)
    (r'صحيح لغيره', 'sahih_li_ghayrihi', 'صحيح لغيره'),
    # Hasan (good chain)
    (r'إسناده حسن', 'hasan', 'إسناده حسن'),
    (r'حديث حسن', 'hasan', 'حديث حسن'),
    # Hasan li-ghayrihi (good by corroboration)
    (r'حسن لغيره', 'hasan_li_ghayrihi', 'حسن لغيره'),
    # Da'if (weak chain)
    (r'إسناده ضعيف', 'daif', 'إسناده ضعيف'),
    (r'حديث ضعيف', 'daif', 'حديث ضعيف'),
    (r'ضعيف جدا', 'daif_jiddan', 'ضعيف جداً'),
    # Generic sahih/hasan (catch-all after specific patterns)
    (r'^صحيح\b', 'sahih', 'صحيح'),
    (r'^حسن\b', 'hasan', 'حسن'),
    (r'^ضعيف\b', 'daif', 'ضعيف'),
]

# Map to standard app grades
GRADE_TO_APP = {
    'sahih': 'Sahih',
    'sahih_li_ghayrihi': 'Sahih',
    'hasan': 'Hasan',
    'hasan_li_ghayrihi': 'Hasan',
    'daif': "Da'if",
    'daif_jiddan': "Da'if",
}


def classify_grade(raw_grade):
    """Classify Arnaut's Arabic grade text into a normalized grade."""
    text = raw_grade.strip()
    for pattern, grade_en, grade_ar in GRADE_PATTERNS:
        if re.search(pattern, text):
            return {
                'grade_en': grade_en,
                'grade_ar': grade_ar,
                'grade_app': GRADE_TO_APP.get(grade_en, 'Unknown'),
                'raw': raw_grade.strip(),
            }
    return {
        'grade_en': 'other',
        'grade_ar': raw_grade.strip()[:80],
        'grade_app': 'Unknown',
        'raw': raw_grade.strip(),
    }


def extract_docx_paragraphs(path):
    """Extract paragraph texts from a DOCX file."""
    z = ZipFile(path)
    xml = z.read('word/document.xml')
    root = ET.fromstring(xml)

    paras = []
    for p in root.iter('{http://schemas.openxmlformats.org/wordprocessingml/2006/main}p'):
        texts = []
        for t in p.iter('{http://schemas.openxmlformats.org/wordprocessingml/2006/main}t'):
            if t.text:
                texts.append(t.text)
        full = ''.join(texts).strip()
        if full:
            paras.append(full)
    return paras


def parse():
    # Prefer DOCX (clean text with proper hadith numbers)
    if INPUT_DOCX.exists():
        print(f"  Source: {INPUT_DOCX.name} (DOCX)")
        lines = extract_docx_paragraphs(INPUT_DOCX)
    elif INPUT_DJVU.exists():
        print(f"  Source: {INPUT_DJVU.name} (DjVu OCR — numbers may have OCR errors)")
        text = INPUT_DJVU.read_text(encoding='utf-8')
        text = fix_ocr(text)
        lines = [l.strip() for l in text.split('\n') if l.strip()]
    else:
        print("  ERROR: No source file found. Run download_openiti_rijal.py first.")
        return {}, 0

    hadith_re = re.compile(r'^(\d+)\s*-\s')
    grade_re = re.compile(r'^تعليق شعيب الأرنؤوط\s*:\s*(.+)')

    grades = {}
    current_hadith = None
    unmatched_grades = 0

    for line in lines:
        # Check for grade (belongs to current_hadith)
        gm = grade_re.match(line)
        if gm:
            raw_grade = fix_ocr(gm.group(1).strip())
            grade_info = classify_grade(raw_grade)

            if current_hadith is not None:
                if current_hadith not in grades:
                    grades[current_hadith] = grade_info
            else:
                unmatched_grades += 1
            continue

        # Check for hadith number
        hm = hadith_re.match(line)
        if hm:
            current_hadith = int(hm.group(1))

    return grades, unmatched_grades


def main():
    print("Parsing Arnaut grades from DjVu text...\n")
    grades, unmatched = parse()

    # Stats
    print(f"Total hadiths graded: {len(grades):,}")
    print(f"Unmatched grade lines: {unmatched}")

    # Distribution
    dist = Counter(g['grade_app'] for g in grades.values())
    print(f"\nGrade distribution (app categories):")
    for grade, count in dist.most_common():
        pct = 100 * count / len(grades)
        print(f"  {grade}: {count:,} ({pct:.1f}%)")

    # Detailed distribution
    dist_en = Counter(g['grade_en'] for g in grades.values())
    print(f"\nDetailed distribution:")
    for grade, count in dist_en.most_common():
        pct = 100 * count / len(grades)
        print(f"  {grade}: {count:,} ({pct:.1f}%)")

    # Hadith number range
    nums = sorted(grades.keys())
    print(f"\nHadith number range: {nums[0]} - {nums[-1]}")

    # Coverage check against expected 26,539
    expected = 26539
    coverage = len(grades) / expected * 100
    print(f"Coverage: {len(grades)}/{expected} ({coverage:.1f}%)")

    # Save
    OUT.parent.mkdir(parents=True, exist_ok=True)

    # Convert to serializable format with string keys
    output = {str(k): v for k, v in sorted(grades.items())}
    with open(OUT, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=1)
    print(f"\nSaved to {OUT.relative_to(ROOT)} ({OUT.stat().st_size:,} bytes)")

    # Also save a compact grades.json for the app (hadith_number → grade string)
    compact_out = OUT.parent / "grades.json"
    compact = {str(k): v['grade_app'] for k, v in sorted(grades.items())}
    with open(compact_out, 'w', encoding='utf-8') as f:
        json.dump(compact, f, ensure_ascii=False)
    print(f"Saved compact grades to {compact_out.relative_to(ROOT)} ({compact_out.stat().st_size:,} bytes)")


if __name__ == '__main__':
    main()
