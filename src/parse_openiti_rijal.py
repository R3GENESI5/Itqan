"""
parse_openiti_rijal.py
======================
Parses 8 classical Arabic rijal (narrator criticism) texts from OpenITI
mARkdown format into structured JSON.

Texts parsed:
  1. Taqrib al-Tahdhib (Ibn Hajar)      — compact grades (highest priority)
  2. Tahdhib al-Kamal (al-Mizzi)         — Six Books narrator encyclopedia
  3. Tahdhib al-Tahdhib (Ibn Hajar)      — condensed encyclopedia
  4. Mizan al-I'tidal (al-Dhahabi)       — critical narrator assessments
  5. Al-Jarh wa al-Ta'dil (Ibn Abi Hatim)— reliability evaluations
  6. Al-Thiqat (Ibn Hibban)              — reliable narrator list
  7. Al-Kamil fi Du'afa (Ibn 'Adi)       — weak narrator catalog
  8. Tarikh Baghdad (al-Khatib)          — Baghdad scholar biographies

Output: src/rijal_parsed/{text_id}.json

Usage:
    python src/parse_openiti_rijal.py              # parse all
    python src/parse_openiti_rijal.py taqrib        # parse one
    python src/parse_openiti_rijal.py --stats        # show stats only
"""

import json, re, sys
from pathlib import Path
from collections import Counter

ROOT = Path(__file__).resolve().parent.parent
RAW  = ROOT / "src" / "rijal_raw"
OUT  = ROOT / "src" / "rijal_parsed"
OUT.mkdir(exist_ok=True)

# ──────────────────────────────────────────────────────────────────────
# Shared utilities
# ──────────────────────────────────────────────────────────────────────

DIACRITICS_RE = re.compile(
    r'[\u0610-\u061A\u064B-\u065F\u0670\u06D6-\u06DC'
    r'\u06DF-\u06E4\u06E7\u06E8\u06EA-\u06ED]'
)
PAGE_RE    = re.compile(r'PageV\d+P\d+')
MS_RE      = re.compile(r'ms\d+')
TATWEEL_RE = re.compile(r'\u0640')


def strip_diacritics(t):
    return DIACRITICS_RE.sub('', t)


def clean_openiti(text):
    """Remove OpenITI markers and join continuation lines."""
    lines = text.split('\n')
    cleaned = []
    for line in lines:
        line = line.rstrip()
        if line.startswith('#META#') or line.startswith('######'):
            continue
        # Continuation lines start with ~~
        if line.startswith('~~'):
            if cleaned:
                cleaned[-1] += ' ' + line[2:].strip()
            else:
                cleaned.append(line[2:].strip())
        else:
            cleaned.append(line)
    # Join and clean markers
    result = '\n'.join(cleaned)
    result = PAGE_RE.sub('', result)
    result = MS_RE.sub('', result)
    result = re.sub(r'\s+', ' ', result.replace('\n', '\n')).strip()
    return result


def split_entries(text, pattern):
    """Split text into entries based on a regex pattern.
    Returns list of (match_object, body_text) tuples."""
    matches = list(pattern.finditer(text))
    entries = []
    for i, m in enumerate(matches):
        start = m.end()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        body = text[start:end].strip()
        entries.append((m, body))
    return entries


# Six Books sigla → full name mapping
SIGLA_MAP = {
    'خ': 'البخاري', 'م': 'مسلم', 'د': 'أبو داود',
    'ت': 'الترمذي', 'س': 'النسائي', 'ق': 'ابن ماجه',
    'ع': 'الستة', 'بخ': 'الأدب المفرد',
    'كن': 'مسند مالك', 'فق': 'التفسير',
    'ر': 'الرموز', '4': 'الأربعة',
    'تمييز': 'تمييز',
}

# Grade keywords → normalized grade (priority order)
GRADE_KEYWORDS = [
    # Companion
    ('صحابي', 'companion'), ('صحابية', 'companion'), ('له صحبة', 'companion'),
    ('لها صحبة', 'companion'),
    # Reliable
    ('ثقة ثبت', 'reliable'), ('ثقة حافظ', 'reliable'), ('ثقة متقن', 'reliable'),
    ('ثقة', 'reliable'),
    # Mostly reliable
    ('صدوق حسن', 'mostly_reliable'), ('صدوق له أوهام', 'mostly_reliable'),
    ('صدوق يخطئ', 'mostly_reliable'), ('صدوق يهم', 'mostly_reliable'),
    ('صدوق', 'mostly_reliable'),
    ('لا بأس به', 'mostly_reliable'), ('مقبول', 'mostly_reliable'),
    ('حسن الحديث', 'mostly_reliable'),
    # Weak
    ('ضعيف', 'weak'), ('لين الحديث', 'weak'), ('لين', 'weak'),
    ('فيه لين', 'weak'), ('فيه ضعف', 'weak'), ('سيئ الحفظ', 'weak'),
    # Abandoned
    ('متروك', 'abandoned'), ('منكر الحديث', 'abandoned'),
    # Fabricator
    ('كذاب', 'fabricator'), ('وضاع', 'fabricator'), ('يضع', 'fabricator'),
    # Unknown
    ('مجهول الحال', 'unknown'), ('مجهول', 'unknown'), ('مستور', 'unknown'),
]

GRADE_COLORS = {
    'companion':       '#9b59b6',
    'reliable':        '#2ecc71',
    'mostly_reliable': '#f39c12',
    'weak':            '#e74c3c',
    'abandoned':       '#c0392b',
    'fabricator':      '#8b0000',
    'unknown':         '#95a5a6',
}


def extract_grade(text):
    """Extract the highest-priority grade from Arabic text."""
    clean = strip_diacritics(text)
    for keyword, grade in GRADE_KEYWORDS:
        if strip_diacritics(keyword) in clean:
            return grade, keyword
    return None, None


def clean_name(name):
    """Remove phonetic/explanatory notes from narrator names."""
    # Remove بضم/بفتح/بكسر pronunciation guides
    name = re.sub(r'\s+ب(?:ضم|فتح|كسر|سكون)\s+[\u0600-\u06FF]+(?:\s+[\u0600-\u06FF]+)*', '', name)
    # Remove بعدها/وبعدها descriptors
    name = re.sub(r'\s+(?:و)?بعدها\s+[\u0600-\u06FF]+', '', name)
    # Remove وآخره descriptor
    name = re.sub(r'\s+وآخره\s+[\u0600-\u06FF]+', '', name)
    # Remove باسم الحيوان المعروف and similar glosses
    name = re.sub(r'\s+باسم\s+[\u0600-\u06FF\s]+', '', name)
    # Remove يكنى أبا/يعرف ب descriptors
    name = re.sub(r'\s+(?:يكنى|يعرف)\s+.*$', '', name)
    # Remove بالتحتانية and similar
    name = re.sub(r'\s+بال[\u0600-\u06FF]+ية', '', name)
    # Remove مصغر/مكبر
    name = re.sub(r'\s+مصغر[ا]?', '', name)
    return name.strip()


def extract_death_year(text):
    """Extract death year from Arabic text like 'مات سنة ست وثلاثين'."""
    # Numeric: مات سنة 197
    m = re.search(r'(?:مات|توفي|قتل)\s+سنة\s+(\d+)', strip_diacritics(text))
    if m:
        return m.group(1) + ' هـ'
    # Word-form: مات سنة ست وثلاثين
    m = re.search(
        r'(?:مات|توفي|قتل)\s+سنة\s+([\u0600-\u06FF\s]+?)(?:\s+(?:وله|وقد|وقيل|[دتسقخمع]|$))',
        strip_diacritics(text)
    )
    if m:
        return m.group(1).strip() + ' هـ'
    return ''


def extract_tabaqah(text):
    """Extract tabaqah (generation) from text like 'من العاشرة'."""
    m = re.search(r'من\s+(ال[\u0600-\u06FF]+(?:\s+عشر[ة]?)?)\s', text)
    if m:
        tab = m.group(1)
        # Remove trailing verbs that aren't part of the tabaqah
        tab = re.sub(r'\s+(?:مات|توفي|قتل)$', '', tab)
        return tab
    return ''


def extract_kunya(text):
    """Extract kunya (patronymic) like أبو بكر, أبو عبد الله."""
    # Match أبو/أبي followed by 1-2 name tokens (not nisba/laqab descriptors)
    m = re.search(r'(أب[وي]\s+[\u0600-\u06FF]+(?:\s+[\u0600-\u06FF]+)?)', text)
    if m:
        kunya = m.group(1)
        # Remove common suffixes that aren't part of the kunya
        kunya = re.sub(r'\s+(?:نزيل|من|بن|بنت|مولى|صاحب|البصري|الكوفي|المدني|الشامي|المصري|البغدادي|الحراني|الموصلي|النيسابوري).*$', '', kunya)
        return kunya.strip()
    return ''


def extract_teachers_students(text):
    """Extract teacher/student lists from structured texts."""
    teachers, students = [], []

    # Pattern: روى عن NAME (and NAME)
    m = re.search(r'(?:روى|يروي)\s+عن\s+(.*?)(?:روى\s+عنه|$)', text, re.DOTALL)
    if m:
        raw = m.group(1)
        # Split on و at word boundary
        names = re.split(r'\s*و(?=\s)', raw)
        for n in names:
            n = re.sub(r'[،,.].*', '', n).strip()
            if 3 < len(n) < 80:
                teachers.append(n)

    # Pattern: روى عنه NAME (and NAME)
    m = re.search(r'روى\s+عنه\s+(.*?)(?:\.|$)', text, re.DOTALL)
    if m:
        raw = m.group(1)
        names = re.split(r'\s*و(?=\s)', raw)
        for n in names:
            n = re.sub(r'[،,.].*', '', n).strip()
            if 3 < len(n) < 80:
                students.append(n)

    return teachers, students


# ──────────────────────────────────────────────────────────────────────
# Per-text parsers
# ──────────────────────────────────────────────────────────────────────

def parse_taqrib(text):
    """Taqrib al-Tahdhib — compact grading manual.
    Format: ### $ NUM NAME GRADE من الTABAQA مات سنة DEATH SIGLA
    """
    lines = text.split('\n')
    # Rejoin ~~ continuation lines
    joined = []
    for line in lines:
        if line.startswith('~~'):
            if joined:
                joined[-1] += ' ' + line[2:].strip()
            else:
                joined.append(line[2:].strip())
        else:
            joined.append(line)

    # Match entry lines
    entry_re = re.compile(r'^### \$ (\d+)\s+(.+)')
    xref_re = re.compile(r'^### \$\$\$ ')

    entries = []
    for line in joined:
        line = PAGE_RE.sub('', line).strip()
        line = MS_RE.sub('', line).strip()

        if xref_re.match(line):
            continue  # Skip cross-references

        m = entry_re.match(line)
        if not m:
            continue

        num = int(m.group(1))
        body = m.group(2).strip()

        # Extract grade
        grade_en, grade_ar = extract_grade(body)

        # Extract tabaqah
        tabaqah = extract_tabaqah(body)

        # Extract death
        death = extract_death_year(body)

        # Extract sigla (book abbreviations at end of line)
        sigla_pattern = re.compile(
            r'\s+((?:[خمدتسقع]|بخ|كن|فق|تمييز|ر\s*4?)(?:\s+(?:[خمدتسقع]|بخ|كن|فق|تمييز|ر\s*4?))*)$'
        )
        sigla_match = sigla_pattern.search(body)
        books = []
        if sigla_match:
            raw_sigla = sigla_match.group(1).split()
            books = [s for s in raw_sigla if s]
            body = body[:sigla_match.start()].strip()

        # Extract name — everything before the grade keyword
        name = body
        if grade_ar:
            idx = strip_diacritics(name).find(strip_diacritics(grade_ar))
            if idx > 0:
                name = name[:idx].strip()

        # Remove tabaqah and death from name
        name = re.sub(r'\s+من\s+ال[\u0600-\u06FF]+.*', '', name).strip()
        name = re.sub(r'\s+مات\s+.*', '', name).strip()
        name = clean_name(name)

        # Extract kunya from name
        kunya = extract_kunya(name)

        entries.append({
            'id': num,
            'name': name.strip(),
            'kunya': kunya,
            'grade_en': grade_en or 'unknown',
            'grade_ar': grade_ar or '',
            'color': GRADE_COLORS.get(grade_en or 'unknown', '#95a5a6'),
            'death': death,
            'tabaqah': tabaqah,
            'books': books,
            'source': 'taqrib_tahdhib',
        })

    return entries


def parse_tahdhib_kamal(text):
    """Tahdhib al-Kamal — the primary Six Books encyclopedia.
    Format: ### $ NUM- SIGLA: NAME, kunya, nisba.
    Body contains روى عن / روى عنه sections.
    """
    lines = text.split('\n')
    joined = []
    for line in lines:
        if line.startswith('~~'):
            if joined:
                joined[-1] += ' ' + line[2:].strip()
            else:
                joined.append(line[2:].strip())
        else:
            joined.append(line)

    full = '\n'.join(joined)
    full = PAGE_RE.sub('', full)
    full = MS_RE.sub('', full)

    # Entry pattern: ### $ NUM with optional -/space then sigla: name
    entry_re = re.compile(
        r'^### \$ (\d+)\s*[-\s]*(?:ومن الأوهام\s*:\s*)?(.+)',
        re.MULTILINE
    )

    raw_entries = split_entries(full, entry_re)
    entries = []

    for match, body in raw_entries:
        num = int(match.group(1))
        header = match.group(2).strip()

        # Separate sigla from name
        # Format: "خ م د: اسم الراوي" or "دفق: اسم"
        sigla_name = re.match(
            r'^([خمدتسقع\s]+(?:بخ|كن|فق)?[\s:]*)?:?\s*(.+)',
            header
        )
        books = []
        name = header
        if sigla_name and sigla_name.group(1):
            raw_s = sigla_name.group(1).replace(':', '').strip()
            books = raw_s.split()
            name = sigla_name.group(2).strip()

        # Name ends at first period or comma typically
        # Take the first sentence as the name
        name_end = re.search(r'[.،]', name)
        full_name = name[:name_end.start()].strip() if name_end else name.strip()

        # Extract kunya from header
        kunya = extract_kunya(header)

        # Extract teachers/students from body
        teachers, students = extract_teachers_students(body)

        entries.append({
            'id': num,
            'name': full_name,
            'kunya': kunya,
            'grade_en': 'unknown',  # Tahdhib al-Kamal doesn't grade directly
            'grade_ar': '',
            'color': '#95a5a6',
            'death': extract_death_year(body),
            'books': books,
            'teachers': teachers[:20],  # cap to avoid noise
            'students': students[:20],
            'source': 'tahdhib_kamal',
        })

    return entries


def parse_mizan(text):
    """Mizan al-I'tidal — critical narrator assessments.
    Format: ### $ NUM [ REF ] - NAME [ SIGLA ] description
    """
    lines = text.split('\n')
    joined = []
    for line in lines:
        if line.startswith('~~'):
            if joined:
                joined[-1] += ' ' + line[2:].strip()
            else:
                joined.append(line[2:].strip())
        else:
            joined.append(line)

    full = '\n'.join(joined)
    full = PAGE_RE.sub('', full)
    full = MS_RE.sub('', full)

    # Match: ### $ NUM then everything after (name, brackets, etc.)
    # We parse the bracket/dash structure in post-processing
    entry_re = re.compile(
        r'^### \$ (\d+)\s+(.+)',
        re.MULTILINE
    )

    raw_entries = split_entries(full, entry_re)
    entries = []

    for match, body in raw_entries:
        num = int(match.group(1))
        raw_header = match.group(2).strip()

        # Parse: optional [REF] then - then NAME
        # Or: [REF ت] - NAME
        # Or: just NAME directly
        ref_num = None
        header = raw_header

        # Extract leading [ bracket ] and dash
        bm = re.match(r'\[([^\]]*)\]\s*-?\s*', header)
        if bm:
            bracket = bm.group(1).strip()
            rm = re.match(r'(\d+)', bracket)
            if rm:
                ref_num = rm.group(1)
            header = header[bm.end():].strip()
        elif header.startswith('-'):
            header = header[1:].strip()

        # Extract book sigla from brackets in header
        books = []
        for sm in re.finditer(r'\[\s*([^\]]+)\s*\]', header):
            content = sm.group(1).strip()
            tokens = content.split()
            if all(len(t) <= 3 and any(c in t for c in 'خمدتسقع') for t in tokens):
                books.extend(tokens)
        # Strip all brackets from header for name extraction
        header_clean = re.sub(r'\[([^\]]*)\]', '', header).strip()
        # Remove parenthetical numbers like ( 2 )
        header_clean = re.sub(r'\(\s*\d+\s*\)', '', header_clean).strip()
        # Clean extra spaces
        header_clean = re.sub(r'\s+', ' ', header_clean).strip()

        name = header_clean
        # Name is typically everything up to first descriptor
        name_end = re.search(r'\s+(?:عن|روى|من مشيخة|شيخ|بصري|كوفي|مدني|شامي|قال|ليس|تركوه|صدوق|ثقة|ضعيف|مجهول|متروك|كذاب|هالك|لا يصح|لا يعرف|اراه)', name)
        if name_end and name_end.start() > 5:
            full_name = name[:name_end.start()].strip()
        else:
            full_name = re.split(r'[،,]', name)[0].strip()

        # Extract grade from header first (Mizan often has it inline), then body
        grade_en, grade_ar = extract_grade(header)
        if not grade_en:
            grade_en, grade_ar = extract_grade(body)

        kunya = extract_kunya(header_clean)

        entries.append({
            'id': num,
            'ref_num': int(ref_num) if ref_num else None,
            'name': full_name,
            'kunya': kunya,
            'grade_en': grade_en or 'unknown',
            'grade_ar': grade_ar or '',
            'color': GRADE_COLORS.get(grade_en or 'unknown', '#95a5a6'),
            'death': extract_death_year(body),
            'books': books,
            'source': 'mizan_itidal',
        })

    return entries


def parse_jarh_tadil(text):
    """Al-Jarh wa al-Ta'dil — two-line entry format.
    Format: ### $ NUM -
            # NAME.
            # Evaluation text...
    """
    lines = text.split('\n')
    joined = []
    for line in lines:
        if line.startswith('~~'):
            if joined:
                joined[-1] += ' ' + line[2:].strip()
            else:
                joined.append(line[2:].strip())
        else:
            joined.append(line)

    full = '\n'.join(joined)
    full = PAGE_RE.sub('', full)
    full = MS_RE.sub('', full)

    entry_re = re.compile(r'^### \$ (\d+)\s*-\s*$', re.MULTILINE)
    raw_entries = split_entries(full, entry_re)
    entries = []

    for match, body in raw_entries:
        num = int(match.group(1))

        # First # line after entry header is the name
        name_lines = body.split('\n')
        name = ''
        eval_text = ''
        for i, ln in enumerate(name_lines):
            ln = ln.strip()
            if ln.startswith('# ') and not name:
                name = ln[2:].strip()
                # Remove trailing reference markers like (32 م)
                name = re.sub(r'\(\d+\s*[مك]\)', '', name).strip()
                eval_text = '\n'.join(name_lines[i+1:])
                break

        if not name:
            continue

        # Name is typically up to first 'روى' or 'حدثنا' or period
        name_end = re.search(r'\s+(?:روى|حدثنا|سمعت|نا\s)', name)
        full_name = name[:name_end.start()].strip() if name_end else name.strip()
        full_name = re.sub(r'\.\s*$', '', full_name)

        grade_en, grade_ar = extract_grade(eval_text)
        kunya = extract_kunya(name)

        entries.append({
            'id': num,
            'name': full_name,
            'kunya': kunya,
            'grade_en': grade_en or 'unknown',
            'grade_ar': grade_ar or '',
            'color': GRADE_COLORS.get(grade_en or 'unknown', '#95a5a6'),
            'death': extract_death_year(eval_text),
            'source': 'jarh_tadil',
        })

    return entries


def parse_thiqat(text):
    """Al-Thiqat — reliable narrator list.
    Format: ### $ NUM - NAME يروي عن X روى عنه Y
    All narrators are implicitly 'thiqa' (reliable).
    """
    lines = text.split('\n')
    joined = []
    for line in lines:
        if line.startswith('~~'):
            if joined:
                joined[-1] += ' ' + line[2:].strip()
            else:
                joined.append(line[2:].strip())
        else:
            joined.append(line)

    full = '\n'.join(joined)
    full = PAGE_RE.sub('', full)
    full = MS_RE.sub('', full)

    # Match entries like: ### $ 1687 - NAME ...
    entry_re = re.compile(r'^### \$ (\d+)\s*-\s*(.+)', re.MULTILINE)
    raw_entries = split_entries(full, entry_re)
    entries = []

    for match, body in raw_entries:
        num = int(match.group(1))
        header = match.group(2).strip()

        # Full text includes header + body
        full_text = header + ' ' + body

        # Name: everything before يروي عن / من أهل / كنيته / etc.
        name_end = re.search(
            r'\s+(?:يروي|روى|من أهل|كنيته|كان|مات|أخو|حليف|مولى\s)',
            header
        )
        name = header[:name_end.start()].strip() if name_end else header.strip()

        # All narrators in Thiqat are implicitly reliable
        # But check if there's an explicit grade mentioned
        grade_en, grade_ar = extract_grade(full_text)
        if not grade_en:
            grade_en, grade_ar = 'reliable', 'ثقة'

        kunya = extract_kunya(header)
        teachers, students = extract_teachers_students(full_text)

        entries.append({
            'id': num,
            'name': name,
            'kunya': kunya,
            'grade_en': grade_en,
            'grade_ar': grade_ar,
            'color': GRADE_COLORS.get(grade_en, '#95a5a6'),
            'death': extract_death_year(full_text),
            'teachers': teachers[:10],
            'students': students[:10],
            'source': 'thiqat',
        })

    return entries


def parse_kamil_duafa(text):
    """Al-Kamil fi Du'afa — weak narrator catalog.
    Format: ### |||| NUM- NAME.
    Body contains evaluation with isnads.
    """
    lines = text.split('\n')
    joined = []
    for line in lines:
        if line.startswith('~~'):
            if joined:
                joined[-1] += ' ' + line[2:].strip()
            else:
                joined.append(line[2:].strip())
        else:
            joined.append(line)

    full = '\n'.join(joined)
    full = PAGE_RE.sub('', full)
    full = MS_RE.sub('', full)

    # Entry pattern: ### |||| NUM- NAME.
    entry_re = re.compile(r'^### \|\|\|\| (\d+)-\s*(.+)', re.MULTILINE)
    raw_entries = split_entries(full, entry_re)
    entries = []

    for match, body in raw_entries:
        num = int(match.group(1))
        header = match.group(2).strip()

        # Name ends at period
        name = re.split(r'\.', header)[0].strip()

        # Extract grade from body — look for قال الشيخ (Ibn Adi's verdict)
        grade_en, grade_ar = extract_grade(body)
        # In Kamil fi Du'afa, most are weak unless stated otherwise
        if not grade_en:
            grade_en, grade_ar = 'weak', 'ضعيف'

        kunya = extract_kunya(header)

        entries.append({
            'id': num,
            'name': name,
            'kunya': kunya,
            'grade_en': grade_en,
            'grade_ar': grade_ar,
            'color': GRADE_COLORS.get(grade_en, '#95a5a6'),
            'death': extract_death_year(body),
            'source': 'kamil_duafa',
        })

    return entries


def parse_tarikh_baghdad(text):
    """Tarikh Baghdad — Baghdad scholar biographies.
    Format: ### $ (NAME) or ### $ NAME
    """
    lines = text.split('\n')
    joined = []
    for line in lines:
        if line.startswith('~~'):
            if joined:
                joined[-1] += ' ' + line[2:].strip()
            else:
                joined.append(line[2:].strip())
        else:
            joined.append(line)

    full = '\n'.join(joined)
    full = PAGE_RE.sub('', full)
    full = MS_RE.sub('', full)

    # Entry: ### $ (NAME) or ### $ NAME
    entry_re = re.compile(r'^### \$\s+\(?([^)\n]+)\)?\s*$', re.MULTILINE)
    raw_entries = split_entries(full, entry_re)
    entries = []

    for idx, (match, body) in enumerate(raw_entries):
        name = match.group(1).strip()
        if len(name) < 3:
            continue

        grade_en, grade_ar = extract_grade(body)
        kunya = extract_kunya(name + ' ' + body[:200])

        entries.append({
            'id': idx + 1,
            'name': name,
            'kunya': kunya,
            'grade_en': grade_en or 'unknown',
            'grade_ar': grade_ar or '',
            'color': GRADE_COLORS.get(grade_en or 'unknown', '#95a5a6'),
            'death': extract_death_year(body),
            'source': 'tarikh_baghdad',
        })

    return entries


def parse_tahdhib_tahdhib(text):
    """Tahdhib al-Tahdhib — condensed version of Tahdhib al-Kamal.
    Format: ### $ NUM SIGLA NAME
    """
    lines = text.split('\n')
    joined = []
    for line in lines:
        if line.startswith('~~'):
            if joined:
                joined[-1] += ' ' + line[2:].strip()
            else:
                joined.append(line[2:].strip())
        else:
            joined.append(line)

    full = '\n'.join(joined)
    full = PAGE_RE.sub('', full)
    full = MS_RE.sub('', full)

    entry_re = re.compile(r'^### \$ (\d+)\s+(.+)', re.MULTILINE)
    raw_entries = split_entries(full, entry_re)
    entries = []

    for match, body in raw_entries:
        num = int(match.group(1))
        header = match.group(2).strip()

        # Strip leading sigla and full book names
        # Pattern: single letters (خ م د ت س ق ع) or book names (البخاري, مسلم, etc.)
        # at the start, followed by the actual narrator name
        BOOK_NAMES = [
            'البخاري', 'مسلم', 'أبي داود', 'الترمذي', 'النسائي',
            'ابن ماجة', 'مسند مالك', 'في التفسير', 'تمييز',
            'الأدب المفرد', 'الستة', 'الأربعة',
        ]
        name = header
        # Strip sigla prefix: single letters, multi-letter codes, and full book names
        # Loop to handle chained sigla like "م د ت ق مسلم وأبي داود والترمذي وابن ماجة"
        SIGLA_CODES = {'خ', 'م', 'د', 'ت', 'س', 'ق', 'ع', 'بخ', 'كن', 'فق', 'ر'}
        for _ in range(10):  # max iterations
            old = name
            # Strip single/multi-letter sigla codes
            m = re.match(r'^(' + '|'.join(re.escape(s) for s in sorted(SIGLA_CODES, key=len, reverse=True)) + r')\s+', name)
            if m:
                name = name[m.end():]
                continue
            # Strip full book names
            stripped = False
            for bn in BOOK_NAMES:
                if name.startswith(bn):
                    name = name[len(bn):].lstrip()
                    if name.startswith('و'):
                        name = name[1:].lstrip()
                    stripped = True
                    break
            if stripped:
                continue
            # Strip leading و before sigla
            if name.startswith('و') and len(name) > 1:
                rest = name[1:].lstrip()
                if any(rest.startswith(s + ' ') or rest.startswith(s + '\t') for s in SIGLA_CODES) or any(rest.startswith(bn) for bn in BOOK_NAMES):
                    name = rest
                    continue
            break
        # Trim name at "روى عن" or similar
        name_end = re.search(r'\s+(?:روى\s|نزيل\s|والد\s|صوابه\s)', name)
        if name_end and name_end.start() > 10:
            name = name[:name_end.start()].strip()

        # Extract grade from body
        grade_en, grade_ar = extract_grade(body)
        kunya = extract_kunya(header)
        death = extract_death_year(body)

        entries.append({
            'id': num,
            'name': name,
            'kunya': kunya,
            'grade_en': grade_en or 'unknown',
            'grade_ar': grade_ar or '',
            'color': GRADE_COLORS.get(grade_en or 'unknown', '#95a5a6'),
            'death': death,
            'source': 'tahdhib_tahdhib',
        })

    return entries


# ──────────────────────────────────────────────────────────────────────
# Registry and main
# ──────────────────────────────────────────────────────────────────────

PARSERS = {
    'taqrib': {
        'file': 'taqrib_tahdhib.txt',
        'parser': parse_taqrib,
        'title': 'Taqrib al-Tahdhib (Ibn Hajar)',
    },
    'tahdhib_kamal': {
        'file': 'tahdhib_kamal.txt',
        'parser': parse_tahdhib_kamal,
        'title': 'Tahdhib al-Kamal (al-Mizzi)',
    },
    'tahdhib_tahdhib': {
        'file': 'tahdhib_tahdhib.txt',
        'parser': parse_tahdhib_tahdhib,
        'title': 'Tahdhib al-Tahdhib (Ibn Hajar)',
    },
    'mizan': {
        'file': 'mizan_itidal.txt',
        'parser': parse_mizan,
        'title': "Mizan al-I'tidal (al-Dhahabi)",
    },
    'jarh': {
        'file': 'jarh_tadil.txt',
        'parser': parse_jarh_tadil,
        'title': "Al-Jarh wa al-Ta'dil (Ibn Abi Hatim)",
    },
    'thiqat': {
        'file': 'thiqat.txt',
        'parser': parse_thiqat,
        'title': 'Al-Thiqat (Ibn Hibban)',
    },
    'kamil': {
        'file': 'kamil_duafa.txt',
        'parser': parse_kamil_duafa,
        'title': "Al-Kamil fi Du'afa (Ibn 'Adi)",
    },
    'tarikh': {
        'file': 'tarikh_baghdad.txt',
        'parser': parse_tarikh_baghdad,
        'title': 'Tarikh Baghdad (al-Khatib)',
    },
}


def run_parser(key, stats_only=False):
    info = PARSERS[key]
    path = RAW / info['file']
    if not path.exists():
        print(f"  [SKIP] {info['title']} — file not found: {path.name}")
        return None

    print(f"  Parsing {info['title']}...")
    text = path.read_text(encoding='utf-8')
    entries = info['parser'](text)

    # Stats
    grade_dist = Counter(e['grade_en'] for e in entries)
    print(f"    -> {len(entries)} entries")
    for g, c in grade_dist.most_common():
        print(f"       {g}: {c}")

    if stats_only:
        return entries

    # Save
    out_path = OUT / f"{key}.json"
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(entries, f, ensure_ascii=False, indent=1)
    print(f"    -> saved to {out_path.name} ({out_path.stat().st_size:,} bytes)")

    return entries


def main():
    args = sys.argv[1:]
    stats_only = '--stats' in args
    args = [a for a in args if not a.startswith('--')]

    targets = args if args else list(PARSERS.keys())

    print(f"OpenITI Rijal Parser — {len(targets)} text(s)\n")

    total = 0
    for key in targets:
        if key not in PARSERS:
            print(f"  [ERROR] Unknown text: {key}")
            print(f"          Available: {', '.join(PARSERS.keys())}")
            continue
        entries = run_parser(key, stats_only)
        if entries:
            total += len(entries)
        print()

    print(f"Total entries parsed: {total:,}")


if __name__ == '__main__':
    main()
