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

# Import shared lexicon (single source of truth for boundary logic)
sys.path.insert(0, str(Path(__file__).resolve().parent))
from narrator_lexicon import (
    GRADE_KEYWORDS, GRADE_COLORS, ABD_COMPOUNDS, SIGLA, BOOK_NAMES,
    COMPANION_MARKERS, BOOK_GRADE_DEFAULTS,
    GRADE_VERB_KEYWORDS, GRADE_TITLE_KEYWORDS,
    DIACRITICS_RE, strip_diacritics,
    extract_grade, extract_grade_condensed, apply_book_default,
    clean_narrator_name, strip_book_prefix, truncate_at_biography,
    fix_abd_compound, is_valid_name, is_cross_reference,
)

PAGE_RE    = re.compile(r'PageV\d+P\d+')
MS_RE      = re.compile(r'ms\d+')


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


# (SIGLA_MAP, GRADE_KEYWORDS, GRADE_COLORS, extract_grade, clean_name
#  are now imported from narrator_lexicon.py above)

# Legacy alias for backward compat in parsers that call clean_name()
def clean_name(name):
    """Legacy wrapper. Use clean_narrator_name() for new code."""
    return clean_narrator_name(name, has_sigla_prefix=False)


def extract_death_year(text):
    """Extract death year from Arabic text, using both numeric and word-form parsing."""
    import sys, os
    sys.path.insert(0, os.path.dirname(__file__))
    from arabic_year_parser import extract_death_year_word

    clean = strip_diacritics(text)

    # Try the full word-form parser (handles both numeric and word-form)
    year = extract_death_year_word(clean)
    if year:
        return str(year) + ' هـ'

    # Fallback: raw word capture for non-standard patterns
    m = re.search(
        r'(?:مات|توفي|قتل)\s+سنة\s+([\u0600-\u06FF\s]+?)(?:\s+(?:وله|وقد|وقيل|[دتسقخمع]|$))',
        clean
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
        full_name = clean_name(full_name)

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
        full_name = clean_name(full_name)

        # Extract grade from header first (Mizan often has it inline), then body
        grade_en, grade_ar = extract_grade(header)
        if not grade_en:
            grade_en, grade_ar = extract_grade(body)

        kunya = extract_kunya(header_clean)

        # Mizan is a criticism book — default to weak if no grade found
        grade_en, grade_ar = apply_book_default(grade_en, grade_ar, 'mizan_itidal')

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
        full_name = clean_name(full_name)

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

        # Use lexicon's strip_book_prefix — handles مسلم/مالك ambiguity
        name = strip_book_prefix(header)

        # Check for cross-reference entries (بن X هو Y)
        is_xref, xref_target = is_cross_reference(name, body[:100])

        # Trim name at "روى عن" or similar
        name_end = re.search(r'\s+(?:روى\s|نزيل\s|والد\s|صوابه\s)', name)
        if name_end and name_end.start() > 10:
            name = name[:name_end.start()].strip()

        # Restore عبد compounds
        name = fix_abd_compound(name)

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
            'is_xref': is_xref,
            'xref_target': xref_target or '',
        })

    return entries


def parse_tabaqat_ibn_saad(text):
    """Tabaqat al-Kubra (Ibn Sa'd, d.230) — earliest biographical dictionary.
    Format: ### $ NUM- NAME, optional kunya/nisba
    Body: prose biography with hadiths, death info, etc.
    First ~20k lines are Sira (Prophet's biography), entries start after.
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

    entry_re = re.compile(r'^### \$ (\d+)\s*-?\s*(.+)', re.MULTILINE)
    raw_entries = split_entries(full, entry_re)
    entries = []

    for match, body in raw_entries:
        num = int(match.group(1))
        header = match.group(2).strip()

        # Name: everything up to first comma or period
        name_end = re.search(r'[،,.]', header)
        name = header[:name_end.start()].strip() if name_end else header.strip()
        name = re.sub(r'\s+رضي\s+الله\s+عن[هـ].*', '', name).strip()
        name = re.sub(r'\s+رحمه\s+الله.*', '', name).strip()
        # Strip "ذكر" prefix common in Tabaqat headers
        name = re.sub(r'^ذكر\s+', '', name).strip()
        name = clean_name(name)

        kunya = extract_kunya(header)
        death = extract_death_year(body)
        grade_en, grade_ar = extract_grade(body[:500])

        # Tabaqat are mostly companions/tabi'in
        if not grade_en:
            # Check for companion markers in body
            comp_markers = ['صحابي', 'صحب النبي', 'شهد بدرا', 'شهد أحدا',
                           'هاجر إلى', 'بايع', 'أسلم يوم']
            if any(m in body[:300] for m in comp_markers):
                grade_en = 'companion'
                grade_ar = 'صحابي'

        entries.append({
            'id': num,
            'name': name,
            'kunya': kunya,
            'grade_en': grade_en or 'unknown',
            'grade_ar': grade_ar or '',
            'color': GRADE_COLORS.get(grade_en or 'unknown', '#95a5a6'),
            'death': death,
            'source': 'tabaqat_ibn_saad',
        })

    return entries


def parse_siyar(text):
    """Siyar A'lam al-Nubala (al-Dhahabi, d.748) — major biographical encyclopedia.
    Format: ### $ NUM - NAME * (sigla)
    Body: detailed biography with death year, city, grades, teachers/students.
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

    entry_re = re.compile(r'^### \$ (\d+)\s*-?\s*(.+)', re.MULTILINE)
    raw_entries = split_entries(full, entry_re)
    entries = []

    for match, body in raw_entries:
        num = int(match.group(1))
        header = match.group(2).strip()
        # Clean body: strip # line markers for text analysis
        body_clean = re.sub(r'^#\s+', '', body, flags=re.MULTILINE)
        body_clean = re.sub(r'\s+', ' ', body_clean).strip()

        # Remove sigla markers: * (م، ق) or * (ع)
        header_clean = re.sub(r'\s*\*\s*(\([^)]*\)\s*)?\.?$', '', header).strip()
        # Remove trailing period
        header_clean = header_clean.rstrip('.')

        # Name: everything up to first period, comma, or # boundary
        name_end = re.search(r'[،,]', header_clean)
        name = header_clean[:name_end.start()].strip() if name_end else header_clean.strip()
        name = clean_name(name)

        # Extract sigla from header
        books = []
        sigla_m = re.search(r'\*\s*\(([^)]+)\)', header)
        if sigla_m:
            raw = sigla_m.group(1).replace('،', ' ').replace(',', ' ')
            books = [s.strip() for s in raw.split() if s.strip()]

        # Kunya from NAME portion only (not body text)
        kunya = extract_kunya(name)

        # Death from body -- Siyar uses various patterns
        death = extract_death_year(body_clean)
        if not death:
            m = re.search(r'(?:توفي|مات)\s+(?:في\s+)?سنة\s+(\d+)', body_clean)
            if m:
                death = m.group(1) + ' هـ'

        # Check companion markers FIRST (before general grade extraction)
        grade_en, grade_ar = None, None
        comp_markers = ['صحابي', 'صاحب رسول', 'شهد بدرا', 'من السابقين',
                       'أحد العشرة', 'من المهاجرين', 'حواري رسول',
                       'أسلم قديما', 'من أهل بدر', 'بايع تحت الشجرة',
                       'أحد السابقين']
        if any(m in body_clean[:600] for m in comp_markers):
            grade_en = 'companion'
            grade_ar = 'صحابي'

        # If not companion, extract grade from body
        if not grade_en:
            grade_en, grade_ar = extract_grade(body_clean[:600])

        # City extraction from body
        city = ''
        city_m = re.search(
            r'(?:نزيل|سكن|من أهل)\s+([\u0600-\u06FF]+(?:\s+[\u0600-\u06FF]+)?)',
            body_clean[:600]
        )
        if city_m:
            city = city_m.group(1).strip()

        entry = {
            'id': num,
            'name': name,
            'kunya': kunya,
            'grade_en': grade_en or 'unknown',
            'grade_ar': grade_ar or '',
            'color': GRADE_COLORS.get(grade_en or 'unknown', '#95a5a6'),
            'death': death,
            'city': city,
            'books': books,
            'source': 'siyar',
        }
        entries.append(entry)

    return entries


def parse_isaba(text):
    """Al-Isaba fi Tamyiz al-Sahaba (Ibn Hajar, d.852) — companion encyclopedia.
    Format: ### $ NUM NAME inline_biography
    Body: continuation of biography. All entries are companions.
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

        # The name runs until a descriptor keyword or clause
        # Common patterns: NAME ... قال ... / NAME ... روى ... / NAME ... صحابي ...
        name_end = re.search(
            r'\s+(?:قال|روى|صحابي[ة]?|له صحبة|لها صحبة|ذكره|أخرج|كان|هو|يأتي|تقدم|مشهور|من بني|ممن|شهد|أسلم|هاجر)',
            header
        )
        if name_end and name_end.start() > 3:
            name = header[:name_end.start()].strip()
        else:
            # Fallback: take up to first sentence break
            name_end2 = re.search(r'[.،]', header)
            name = header[:name_end2.start()].strip() if name_end2 else header[:80].strip()

        name = clean_name(name)
        kunya = extract_kunya(name)  # from name only, not full header

        # All Isaba entries are companions (the book's scope)
        grade_en = 'companion'
        grade_ar = 'صحابي'

        # Death from body
        death = extract_death_year(header + ' ' + body)

        entries.append({
            'id': num,
            'name': name,
            'kunya': kunya,
            'grade_en': grade_en,
            'grade_ar': grade_ar,
            'color': GRADE_COLORS.get(grade_en, '#95a5a6'),
            'death': death,
            'source': 'isaba',
        })

    return entries


def parse_tarikh_islam(text):
    """Tarikh al-Islam (al-Dhahabi, d.748) — chronological history, organized by decade.
    Format: ### $BIO_MAN$ followed by name with [الوفاة: N ه] inline.
    30,000+ biographical entries, each with structured death year.
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

    entry_re = re.compile(r'^### \$BIO_MAN\$\s*$', re.MULTILINE)
    raw_entries = split_entries(full, entry_re)
    entries = []

    for idx, (match, body) in enumerate(raw_entries):
        # Clean body
        body_clean = re.sub(r'^#\s+', '', body, flags=re.MULTILINE)
        body_clean = re.sub(r'--- misc', '', body_clean)
        body_clean = re.sub(r'### NB.*', '', body_clean)
        body_clean = re.sub(r'\s+', ' ', body_clean).strip()

        # Extract death year from [الوفاة: N ه] marker
        death = ''
        death_m = re.search(r'\[الوفاة\s*:\s*(\d+)(?:\s*-\s*(\d+))?\s*ه\s*\]', body_clean)
        if death_m:
            if death_m.group(2):
                # Range: take midpoint or first value
                death = death_m.group(1) + ' هـ'
            else:
                death = death_m.group(1) + ' هـ'

        # Extract name: first line of body, before [الوفاة]
        name_line = body_clean.split('[الوفاة')[0] if '[الوفاة' in body_clean else body_clean[:200]
        name = name_line.strip()

        # Strip leading junk: dashes, entry numbers, sigla, section markers
        # Pattern: optional "- NUM -" then optional "sigla:" then the name
        name = re.sub(r'^-\s*', '', name)  # leading dash
        name = re.sub(r'^\d+\s*-\s*', '', name)  # leading number + dash
        name = re.sub(r'^[خمدتسقعبرف\s]+:\s*', '', name)  # sigla prefix like "ع:" or "ت ق:"
        name = re.sub(r'^-\s*', '', name)  # another dash after sigla removal
        # Strip section-style prefixes: "ترجمة", "وفاة", "موت", "ذكر"
        name = re.sub(r'^(?:ترجمة|وفاة|موت|ذكر|وفيات|بقية)\s+', '', name)
        # Strip brackets
        name = re.sub(r'\[([^\]]*)\]', r'\1', name)

        # Take up to first comma, period, or sentence break
        name_end = re.search(r'[،,.]|\s+(?:قال|كان|سمع|روى|ولد|أخذ|له|هو|من أهل)', name)
        if name_end and name_end.start() > 3:
            name = name[:name_end.start()].strip()
        else:
            name = name[:100].strip()
        # Remove honorifics
        name = re.sub(r'\s*-\s*رضي\s*الله\s*عن[هاـ]\s*-\s*', ' ', name).strip()
        name = re.sub(r'\s*رضي\s*الله\s*عن[هاـ].*', '', name).strip()
        name = re.sub(r'\s*صلى\s*الله\s*عليه\s*وسلم.*', '', name).strip()
        name = clean_name(name)

        if not name or len(name) < 3:
            continue

        kunya = extract_kunya(name)
        grade_en, grade_ar = extract_grade(body_clean[:500])

        # Companion detection (shared markers from lexicon)
        if not grade_en:
            if any(m in body_clean[:400] for m in COMPANION_MARKERS):
                grade_en = 'companion'
                grade_ar = 'صحابي'

        entries.append({
            'id': idx,
            'name': name,
            'kunya': kunya,
            'grade_en': grade_en or 'unknown',
            'grade_ar': grade_ar or '',
            'color': GRADE_COLORS.get(grade_en or 'unknown', '#95a5a6'),
            'death': death,
            'source': 'tarikh_islam',
        })

    return entries


def parse_lisan_mizan(text):
    """Lisan al-Mizan (Ibn Hajar, d.852) — expansion of Mizan al-I'tidal.
    Format: ### $ NUM - NAME. Body has grades and biographical info.
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

    entry_re = re.compile(r'^### \$ (\d+)\s*-?\s*(.+)', re.MULTILINE)
    raw_entries = split_entries(full, entry_re)
    entries = []

    for match, body in raw_entries:
        num = int(match.group(1))
        header = match.group(2).strip()

        # Remove (ز) marker (indicates addition by editor)
        header = re.sub(r'^\s*\(ز\)\s*:?\s*', '', header).strip()

        # Name: up to first period or # boundary
        name_end = re.search(r'[.،#]', header)
        name = header[:name_end.start()].strip() if name_end else header[:100].strip()
        name = clean_name(name)

        body_clean = re.sub(r'^#\s+', '', body, flags=re.MULTILINE)
        body_clean = re.sub(r'\s+', ' ', body_clean).strip()

        kunya = extract_kunya(name)
        death = extract_death_year(header + ' ' + body_clean[:500])
        grade_en, grade_ar = extract_grade(header + ' ' + body_clean[:500])
        # Lisan al-Mizan is an expansion of Mizan — default to weak
        grade_en, grade_ar = apply_book_default(grade_en, grade_ar, 'lisan_mizan')

        entries.append({
            'id': num,
            'name': name,
            'kunya': kunya,
            'grade_en': grade_en or 'unknown',
            'grade_ar': grade_ar or '',
            'color': GRADE_COLORS.get(grade_en or 'unknown', '#95a5a6'),
            'death': death,
            'source': 'lisan_mizan',
        })

    return entries


def parse_durar_kamina(text):
    """Al-Durar al-Kamina (Ibn Hajar, d.852) — 8th century scholars.
    Format: ### $ NUM - followed by name on next line.
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

    entry_re = re.compile(r'^### \$ (\d+)\s*-?\s*(.*)', re.MULTILINE)
    raw_entries = split_entries(full, entry_re)
    entries = []

    for match, body in raw_entries:
        num = int(match.group(1))
        header = match.group(2).strip()

        body_clean = re.sub(r'^#\s+', '', body, flags=re.MULTILINE)
        body_clean = re.sub(r'\s+', ' ', body_clean).strip()

        # Name is often in the body (header might be empty after the number)
        name_text = (header + ' ' + body_clean[:200]).strip()
        # Take up to common break words
        name_end = re.search(r'\s+(?:ولد|مات|توفي|سمع|كان|برع|ناب|ذكره|قال|روى|أخذ)', name_text)
        if name_end and name_end.start() > 3:
            name = name_text[:name_end.start()].strip()
        else:
            name_end2 = re.search(r'[.،]', name_text)
            name = name_text[:name_end2.start()].strip() if name_end2 else name_text[:80].strip()

        name = clean_name(name)

        # Death: Durar often has "مات سنة NNN" or "سنة NNN"
        death = extract_death_year(body_clean[:500])
        if not death:
            # Try: مات في المحرم سنة 774
            dm = re.search(r'(?:مات|توفي)\s+(?:في\s+)?(?:[\u0600-\u06FF]+\s+)?سنة\s+(\d+)', body_clean)
            if dm:
                death = dm.group(1) + ' هـ'

        kunya = extract_kunya(name)
        grade_en, grade_ar = extract_grade(body_clean[:400])

        entries.append({
            'id': num,
            'name': name,
            'kunya': kunya,
            'grade_en': grade_en or 'unknown',
            'grade_ar': grade_ar or '',
            'color': GRADE_COLORS.get(grade_en or 'unknown', '#95a5a6'),
            'death': death,
            'source': 'durar_kamina',
        })

    return entries


def parse_kashif(text):
    """Al-Kashif (al-Dhahabi, d.748) — condensed version of Tahdhib al-Kamal.
    Format: ### $ NUM - followed by name and brief bio.
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

    entry_re = re.compile(r'^### \$ (\d+)\s*-?\s*(.*)', re.MULTILINE)
    raw_entries = split_entries(full, entry_re)
    entries = []

    for match, body in raw_entries:
        num = int(match.group(1))
        header = match.group(2).strip()

        body_clean = re.sub(r'^#\s+', '', body, flags=re.MULTILINE)
        body_clean = re.sub(r'\s+', ' ', body_clean).strip()
        full_text = (header + ' ' + body_clean).strip()

        # Name: up to "عن" (narrated from) or comma
        name_end = re.search(r'[،,]\s*عن\s+|،', full_text)
        name = full_text[:name_end.start()].strip() if name_end and name_end.start() > 3 else full_text[:80].strip()
        name = clean_name(name)

        kunya = extract_kunya(name)

        # Death: Kashif often ends entries with "توفي NNN" or just a number + sigla
        death = ''
        dm = re.search(r'(?:توفي|مات)\s+(\d+)', full_text)
        if dm:
            death = dm.group(1) + ' هـ'
        else:
            death = extract_death_year(full_text)

        # Kashif is condensed — uses verb forms (وثق, ضعفه) not adjectives
        grade_en, grade_ar = extract_grade_condensed(full_text[:500])

        # Sigla at end (خ م د ت س ق ع)
        books = []
        sigla_m = re.search(r'[.]\s*([خمدتسقع](?:\s+[خمدتسقع])*)\s*[.#]?\s*$', full_text)
        if sigla_m:
            books = sigla_m.group(1).split()

        entries.append({
            'id': num,
            'name': name,
            'kunya': kunya,
            'grade_en': grade_en or 'unknown',
            'grade_ar': grade_ar or '',
            'color': GRADE_COLORS.get(grade_en or 'unknown', '#95a5a6'),
            'death': death,
            'books': books,
            'source': 'kashif',
        })

    return entries


def parse_dhahabi_generic(text, source_id, numbered=True, default_grade=None,
                          detect_companion_sections=False):
    """Generic parser for al-Dhahabi's shorter biographical works.
    Uses shared lexicon for name cleaning and grade extraction.

    Args:
        detect_companion_sections: If True, tracks section headers and grades
            entries before the tabi'in section as 'companion'. Used for
            mucin_tabaqat which has no inline grades — the book structure
            IS the classification.
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

    # Build section map for structural grading
    # Sections before "تابعين" / "الطبقة" are companion sections
    in_companion_section = True if detect_companion_sections else False
    section_boundaries = {}  # line_offset -> is_companion
    if detect_companion_sections:
        for m in re.finditer(r'^### \|+\s*(.+)', full, re.MULTILINE):
            sec_text = m.group(1)
            # Once we hit tabi'in or tabaqat markers, we're past companions
            if 'تابع' in sec_text or 'الطبقة' in sec_text or 'طبقة' in sec_text:
                section_boundaries[m.start()] = False
            elif 'صحاب' in sec_text or 'النساء' in sec_text:
                section_boundaries[m.start()] = True

    if numbered:
        entry_re = re.compile(r'^### \$ (\d+)\s*(.*)', re.MULTILINE)
    else:
        entry_re = re.compile(r'^### \$ ()(.*)', re.MULTILINE)

    raw_entries = split_entries(full, entry_re)
    entries = []

    for idx, (match, body) in enumerate(raw_entries):
        num = int(match.group(1)) if match.group(1) else idx
        header = match.group(2).strip()

        # Track section for structural grading
        if detect_companion_sections:
            entry_pos = match.start()
            for boundary_pos in sorted(section_boundaries.keys()):
                if boundary_pos < entry_pos:
                    in_companion_section = section_boundaries[boundary_pos]

        body_clean = re.sub(r'^#\s+', '', body, flags=re.MULTILINE)
        body_clean = re.sub(r'\s+', ' ', body_clean).strip()

        # Determine if header has the name or just sigla
        header_stripped = re.sub(r'^[خمدتسقعبرفه\s]+$', '', header).strip()

        if header_stripped:
            name_text = header_stripped
        else:
            name_text = body_clean[:200]

        # Clean using shared lexicon
        name = clean_narrator_name(name_text, has_sigla_prefix=False)

        if not name or len(name) < 3 or not re.search(r'[\u0600-\u06FF]', name):
            continue

        kunya = extract_kunya(name)
        full_text = header + ' ' + body_clean
        death = extract_death_year(full_text[:600])
        grade_en, grade_ar = extract_grade(full_text[:600])

        # Apply default grade for du'afa books
        if not grade_en and default_grade:
            grade_en = default_grade
            grade_ar = ''

        # Apply book-membership default
        grade_en, grade_ar = apply_book_default(grade_en, grade_ar, source_id)

        # Companion detection: inline markers
        if grade_en == 'unknown':
            if any(m in full_text[:500] for m in COMPANION_MARKERS):
                grade_en = 'companion'
                grade_ar = 'صحابي'

        # Companion detection: structural (section-based)
        if detect_companion_sections and in_companion_section and grade_en == 'unknown':
            grade_en = 'companion'
            grade_ar = 'قسم الصحابة'

        entries.append({
            'id': num,
            'name': name,
            'kunya': kunya,
            'grade_en': grade_en or 'unknown',
            'grade_ar': grade_ar or '',
            'color': GRADE_COLORS.get(grade_en or 'unknown', '#95a5a6'),
            'death': death,
            'source': source_id,
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
    'tabaqat': {
        'file': 'tabaqat_ibn_saad.txt',
        'parser': parse_tabaqat_ibn_saad,
        'title': "Tabaqat al-Kubra (Ibn Sa'd)",
    },
    'siyar': {
        'file': 'siyar.txt',
        'parser': parse_siyar,
        'title': "Siyar A'lam al-Nubala (al-Dhahabi)",
    },
    'isaba': {
        'file': 'isaba.txt',
        'parser': parse_isaba,
        'title': 'Al-Isaba fi Tamyiz al-Sahaba (Ibn Hajar)',
    },
    'tarikh_islam': {
        'file': 'tarikh_islam.txt',
        'parser': parse_tarikh_islam,
        'title': 'Tarikh al-Islam (al-Dhahabi)',
    },
    'lisan_mizan': {
        'file': 'lisan_mizan.txt',
        'parser': parse_lisan_mizan,
        'title': 'Lisan al-Mizan (Ibn Hajar)',
    },
    'durar_kamina': {
        'file': 'durar_kamina.txt',
        'parser': parse_durar_kamina,
        'title': 'Al-Durar al-Kamina (Ibn Hajar)',
    },
    'kashif': {
        'file': 'kashif.txt',
        'parser': parse_kashif,
        'title': 'Al-Kashif (al-Dhahabi)',
    },
    'tadhkirat_huffaz': {
        'file': 'tadhkirat_huffaz.txt',
        'parser': lambda text: parse_dhahabi_generic(text, 'tadhkirat_huffaz', numbered=True),
        'title': 'Tadhkirat al-Huffaz (al-Dhahabi)',
    },
    'mughni_ducafa': {
        'file': 'mughni_ducafa.txt',
        'parser': lambda text: parse_dhahabi_generic(text, 'mughni_ducafa', numbered=True, default_grade='weak'),
        'title': "Al-Mughni fi al-Du'afa (al-Dhahabi)",
    },
    'diwan_ducafa': {
        'file': 'diwan_ducafa.txt',
        'parser': lambda text: parse_dhahabi_generic(text, 'diwan_ducafa', numbered=True, default_grade='weak'),
        'title': "Diwan al-Du'afa (al-Dhahabi)",
    },
    'dhayl_diwan': {
        'file': 'dhayl_diwan.txt',
        'parser': lambda text: parse_dhahabi_generic(text, 'dhayl_diwan', numbered=True, default_grade='weak'),
        'title': "Dhayl Diwan al-Du'afa (al-Dhahabi)",
    },
    'mucjam_shuyukh': {
        'file': 'mucjam_shuyukh.txt',
        'parser': lambda text: parse_dhahabi_generic(text, 'mucjam_shuyukh', numbered=False),
        'title': "Mu'jam al-Shuyukh (al-Dhahabi)",
    },
    'macrifa_qurra': {
        'file': 'macrifa_qurra.txt',
        'parser': lambda text: parse_dhahabi_generic(text, 'macrifa_qurra', numbered=True),
        'title': "Ma'rifat al-Qurra al-Kibar (al-Dhahabi)",
    },
    'mucin_tabaqat': {
        'file': 'mucin_tabaqat.txt',
        'parser': lambda text: parse_dhahabi_generic(text, 'mucin_tabaqat', numbered=True, detect_companion_sections=True),
        'title': "Al-Mu'in fi Tabaqat al-Muhaddithin (al-Dhahabi)",
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

    # ── Universal post-processing gate ──────────────────────────────
    # Every name passes through clean_narrator_name() regardless of
    # what the individual parser did. This catches transmission chains,
    # reference numbers, verb leakage, and other artifacts that
    # per-parser cleaning may have missed.
    cleaned = 0
    dropped = 0
    for e in entries:
        raw = e.get('name', '')
        fixed = clean_narrator_name(raw, has_sigla_prefix=False)
        # Also strip lisan_mizan reference patterns: (1: 5/ 1)
        fixed = re.sub(r'\(\d+:\s*\d+[^)]*\)', '', fixed).strip()
        fixed = re.sub(r'\(\d+\s*/\s*\d+[^)]*\)', '', fixed).strip()
        # Strip leading sigla + comma: "خت4 ," or "د س ,"
        fixed = re.sub(r'^[خمدتسقعبرفهصنل\d\s]+[,،]\s*', '', fixed).strip()
        # Strip leading (ه) or (ز) editor markers
        fixed = re.sub(r'^\([هزعص]\)\s*', '', fixed).strip()
        if fixed != raw:
            cleaned += 1
        e['name'] = fixed
    # Drop entries with empty/invalid names
    before = len(entries)
    entries = [e for e in entries if e.get('name', '').strip() and len(e['name']) >= 2]
    dropped = before - len(entries)
    if cleaned or dropped:
        print(f"    -> post-clean: {cleaned} names fixed, {dropped} dropped")

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
