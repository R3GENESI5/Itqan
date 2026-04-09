"""
Hadith Enrichment Pipeline
===========================
Generates all derived JSON data files for the Hadith app.

Steps:
  A. lexicon      — download Lane's Lexicon roots → roots_lexicon.json
  B. stemmer      — build Arabic light stemmer + rebuild word_defs.json
  C. freq         — word frequency per book → word_freq.json
  D. narrators    — extract narrator index → narrator_index.json
  E. connections  — hadith-bil-hadith (matn-only, tiered) → hadith_connections.json
  F. concordance  — word-level inverted index → concordance.json
  G. wensinck     — root-organised Mu'jam al-Mufahras → wensinck.json

Usage:
    python src/enrich_data.py                        # run all
    python src/enrich_data.py --step lexicon
    python src/enrich_data.py --step stemmer
    python src/enrich_data.py --step narrators
    python src/enrich_data.py --step connections
    python src/enrich_data.py --step wensinck
"""

import argparse
import json
import re
from collections import Counter, defaultdict
from pathlib import Path

import requests

ROOT       = Path(__file__).parent.parent
DATA       = ROOT / "app" / "data"
DATA_SUNNI = DATA / "sunni"
DATA_SHIA  = DATA / "shia"

# ── Arabic helpers ────────────────────────────────────────────────────────────

DIACRITICS = re.compile(
    r'[\u0610-\u061A\u064B-\u065F\u0670\u06D6-\u06DC\u06DF-\u06E4\u06E7\u06E8\u06EA-\u06ED]')

def strip_diacritics(t: str) -> str:
    return DIACRITICS.sub('', t)

def normalize(w: str) -> str:
    """Strip diacritics + normalize alef/teh variants."""
    w = strip_diacritics(w)
    w = w.replace('أ','ا').replace('إ','ا').replace('آ','ا')
    w = w.replace('ة','ه').replace('ى','ي')
    return w

# Arabic prefixes and suffixes for light stemmer
PREFIXES = ['وال','بال','فال','كال','لل','وال','ال','و','ف','ب','ك','ل','س']
SUFFIXES = ['ونه','وها','وهم','تهم','تها','كم','هم','ها','ون','ين','ات','ان',
            'ته','تي','ني','نا','كن','هن','وا','ة','ه','ي','ت','ن','ا']

def light_stem(word: str) -> str:
    """Strip common Arabic prefixes and suffixes to get approximate root/stem."""
    w = normalize(word)
    if len(w) <= 3:
        return w
    # Strip prefixes (longest first)
    for p in sorted(PREFIXES, key=len, reverse=True):
        if w.startswith(p) and len(w) - len(p) >= 2:
            w = w[len(p):]
            break
    if len(w) <= 3:
        return w
    # Strip suffixes (longest first)
    for s in sorted(SUFFIXES, key=len, reverse=True):
        if w.endswith(s) and len(w) - len(s) >= 2:
            w = w[:-len(s)]
            break
    return w

# Isnad transmission verbs (matn starts after these)
ISNAD_VERBS = {'حدثنا','حدثني','أخبرنا','أخبرني','أنبأنا','أنبأني',
               'روى','رواه','ذكر','سمعت','سمعنا'}

# Chain connectors
CHAIN_WORDS = {'عن','من','إلى','بن','أبي','أبو','بنت','ابن','ابنة','أم'}

# Ruling pattern markers (Tier 2 — legal structure)
RULING_PATTERNS = {
    'prohibition': [r'نَهَى', r'نهى', r'لا يحل', r'حرام', r'لا يجوز', r'حُرِّمَ'],
    'obligation':  [r'أُمِرْ', r'أمر', r'فرض', r'وجب', r'يجب', r'افترض'],
    'permission':  [r'أحل', r'أباح', r'رخص', r'لا بأس', r'جائز'],
    'reward':      [r'من فعل', r'من قال', r'من صلى', r'له أجر', r'غفر'],
    'fitra':       [r'من الفطرة', r'السنة', r'سنة النبي'],
    'warning':     [r'ويل', r'عذاب', r'لعن', r'لعنة', r'يلعن'],
    'prophecy':    [r'يأتي زمان', r'سيكون', r'يكون', r'من أشراط', r'آخر الزمان'],
}

# Circumstance markers (Tier 3 — occasion/setting)
CIRCUMSTANCE_MARKERS = {
    'time':    ['رمضان','الجمعة','العيد','الحج','ليلة القدر','السحر','الفجر',
                'الظهر','العصر','المغرب','العشاء','يوم القيامة'],
    'place':   ['المسجد','الكعبة','المدينة','مكة','الحرم','السفر','الغزو',
                'البيت','المنبر','الطريق'],
    'trigger': ['جاء رجل','سئل النبي','سألت','قيل يا رسول','فسألته',
                'فقال رجل','جاءت امرأة'],
}

# Fiqh topic vocabulary — words that belong to specific Islamic topics
FIQH_TOPICS = {
    'prayer':    ['صلاة','صلى','ركوع','سجود','قيام','وضوء','طهارة','أذان','إمام',
                  'مسجد','قبلة','ركعة','تشهد','فاتحة'],
    'fasting':   ['صوم','صام','رمضان','إفطار','سحور','اعتكاف','صيام'],
    'zakat':     ['زكاة','صدقة','عشر','نصاب','فقير','مسكين'],
    'hajj':      ['حج','عمرة','إحرام','طواف','سعي','كعبة','منى','عرفة','مزدلفة'],
    'family':    ['نكاح','زواج','طلاق','مهر','نفقة','رضاع','حضانة','ولادة',
                  'زوج','زوجة','أرملة'],
    'trade':     ['بيع','شراء','ربا','دين','قرض','رهن','إجارة','تجارة'],
    'character': ['صدق','كذب','أمانة','خيانة','غيبة','نميمة','حسد','كبر',
                  'تواضع','رحمة','عدل','ظلم'],
    'jihad':     ['جهاد','غزو','قتال','شهيد','فتح','سلاح','سرية'],
    'food':      ['حلال','حرام','ذبح','صيد','خمر','خنزير','ميتة','دم'],
    'death':     ['موت','قبر','جنازة','دفن','صلاة الجنازة','بكاء','وصية'],
    'eschatology':['قيامة','حساب','ميزان','جنة','نار','صراط','حوض','شفاعة'],
    'knowledge': ['علم','تعلم','فقه','قرآن','حديث','رواية','إجازة'],
}

# Build reverse topic map: word → topic
WORD_TO_TOPIC: dict[str,str] = {}
for _topic, _words in FIQH_TOPICS.items():
    for _w in _words:
        WORD_TO_TOPIC[normalize(_w)] = _topic

# Narrator name normalization map (Arabic → canonical English)
NARRATOR_AR_TO_EN = {
    'ابوهريره':'Abu Hurairah', 'ابوهريرة':'Abu Hurairah',
    'عائشه':'Aisha', 'عايشه':'Aisha', 'عائشة':'Aisha',
    'ابنعباس':'Ibn Abbas', 'عبدالله بن عباس':'Ibn Abbas',
    'انس':'Anas bin Malik', 'انس بن مالك':'Anas bin Malik',
    'عبدالله بن عمر':'Ibn Umar', 'ابن عمر':'Ibn Umar',
    'ابوسعيد':'Abu Said al-Khudri', 'ابوسعيد الخدري':'Abu Said al-Khudri',
    'جابر':'Jabir bin Abdullah',
    'ابن مسعود':'Ibn Masud', 'عبدالله بن مسعود':'Ibn Masud',
    'علي':'Ali bin Abi Talib', 'علي بن ابي طالب':'Ali bin Abi Talib',
    'عمر':'Umar bin al-Khattab', 'عمر بن الخطاب':'Umar bin al-Khattab',
    'ابوبكر':'Abu Bakr', 'ابو بكر الصديق':'Abu Bakr',
    'عثمان':'Uthman bin Affan',
    'ابوموسي':'Abu Musa al-Ashari', 'ابوموسى':'Abu Musa al-Ashari',
    'معاذ':'Muadh bin Jabal',
    'سهل بن سعد':'Sahl bin Sad',
    'ابوذر':'Abu Dharr',
    'سلمان':'Salman al-Farisi',
    'بريده':'Buraydah',
    'ام سلمه':'Umm Salama', 'ام سلمة':'Umm Salama',
    'زينب':'Zaynab',
    'حفصه':'Hafsa', 'حفصة':'Hafsa',
    'ام حبيبه':'Umm Habiba',
    'صفيه':'Safiyyah', 'صفية':'Safiyyah',
    'ميمونه':'Maymunah', 'ميمونة':'Maymunah',
}

# Common stop words for matn analysis (isnad verbs + function words)
STOP_WORDS = {
    *ISNAD_VERBS, *CHAIN_WORDS,
    'من','إلى','في','على','عن','مع','قال','قالت','أن','إن','لا','ما','هو',
    'هي','هم','هن','أنا','نحن','أنت','أنتم','كان','كانت','كانوا','يكون',
    'ذلك','هذا','هذه','تلك','الذي','التي','الذين','وقد','قد','لم','لن',
    'لقد','كل','بعض','عليه','عليها','عليهم','له','لها','لهم','به','بها',
    'بهم','منه','منها','منهم','فيه','فيها','وسلم','صلى','الله','رسول',
    'النبي','عنه','عنها','رضي','ال','و','ف','ب','ك','ل','إله','إلا',
    'يا','أو','أم','بل','ثم','حتى','إذا','لما','لو','لولا','كذلك',
    'أيضا','إنما','إنه','وإن','فإن','وكان','فقال','وقال','قلت','قلنا',
}

# ── File helpers ──────────────────────────────────────────────────────────────

def write_json(path: Path, data, compact=True):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        if compact:
            json.dump(data, f, ensure_ascii=False, separators=(',',':'))
        else:
            json.dump(data, f, ensure_ascii=False, indent=2)

def read_json(path: Path):
    with open(path, encoding='utf-8') as f:
        return json.load(f)

def iter_all_hadiths(collection='sunni'):
    base = DATA_SUNNI if collection == 'sunni' else DATA_SHIA
    for book_dir in sorted(base.iterdir()):
        if not book_dir.is_dir():
            continue
        idx_path = book_dir / 'index.json'
        if not idx_path.exists():
            continue
        chapters = read_json(idx_path)
        for ch_idx, ch in enumerate(chapters):
            ch_file = book_dir / ch['file']
            if not ch_file.exists():
                continue
            hadiths = read_json(ch_file)
            for h in hadiths:
                yield book_dir.name, ch_idx, h

# ── Isnad / Matn splitter ─────────────────────────────────────────────────────

# Patterns that signal end of isnad / start of matn
MATN_START_PATTERNS = [
    # "that the Prophet said" / "that the Messenger of Allah said"
    re.compile(r'أَنَّ\s+(?:النَّبِيَّ|رَسُولَ اللَّهِ|النبي|رسول الله)\s+(?:صَلَّى|صلى)[^،،]*?قَالَ[:\s]'),
    re.compile(r'أن\s+(?:النبي|رسول الله)\s+قال[:\s]'),
    # "he said: I heard the Messenger say"
    re.compile(r'سَمِعْتُ\s+(?:النَّبِيَّ|رَسُولَ اللَّهِ|النبي|رسول الله)'),
    re.compile(r'سمعت\s+(?:النبي|رسول الله)'),
    # "the Prophet said" after a chain
    re.compile(r'قَالَ\s+(?:النَّبِيُّ|رَسُولُ اللَّهِ|النبي|رسول الله)'),
]

def split_isnad_matn(arabic: str) -> tuple[str, str]:
    """
    Split Arabic hadith text into (isnad, matn).
    Returns (full_text, '') if split not found.
    """
    if not arabic:
        return '', ''

    clean = strip_diacritics(arabic)

    # Try pattern-based split first
    for pat in MATN_START_PATTERNS:
        m = pat.search(arabic)
        if m:
            # matn starts at the quoted speech after the match
            rest = arabic[m.end():]
            # strip leading quotes / punctuation
            rest = rest.lstrip('«»"\'‹›:،؛ ')
            return arabic[:m.start()], rest

    # Fallback: find last قال + colon/quote and split there
    # This handles nested chains like "X said: Y said: Z said: [matn]"
    last_qal = -1
    for m in re.finditer(r'قال[:\s]', clean):
        last_qal = m.end()

    if last_qal > 0 and last_qal < len(arabic) * 0.8:
        return arabic[:last_qal], arabic[last_qal:].lstrip(' :')

    # Can't split — treat whole text as matn
    return '', arabic


def extract_narrator_name(english_narrator: str) -> str:
    """Extract the primary narrator name from English narrator string."""
    if not english_narrator:
        return ''
    # "Narrated X:" or "X reported" patterns
    m = re.match(r"Narrated\s+([^:]+):", english_narrator)
    if m:
        return m.group(1).strip()
    m = re.match(r"([^:]+)\s+reported", english_narrator, re.IGNORECASE)
    if m:
        return m.group(1).strip()
    # Just return first part up to colon
    return english_narrator.split(':')[0].strip()


def tokenize_matn(matn: str) -> list[str]:
    """Tokenize matn text, strip diacritics, filter stop words."""
    clean = strip_diacritics(matn)
    # Use Arabic letters only (U+0621-U+063A, U+0641-U+064A) — excludes
    # punctuation (،؟؛ U+060C/061B/061F), tatweel (ـ U+0640), and digits.
    words = re.findall(r'[\u0621-\u063A\u0641-\u064A]+', clean)
    return [normalize(w) for w in words
            if len(w) >= 3 and normalize(w) not in STOP_WORDS]


def get_ruling_patterns(text: str) -> set[str]:
    """Identify which ruling pattern types appear in this text."""
    found = set()
    for ptype, patterns in RULING_PATTERNS.items():
        for pat in patterns:
            if re.search(pat, text):
                found.add(ptype)
    return found


def get_circumstances(text: str) -> dict[str, list[str]]:
    """Find time/place/trigger circumstance markers in text."""
    found = defaultdict(list)
    norm_text = normalize(strip_diacritics(text))
    for ctype, markers in CIRCUMSTANCE_MARKERS.items():
        for marker in markers:
            if normalize(marker) in norm_text:
                found[ctype].append(marker)
    return dict(found)


def get_topics(matn_tokens: list[str]) -> list[str]:
    """Map matn tokens to fiqh topics."""
    topics = set()
    for tok in matn_tokens:
        if tok in WORD_TO_TOPIC:
            topics.add(WORD_TO_TOPIC[tok])
        # Also try light stem
        stem = light_stem(tok)
        if stem in WORD_TO_TOPIC:
            topics.add(WORD_TO_TOPIC[stem])
    return sorted(topics)


# ── Step A: Roots Lexicon ─────────────────────────────────────────────────────

def build_roots_lexicon():
    print('\n── Step A: Building roots lexicon ──────────────────')
    url = 'https://raw.githubusercontent.com/aliozdenisik/quran-arabic-roots-lane-lexicon/main/quran_arabic_roots_lane_lexicon_2026-02-12.json'
    print('  Fetching Lane\'s Lexicon roots…')
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    raw = r.json()

    lexicon = {}
    for entry in raw.get('roots', []):
        root_ar = entry.get('root', '').strip()
        if not root_ar:
            continue
        lexicon[root_ar] = {
            'root':          root_ar,
            'buckwalter':    entry.get('root_buckwalter', ''),
            'definition_en': (entry.get('definition_en') or '').strip()[:800],
            'summary_en':    (entry.get('summary_en') or entry.get('summary_tr') or '').strip()[:200],
            'quran_freq':    entry.get('quran_frequency') or entry.get('frequency') or 0,
        }

    write_json(DATA / 'roots_lexicon.json', lexicon)
    print(f'  ✓ {len(lexicon)} roots → roots_lexicon.json')
    return lexicon


# ── Step B: Light stemmer + word_defs ─────────────────────────────────────────

def build_word_defs(lexicon: dict = None):
    """
    Build word_defs.json with improved coverage using light stemmer.
    Maps every significant Arabic word surface/stem to its root definition.

    Output: app/data/word_defs.json
    { "norm_word": { "r": root_ar, "s": buckwalter, "g": gloss, "d": definition, "n": freq } }
    """
    print('\n── Step B: Building word_defs with light stemmer ────')

    if lexicon is None:
        lex_path = DATA / 'roots_lexicon.json'
        if not lex_path.exists():
            print('  ✗ roots_lexicon.json not found — run --step lexicon first')
            return {}
        lexicon = read_json(lex_path)

    # Build stem → root mapping from lexicon
    # Both exact root match and normalized root match
    root_index: dict[str, tuple] = {}  # stem → (root_ar, entry)
    for root_ar, entry in lexicon.items():
        norm_root = normalize(root_ar)
        root_index[norm_root] = (root_ar, entry)
        # Also index buckwalter-derived stem variants
        bw = entry.get('buckwalter', '')
        if bw:
            root_index[bw.lower()] = (root_ar, entry)

    def find_root(word: str):
        """Try to find a root entry for a word using progressive stemming."""
        norm = normalize(word)

        # 1. Exact match
        if norm in root_index:
            return root_index[norm]

        # 2. Light stem match
        stem = light_stem(norm)
        if stem in root_index:
            return root_index[stem]

        # 3. Try stripping just the definite article
        no_al = norm[2:] if norm.startswith('ال') else norm
        if no_al in root_index:
            return root_index[no_al]
        stem2 = light_stem(no_al)
        if stem2 in root_index:
            return root_index[stem2]

        # 4. Final-letter weak-root variants:
        #    Many Arabic roots end in و or ي but the normalized form ends in ا or ه
        #    e.g. زكاه (زكاة) → root زكو; صلاه (صلاة) → root صلو
        #    Also: masdar forms end in ء (حياء→حيي, دعاء→دعو, سماء→سمو)
        for base in [stem2, stem, no_al]:
            if not base:
                continue
            # ء at end: masdar form — strip it and re-stem (حياء→حيا→حي→حيي)
            if base.endswith('ء'):
                no_hamza = base[:-1]
                if no_hamza in root_index: return root_index[no_hamza]
                stemmed_nh = light_stem(no_hamza)
                if stemmed_nh in root_index: return root_index[stemmed_nh]
                # also try weak-root variants of the de-hamzated form
                if no_hamza.endswith('ا'):
                    for sfx in ['و', 'ي']:
                        alt = no_hamza[:-1] + sfx
                        if alt in root_index: return root_index[alt]
                if len(stemmed_nh) == 2:          # حيا → حي → حيي
                    gem = stemmed_nh + stemmed_nh[-1]
                    if gem in root_index: return root_index[gem]
            # ا → و (e.g. زكا → زكو)
            if base.endswith('ا'):
                alt = base[:-1] + 'و'
                if alt in root_index: return root_index[alt]
            # ه → و (e.g. زكاه → زكاو → زكو via 3-char)
            if base.endswith('ه'):
                alt_w = base[:-1] + 'و'
                alt_y = base[:-1] + 'ي'
                if alt_w in root_index: return root_index[alt_w]
                if alt_y in root_index: return root_index[alt_y]
                # try 3-char stem without last ه
                trimmed = base[:-1]
                if trimmed in root_index: return root_index[trimmed]
            # ي → و
            if base.endswith('ي'):
                alt = base[:-1] + 'و'
                if alt in root_index: return root_index[alt]

        # 5. Geminated root: 2-char stem → try tripling last letter (حج→حجج, مد→مدد)
        for base in [stem2, stem, no_al]:
            if len(base) == 2:
                gem = base + base[-1]
                if gem in root_index: return root_index[gem]

        # Step 6 (substring match) removed — it caused false positives for
        # proper names (محمد→حمد, اللهم→لهم, الحديث→لحد).
        return None

    # Build corpus word frequency
    print('  Pass 1: collecting corpus word frequency…')
    corpus_freq: Counter = Counter()
    for book_id, ch_idx, h in iter_all_hadiths('sunni'):
        arabic = h.get('arabic', '')
        if not arabic:
            continue
        _, matn = split_isnad_matn(arabic)
        tokens = tokenize_matn(matn or arabic)
        for tok in tokens:
            corpus_freq[tok] += 1

    print(f'  {len(corpus_freq):,} unique word forms found')

    # Build word_defs: only words with a found root
    print('  Pass 2: matching words to roots…')
    word_defs = {}
    matched = 0
    for word, freq in corpus_freq.items():
        result = find_root(word)
        if result:
            root_ar, entry = result
            gloss = (entry.get('summary_en') or '').strip()[:120]
            defn  = (entry.get('definition_en') or '').strip()[:600]
            if gloss:
                word_defs[word] = {
                    'r': root_ar,
                    's': entry.get('buckwalter', ''),
                    'g': gloss,
                    'n': freq,
                }  # 'd' (full definition) omitted — loaded lazily from roots_lexicon.json
                matched += 1

    # Manual overrides for forms the light stemmer can't reach:
    # (a) Highly irregular roots  (b) Masdar patterns with internal vowels
    MANUAL = {
        # Water — root موه
        'ماء':    ('موه', 'mwh'),
        'مياه':   ('موه', 'mwh'),
        # Faith/belief — root أمن
        'ايمان':  ('أمن', 'Amn'),
        'الايمان':('أمن', 'Amn'),
        'ايمانه': ('أمن', 'Amn'),
        # Jihad — root جهد
        'جهاد':   ('جهد', 'jhd'),
        'الجهاد': ('جهد', 'jhd'),
        # Marriage — root نكح
        'نكاح':   ('نكح', 'nkH'),
        'النكاح': ('نكح', 'nkH'),
        # Divorce — root طلق
        'طلاق':   ('طلق', 'Tlq'),
        'الطلاق': ('طلق', 'Tlq'),
        # Pilgrimage — root حجج
        'حج':     ('حجج', 'Hjj'),
        'الحج':   ('حجج', 'Hjj'),
        'حجه':    ('حجج', 'Hjj'),
        # Fasting — root صوم (صوم already matched, but include صيام masdar)
        'صيام':   ('صوم', 'Swm'),
        'الصيام': ('صوم', 'Swm'),
        # Zakat — root زكو (زكاه already matched, include variants)
        'زكات':   ('زكو', 'zkw'),
        'الزكات': ('زكو', 'zkw'),
        # Prayer — root صلو (صلاه already matched, include variants)
        'صلاة':   ('صلو', 'Slw'),
        # Qur'an — root قرأ
        'قران':   ('قرأ', 'qrA'),
        'القران': ('قرأ', 'qrA'),
        # Prophet — root نبأ (Lane's: one who receives Divine tidings)
        'نبي':    ('نبأ', 'nbA'),
        'النبي':  ('نبأ', 'nbA'),
        'نبيه':   ('نبأ', 'nbA'),
    }
    for form, (root_key, bw_key) in MANUAL.items():
        norm_form = normalize(form)
        norm_root = normalize(root_key)        # root_index keys are also normalized
        lookup_key = norm_root if norm_root in root_index else root_key
        if lookup_key in root_index:
            _, entry = root_index[lookup_key]
            gloss = (entry.get('summary_en') or '').strip()[:120]
            if gloss:
                word_defs[norm_form] = {
                    'r': root_key,             # store original Arabic for display
                    's': bw_key,
                    'g': gloss,
                    'n': corpus_freq.get(norm_form, 1),
                }

    total = len(corpus_freq)
    print(f'  ✓ {matched:,}/{total:,} words matched ({matched*100//total}% coverage)')

    write_json(DATA / 'word_defs.json', word_defs)
    kb = (DATA / 'word_defs.json').stat().st_size // 1024
    print(f'  ✓ word_defs.json: {kb} KB, {len(word_defs):,} entries')
    return word_defs


# ── Step C: Word frequency ────────────────────────────────────────────────────

def build_word_frequency():
    print('\n── Step C: Building word frequency ─────────────────')
    book_counts: dict[str, Counter] = defaultdict(Counter)
    book_raw: dict[str, dict] = defaultdict(dict)
    total = 0

    for book_id, ch_idx, h in iter_all_hadiths('sunni'):
        arabic = h.get('arabic', '')
        if not arabic:
            continue
        _, matn = split_isnad_matn(arabic)
        tokens = tokenize_matn(matn or arabic)
        for tok in tokens:
            book_counts[book_id][tok] += 1
            if tok not in book_raw[book_id]:
                book_raw[book_id][tok] = strip_diacritics(tok)
        total += 1

    result = {}
    for book_id, counter in book_counts.items():
        top = counter.most_common(500)
        result[book_id] = [
            {'word': book_raw[book_id].get(n, n), 'norm': n, 'count': c}
            for n, c in top
        ]

    write_json(DATA / 'word_freq.json', result)
    print(f'  ✓ Word frequency for {len(result)} books ({total:,} hadiths)')
    return result


# ── Step D: Narrator index ────────────────────────────────────────────────────

def build_narrator_index():
    """
    Build narrator_index.json — full profile for each narrator.

    Output structure:
    {
      "Abu Hurairah": {
        "hadith_ids": ["bukhari:1", "muslim:5", ...],
        "total": 5374,
        "books": {"bukhari": 450, "muslim": 600, ...},
        "topics": {"prayer": 31, "character": 22, ...},   // % per topic
        "grade_profile": {"sahih": 71, "hasan": 18, "daif": 11},
        "unique_hadiths": ["bukhari:45", ...],             // only in one book
        "sample_hadith": "bukhari:1"
      }, ...
    }
    """
    print('\n── Step D: Building narrator index ─────────────────')

    # Load all grades
    grade_data: dict[str, str] = {}
    for book_dir in sorted(DATA_SUNNI.iterdir()):
        if not book_dir.is_dir():
            continue
        g_path = book_dir / 'grades.json'
        if g_path.exists():
            grades = read_json(g_path)
            for id_in_book, grade in grades.items():
                grade_data[f'{book_dir.name}:{id_in_book}'] = grade

    narrators: dict[str, dict] = defaultdict(lambda: {
        'hadith_ids': [],
        'total': 0,
        'books': Counter(),
        'topic_counts': Counter(),
        'grade_counts': Counter(),
    })

    # Also track: for each hadith_id, which narrators transmitted it
    hadith_narrators: dict[str, set] = defaultdict(set)

    total_processed = 0
    for book_id, ch_idx, h in iter_all_hadiths('sunni'):
        narrator_en = extract_narrator_name(h.get('english', {}).get('narrator', ''))
        if not narrator_en:
            continue

        h_id = f"{book_id}:{h.get('idInBook','')}"
        arabic = h.get('arabic', '')
        _, matn = split_isnad_matn(arabic)
        matn_tokens = tokenize_matn(matn or arabic)
        topics = get_topics(matn_tokens)

        grade_str = grade_data.get(h_id, h.get('grade', ''))
        grade_class = _grade_class(grade_str)

        nd = narrators[narrator_en]
        nd['hadith_ids'].append(h_id)
        nd['total'] += 1
        nd['books'][book_id] += 1
        for topic in topics:
            nd['topic_counts'][topic] += 1
        if grade_class:
            nd['grade_counts'][grade_class] += 1

        hadith_narrators[h_id].add(narrator_en)
        total_processed += 1

    # Build uniqueness: hadiths only transmitted through one narrator
    hadith_book_count: dict[str, int] = defaultdict(int)
    for book_id, ch_idx, h in iter_all_hadiths('sunni'):
        # count across books using idInBook as proxy for hadith identity via narrator
        pass  # we'll use book distribution: if a narrator has hadiths in only 1 book, those are "unique"

    # Finalize narrator profiles
    result = {}
    for name, nd in narrators.items():
        if nd['total'] < 3:  # skip very minor narrators
            continue

        total = nd['total']

        # Topic percentages
        total_topic_hits = sum(nd['topic_counts'].values()) or 1
        topics_pct = {
            t: round(c * 100 / total_topic_hits)
            for t, c in nd['topic_counts'].most_common(8)
            if c * 100 // total_topic_hits >= 2
        }

        # Grade percentages
        total_graded = sum(nd['grade_counts'].values()) or None
        grade_pct = {}
        if total_graded:
            grade_pct = {
                g: round(c * 100 / total_graded)
                for g, c in nd['grade_counts'].items()
            }

        # Unique hadiths (only in 1 book)
        book_hadith_sets: dict[str, list] = defaultdict(list)
        for h_id in nd['hadith_ids']:
            book = h_id.split(':')[0]
            book_hadith_sets[book].append(h_id)

        # Hadiths unique to one book (not cross-verified)
        unique = []
        for h_id in nd['hadith_ids']:
            if len(hadith_narrators.get(h_id, set())) == 1:
                unique.append(h_id)

        result[name] = {
            'total':         total,
            'books':         dict(nd['books'].most_common()),
            'topics':        topics_pct,
            'grade_profile': grade_pct,
            'unique_count':  len(unique),
            'unique_sample': unique[:5],
            'sample_hadith': nd['hadith_ids'][0] if nd['hadith_ids'] else '',
            'hadith_ids':    nd['hadith_ids'][:2000],  # cap at 2k for file size
        }

    write_json(DATA / 'narrator_index.json', result)
    kb = (DATA / 'narrator_index.json').stat().st_size // 1024
    print(f'  ✓ {len(result)} narrators → narrator_index.json ({kb} KB)')
    print(f'  Top 5: {", ".join(list(result.keys())[:5])}')
    return result


def _grade_class(grade: str) -> str:
    if not grade:
        return ''
    g = grade.lower()
    if 'sahih' in g:   return 'sahih'
    if 'hasan' in g:   return 'hasan'
    if 'da\'if' in g or 'daif' in g or 'weak' in g: return 'daif'
    return ''


# ── Step E: Hadith connections (tiered) ──────────────────────────────────────

def build_hadith_connections():
    """
    Build tiered hadith-bil-hadith connections.

    Three tiers per connection:
      t1 = shared rare matn keywords (TF-IDF style, matn-only)
      t2 = shared ruling patterns (prohibition/obligation/permission/etc.)
      t3 = shared circumstances (time/place/trigger)

    Connection score = t1_shared_words * 3 + t2_match * 2 + t3_match * 1
    Only cross-book connections kept. Min score 4.

    Output: app/data/hadith_connections.json
    {
      "bukhari:1": [
        {"id": "muslim:15", "t1": ["صلاة","طهارة"], "t2": ["obligation"], "t3": ["prayer"]},
        ...
      ]
    }
    """
    print('\n── Step E: Building tiered hadith connections ───────')

    # Pass 1: compute document frequency of matn words
    print('  Pass 1: matn word document frequency…')
    doc_freq: Counter = Counter()
    total_hadiths = 0
    for book_id, ch_idx, h in iter_all_hadiths('sunni'):
        arabic = h.get('arabic', '')
        _, matn = split_isnad_matn(arabic)
        tokens = set(tokenize_matn(matn or arabic))
        for t in tokens:
            doc_freq[t] += 1
        total_hadiths += 1

    # Significant: 3–150 hadiths (rare enough to be discriminating)
    sig_words = {w for w, c in doc_freq.items() if 3 <= c <= 150}
    print(f'  {len(sig_words):,} significant matn words from {total_hadiths:,} hadiths')

    # Pass 2: build per-hadith feature sets
    print('  Pass 2: extracting features per hadith…')
    hadith_features: dict[str, dict] = {}

    for book_id, ch_idx, h in iter_all_hadiths('sunni'):
        arabic = h.get('arabic', '')
        h_id = f"{book_id}:{h.get('idInBook','')}"

        isnad, matn = split_isnad_matn(arabic)
        text_for_analysis = matn or arabic

        matn_tokens = set(tokenize_matn(text_for_analysis))
        sig = matn_tokens & sig_words

        ruling = get_ruling_patterns(text_for_analysis)
        circs  = get_circumstances(text_for_analysis)
        topics = get_topics(list(matn_tokens))

        hadith_features[h_id] = {
            'book':    book_id,
            'sig':     sig,
            'ruling':  ruling,
            'circs':   circs,
            'topics':  set(topics),
        }

    # Build inverted index on sig words
    inverted: dict[str, list[str]] = defaultdict(list)
    for h_id, feat in hadith_features.items():
        for kw in feat['sig']:
            inverted[kw].append(h_id)

    # Also inverted on ruling patterns
    ruling_inv: dict[str, list[str]] = defaultdict(list)
    for h_id, feat in hadith_features.items():
        for r in feat['ruling']:
            ruling_inv[r].append(h_id)

    # Also inverted on topics
    topic_inv: dict[str, list[str]] = defaultdict(list)
    for h_id, feat in hadith_features.items():
        for t in feat['topics']:
            topic_inv[t].append(h_id)

    # Pass 3: compute connections
    print('  Pass 3: computing connections…')
    connections: dict[str, list] = {}
    processed = 0

    for h_id, feat in hadith_features.items():
        my_book = feat['book']
        candidates: dict[str, dict] = defaultdict(lambda: {
            't1': [], 't2': set(), 't3': set(), 'score': 0
        })

        # T1: shared sig matn words
        for kw in feat['sig']:
            for other_id in inverted.get(kw, []):
                if other_id != h_id and hadith_features[other_id]['book'] != my_book:
                    candidates[other_id]['t1'].append(kw)
                    candidates[other_id]['score'] += 3

        # T2: shared ruling patterns
        for r in feat['ruling']:
            for other_id in ruling_inv.get(r, []):
                if other_id != h_id and hadith_features[other_id]['book'] != my_book:
                    if r not in candidates[other_id]['t2']:
                        candidates[other_id]['t2'].add(r)
                        candidates[other_id]['score'] += 2

        # T3: shared topics (as circumstance proxy)
        for t in feat['topics']:
            for other_id in topic_inv.get(t, []):
                if other_id != h_id and hadith_features[other_id]['book'] != my_book:
                    if t not in candidates[other_id]['t3']:
                        candidates[other_id]['t3'].add(t)
                        candidates[other_id]['score'] += 1

        # Filter: must have score >= 4 AND at least 1 T1 word OR (T2 + T3)
        valid = [
            (cid, cdata) for cid, cdata in candidates.items()
            if cdata['score'] >= 4 and (len(cdata['t1']) >= 1 or (cdata['t2'] and cdata['t3']))
        ]

        # Sort by score desc, take top 10
        top = sorted(valid, key=lambda x: x[1]['score'], reverse=True)[:10]

        if top:
            connections[h_id] = [
                {
                    'id': cid,
                    't1': sorted(cdata['t1'])[:5],       # top shared matn words
                    't2': sorted(cdata['t2']),             # ruling pattern types
                    't3': sorted(cdata['t3']),             # topic/circumstance types
                    'score': cdata['score'],
                }
                for cid, cdata in top
            ]

        processed += 1
        if processed % 5000 == 0:
            print(f'    {processed:,}/{total_hadiths:,} hadiths processed…')

    write_json(DATA / 'hadith_connections.json', connections)
    kb = (DATA / 'hadith_connections.json').stat().st_size // 1024
    print(f'  ✓ {len(connections):,} hadiths with connections → hadith_connections.json ({kb} KB)')
    return connections


# ── Entry point ───────────────────────────────────────────────────────────────

# ── Step F: Concordance index (Mu'jam al-Mufahris) ───────────────────────────

def build_concordance_index():
    """
    Build a full corpus concordance: every significant word → list of hadith IDs.
    This is the Mu'jam al-Mufahris — clicking any word shows ALL hadiths containing it.

    Output: app/data/concordance.json
    {
      "صلاه": ["bukhari:1", "muslim:4", "bukhari:120", ...],
      "زكاه": [...],
      ...
    }
    Only stores words that appear in word_defs.json (have a known definition).
    Capped at 500 hadith IDs per word to keep file manageable.
    """
    print('\n── Step F: Building concordance index (Mu\'jam) ─────')

    # Load word_defs to know which words to index — prefer v2 (broader vocabulary)
    defs_v2_path = DATA / 'word_defs_v2.json'
    defs_path    = DATA / 'word_defs.json'
    if defs_v2_path.exists():
        known_words = set(read_json(defs_v2_path).keys())
        print(f'  Indexing {len(known_words):,} known words (word_defs_v2)…')
    elif defs_path.exists():
        known_words = set(read_json(defs_path).keys())
        print(f'  Indexing {len(known_words):,} known words (word_defs)…')
    else:
        print('  ✗ word_defs.json not found — run --step stemmer first')
        return {}

    # Build inverted index
    concordance: dict[str, list[str]] = defaultdict(list)

    total = 0
    for book_id, ch_idx, h in iter_all_hadiths('sunni'):
        arabic = h.get('arabic', '')
        if not arabic:
            continue
        h_id = f"{book_id}:{ch_idx}:{h.get('idInBook','')}"
        tokens = set(tokenize_matn(arabic))  # use full text for concordance, not just matn
        for tok in tokens:
            if tok in known_words:
                concordance[tok].append(h_id)
        total += 1

    # Cap and sort (most-cited books first within each word)
    result = {}
    for word, ids in concordance.items():
        result[word] = ids[:2000]  # cap at 2000 per word (raised from 500)

    write_json(DATA / 'concordance.json', result)
    kb = (DATA / 'concordance.json').stat().st_size // 1024
    total_entries = sum(len(v) for v in result.values())
    print(f'  ✓ {len(result):,} words indexed, {total_entries:,} total entries → concordance.json ({kb} KB)')
    return result


# ── Step G: CAMeL Tools morphological analysis → word_defs_v2.json ───────────

def build_word_defs_v2():
    """
    Version 2 of word_defs using CAMeL Tools for accurate morphological analysis.
    Replaces the light stemmer with proper MSA morphological analysis.

    For each corpus word:
      - Run CAMeL MorphAnalyzer to get: root, lemma, POS, form (verb form I-X), voice, aspect
      - Cross-reference root with Lane's Lexicon for definition
      - Store extended info: morph features visible in word panel

    Output: app/data/word_defs_v2.json
    {
      "norm_word": {
        "r": "root_ar (dot-separated, CAMeL format)",
        "rl": "root_ar (clean, e.g. زكو)",
        "s": "buckwalter",
        "g": "gloss (from Lane's)",
        "d": "definition_en (from Lane's, truncated)",
        "n": corpus_freq,
        "lem": "lemma",         // CAMeL
        "pos": "noun/verb/...", // CAMeL
        "form": "I/II/III/...", // verb form number
        "voice": "act/pass",    // if verb
      }
    }
    """
    print('\n── Step G: Building word_defs_v2 with CAMeL Tools ──')

    try:
        from camel_tools.morphology.database import MorphologyDB
        from camel_tools.morphology.analyzer import Analyzer
        db = MorphologyDB.builtin_db()
        analyzer = Analyzer(db)
        print('  CAMeL MorphAnalyzer loaded')
    except Exception as e:
        print(f'  ✗ CAMeL not available: {e}')
        print('  Run: pip install camel-tools && camel_data -i morphology-db-msa-r13')
        return {}

    # Load Lane's Lexicon for definitions
    lex_path = DATA / 'roots_lexicon.json'
    if not lex_path.exists():
        print('  ✗ roots_lexicon.json not found — run --step lexicon first')
        return {}
    lexicon = read_json(lex_path)

    # Build root lookup: clean root (e.g. زكو) → lexicon entry
    # CAMeL roots use dot-separated format: ز.ك.و or # for weak letters
    def camel_root_to_clean(r: str) -> str:
        """Convert CAMeL root format (ز.ك.#) to clean Arabic (زكو/زكي)."""
        parts = r.split('.')
        clean = ''
        for p in parts:
            if p == '#':
                clean += 'و'  # default weak letter; try ي too
            else:
                clean += p
        return clean

    def lookup_lane(camel_root: str):
        """Try to find Lane's definition via CAMeL root."""
        clean = camel_root_to_clean(camel_root)
        # Exact
        if clean in lexicon:
            return lexicon[clean]
        # Try ي variant for weak roots
        clean_y = camel_root_to_clean(camel_root.replace('#', 'ي'))
        if clean_y in lexicon:
            return lexicon[clean_y]
        # Normalized
        norm_root = normalize(clean)
        for root_ar, entry in lexicon.items():
            if normalize(root_ar) == norm_root:
                return entry
        return None

    # Pass 1: collect all unique words from corpus
    print('  Pass 1: collecting corpus words…')
    corpus_freq: Counter = Counter()
    for book_id, ch_idx, h in iter_all_hadiths('sunni'):
        arabic = h.get('arabic', '')
        if not arabic:
            continue
        tokens = tokenize_matn(arabic)
        for tok in tokens:
            corpus_freq[tok] += 1

    unique_words = [w for w, c in corpus_freq.items() if c >= 2]
    print(f'  {len(unique_words):,} unique words (freq >= 2)')

    # Pass 2: CAMeL analysis
    print('  Pass 2: morphological analysis…')
    word_defs_v2 = {}
    matched = 0
    no_analysis = 0

    VERB_FORMS = {
        'I': ['فعل','فعلة'], 'II': ['فعّل','تفعيل'], 'III': ['فاعل','مفاعلة'],
        'IV': ['أفعل','إفعال'], 'V': ['تفعّل'], 'VI': ['تفاعل'],
        'VII': ['انفعل'], 'VIII': ['افتعل'], 'IX': ['افعلّ'], 'X': ['استفعل'],
    }

    for i, word in enumerate(unique_words):
        if i % 5000 == 0:
            print(f'    {i:,}/{len(unique_words):,}…')

        try:
            analyses = analyzer.analyze(word)
        except Exception:
            analyses = []

        if not analyses:
            no_analysis += 1
            continue

        # Pick best analysis (first is usually most likely for MSA)
        best = analyses[0]
        camel_root = best.get('root', '')
        lemma      = best.get('lex', '')
        pos        = best.get('pos', '')
        form_num   = best.get('form_num', '')
        voice      = best.get('vox', '')   # 'a'=active, 'p'=passive
        aspect     = best.get('asp', '')   # 'p'=perfect, 'i'=imperfect, 'c'=command

        # Get Lane's definition via root
        lane_entry = lookup_lane(camel_root) if camel_root else None
        gloss      = (lane_entry.get('summary_en', '') if lane_entry else '').strip()[:100]
        defn       = (lane_entry.get('definition_en', '') if lane_entry else '').strip()[:400]
        buckwalter = (lane_entry.get('buckwalter', '') if lane_entry else '')

        if not gloss and not defn:
            continue  # no definition available

        entry = {
            'r':    camel_root_to_clean(camel_root) if camel_root else '',
            'rc':   camel_root,   # CAMeL dot format with # markers
            's':    buckwalter,
            'g':    gloss,
            'n':    corpus_freq[word],
            'lem':  normalize(lemma) if lemma else '',
            'pos':  pos,
        }
        # Add verb-specific features
        if pos == 'verb':
            if form_num: entry['form'] = form_num
            if voice == 'p': entry['voice'] = 'passive'
            if aspect:
                entry['asp'] = {'p': 'perfect', 'i': 'imperfect', 'c': 'command'}.get(aspect, aspect)

        word_defs_v2[word] = entry
        matched += 1

    total = len(unique_words)
    print(f'  ✓ {matched:,}/{total:,} words analysed ({matched*100//total if total else 0}% coverage)')
    print(f'  {no_analysis:,} words had no CAMeL analysis')

    write_json(DATA / 'word_defs_v2.json', word_defs_v2)
    kb = (DATA / 'word_defs_v2.json').stat().st_size // 1024
    print(f'  ✓ word_defs_v2.json: {kb} KB, {len(word_defs_v2):,} entries')
    return word_defs_v2


# ── Step G: Wensinck Mu'jam al-Mufahras (root-based concordance) ──────────────

# Wensinck's original 9 canonical collections + abbreviations
WENSINCK_BOOKS = {
    'bukhari':  'خ',
    'muslim':   'م',
    'abudawud': 'د',
    'tirmidhi': 'ت',
    'nasai':    'ن',
    'ibnmajah': 'جه',
    'ahmed':    'حم',
    'malik':    'ط',
    'darimi':   'دي',
}

# Extended books beyond Wensinck's original scope
EXTRA_BOOK_ABBR = {
    'aladab_almufrad':         'أدب',
    'bulugh_almaram':          'بلغ',
    'mishkat_almasabih':       'مشك',
    'musannaf_ibnabi_shaybah': 'مصن',
    'nawawi40':                'نو٤٠',
    'qudsi40':                 'قد٤٠',
    'riyad_assalihin':         'رياض',
    'shahwaliullah40':         'شاه',
    'shamail_muhammadiyah':    'شمائل',
}

ALL_BOOK_ABBR = {**WENSINCK_BOOKS, **EXTRA_BOOK_ABBR}


def build_wensinck_index():
    """
    Build a root-organised Mu'jam al-Mufahras — the concordance Wensinck
    compiled by hand in 1936, now generated from all 18 Sunni books.

    For each Arabic root: every derived surface form found, a Lane's Lexicon
    gloss, and hadith references grouped by book using traditional abbreviations.

    Output: app/data/wensinck.json
    {
      "صلو": {
        "root": "صلو",
        "buckwalter": "Slw",
        "gloss": "prayer, to pray, bless",
        "forms": ["صلاه", "صلي", "يصلي", ...],
        "books": {
          "خ":  ["2:3", "4:12", ...],   // chapter:idInBook
          "م":  ["1:5", ...],
          ...
        },
        "total": 4523,
        "wensinck9": true   // true if root appears in the original 9 books
      }
    }
    """
    print('\n── Step G: Building Wensinck Mu\'jam al-Mufahras ─────')

    # Load roots lexicon for definitions
    lex_path = DATA / 'roots_lexicon.json'
    if not lex_path.exists():
        print('  ✗ roots_lexicon.json not found — run --step lexicon first')
        return {}
    lexicon = read_json(lex_path)

    # Build root_index for find_root (same approach as build_word_defs)
    root_index: dict[str, tuple] = {}
    for root_ar, entry in lexicon.items():
        norm_root = normalize(root_ar)
        root_index[norm_root] = (root_ar, entry)
        bw = entry.get('buckwalter', '')
        if bw:
            root_index[bw.lower()] = (root_ar, entry)

    def find_root(word: str):
        norm = normalize(word)
        if norm in root_index: return root_index[norm]
        stem = light_stem(norm)
        if stem in root_index: return root_index[stem]
        no_al = norm[2:] if norm.startswith('ال') else norm
        if no_al in root_index: return root_index[no_al]
        stem2 = light_stem(no_al)
        if stem2 in root_index: return root_index[stem2]
        for base in [stem2, stem, no_al]:
            if not base: continue
            if base.endswith('ء'):
                no_hamza = base[:-1]
                if no_hamza in root_index: return root_index[no_hamza]
                stemmed_nh = light_stem(no_hamza)
                if stemmed_nh in root_index: return root_index[stemmed_nh]
                if no_hamza.endswith('ا'):
                    for sfx in ['و', 'ي']:
                        alt = no_hamza[:-1] + sfx
                        if alt in root_index: return root_index[alt]
                if len(stemmed_nh) == 2:
                    gem = stemmed_nh + stemmed_nh[-1]
                    if gem in root_index: return root_index[gem]
            if base.endswith('ا'):
                alt = base[:-1] + 'و'
                if alt in root_index: return root_index[alt]
            if base.endswith('ه'):
                alt_w = base[:-1] + 'و'
                alt_y = base[:-1] + 'ي'
                if alt_w in root_index: return root_index[alt_w]
                if alt_y in root_index: return root_index[alt_y]
                trimmed = base[:-1]
                if trimmed in root_index: return root_index[trimmed]
            if base.endswith('ي'):
                alt = base[:-1] + 'و'
                if alt in root_index: return root_index[alt]
        return None

    # root_ar → { forms: set, books: { book_id: set of "ch:hadith" } }
    index: dict[str, dict] = defaultdict(lambda: {
        'forms': set(),
        'books': defaultdict(set),
    })

    total_hadiths = 0
    for book_id, ch_idx, h in iter_all_hadiths('sunni'):
        arabic = h.get('arabic', '')
        if not arabic:
            continue
        h_num = h.get('idInBook', '')
        ref = f"{ch_idx}:{h_num}"

        tokens = set(tokenize_matn(arabic))
        for tok in tokens:
            result = find_root(tok)
            if result:
                root_ar, _ = result
                entry = index[root_ar]
                entry['forms'].add(tok)
                entry['books'][book_id].add(ref)
        total_hadiths += 1

    print(f'  Scanned {total_hadiths:,} hadiths, found {len(index):,} roots')

    # Build final output
    wensinck = {}
    for root_ar in sorted(index.keys()):
        entry = index[root_ar]
        lex_entry = lexicon.get(root_ar, {})

        # Convert book_id → abbreviation, refs as sorted lists
        books_out = {}
        in_wensinck9 = False
        for book_id, refs in sorted(entry['books'].items()):
            abbr = ALL_BOOK_ABBR.get(book_id, book_id)
            books_out[abbr] = sorted(refs, key=lambda r: (
                int(r.split(':')[0]) if r.split(':')[0].isdigit() else 0,
                int(r.split(':')[1]) if len(r.split(':')) > 1 and r.split(':')[1].isdigit() else 0
            ))
            if book_id in WENSINCK_BOOKS:
                in_wensinck9 = True

        total = sum(len(v) for v in books_out.values())

        wensinck[root_ar] = {
            'root': root_ar,
            'buckwalter': lex_entry.get('buckwalter', ''),
            'gloss': lex_entry.get('summary_en', ''),
            'forms': sorted(entry['forms']),
            'books': books_out,
            'total': total,
            'wensinck9': in_wensinck9,
        }

    write_json(DATA / 'wensinck.json', wensinck)
    kb = (DATA / 'wensinck.json').stat().st_size // 1024
    total_refs = sum(e['total'] for e in wensinck.values())
    w9 = sum(1 for e in wensinck.values() if e['wensinck9'])
    print(f'  ✓ {len(wensinck):,} roots, {total_refs:,} total references → wensinck.json ({kb:,} KB)')
    print(f'    {w9:,} roots in Wensinck\'s original 9 books')
    return wensinck


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description='Hadith enrichment pipeline')
    parser.add_argument('--step', choices=['lexicon','stemmer','freq','narrators',
                                           'connections','concordance','camel','wensinck','all'])
    args = parser.parse_args()

    if args.step == 'lexicon':
        build_roots_lexicon()
    elif args.step == 'stemmer':
        build_word_defs()
    elif args.step == 'freq':
        build_word_frequency()
    elif args.step == 'narrators':
        build_narrator_index()
    elif args.step == 'connections':
        build_hadith_connections()
    elif args.step == 'concordance':
        build_concordance_index()
    elif args.step == 'camel':
        build_word_defs_v2()
    elif args.step == 'wensinck':
        build_wensinck_index()
    else:
        # Full pipeline
        lexicon = build_roots_lexicon()
        build_word_defs(lexicon)
        build_word_frequency()
        build_narrator_index()
        build_hadith_connections()
        build_concordance_index()
        build_word_defs_v2()
        build_wensinck_index()

    print('\n✓ Done.')
    for f in ['roots_lexicon.json','word_defs.json','word_defs_v2.json',
              'word_freq.json','narrator_index.json',
              'hadith_connections.json','concordance.json','wensinck.json']:
        p = DATA / f
        if p.exists():
            print(f'  {f}: {p.stat().st_size//1024} KB')


if __name__ == '__main__':
    main()
