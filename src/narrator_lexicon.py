"""
narrator_lexicon.py
===================
Shared boundary markers, book names, sigla, compound names, cities, verbs,
and other knowledge that ALL parsers need to correctly extract narrator names
from classical Arabic biographical texts.

This is the single source of truth for name extraction logic.
Import this into any parser instead of reinventing these lists.
"""

import re

# ── SIGLA: Single/multi-letter book abbreviations ────────────────────
# These appear at the start of entries in Tahdhib-family texts
SIGLA = {
    # Single letter
    'خ', 'م', 'د', 'ت', 'س', 'ق', 'ع', 'ر', 'ه',
    # Multi-letter
    'بخ', 'كن', 'فق', 'عه', 'خت', 'مد', 'سي', 'عس', 'تم',
    'مق', 'عخ', 'ص', 'قد',
    # Numeric sigla
    '3',  # shorthand for three of the four sunan
    '4',  # shorthand for الأربعة (the Four Books)
    '6',  # occasional shorthand for the six books
}

# ── BOOK NAMES: Full expansions of the sigla ─────────────────────────
# These appear after sigla in Tahdhib al-Tahdhib headers
# Order matters: longer first for greedy matching
BOOK_NAMES = [
    # Compound (with conjunctions) — longest first
    'مسلم وأبي داود والترمذي وابن ماجة',
    'البخاري ومسلم وأبي داود والترمذي وابن ماجة',
    'البخاري ومسلم وأبي داود والنسائي وابن ماجة',
    'البخاري ومسلم والنسائي وابن ماجة',
    'البخاري ومسلم وأبي داود والنسائي',
    'البخاري والترمذي وابن ماجة',
    'أبي داود والترمذي وابن ماجة',
    'الترمذي وابن ماجة والنسائي',
    'أبي داود وابن ماجة في التفسير',
    'أبي داود والنسائي وابن ماجة',
    'النسائي في اليوم والليلة',
    'النسائي في عمل اليوم والليلة',
    'النسائي في خصائص علي',
    'أبي داود في المراسيل',
    'أبي داود في كتاب القدر',
    'أبي داود في القدر',
    'البخاري في خلق أفعال العباد',
    'البخاري في الأدب المفرد',
    'البخاري في التعاليق',
    'البخاري في التعليق',
    'الترمذي في الشمائل',
    'مسلم في المقدمة',
    # Two-book conjunctions
    'الترمذي وابن ماجة',
    'النسائي وابن ماجة',
    'أبي داود والترمذي',
    'أبي داود والنسائي',
    'وأبي داود والترمذي والنسائي وابن ماجة',
    'وأبي داود والترمذي وابن ماجة',
    'وأبي داود والنسائي وابن ماجة',
    'والترمذي والنسائي وابن ماجة',
    'والترمذي وابن ماجة',
    'والنسائي وابن ماجة',
    'وأبي داود والترمذي',
    'وأبي داود والنسائي',
    'وابن ماجة والنسائي',
    'وابن ماجة',
    'وأبي داود',
    'والترمذي',
    'والنسائي',
    'ومسلم',
    'والبخاري',
    'والأربعة',
    # Standalone book names
    'ابن ماجة',
    'بن ماجة',
    'أبي داود',
    'أبو داود',
    'مسند مالك',
    'الأدب المفرد',
    'في التفسير',
    'في المراسيل',
    'في القدر',
    'في التعاليق',
    'في التعليق',
    'اليوم والليلة',
    'البخاري',
    'الترمذي',
    'النسائي',
    'الستة',
    'الأربعة',
    'تمييز',
    'مسلم',    # AMBIGUOUS — see strip_book_prefix()
    'مالك',    # AMBIGUOUS — see strip_book_prefix()
]

# ── COMPOUND NAMES: Two-word names that must never be split ──────────
# If the first word appears, the second word is PART OF THE NAME
# Pattern: عبد + DIVINE_ATTRIBUTE
ABD_COMPOUNDS = {
    'الله', 'الرحمن', 'الرحيم', 'الملك', 'الوهاب', 'العزيز',
    'السلام', 'المنعم', 'الباقي', 'الحميد', 'القادر', 'الصمد',
    'الجبار', 'الكريم', 'الواحد', 'الرزاق', 'القيوم', 'الأعلى',
    'الحق', 'المؤمن', 'العظيم', 'المطلب', 'القدوس', 'الخالق',
    'البارئ', 'المصور', 'الغفار', 'القهار', 'الفتاح', 'العليم',
    'القابض', 'الباسط', 'الخافض', 'الرافع', 'المعز', 'المذل',
    'السميع', 'البصير', 'اللطيف', 'الخبير', 'الحليم', 'الشكور',
    'الكبير', 'المتعال', 'الحفيظ', 'المقيت', 'الحسيب', 'الجليل',
    'الرقيب', 'المجيب', 'الواسع', 'الحكيم', 'الودود', 'المجيد',
    'الباعث', 'الشهيد', 'الوكيل', 'القوي', 'المتين', 'الولي',
    'المحصي', 'المبدئ', 'المعيد', 'المحيي', 'المميت', 'الوارث',
    'الرشيد', 'الصبور', 'النور', 'الهادي', 'البديع', 'الغني',
    'المغني', 'المانع', 'الضار', 'النافع', 'الجامع', 'المقدم',
    'المؤخر', 'الأول', 'الآخر', 'الظاهر', 'الباطن',
}

# Other two-word name patterns (not عبد)
# These ALWAYS go together as a name unit
COMPOUND_NAME_PAIRS = {
    ('أبو', 'بكر'), ('أبو', 'هريرة'), ('أبو', 'موسى'), ('أبو', 'ذر'),
    ('أبو', 'سفيان'), ('أبو', 'طالب'), ('أبو', 'لهب'), ('أبو', 'جهل'),
    ('أم', 'سلمة'), ('أم', 'حبيبة'), ('أم', 'كلثوم'), ('أم', 'أيمن'),
    ('ابن', 'عباس'), ('ابن', 'مسعود'), ('ابن', 'عمر'), ('ابن', 'الزبير'),
}

# ── CITIES: Common nisba locations ───────────────────────────────────
# These appear as suffixes (البصري, الكوفي) or standalone
CITIES = {
    'البصري', 'الكوفي', 'المدني', 'المكي', 'الشامي', 'الدمشقي',
    'البغدادي', 'المصري', 'الحراني', 'الموصلي', 'النيسابوري',
    'الأصبهاني', 'الخراساني', 'الواسطي', 'الحمصي', 'الرقي',
    'البلخي', 'المروزي', 'الهمداني', 'الرازي', 'الجرجاني',
    'الطبراني', 'الفلسطيني', 'اليمني', 'الأندلسي', 'القيرواني',
    'السمرقندي', 'البخاري', 'الطوسي', 'القزويني', 'الأهوازي',
    'الحلبي', 'البعلبكي', 'القرطبي', 'الإشبيلي', 'الصنعاني',
}

# City names (not nisba form) -- for "نزيل بغداد" or "من أهل الكوفة"
CITY_NAMES = {
    'بغداد', 'البصرة', 'الكوفة', 'المدينة', 'مكة', 'دمشق',
    'مصر', 'نيسابور', 'أصبهان', 'خراسان', 'واسط', 'حمص',
    'الري', 'مرو', 'هراة', 'بلخ', 'سمرقند', 'بخارى',
}

# ── VERBS: Biographical prose markers (where name ENDS) ──────────────
# If these appear in text, everything after them is biography, not name
# Split into categories for precision

# Scholarly citations (قال ابن X) and speech (قال له)
CITATION_VERBS = [
    r'قال\s+(?:ابن|أبو|أبي|البخاري|الذهبي|مطين|أحمد|بعضهم|الحافظ|أبي حاتم|الواقدي|محمد|الخطيب|النسائي)',
    r'قال\s+له\s+(?:النبي|رسول)',
    r'قال\s+(?:سمعت|النبي|رسول)',
]

# Editorial notes
EDITORIAL_MARKERS = [
    r'قيل\s+(?:اسم|إن|له|كان|هو|نسب)',
    r'يقال\s+(?:اسم|إن|له|كان|هو|نسب)',
    r'صوابه\s',
    r'يأتي\s',
    r'تقدم\s',
    r'سيأتي\s',
    r'سبق\s',
]

# Isnad fragments (these are hadith chain text, not names)
ISNAD_VERBS = [
    r'حدثنا\s',
    r'أخبرنا\s',
    r'أخبرنيها\s',
    r'أنبأنا\s',
]

# Transmission notes
TRANSMISSION_VERBS = [
    r'(?:روى|يروي|حدث)\s+عن\s',
    r'حدث\s+عن\s',
    r'سمع\s+(?:من|أبا|ابن|عبد|النبي)',
    r'(?:أخرج|روى)\s+له\s',
    r'\bعن\s+[\u0600-\u06FF]',   # standalone "عن X" — transmission marker
    r'\bعنه\b',                   # "عنه" = "from him" — transmission marker
]

# Biographical prose
BIO_VERBS = [
    r'(?:برع|اشتغل|تفقه|صنف|ولي|درس|ناب)\s+(?:في|على|بـ)',
    r'(?:مات|توفي|قتل)\s+(?:سنة|في|بعد|قبل)',
    r'ذكره\s+(?:ابن|أبو|في|البخاري)',
    r'له\s+(?:صحبة|حديث|رواية|كتاب|تصانيف)',
    r'كان\s+(?:يحدث|يروي|من|عالما|فقيها|حافظا)',
    r'أحد\s+(?:الطرقية|الكذابين|الضعفاء|الحفاظ|الأئمة|العشرة|السابقين)',
]

# Name-variant markers (everything after this is an alternate name)
ALTERNATE_MARKERS = [
    r'[،,]\s*ويقال',
    r'[،,]\s*وقيل',
    r'\s+ويقال',
    r'\s+وقيل',
    r'[،,]\s*وهو\s',
]

# ── COMPANION MARKERS: Words that identify a Sahabi ──────────────────
COMPANION_MARKERS = [
    'صحابي', 'صحابية', 'صاحب رسول', 'شهد بدرا', 'من السابقين',
    'أحد العشرة', 'من المهاجرين', 'حواري رسول', 'أسلم قديما',
    'من أهل بدر', 'بايع تحت الشجرة', 'أحد السابقين', 'له صحبة',
    'لها صحبة', 'شهد أحدا', 'هاجر إلى', 'بايع',
]


# ── GRADE KEYWORDS: Reliability grades from classical texts ──────────
# Order matters: more specific multi-word phrases MUST come before their
# single-word substrings.  e.g. "ثقة ثبت" before "ثقة", "ضعيف جدا"
# before "ضعيف".  Priority within the same grade level goes to the
# stronger/more explicit form.
#
# Sources: Taqrib terminology (Ibn Hajar), Jarh wa al-Ta'dil (Ibn Abi
# Hatim), Mizan/Lisan (al-Dhahabi/Ibn Hajar), Tarikh Baghdad/Islam.

GRADE_KEYWORDS = [
    # ── Companion ────────────────────────────────────────────────────
    ('صحابي', 'companion'),
    ('صحابية', 'companion'),
    ('له صحبة', 'companion'),
    ('لها صحبة', 'companion'),
    ('من الصحابة', 'companion'),
    ('أدرك النبي', 'companion'),
    ('رأى النبي', 'companion'),

    # ── Fabricator (check before weak — "يضع" substring overlap) ─────
    ('كذاب', 'fabricator'),
    ('وضاع', 'fabricator'),
    ('يضع الحديث', 'fabricator'),
    ('يضع', 'fabricator'),
    ('كان يكذب', 'fabricator'),
    ('دجال', 'fabricator'),
    ('يختلق', 'fabricator'),

    # ── Abandoned ────────────────────────────────────────────────────
    ('متروك الحديث', 'abandoned'),
    ('متروك', 'abandoned'),
    ('تركوه', 'abandoned'),
    ('تركه الناس', 'abandoned'),
    ('لا شيء', 'abandoned'),

    # ── Weak (check multi-word before single-word) ───────────────────
    ('منكر الحديث', 'weak'),
    ('ضعيف جدا', 'weak'),
    ('ضعيف الحديث', 'weak'),
    ('ضعيف', 'weak'),
    ('واهي الحديث', 'weak'),
    ('واه', 'weak'),
    ('ذاهب الحديث', 'weak'),
    ('مضطرب الحديث', 'weak'),
    ('لين الحديث', 'weak'),
    ('لين', 'weak'),
    ('فيه لين', 'weak'),
    ('فيه ضعف', 'weak'),
    ('فيه نظر', 'weak'),
    ('فيه مقال', 'weak'),
    ('فيه كلام', 'weak'),
    ('سيئ الحفظ', 'weak'),
    ('كثير الوهم', 'weak'),
    ('كثير الخطأ', 'weak'),
    ('ليس بالقوي', 'weak'),
    ('ليس بقوي', 'weak'),
    ('ليس بذاك', 'weak'),
    ('ليس بثقة', 'weak'),
    ('ليس بشيء', 'weak'),
    ('ليس بمرضي', 'weak'),
    ('لا يحتج بحديثه', 'weak'),
    ('لا يحتج به', 'weak'),
    ('يكتب حديثه ولا يحتج', 'weak'),
    ('غير محتج به', 'weak'),
    ('تكلموا فيه', 'weak'),
    ('تكلم فيه', 'weak'),
    ('ضعفوه', 'weak'),
    ('مطرح', 'weak'),
    ('طرحوه', 'weak'),
    ('هالك', 'weak'),
    ('ساقط', 'weak'),
    ('تالف', 'weak'),
    ('لا يساوي شيئا', 'weak'),

    # ── Reliable (multi-word first) ──────────────────────────────────
    ('ثقة ثبت', 'reliable'),
    ('ثقة حافظ', 'reliable'),
    ('ثقة متقن', 'reliable'),
    ('ثقة مأمون', 'reliable'),
    ('ثقة حجة', 'reliable'),
    ('ثقة ثقة', 'reliable'),
    ('ثقة صالح', 'reliable'),
    ('ثقة عدل', 'reliable'),
    ('ثقة', 'reliable'),
    ('ثقه', 'reliable'),
    ('مجمع على ثقته', 'reliable'),
    ('أجمعوا على ثقته', 'reliable'),
    ('من أثبت الناس', 'reliable'),
    ('أوثق الناس', 'reliable'),
    ('أثبت الناس', 'reliable'),
    ('لا يسأل عن مثله', 'reliable'),
    ('لا يسأل عنه', 'reliable'),
    ('يحتج به', 'reliable'),
    ('حجة', 'reliable'),
    ('محله الصدق', 'reliable'),
    ('محل الصدق', 'reliable'),
    ('ليس به بأس', 'reliable'),

    # ── Mostly reliable ──────────────────────────────────────────────
    ('صدوق حسن', 'mostly_reliable'),
    ('صدوق له أوهام', 'mostly_reliable'),
    ('صدوق يخطئ', 'mostly_reliable'),
    ('صدوق يهم', 'mostly_reliable'),
    ('صدوق سيئ الحفظ', 'mostly_reliable'),
    ('صدوق', 'mostly_reliable'),
    ('حسن الحديث', 'mostly_reliable'),
    ('جيد الحديث', 'mostly_reliable'),
    ('صالح الحديث', 'mostly_reliable'),
    ('مقارب الحديث', 'mostly_reliable'),
    ('مستقيم الحديث', 'mostly_reliable'),
    ('لا بأس به', 'mostly_reliable'),
    ('لا بأس', 'mostly_reliable'),
    ('ما به بأس', 'mostly_reliable'),
    ('مقبول', 'mostly_reliable'),
    ('وسط', 'mostly_reliable'),
    ('يكتب حديثه', 'mostly_reliable'),
    ('شيخ', 'mostly_reliable'),
    ('إمام', 'mostly_reliable'),

    # ── Unknown ──────────────────────────────────────────────────────
    ('مجهول الحال', 'unknown'),
    ('مجهول العين', 'unknown'),
    ('مجهول', 'unknown'),
    ('مستور', 'unknown'),
    ('لا يعرف', 'unknown'),
    ('لا أعرفه', 'unknown'),
    ('نكرة', 'unknown'),
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


# ── VERB-FORM GRADE KEYWORDS ────────────────────────────────────────
# Condensed Dhahabi texts (Kashif, Mughni, Diwan) use VERB forms instead
# of adjectives.  "وثق" (was authenticated) instead of "ثقة" (trustworthy).
#
# These are ONLY safe in condensed texts where the verb is a standalone
# evaluation, not embedded in longer prose.  In encyclopedic texts
# (Tarikh, Siyar), "وثق" might appear in "ولم يوثقه أحد" (nobody
# trusted him) — the opposite meaning.
#
# Use extract_grade_verbs() for Kashif-family texts.  Use extract_grade()
# for everything else.

GRADE_VERB_KEYWORDS = [
    # Reliable verbs
    ('وثقوه', 'reliable'),      # "they authenticated him"
    ('وثقه', 'reliable'),       # "he authenticated him"
    ('وثق', 'reliable'),        # "was authenticated"
    ('احتج به', 'reliable'),    # "was used as proof"
    # Weak verbs
    ('ضعفوه', 'weak'),          # "they weakened him"
    ('ضعفه', 'weak'),           # "he weakened him"
    ('لينوه', 'weak'),          # "they deemed him soft"
    ('لينه', 'weak'),           # "he deemed him soft"
    # Abandoned verbs
    ('تركوه', 'abandoned'),     # "they abandoned him"
    ('تركه', 'abandoned'),      # "he abandoned him"
    # Fabricator verbs
    ('كذبوه', 'fabricator'),    # "they called him a liar"
    ('كذبه', 'fabricator'),     # "he called him a liar"
]

# Title-as-grade: in condensed texts, these titles ARE the evaluation
GRADE_TITLE_KEYWORDS = [
    ('الحافظ', 'reliable'),     # "the memorizer" = high praise
    ('الإمام', 'reliable'),     # "the leader/imam" = high praise
    ('حافظ كبير', 'reliable'),  # "great memorizer"
]


# ── BOOK-MEMBERSHIP GRADING ─────────────────────────────────────────
# When extract_grade() finds nothing in the entry text, the *book itself*
# is evidence.  A narrator listed in a du'afa collection is weak; one
# listed in al-Thiqat is reliable.  Encyclopedic works (tarikh, siyar,
# tabaqat) carry no default — they list everyone.
#
# Format: source_key -> (grade_en, grade_ar)
BOOK_GRADE_DEFAULTS = {
    # Du'afa / criticism books -> weak
    'mizan_itidal':   ('weak',     'ذكره الذهبي في الميزان'),
    'lisan_mizan':    ('weak',     'ذكره ابن حجر في لسان الميزان'),
    'diwan_ducafa':   ('weak',     'ذكره الذهبي في ديوان الضعفاء'),
    'mughni_ducafa':  ('weak',     'ذكره الذهبي في المغني في الضعفاء'),
    'dhayl_diwan':    ('weak',     'ذكره الذهبي في ذيل الديوان'),
    'kamil':          ('weak',     'ذكره ابن عدي في الكامل في الضعفاء'),
    # Trustworthy narrator list -> reliable
    'thiqat':         ('reliable', 'ذكره ابن حبان في الثقات'),
    # Companion encyclopedia -> companion
    'isaba':          ('companion','ذكره ابن حجر في الإصابة'),
    # Encyclopedic works -> NO default (they list everyone)
    # 'tarikh_islam': None,
    # 'tarikh':       None,
    # 'siyar':        None,
    # 'tabaqat':      None,
    # 'tahdhib_kamal': None,
    # 'tahdhib_tahdhib': None,
    # Reference works with mixed content -> NO default
    # 'jarh_tadil':   None,   # has own grades, expand keywords instead
    # 'taqrib':       None,   # already graded inline
    # 'kashif':       None,
    # 'durar_kamina': None,
}


# ── CROSS-REFERENCE PATTERNS ────────────────────────────────────────
# Tahdhib-family texts have shorthand cross-reference entries that are
# NOT biographical entries.  They look like:
#   "بن أبجر هو عبد الملك بن سعيد"
#   "ابن إسحاق هو محمد"
# Pattern: starts with بن/ابن + name, then "هو" + real name.
# These should be parsed as aliases, not narrator profiles.

CROSS_REF_RE = re.compile(
    r'^(?:بن|ابن)\s+[\u0600-\u06FF]+\s+'   # بن/ابن + one word
    r'(?:هو|هي|اسمه|اسمها|اثنان|ثلاثة)\s'  # followed by "he is" / "his name is" / "two/three"
)


def is_cross_reference(name, body=''):
    """Detect if an entry is a cross-reference, not a real biography.

    Cross-references look like:
      name: "بن إسحاق"      body: "هو محمد"
      name: "بن الأدرع"      body: "هو محجن"
      name: "بن أرقم اثنان عبد الله وسليمان"

    Returns (True, target_name) or (False, None).
    """
    full = (name + ' ' + body).strip()

    m = CROSS_REF_RE.match(full)
    if m:
        rest = full[m.end():].strip()
        # Extract the target name (everything before comma/period/verb)
        target_end = re.search(r'[،,.]|\s+(?:روى|من|عن|قال|كان)', rest)
        target = rest[:target_end.start()].strip() if target_end else rest[:80].strip()
        return True, target

    return False, None


# ── NAME VALIDATION ──────────────────────────────────────────────────

DIACRITICS_RE = re.compile(
    r'[\u0610-\u061A\u064B-\u065F\u0670\u06D6-\u06DC'
    r'\u06DF-\u06E4\u06E7\u06E8\u06EA-\u06ED]'
)

def strip_diacritics(t):
    return DIACRITICS_RE.sub('', t)


def extract_grade(text):
    """Extract the highest-priority grade from Arabic text.
    Searches for grade keywords in priority order (fabricator > abandoned
    > weak > reliable > mostly_reliable > unknown).
    Returns (grade_en, grade_ar) or (None, None).
    """
    clean = strip_diacritics(text)
    for keyword, grade in GRADE_KEYWORDS:
        if strip_diacritics(keyword) in clean:
            return grade, keyword
    return None, None


def extract_grade_condensed(text):
    """Extract grade from CONDENSED texts (Kashif, Mughni, Diwan).

    These texts use verb forms ('وثق') instead of adjectives ('ثقة').
    Checks negation first, then standard keywords, then verb forms, then titles.

    IMPORTANT: Do NOT use this for encyclopedic texts (Tarikh, Siyar)
    where verb forms appear in longer sentences with different meanings.
    e.g. 'ولم يوثقه أحد' = 'nobody trusted him' — the opposite!

    Returns (grade_en, grade_ar) or (None, None).
    """
    clean = strip_diacritics(text)

    # 1. Check for NEGATION patterns first — these override everything.
    #    'لم يوثقه' / 'ما وثقه أحد' = NOT trusted -> weak
    #    Must check BEFORE standard keywords because 'ثقه' (a keyword)
    #    is a substring of 'يوثقه' and 'وثقه'.
    if re.search(r'(?:لم\s+ي|ما\s+)(?:وثق|حتج)', clean):
        return 'weak', 'لم يوثق'

    # 2. Try standard adjective/noun keywords
    grade, keyword = extract_grade(text)
    if grade:
        return grade, keyword

    # 3. Try verb forms (condensed-text specific)
    for keyword, grade in GRADE_VERB_KEYWORDS:
        if strip_diacritics(keyword) in clean:
            return grade, keyword

    # 4. Try title-as-grade
    for keyword, grade in GRADE_TITLE_KEYWORDS:
        if strip_diacritics(keyword) in clean:
            return grade, keyword

    return None, None


def apply_book_default(grade_en, grade_ar, source):
    """If extract_grade() returned unknown/None, fall back to book membership.
    Call this AFTER extract_grade(), passing its results + the source key.
    Returns (grade_en, grade_ar).
    """
    if grade_en and grade_en != 'unknown':
        return grade_en, grade_ar
    if source in BOOK_GRADE_DEFAULTS:
        return BOOK_GRADE_DEFAULTS[source]
    return grade_en or 'unknown', grade_ar or ''


def is_valid_name(name):
    """Check if a string looks like a valid Arabic narrator name."""
    if not name or len(name.strip()) < 3:
        return False
    if not re.search(r'[\u0600-\u06FF]', name):
        return False
    if name.startswith('بن '):
        return False  # missing personal name
    first = name.split()[0]
    if first in ABD_COMPOUNDS:
        return False  # truncated عبد
    if re.match(r'^\d', name):
        return False
    if name.startswith('-') or name.startswith('#'):
        return False
    return True


def fix_abd_compound(name):
    """Restore عبد to truncated compound names."""
    if not name:
        return name
    first = name.split()[0] if name.split() else ''
    if first in ABD_COMPOUNDS:
        return 'عبد ' + name
    return name


def strip_book_prefix(text):
    """Strip sigla and book name expansions from a Tahdhib-style header.
    Returns the narrator name portion.

    Key challenge: some words are both book names AND personal names.
      - 'مسلم' = Muslim (Sahih Muslim) AND a common first name
      - 'مالك' = Muwatta Malik AND a common first name
    Rule: these are book names ONLY when NOT followed by 'بن' (patronymic).
    If followed by 'بن', it's a person's name.

    Secondary challenge: 'بن ماجة' looks like a patronymic but is a book.
    Rule: 'بن ماجة' is a book name because 'ماجة' never appears as a
    standalone patronym in the hadith corpus.  It is safe to always strip.
    """
    # Strip leading entry number and ms-page markers
    text = re.sub(r'^\d+\s+', '', text.strip())
    text = re.sub(r'ms\d+\s*', '', text)
    # Normalize whitespace (double spaces break regex matching)
    text = re.sub(r'\s+', ' ', text).strip()

    # Ambiguous words: book names that are ALSO common personal names.
    # Only strip these when NOT followed by 'بن'.
    AMBIGUOUS = {'مسلم', 'مالك'}

    # Safe book names (never personal names)
    SAFE_BOOKS = [b for b in BOOK_NAMES if b.split()[0] not in AMBIGUOUS]
    # Ambiguous book names -- only match if NOT followed by 'بن'
    AMBIG_BOOKS = [b for b in BOOK_NAMES if b.split()[0] in AMBIGUOUS]

    sigla_pattern = '|'.join(re.escape(s) for s in sorted(SIGLA, key=len, reverse=True))
    safe_book_pattern = '|'.join(re.escape(b) for b in SAFE_BOOKS)
    # Ambiguous: match only if followed by و or end-of-prefix (not بن)
    ambig_book_pattern = '|'.join(re.escape(b) + r'(?=\s+(?:و|$))' for b in AMBIG_BOOKS)

    all_books = safe_book_pattern
    if ambig_book_pattern:
        all_books += '|' + ambig_book_pattern

    prefix_re = re.compile(
        r'^(?:(?:' + sigla_pattern + r')\s+)*'
        r'(?:(?:' + all_books + r')\s*)*'
    )

    m = prefix_re.match(text)
    if m and m.end() > 0:
        text = text[m.end():].lstrip()

    # Second pass: handle standalone ambiguous book names after sigla stripping.
    # After stripping 'م ', we may be left with 'مسلم أحمد بن ...'
    # If an ambiguous word is followed by a proper name (not بن), it's a book.
    for amb in AMBIGUOUS:
        if text.startswith(amb + ' '):
            after = text[len(amb):].lstrip()
            if not after.startswith('بن ') and not after.startswith('بن\t'):
                text = after
                break

    return text


def truncate_at_biography(name):
    """Truncate a name string at the EARLIEST biographical marker.
    Checks all patterns and cuts at the one that appears first in the text."""
    all_patterns = CITATION_VERBS + EDITORIAL_MARKERS + ISNAD_VERBS + TRANSMISSION_VERBS + BIO_VERBS + ALTERNATE_MARKERS

    earliest_pos = len(name)
    for pattern in all_patterns:
        m = re.search(r'[،,]?\s*' + pattern, name)
        if m and m.start() > 5 and m.start() < earliest_pos:
            earliest_pos = m.start()

    if earliest_pos < len(name):
        name = name[:earliest_pos].rstrip('،, ')

    return name


def clean_narrator_name(raw_name, has_sigla_prefix=False):
    """The master cleaning function. Every parser calls this after
    extracting the raw name from its source-specific format.

    Pipeline:
      1. Strip OpenITI markers (#, PageV, ms)
      2. Strip book sigla + name prefixes (if Tahdhib-family)
      3. Strip leading junk (dashes, numbers, brackets)
      4. Truncate at biographical prose markers
      5. Strip alternate name forms (ويقال, وهو)
      6. Restore عبد compounds
      7. Strip trailing punctuation
      8. Validate

    Args:
        raw_name: The raw extracted name string
        has_sigla_prefix: True for Tahdhib-family texts that have
                         sigla + book names before the narrator name

    Returns:
        Cleaned name string, or empty string if invalid
    """
    name = raw_name
    if not name:
        return ''

    # 1. Strip OpenITI markers
    name = re.sub(r'^#\s+', '', name)
    name = name.replace('#', '')
    name = re.sub(r'PageV\d+P\d+', '', name)
    name = re.sub(r'ms\d+', '', name)

    # 2. Strip book sigla prefix (Tahdhib-family texts)
    if has_sigla_prefix:
        name = strip_book_prefix(name)

    # 3. Strip editorial annotations from critical editions
    # [الحمصي - 6] -> الحمصي   (manuscript variant footnotes)
    # (32 م) -> ''              (reference markers)
    name = re.sub(r'\[\s*([^\]]*?)\s*-\s*\d+\s*\]', r'\1', name)
    name = re.sub(r'\(\s*\d+\s*[مكدق]?\s*\)', '', name)
    name = re.sub(r'^\(\s*\d+\s*[مكدق]?\s*\)\s*', '', name)

    # 4. Strip sigla prefix + colon: "م د ت ن: NAME" or "بخ: NAME"
    #    Also handles "خ NAME" (sigla without colon)
    name = re.sub(r'^[خمدتسقعبرفهصنل\s]+:\s*', '', name)
    # Standalone single-letter sigla at start: "خ صدقة" -> "صدقة"
    # But NOT "خالد" or "محمد" (only match if next char is space)
    name = re.sub(r'^([خمدتسقعبرفهصن])\s+(?=[^\s])', '', name)

    # 5. Strip evaluation text after colon in name
    # "أحمد بن الأزهر: حافظ، ثقة" -> "أحمد بن الأزهر"
    # Only strip if what follows the colon looks like an evaluation, not a name
    colon_m = re.search(r':\s+', name)
    if colon_m and colon_m.start() > 5:
        after_colon = name[colon_m.end():]
        # It's evaluation text if it starts with a grade word, "قال", or bio marker
        if re.match(r'(?:ثقة|صدوق|ضعيف|متروك|كذاب|مجهول|حافظ|مقبول|واه|وسط|مقل|لم |لا |قال |كان |له |من |عن |محدث|صاحب)', after_colon):
            name = name[:colon_m.start()]
        # Or if what follows is a name (starts with proper name), keep it — it's a kunya/alias
        # "أحمد بن أبي الطيب: سليمان" -> keep both (alias)

    # 6. Strip leading conjunction prefix: "والحافظ X" -> "الحافظ X", "وأحمد" -> "أحمد"
    if name.startswith('و') and len(name) > 2:
        after_waw = name[1:]
        # Only strip if what follows is a name-like word, not "وهب"/"واصل" etc
        first_after = after_waw.split()[0] if after_waw.split() else ''
        REAL_WAW_NAMES = {'هب', 'اصل', 'ائل', 'كيع', 'اقد', 'اسط', 'اثلة', 'الد', 'رقاء', 'حشي', 'ابصة'}
        if first_after and first_after not in REAL_WAW_NAMES and not after_waw.startswith('و'):
            name = after_waw

    # 7. Strip leading junk
    name = re.sub(r'^[-–—]+\s*', '', name)
    name = re.sub(r'^\(\s*\d+\s*[تقخمدسعم\s]*\d*\s*\)\s*', '', name)
    name = re.sub(r'^\d+\s*\|\s*', '', name)
    name = re.sub(r'^\d+\s*-\s*', '', name)
    name = re.sub(r'^\d+\s*:\s*', '', name)
    name = re.sub(r'^(?:ترجمة|وفاة|وفيات|موت|ذكر|بقية)\s+', '', name)
    name = re.sub(r'^(?:أخوه|أخته|ابنه|ابنته|أمه|أبوه)\s*:\s*', '', name)
    # Strip orphan brackets
    name = re.sub(r'^\[\s*', '', name)
    name = re.sub(r'^\(\s*', '', name)
    # Strip inline sigla parens: (ق), (ع), (صح)
    name = re.sub(r'\(\s*[خمدتسقعبرف\s.صح]+\s*\)', '', name)
    name = re.sub(r'\(\s*\d+\s*[تقخمدسعم\s]*\d*\s*\)', '', name)
    # Strip [الوفاة: ...]
    name = re.sub(r'\[\s*الوفاة[^\]]*\]', '', name)
    name = re.sub(r'[\[\(]\s*\d+\s*[\]\)]', '', name)

    # 4. Truncate at biographical prose
    name = truncate_at_biography(name)

    # 5. Strip alternate name forms
    name = re.sub(r'\s*[،,]\s*ويقال\s*:?\s*.*$', '', name)
    name = re.sub(r'\s*[،,]\s*وقيل\s*:?\s*.*$', '', name)
    name = re.sub(r'\s+ويقال\s*:?\s*.*$', '', name)
    name = re.sub(r'\s+وقيل\s*:?\s*.*$', '', name)
    name = re.sub(r'[،,]\s*وهو\s+.*$', '', name)
    name = re.sub(r'\s+وهو\s+', ' ', name)  # if in the middle, just remove

    # 6. Restore عبد compounds
    name = fix_abd_compound(name)

    # 7. Strip trailing punctuation and orphan brackets
    name = re.sub(r'\s*\(\s*[خمدتسقعبرف\s،,]*$', '', name)
    name = re.sub(r'\s*\)\s*$', '', name)
    name = re.sub(r'\s*\]\s*$', '', name)
    name = name.rstrip('،,.: ')

    # 8. Normalize whitespace
    name = re.sub(r'\s+', ' ', name).strip()

    # 9. Remove HTML/stray markup and symbols
    name = re.sub(r'<[^>]+>', '', name)
    name = name.replace('@', '').replace('$', '').replace('*', '')

    # 10. Strip trailing digits (death years, ref numbers leaked into name)
    name = re.sub(r'\s+\d{1,4}\s*$', '', name)
    # Strip sigla-colon at start (سعيد: عن أبي → سعيد)
    name = re.sub(r':\s+عن\s.*$', '', name)
    name = re.sub(r':\s+(?:محدث|مجهول|مقبول|وسط|مقل|لا يعرف).*$', '', name)

    # 10. Length limit (names > 120 chars have prose leak)
    if len(name) > 120:
        cut = name[:120].rfind('،')
        if cut > 40:
            name = name[:cut].strip()
        else:
            name = name[:120].strip()

    # 11. Validate
    if not is_valid_name(name):
        # Last resort: try stripping everything before first Arabic
        stripped = re.sub(r'^[^\u0600-\u06FF]+', '', name)
        stripped = re.sub(r'^[تقخمدسعم]\s*\)\s*', '', stripped)
        if is_valid_name(stripped):
            return stripped
        return name  # return anyway, caller can decide

    return name
