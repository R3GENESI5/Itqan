"""Structured Buckwalter/AraMorph analysis → (English gloss, Arabic root).

pyaramorph's own analyze_word() returns preformatted strings; this walks the
same prefix/stem/suffix tables but keeps the fields, so we can rank analyses
and derive a root from the lemma id.
"""
import re, json, math, functools, collections
import pyaramorph
from pyaramorph import buckwalter
from function_words import TABLE as FUNCTION_WORDS
from tafsir_terms import TABLE as TAFSIR_TERMS

DIAC = re.compile('[ً-ْٰـ]')

# Buckwalter marks form I — the plain triliteral verb — by carrying the
# imperfect vowel in the lemma id: xaraj-u_1, jAz-u_1, kafar-i_1. Derived
# forms (II, IV, VII…) never do: xar~aj_1, >axoraj_1, kaf~ar_1.
FORM_I = re.compile(r'-[aiu]_\d+$')

# A handful of very frequent forms the analyser gets wrong for structural
# reasons (irregular orthography, or a root the Buckwalter lemma hides).
# Kept deliberately small — everything else is derived, not hand-written.
OVERRIDES = {
    'الله':    ('God, Allah', 'أله'),
    'اللهم':   ('O God', 'أله'),
    'لله':     ('to + God', 'أله'),
    'بالله':   ('by + God', 'أله'),
    'والله':   ('and + God', 'أله'),
    'تعالى':   ('exalted be He', 'علو'),
    'سبحانه':  ('glory be to Him', 'سبح'),
    'صلى':     ('may He bless', 'صلو'),
    'رضي':     ('may He be pleased', 'رضو'),
    'وسلم':    ('and + grant peace', 'سلم'),
    # Frequent in this genre in the sense the corpus statistics under-rate:
    # "ذهب الزمخشري إلى" is "held the view", not "gold".
    'ذهب':     ('go; depart; hold the view / gold', 'ذهب'),
    'وذهب':    ('and + go; depart; hold the view', 'ذهب'),
    'رجل':     ('man / leg', 'رجل'),
    'أثر':     ('effect; trace; report / prefer', 'أثر'),
    'الأثر':   ('the + effect; trace; transmitted report', 'أثر'),
}

# The closed class wins over the analyser; these hand-written irregulars win
# over both.
LOOKUP = dict(FUNCTION_WORDS)
LOOKUP.update(TAFSIR_TERMS)
LOOKUP.update(OVERRIDES)

# Clitics that can sit in front of a word. Tried longest-first, and only as a
# fallback, so a real dictionary entry is never overridden by a segmentation.
CLITICS = [
    ('وال', 'and + the'), ('فال', 'so + the'), ('بال', 'with + the'),
    ('كال', 'like + the'), ('لل', 'for the'),
    ('ال', 'the'),
    ('و', 'and'), ('ف', 'so'), ('ب', 'with'), ('ل', 'for'), ('ك', 'like'),
    ('أ', 'is it that'),   # interrogative hamza: أحسبوا "did they reckon"
]

# Hamza is written inconsistently in printed editions: قرىء for قرئ, ينبىء for
# ينبئ. Retry the analysis under the regular spelling before giving up.
SPELLING_FIXES = [
    ('ىء', 'ئ'), ('ىٕ', 'ئ'), ('وء', 'ؤ'), ('ىي', 'ي'),
]

# Endings that carry inflection but not meaning, peeled only as a last resort:
# the accusative tanwin alif (محذوفا) and the feminine ta (مستأنفة).
SUFFIXES = ['ا', 'ة', 'ه']


def norm(w):
    """Strip diacritics/tatweel; keep letters as written."""
    return DIAC.sub('', w).replace('ٱ', 'ا')


class Glosser:
    def __init__(self, roots_index_path=None):
        self.az = pyaramorph.Analyzer()
        self.roots = set()
        if roots_index_path:
            self.roots = set(json.load(open(roots_index_path, encoding='utf-8')))
        self.lemma_prior = {}      # lemma → corpus evidence, see build_prior()
        try:
            from nltk.stem.isri import ISRIStemmer
            self._isri = ISRIStemmer()
        except ImportError:
            self._isri = None

    # ── analysis ────────────────────────────────────────────
    def analyses(self, word_bw):
        out = []
        for prefix, stem, suffix in self.az._build_segments(word_bw):
            for pre in self.az.prefixes[prefix]:
                voc_a, cat_a, gloss_a, pos_a = pre[1:5]
                for st in self.az.stems[stem]:
                    voc_b, cat_b, gloss_b, pos_b, lemma = st[1:]
                    if f'{cat_a} {cat_b}' not in self.az.tableAB:
                        continue
                    for suf in self.az.suffixes[suffix]:
                        voc_c, cat_c, gloss_c, pos_c = suf[1:5]
                        if f'{cat_a} {cat_c}' not in self.az.tableAC:
                            continue
                        if f'{cat_b} {cat_c}' not in self.az.tableBC:
                            continue
                        out.append({
                            'pre': gloss_a, 'stem': gloss_b, 'suf': gloss_c,
                            'lemma': lemma, 'pos': f'{pos_a}{pos_b}{pos_c}',
                            'plen': len(prefix),
                        })
        return out

    def _rank(self, a):
        """Rank candidate readings of an ambiguous form.

        Two signals, in order:

        1. *Verb form.* Buckwalter marks form I — the plain triliteral — by
           carrying the imperfect vowel in the lemma id (`xaraj-u_1`,
           `jAz-u_1`), while derived forms do not (`xar~aj_1`, `>axoraj_1`).
           Form I is the unmarked default, and preferring it is what stops
           يجوز reading as "marry off" instead of "be permissible", خرج as
           "oust" instead of "go out", and كفروا as "atone" instead of
           "disbelieve".
        2. *Corpus usage.* How much of this corpus the lemma accounts for,
           on a log scale so a modestly commoner lemma cannot override (1).
        """
        pos = a['pos']
        # Penalise derived verbs only. Promoting form I above *nouns* as well
        # was tempting but much worse: اسم became "poison" rather than "name",
        # رجل "walk" rather than "man", قلب "overthrow" rather than "heart".
        # Between a form I verb and a noun, corpus usage is the better judge.
        tier = 1 if ('VERB' in pos and not FORM_I.search(a['lemma'])) else 0
        content = 1 if ('NOUN' in pos or 'VERB' in pos or 'ADJ' in pos) else 0
        proper = 1 if 'NOUN_PROP' in pos else 0
        weight = int(math.log10(self.lemma_prior.get(a['lemma'], 0) + 1))
        return (tier, -weight, a['plen'], -content, proper, len(a['stem']))

    # ── root ────────────────────────────────────────────────
    @staticmethod
    def root_from_lemma(lemma):
        """`kaf~_2` → كفف.  Shadda doubles its consonant; long vowels drop."""
        # `katab-u_1` → katab : the trailing -a/-i/-u is the imperfect vowel
        bw = re.sub(r'_\d+$', '', lemma)
        bw = re.sub(r'-[aiuo]+$', '', bw)
        bw = bw.replace('{', 'A')          # alif wasla → plain alif
        ar = buckwalter.buck2uni(bw)
        out = []
        for ch in ar:
            if ch == 'ّ' and out:          # shadda → double previous
                out.append(out[-1])
            elif ch in 'ًٌٍَُِْـٰ':        # incl. U+0670 superscript alif
                continue
            else:
                out.append(ch)
        r = ''.join(out)
        # آ is alif+hamza: keep the hamza, it is usually a radical (قرآن → قرأ)
        r = r.replace('آ', 'أ').replace('ٱ', 'ا')
        return r[:-1] if r.endswith('ة') else r   # صلاة → صلا → صلو

    @staticmethod
    def _variants(r):
        """Spelling variants to try against the Quranic root list.

        Roots there are written with hamza on alif and with the weak radical
        spelled out, while lemma/stemmer output uses whatever the surface form
        happened to carry (ؤ, ء, ى, or a dropped weak letter).
        """
        if not r or len(r) < 2:
            return
        seen, queue = set(), [r]
        # hamza seats all collapse to أ / ء
        for a, b in (('ؤ', 'أ'), ('ئ', 'أ'), ('ء', 'أ'), ('آ', 'أ'), ('ى', 'ي')):
            queue += [x.replace(a, b) for x in list(queue)]
        for cand in queue:
            for v in (cand,
                      cand.replace('أ', 'ا'), cand.replace('ا', 'أ'),
                      cand.replace('ء', 'ي'), cand.replace('ء', 'و')):
                if v in seen:
                    continue
                seen.add(v)
                yield v
                # Only resolve a weak/hamza ending — guessing a third radical
                # for any 3-letter candidate invents wrong roots (ناس → نأي).
                if len(v) == 3 and v[-1] in 'اأء':
                    for w in ('و', 'ي', 'أ'):
                        yield v[:2] + w
                # Hollow roots: form I hides the middle radical behind an alif,
                # so the lemma جاز has to be matched against جوز.
                if len(v) == 3 and v[1] in 'اأى':
                    for w in ('و', 'ي'):
                        yield v[0] + w + v[2]
                if len(v) == 2:
                    yield v + v[-1]

    def root_for(self, analysis):
        """Derive the root from the *lemma*, never the inflected surface form.

        Stemming the surface form yields plausible-looking wrong roots
        (والكفان → كفن), so we only trust the dictionary lemma, and only
        accept a candidate that exists in the Quranic root index.
        """
        lemma = self.root_from_lemma(analysis['lemma'])
        cands = [lemma]
        if self._isri:
            cands.append(self._isri.stem(lemma))

        for c in cands:
            for v in self._variants(c):
                if v in self.roots:
                    return v
        return None

    # ── gloss text ──────────────────────────────────────────
    @staticmethod
    def clean_gloss(g, max_senses=3):
        g = (g or '').strip()
        if not g or g == '___':
            return ''
        g = re.sub(r'\s*<[^>]+>\s*', ' ', g)          # <verb>, <pos> markers
        g = re.sub(r'\s*\([^)]*\)\s*', ' ', g)         # parenthetical notes
        senses = [s.strip() for s in g.split(';') if s.strip()]
        return '; '.join(senses[:max_senses]).strip()

    def compose(self, a, senses=2):
        stem = self.clean_gloss(a['stem'], senses)
        if not stem:
            return ''
        pre = self.clean_gloss(a['pre'], 1)
        suf = self.clean_gloss(a['suf'], 1)
        return ' + '.join(p for p in (pre, stem, suf) if p)

    def compose_ranked(self, ranked):
        """Primary reading, plus the runner-up when it is a real alternative.

        Arabic script drops the vowels that would tell ظهر ("appear") from
        ظَهْر ("back"), and nothing in an isolated word can settle it. Rather
        than commit to one and be confidently wrong, show the best reading and
        the next genuinely different one, and let the sentence decide.
        """
        primary = None
        for a in ranked:
            g = self.compose(a)
            if g:
                primary = (a, g)
                break
        if not primary:
            return '', None

        best, text = primary
        head = self.clean_gloss(best['stem'], 2).lower()
        for a in ranked:
            if a is best or a['lemma'] == best['lemma']:
                continue
            alt = self.clean_gloss(a['stem'], 2)
            # skip near-duplicates ("messenger" under "messenger; apostle")
            if not alt or alt.lower() in head or head.split(';')[0] in alt.lower():
                continue
            return f'{text} / {alt}', best
        return text, best

    # ── corpus prior ────────────────────────────────────────
    def build_prior(self, freq):
        """Learn which lemmas this corpus actually uses.

        Every word form spreads its token count evenly across the lemmas that
        could produce it. Counting only *unambiguous* forms looked stricter but
        was badly biased: a lemma whose every surface form happens to be
        ambiguous scored zero, so جاز ("be permissible") lost يجوز outright to
        the rarer جوّز ("marry off"). Spreading the counts gives every lemma
        evidence proportional to how often its forms actually occur.
        """
        prior = collections.Counter()
        for w, count in freq.items():
            an = self.analyses(buckwalter.uni2buck(w))
            if not an:
                continue
            lemmas = {a['lemma'] for a in an}
            share = count / len(lemmas)
            for lemma in lemmas:
                prior[lemma] += share
        self.lemma_prior = prior
        self.lookup.cache_clear()
        return prior

    # ── public ──────────────────────────────────────────────
    def _analyse(self, w):
        """Best analyser reading of `w`, or None."""
        an = self.analyses(buckwalter.uni2buck(w))
        if not an:
            return None
        an.sort(key=self._rank)
        text, best = self.compose_ranked(an)
        if not text:
            return None
        return text, self.root_for(best)

    @functools.lru_cache(maxsize=400000)
    def lookup(self, word, _depth=0):
        """Normalised Arabic word → (gloss, root) or None."""
        w = norm(word)
        if not w:
            return None

        # 1. curated tables — function words, tafsir vocabulary, irregulars
        if w in LOOKUP:
            return LOOKUP[w]

        # 2. the morphological analyser
        hit = self._analyse(w)
        if hit:
            return hit

        # 3. the same word under a regularised hamza spelling
        for bad, good in SPELLING_FIXES:
            if bad in w:
                fixed = w.replace(bad, good)
                hit = LOOKUP.get(fixed) or self._analyse(fixed)
                if hit:
                    return hit

        # 4. peel affixes and retry — this is what lets the curated tables
        #    cover والزمخشري and بمحذوف without listing every inflected form.
        #    Curated entries are checked across *all* affixes first, so
        #    والسدي resolves as و+السدي (the exegete) rather than وال+سدي
        #    ("the dam"), which the analyser would otherwise win with.
        for curated_only in (True, False):
            if not curated_only and _depth >= 2:
                break
            for pre, pre_gloss in CLITICS:
                if not w.startswith(pre) or len(w) - len(pre) < 2:
                    continue
                stem = w[len(pre):]
                rest = LOOKUP.get(stem) if curated_only \
                    else self.lookup(stem, _depth + 1)
                if rest:
                    return f'{pre_gloss} + {rest[0]}', rest[1]
            # bare word carrying only a suffix (محذوفا، محمدا)
            for suf in SUFFIXES:
                if not w.endswith(suf) or len(w) - len(suf) < 3:
                    continue
                stem = w[:-len(suf)]
                rest = LOOKUP.get(stem) if curated_only \
                    else (self._analyse(stem) if _depth == 0 else None)
                if rest:
                    return rest
        return None
