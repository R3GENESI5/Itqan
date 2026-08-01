"""Closed-class Arabic function words, with their clitic combinations.

Particles, prepositions and pronouns are a finite, unchanging set, and they are
also the most frequent words on the page — but they are exactly where a
context-free morphological analyser goes wrong, because a form like وقد is
equally well read as "and + already" or as the verb "to ignite". Deriving them
from the closed class instead of guessing gets the common case right.

Everything here composes mechanically from a small base; nothing downstream is
hand-listed per surface form.
"""

# preposition → (bound stem used before a pronoun, gloss, root)
PREPS = {
    'من':   ('من',  'from',            None),
    'إلى':  ('إلي', 'to, towards',     None),
    'الى':  ('الي', 'to, towards',     None),
    'عن':   ('عن',  'about, from',     None),
    'على':  ('علي', 'upon, on',        'علو'),
    'فى':   ('في',  'in',              None),
    'في':   ('في',  'in',              None),
    'مع':   ('مع',  'with',            None),
    'عند':  ('عند', 'at, with',        'عند'),
    'لدى':  ('لدي', 'at, with',        None),
    'بين':  ('بين', 'between',         'بين'),
    'قبل':  ('قبل', 'before',          'قبل'),
    'بعد':  ('بعد', 'after',           'بعد'),
    'دون':  ('دون', 'without, below',  'دون'),
    'مثل':  ('مثل', 'like, such as',   'مثل'),
    'نحو':  ('نحو', 'like, toward',    'نحو'),
    'حول':  ('حول', 'around',          'حول'),
    'تحت':  ('تحت', 'under',           'تحت'),
    'فوق':  ('فوق', 'above',           'فوق'),
}

# single-letter prepositions, which bind directly
LETTER_PREPS = {
    'ب': ('with, by',  None),
    'ل': ('for, to',   None),
    'ك': ('like, as',  None),
}

PRON = {
    'ه':   'him, it',
    'ها':  'her, it, them',
    'هم':  'them',
    'هن':  'them (f.)',
    'هما': 'them (both)',
    'ك':   'you',
    'كم':  'you (pl.)',
    'كن':  'you (f.pl.)',
    'كما': 'you (both)',
    'نا':  'us',
    'ي':   'me',
}

CONJ = {'و': 'and', 'ف': 'so, then'}

# Bare particles and pronouns: form → (gloss, root)
PARTICLES = {
    'قد':    ('already, indeed', None),
    'لقد':   ('indeed, certainly', None),
    'إن':    ('indeed; if', None),
    'ان':    ('indeed; that; if', None),
    'أن':    ('that', None),
    'إنما':  ('only, rather', None),
    'أنما':  ('that which', None),
    'لكن':   ('but, however', None),
    'لكنه':  ('but + it/he', None),
    'بل':    ('rather, but', None),
    'لا':    ('no, not', None),
    'ما':    ('not; what; that which', None),
    'لم':    ('did not', None),
    'لن':    ('will not', None),
    'لما':   ('when; not yet', None),
    'هل':    ('is it? do?', None),
    'أفلا':  ('do they not then?', None),
    'افلا':  ('do they not then?', None),
    'أولا':  ('is it not?', None),
    'أفلم':  ('have they not then?', None),
    'أولم':  ('have they not?', None),
    'أليس':  ('is it not?', None),
    'اليس':  ('is it not?', None),
    'ألا':   ('lo!, is it not', None),
    'أفهل':  ('is it then that?', None),
    'أوليس': ('is it not?', None),
    'أم':    ('or', None),
    'أو':    ('or', None),
    'او':    ('or', None),
    'ثم':    ('then, thereafter', None),
    'إذا':   ('when, if', None),
    'اذا':   ('when, if', None),
    'إذ':    ('when, since', None),
    'اذ':    ('when, since', None),
    'حتى':   ('until, even', None),
    'كي':    ('so that', None),
    'لو':    ('if (hypothetical)', None),
    'لولا':  ('were it not for', None),
    'إلا':   ('except, unless', None),
    'الا':   ('except, unless', None),
    'غير':   ('other than, not', 'غير'),
    'سوى':   ('other than', 'سوي'),
    'أي':    ('that is, namely', None),
    'اي':    ('that is, namely', None),
    'أيضا':  ('also', None),
    'ايضا':  ('also', None),
    'نعم':   ('yes', None),
    'بلى':   ('yes indeed', None),
    'كل':    ('all, every', 'كلل'),
    'بعض':   ('some, part of', 'بعض'),
    'كما':   ('as, just as', None),
    'مما':   ('from what, than', None),
    'عما':   ('about what', None),
    'فيما':  ('in what, while', None),
    'بما':   ('by what, because', None),
    'لما':   ('when; not yet', None),
    'إنه':   ('indeed + he/it', None),
    'أنه':   ('that + he/it', None),
    'وأن':   ('and + that', None),
    'كذا':   ('thus, such', None),
    'كذلك':  ('likewise, thus', None),
    'هكذا':  ('thus, like this', None),
    'حيث':   ('where, since', None),
    'إذن':   ('then, therefore', None),
    'قط':    ('ever, at all', None),
    'أما':   ('as for', None),
    'اما':   ('as for; either', None),
    'لعل':   ('perhaps', None),
    'ليت':   ('would that', None),
    'كأن':   ('as though', None),
    'كان':   ('was, used to be', 'كون'),
    'يكون':  ('is, becomes', 'كون'),
    'ليس':   ('is not', 'ليس'),
    # relatives
    'الذي':   ('who, which (m.sg.)', None),
    'التي':   ('who, which (f.sg.)', None),
    'الذين':  ('those who (m.pl.)', None),
    'اللذان': ('the two who', None),
    'اللاتي': ('those who (f.pl.)', None),
    'اللائي': ('those who (f.pl.)', None),
    'من':     ('from; who, whoever', None),
    # demonstratives
    'هذا':    ('this (m.)', None),
    'هذه':    ('this (f.)', None),
    'هذان':   ('these two', None),
    'ذلك':    ('that (m.)', None),
    'تلك':    ('that (f.)', None),
    'ذاك':    ('that', None),
    'هؤلاء':  ('these (pl.)', None),
    'أولئك':  ('those (pl.)', None),
    'اولئك':  ('those (pl.)', None),
    'هنا':    ('here', None),
    'هناك':   ('there', None),
    # personal pronouns
    'هو':   ('he, it', None),
    'هي':   ('she, it', None),
    'هما':  ('they (both)', None),
    'هم':   ('they', None),
    'هن':   ('they (f.)', None),
    'أنت':  ('you', None),
    'انت':  ('you', None),
    'أنتم': ('you (pl.)', None),
    'انتم': ('you (pl.)', None),
    'أنا':  ('I', None),
    'انا':  ('I', None),
    'نحن':  ('we', None),
    'إياه': ('him (object)', None),
}


def build():
    """Return {surface form: (gloss, root)} for the whole closed class."""
    out = {}

    def put(form, gloss, root):
        out.setdefault(form, (gloss, root))

    # bare particles, plus و/ف prefixed versions
    for form, (gloss, root) in PARTICLES.items():
        put(form, gloss, root)
        for c, cg in CONJ.items():
            put(c + form, f'{cg} + {gloss}', root)

    # prepositions: bare, + pronoun, each optionally و/ف prefixed
    for prep, (bound, gloss, root) in PREPS.items():
        forms = [(prep, gloss)]
        for p, pg in PRON.items():
            # ل + ه is written له, not لهه; the bound stem already handles على/إلى
            forms.append((bound + p, f'{gloss} + {pg}'))
        for form, g in forms:
            put(form, g, root)
            for c, cg in CONJ.items():
                put(c + form, f'{cg} + {g}', root)

    # single-letter prepositions before a pronoun (به، له، بها، لهم …)
    for lp, (gloss, root) in LETTER_PREPS.items():
        for p, pg in PRON.items():
            if p == lp:                     # بب / لل do not occur
                continue
            put(lp + p, f'{gloss} + {pg}', root)
            for c, cg in CONJ.items():
                put(c + lp + p, f'{cg} + {gloss} + {pg}', root)

    return out


TABLE = build()

if __name__ == '__main__':
    t = build()
    print(len(t), 'function-word forms')
    for k in ['وقد', 'منه', 'عليه', 'فيها', 'به', 'لهم', 'وإذا', 'فلما', 'إليه', 'ولكن']:
        print(f'  {k:8} → {t.get(k)}')
