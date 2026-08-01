#!/usr/bin/env python3
"""Turn pages.jsonl into the /ruh/ reader's data files.

Layout produced under OUT:
  index.json              surah list → sections, with page ranges
  lex.json                core lexicon (frequent word types) → "gloss|root"
  sec/<surah>-<a1>.json   one section: its pages, plus a `lex` of rare types
  front.json              front matter (author's introduction)
"""
import json, os, re, sys, collections

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from gloss import Glosser, norm, LOOKUP

IN = 'pages.jsonl'
OUT = sys.argv[1] if len(sys.argv) > 1 else 'out'
ROOTS = '/Users/landiq/Documents/Ruh ul Maani/itqan-ref/quran/data/roots_index.json'
SURAH_LIST = '/Users/landiq/Documents/Ruh ul Maani/itqan-ref/quran/data/surah_list.json'

CORE_TYPES = 30000          # most frequent types shipped upfront
CHUNK_PAGES = 8             # book pages rendered at once

# Shamela's own section headings, re-parsed here rather than at scrape time so
# the regex can be fixed without re-fetching 6,473 pages. Both الآيات ١ إلى ٥
# and the article-less singular آية ٢٣ occur.
AR_DIGITS = str.maketrans('٠١٢٣٤٥٦٧٨٩', '0123456789')
SEC_RE = re.compile(
    r'\[سورة\s+(.+?)\s*\((\d+)\)\s*:\s*(?:ال)?آي(?:ات|ة|تان|تين)\s*'
    r'(\d+)(?:\s*(?:إلى|الى|و)\s*(\d+))?\]')


def parse_sec(sec):
    """'[سورة النور (٢٤) : الآيات ٢٩ إلى ٣٣]' → (24, 29, 33)."""
    if not sec:
        return None
    m = SEC_RE.search(sec.translate(AR_DIGITS))
    if not m:
        return None
    a1 = int(m.group(3))
    return int(m.group(2)), a1, int(m.group(4) or a1)

# Must include the diacritics, or vocalised Qur'anic quotations get split into
# single letters. Kept identical to RE_SPLIT in ruh/js/app.js.
WORD = re.compile('[ء-ْٰٱ]+')


SENSE_PATTERNS = [
    r'primarily means[:\s]+"([^"]+)"',
    r'primarily means[:\s]+to ([^.,;]+)',
    r'primarily means[:\s]+([^.]+)',
    r'means[:\s]+"([^"]+)"',
    r'means[:\s]+to ([^.,;]+)',
    r'centers on ([^.]+)',
    r'relates to ([^.]+)',
    r'signifies ([^.]+)',
    r'denotes ([^.]+)',
]


def short_sense(meaning, limit=70):
    """Pull a one-line English sense out of a root's prose description.

    Mirrors extractGloss() in the Quran app so the tooltip and the root panel
    agree on what a root 'means'.
    """
    if not meaning:
        return ''
    m = re.sub(r'\s+', ' ', meaning).strip()
    for pat in SENSE_PATTERNS:
        hit = re.search(pat, m, re.I)
        if hit:
            g = hit.group(1).strip().strip('"“”').rstrip('.')
            if g:
                return g[:limit - 1] + '…' if len(g) > limit else g
    first = re.split(r'[.!]', m)[0].strip()
    return first[:limit - 1] + '…' if len(first) > limit else first


def load_pages():
    recs = []
    with open(IN, encoding='utf-8') as f:
        for line in f:
            try:
                recs.append(json.loads(line))
            except Exception:
                pass
    recs.sort(key=lambda r: r['id'])
    return recs


def group_sections(recs):
    """Consecutive pages sharing a section title form one reading unit.

    Keyed by (surah, first ayah) rather than the full range, because that pair
    is what the URL hash and the section filename use — if the same opening
    ayah turns up again later (a split range, or a passage continued across a
    volume break) its pages have to join the existing unit instead of
    overwriting it.
    """
    sections, front, by_key = [], [], {}
    cur = None
    for r in recs:
        if not r.get('paras'):
            continue
        page = {'v': r.get('vol'), 'p': r.get('pg'), 't': r['paras']}
        parsed = parse_sec(r.get('sec'))
        if not parsed:
            front.append(page)
            continue
        surah, a1, a2 = parsed
        key = (surah, a1)
        if cur is None or cur['key'] != key:
            cur = by_key.get(key)
            if cur is None:
                cur = {'key': key, 's': surah, 'a1': a1, 'a2': a2,
                       'sec': r.get('sec', ''), 'pages': []}
                by_key[key] = cur
                sections.append(cur)
        cur['a2'] = max(cur['a2'], a2)
        cur['pages'].append(page)
    return sections, front


def main():
    os.makedirs(f'{OUT}/sec', exist_ok=True)
    recs = load_pages()
    print(f'{len(recs)} pages')
    sections, front = group_sections(recs)
    print(f'{len(sections)} sections, {len(front)} front-matter pages')

    # ── vocabulary ──────────────────────────────────────────
    freq = collections.Counter()
    for sec in sections:
        for pg in sec['pages']:
            for para in pg['t']:
                freq.update(norm(w) for w in WORD.findall(para))
    for pg in front:
        for para in pg['t']:
            freq.update(norm(w) for w in WORD.findall(para))
    print(f'{len(freq)} distinct word types, {sum(freq.values())} tokens')

    G = Glosser(ROOTS)

    # Every clickable root must have a panel behind it. The curated tables name
    # some roots that are perfectly real but never occur in the Qur'an
    # (حذف, نظم …), so drop those rather than offer a dead link.
    dropped = 0
    for k, (gl, rt) in list(LOOKUP.items()):
        if rt and rt not in G.roots:
            LOOKUP[k] = (gl, None)
            dropped += 1
    print(f'{dropped} curated entries had a non-Quranic root, kept gloss only')

    # Surah names are cited constantly ("[النساء: ٨٣]") and are a known finite
    # set, so take them from the Quran app's own list rather than guessing.
    surahs = json.load(open(SURAH_LIST, encoding='utf-8'))
    for s in surahs:
        name = norm(s['name_ar'])
        gl = f"Surah {s['name_en']} ({s['number']}) — {s['name_translation']}"
        LOOKUP.setdefault(name, (gl, None))
        LOOKUP.setdefault('ال' + name, (gl, None))

    print('learning lemma priors from the corpus...', flush=True)
    prior = G.build_prior(freq)
    print(f'  {len(prior)} lemmas with unambiguous corpus evidence')

    lex = {}
    hits = 0
    for i, (w, _) in enumerate(freq.most_common()):
        if i % 25000 == 0:
            print(f'  glossing {i}/{len(freq)}', flush=True)
        r = G.lookup(w)
        if not r:
            continue
        g, root = r
        g = g.replace('|', '/')
        lex[w] = f'{g}|{root}' if root else g
        hits += 1
    cov_types = hits / max(1, len(freq))
    cov_tok = sum(c for w, c in freq.items() if w in lex) / max(1, sum(freq.values()))
    print(f'glossed {hits} types — type coverage {cov_types:.1%}, token coverage {cov_tok:.1%}')

    core_words = {w for w, _ in freq.most_common(CORE_TYPES) if w in lex}
    core = {w: lex[w] for w in core_words}
    json.dump(core, open(f'{OUT}/lex.json', 'w', encoding='utf-8'),
              ensure_ascii=False, separators=(',', ':'))
    core_tok = sum(freq[w] for w in core_words) / max(1, sum(freq.values()))
    print(f'core lexicon: {len(core)} types covering {core_tok:.1%} of tokens')

    # ── section files ───────────────────────────────────────
    # Shamela's table of contents is uneven — An-Nahl has a single node
    # spanning 113 book pages. A passage is still the unit that an ayah
    # reference resolves to, but it is written out in chunks of at most
    # CHUNK_PAGES so no single fetch or render is unbounded.
    index = collections.defaultdict(list)
    biggest = 0
    for sec in sections:
        pages = sec['pages']
        chunks = [pages[i:i + CHUNK_PAGES] for i in range(0, len(pages), CHUNK_PAGES)]
        for k, chunk in enumerate(chunks):
            rare = {}
            for pg in chunk:
                for para in pg['t']:
                    for w in WORD.findall(para):
                        n = norm(w)
                        if n in lex and n not in core_words:
                            rare[n] = lex[n]
            name = f"{sec['s']}-{sec['a1']}-{k}"
            path = f'{OUT}/sec/{name}.json'
            json.dump({'s': sec['s'], 'a1': sec['a1'], 'a2': sec['a2'],
                       'sec': sec['sec'], 'k': k, 'of': len(chunks),
                       'pages': chunk, 'lex': rare},
                      open(path, 'w', encoding='utf-8'),
                      ensure_ascii=False, separators=(',', ':'))
            biggest = max(biggest, os.path.getsize(path))
        index[sec['s']].append({
            'a1': sec['a1'], 'a2': sec['a2'],
            'v': pages[0]['v'], 'p': pages[0]['p'],
            'n': len(pages), 'c': len(chunks),
        })
    print(f'largest chunk file: {biggest // 1024} KB')

    json.dump({'v': sys.argv[2] if len(sys.argv) > 2 else '1',
               'pages': front},
              open(f'{OUT}/front.json', 'w', encoding='utf-8'),
              ensure_ascii=False, separators=(',', ':'))

    # Root → short English sense, so the tooltip can say what the root means
    # without pulling in the whole 750KB roots index.
    roots_index = json.load(open(ROOTS, encoding='utf-8'))
    roots_out = {}
    for r, d in roots_index.items():
        g = short_sense(d.get('m') or '')
        if g:
            roots_out[r] = g
    json.dump(roots_out, open(f'{OUT}/roots.json', 'w', encoding='utf-8'),
              ensure_ascii=False, separators=(',', ':'))
    print(f'root senses: {len(roots_out)}')

    json.dump({
        'surahs': [{
            'n': s['number'], 'ar': s['name_ar'], 'en': s['name_en'],
            'tr': s['name_translation'], 'vc': s['verses_count'],
            'sec': index.get(s['number'], []),
        } for s in surahs],
        'stats': {'pages': len(recs), 'sections': len(sections),
                  'types': len(freq), 'tokens': sum(freq.values()),
                  'glossed_types': hits, 'token_coverage': round(cov_tok, 4)},
    }, open(f'{OUT}/index.json', 'w', encoding='utf-8'),
        ensure_ascii=False, separators=(',', ':'))

    # ── sanity report ───────────────────────────────────────
    missing = [s['number'] for s in surahs if not index.get(s['number'])]
    if missing:
        print(f'WARNING: no sections for surahs {missing}')

    gaps = []
    for s in surahs:
        secs = sorted(index.get(s['number'], []), key=lambda x: x['a1'])
        if not secs:
            continue
        covered, expect = set(), s['verses_count']
        for x in secs:
            covered.update(range(x['a1'], min(x['a2'], expect) + 1))
        miss = expect - len(covered)
        if miss:
            gaps.append((s['number'], s['name_en'], miss, expect))
    if gaps:
        print(f'{len(gaps)} surahs with uncovered ayat (reader falls back to the '
              f'nearest following passage):')
        for n, name, miss, tot in gaps[:10]:
            print(f'  {n:3} {name:<18} {miss}/{tot} ayat not in any passage range')
    else:
        print('every ayah of every surah falls inside a passage range')
    print('done')


if __name__ == '__main__':
    main()
