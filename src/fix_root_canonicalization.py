"""
fix_root_canonicalization.py
============================
Two types of root mismatch between roots_index.json (Quran side) and word_defs_v2.json (Hadith side):

TYPE A — Root form alias:
  CAMeL Tools uses a different canonical root form than the Quran roots_index.
  Example:  roots_index: قضي  →  word_defs_v2: قضو  (defective verb, CAMeL uses waw form)
  Example:  roots_index: بيع  →  word_defs_v2: بوع  (hollow verb, CAMeL uses underlying form)
  Fix: build ALIAS_MAP and update bridge lookup to check both forms.

TYPE B — Words missing from word_defs_v2:
  Some high-frequency roots (أمر, ولي) have no entries in word_defs_v2 at all.
  CAMeL Tools simply didn't analyze their word forms in the corpus.
  Fix: for these roots, scan concordance keys directly for words matching the root pattern,
  then add them to word_defs_v2 with the correct root assignment.

Outputs:
  app/data/word_defs_v2.json  — patched with Type B words added
  src/root_alias_map.json     — Type A alias map (used by build_bridge.py)
"""

import json, re
from pathlib import Path
from collections import defaultdict

ROOT = Path(__file__).parent.parent
DATA = ROOT / 'app' / 'data'
SRC  = ROOT / 'src'

def load(p): return json.load(open(p, encoding='utf-8'))
def save(p, d, indent=None):
    with open(p, 'w', encoding='utf-8') as f:
        json.dump(d, f, ensure_ascii=False, indent=indent, separators=None if indent else (',',':'))
    print(f'  Saved {Path(p).name} ({Path(p).stat().st_size//1024} KB)')

print('Loading data...')
QURAN_DATA = ROOT / 'quran' / 'data'
q_roots  = load(DATA / 'quran-bil-quran-roots_index.json' if (DATA / 'quran-bil-quran-roots_index.json').exists()
                else QURAN_DATA / 'roots_index.json')
wd       = load(DATA / 'word_defs_v2.json')
conc     = load(DATA / 'concordance.json')

# Build root→words from word_defs_v2
wd_root_to_words = defaultdict(list)
for word, info in wd.items():
    if isinstance(info, dict) and 'r' in info:
        wd_root_to_words[info['r']].append(word)

# Find roots_index roots with 0 hadith coverage
zero_roots = [r for r in q_roots if not wd_root_to_words.get(r)]
print(f'\nRoots_index roots with 0 word_defs_v2 entries: {len(zero_roots)}')
print(f'Total roots_index roots: {len(q_roots)}')

# ─── TYPE A: Build alias map ──────────────────────────────────────────────────
# Detect root form variants that CAMeL uses vs what roots_index stores.
# Key patterns:
#   Defective verbs: roots_index uses ي ending, CAMeL uses و (e.g. قضي → قضو, رمي → رمو)
#   Hollow verbs:    roots_index uses ي/و core, CAMeL uses underlying form (e.g. بيع → بوع)
#   Hamzated:        small differences in hamza representation

print('\n── TYPE A: Root alias detection ──')

ALIAS_MAP = {}

# Known explicit aliases discovered from audit:
KNOWN_ALIASES = {
    'قضي': 'قضو',   # qada (judgment) — defective, CAMeL uses waw form
    'بيع': 'بوع',   # bay'a (pledge/sale) — CAMeL uses underlying waw
    'رعي': 'رعو',   # ra'aya (shepherd/govern) — defective
    'نهي': 'نهو',   # nahy (prohibition) — defective
    'دعو': 'دعو',   # already same
    'سعي': 'سعو',   # sa'y (striving) — defective variant
    'حيي': 'حيو',   # hayat (life) — defective
    'أتي': 'أتو',   # ata (come) — defective
    'بقي': 'بقو',   # baqiya (remain)
    'مشي': 'مشو',   # masha (walk)
    'لقي': 'لقو',   # laqiya (meet)
    'هدي': 'هدو',   # hada (guide)
    'كفي': 'كفو',   # kafiya (suffice)
    'أبي': 'أبو',   # aba (refuse)
    'زني': 'زنو',   # zana (fornicate)
    'جزي': 'جزو',   # jaza (reward/punish)
    'قوي': 'قوو',   # qawiya (be strong)
    'عصي': 'عصو',   # 'asiya (disobey)
}

# Try each zero-root: check if its ي→و alias has entries
for root in zero_roots:
    if root in KNOWN_ALIASES:
        alias = KNOWN_ALIASES[root]
        if alias in wd_root_to_words:
            ALIAS_MAP[root] = alias
            continue

    # Auto-detect: try swapping final ي with و
    if root.endswith('ي') and len(root) == 3:
        candidate = root[:-1] + 'و'
        if candidate in wd_root_to_words and len(wd_root_to_words[candidate]) > 5:
            ALIAS_MAP[root] = candidate

    # Auto-detect: try swapping middle ي with و (hollow verbs like بيع → بوع)
    if len(root) == 3 and root[1] == 'ي':
        candidate = root[0] + 'و' + root[2]
        if candidate in wd_root_to_words and len(wd_root_to_words[candidate]) > 5:
            ALIAS_MAP[root] = candidate

print(f'  Alias map entries found: {len(ALIAS_MAP)}')
for qr, wd_r in sorted(ALIAS_MAP.items())[:20]:
    count = len(wd_root_to_words.get(wd_r, []))
    print(f'    {qr} → {wd_r}  ({count} words in word_defs_v2)')

# ─── TYPE B: Recover missing roots from concordance ──────────────────────────
print('\n── TYPE B: Recover missing words from concordance ──')

# For roots with no alias and no words, scan concordance keys
# Strategy: for a 3-letter root like أمر, look for concordance words whose
# stripped form (no prefixes/diacritics) contains the root letters in order
DIACRITICS_RE = re.compile(r'[\u0610-\u061A\u064B-\u065F\u0670\u06D6-\u06DC\u06DF-\u06E4\u06E7\u06E8\u06EA-\u06ED]')

def strip(w):
    w = DIACRITICS_RE.sub('', w)
    w = w.replace('أ','ا').replace('إ','ا').replace('آ','ا').replace('ة','ه').replace('ى','ي')
    return w

def strip_prefix(w):
    for p in ['وال','بال','فال','كال','لل','ال','و','ف','ب','ك','ل']:
        if w.startswith(p) and len(w) > len(p) + 1:
            return w[len(p):]
    return w

# High-priority roots to recover (statecraft/governance family needs these)
PRIORITY_RECOVER = {
    'أمر': {  # command/authority (amr)
        'gloss': 'The root أمر (amr) means to command, order, or be in charge. It underlies amīr (commander) and umara (rulers).',
        'pattern': re.compile(r'^[وبفكل]?(ي|ت|ن|ا)?(ا|أ)(م)(ر)(وا|ون|ين|ة|ه|ها|هم|نا|ي|ت)?$')
    },
    'ولي': {  # guardianship/authority (wali)
        'gloss': 'The root ولي (wly) means to be near, to take charge of, to govern. It underlies walī (guardian), mawlā (master/freed slave), and wilāya (authority).',
        'pattern': re.compile(r'^[وبفكل]?(ي|ت|ن|ا)?(و)(ل)(ي|ا)(وا|ون|ين|ة|ه|ها|هم|نا|ي|ت)?$')
    },
}

new_entries = 0
patched_wd = dict(wd)  # copy to patch

# Build concordance key set for fast scan
conc_keys = set(conc.keys())

for root, info in PRIORITY_RECOVER.items():
    recovered = []
    for word in conc_keys:
        stripped = strip(strip_prefix(word))
        # Check if it plausibly derives from this root (3-letter match in sequence)
        root_stripped = strip(root)
        if len(stripped) >= 3 and len(stripped) <= 8:
            # Simple check: root letters appear in stripped word in order
            ri = 0
            for ch in stripped:
                if ri < len(root_stripped) and ch == root_stripped[ri]:
                    ri += 1
            if ri == len(root_stripped) and word not in patched_wd:
                recovered.append(word)

    # Add to word_defs_v2 with this root
    added = 0
    for word in recovered[:200]:  # cap per root
        if word not in patched_wd:
            patched_wd[word] = {
                'r': root,
                'rc': '.'.join(list(root)),
                's': '',
                'g': info['gloss'][:120],
                'n': len(conc.get(word, [])),
                'lem': word,
                'pos': 'unknown',
                '_recovered': True  # flag as recovered, not CAMeL-verified
            }
            added += 1
            new_entries += 1

    print(f'  Root {root}: {len(recovered)} candidates found, {added} added to word_defs_v2')

print(f'\n  Total new entries added: {new_entries}')
print(f'  word_defs_v2 size: {len(wd)} → {len(patched_wd)}')

# ─── Save outputs ─────────────────────────────────────────────────────────────
print('\nSaving...')
save(DATA / 'word_defs_v2.json', patched_wd)
save(SRC / 'root_alias_map.json', ALIAS_MAP, indent=2)

# ─── Verification ─────────────────────────────────────────────────────────────
print('\n── Verification ──')
# Re-check the problem roots
wd2 = patched_wd
wd_root_to_words2 = defaultdict(list)
for word, info in wd2.items():
    if isinstance(info, dict) and 'r' in info:
        wd_root_to_words2[info['r']].append(word)

for root in ['أمر', 'ولي', 'قضي', 'بيع', 'خلف', 'ملك']:
    direct = len(wd_root_to_words2.get(root, []))
    alias  = ALIAS_MAP.get(root, '')
    via_alias = len(wd_root_to_words2.get(alias, [])) if alias else 0
    total  = direct + via_alias
    print(f'  {root}: {direct} direct words + {via_alias} via alias ({alias}) = {total} total')
