"""
match_narrator_grades.py
─────────────────────────────────────────────────────────────────────────────
Fuzzy-match isnad narrator names (short/kunya forms extracted by the parser)
against KASHAF's full biographical names, then update isnad_graph.json with
matched grades so the Sankey visualizer shows coloured nodes.

Strategy (in order of confidence):
  1. Exact match on normalized primary name (before first colon)
  2. Multi-token: all non-trivial tokens of the isnad name appear in the
     KASHAF primary name  (e.g. "عبد الرحمن بن مهدي")
  3. Kunya: isnad name starts with أبو/أبي/ابو — match KASHAF entries that
     contain the same kunya word (e.g. "أبي هريرة" → "أبو هريرة")
  4. Laqab/nisba: isnad name starts with ال (e.g. "الزهري", "الأعمش") —
     match KASHAF entries where this word appears and is the ONLY match
  5. Single first-name: isnad name is one token (e.g. "شعبة", "مالك") —
     match KASHAF entries where it is the FIRST word of the primary name,
     ONLY if exactly one such entry exists (avoids ambiguity)
"""

import json, re, unicodedata, sys
from collections import defaultdict
from pathlib import Path

# ─── Paths ──────────────────────────────────────────────────────────────────
BASE       = Path(__file__).parent.parent
GRADES_F   = BASE / "app/data/narrator_grades.json"
ISNAD_F    = BASE / "app/data/isnad_graph.json"

GRADE_COLORS = {
    "companion":       "#9b59b6",
    "reliable":        "#2ecc71",
    "mostly_reliable": "#f39c12",
    "weak":            "#e74c3c",
    "abandoned":       "#c0392b",
    "fabricator":      "#8b0000",
    "unknown":         "#95a5a6",
}

# ─── Normalisation helpers ───────────────────────────────────────────────────
DIACRITICS  = re.compile(r'[\u0610-\u061A\u064B-\u065F\u0670\u06D6-\u06DC\u06DF-\u06E4\u06E7\u06E8\u06EA-\u06ED]')
TATWEEL     = re.compile(r'\u0640')
ALEF        = re.compile(r'[أإآا]')

# Words that carry no identity information
STOP_WORDS  = {
    'بن','ابن','بنت','بن','أبي','أبو','ابو','أم','بنت',
    'رضى','رضي','الله','عنه','عنها','رحمه','رحمها',
    'ـ','-','،',':','.',',',
    'عليه','السلام','عليها','صلى','عن',
}

# Manual aliases for narrators whose names in isnad chains can't be
# matched algorithmically (laqabs, kunyas not in the KASHAF key itself)
_ZUHRI_PN  = 'محمد بن مسلم بن عبيد الله بن عبد الله بن شهاب بن عبد الله بن الحارث بن زهره بن ك'
_MALIK_PN  = 'مالك بن انس بن مالك بن ابي عامر بن عمرو بن الحارث بن غيمان ب'

# Maps normalized isnad name → normalized KASHAF primary name prefix
# Use None to explicitly skip (no KASHAF entry exists)
MANUAL_ALIASES = {
    # Laqab (nickname) → biographical name
    'الزهري':        _ZUHRI_PN,
    'ابن شهاب':     _ZUHRI_PN,
    'الاعمش':       'سليمان بن مهران',
    # Imam Malik
    'مالك':          _MALIK_PN,
    # Abu al-Yaman = al-Hakam ibn Nafi'
    'ابو اليمان':    'الحكم بن نافع',
    # Abu Nu'aym = al-Fadl ibn Dukayn
    'ابو نعيم':      'الفضل بن دكين',
    # يحيى بن بكير = يحيى بن عبد الله بن بكير (known by grandfather's name)
    'يحيى بن بكير': 'يحيى بن عبد الله بن بكير',
    # محمد بن بشار (Bundar)
    'محمد بن بشار': 'محمد بن بشار بن عثمان',
    # ابن عباس = عبد الله بن عباس
    'ابن عباس':     'عبد الله بن عباس بن عبد المطلب',
    # ابن عمر = عبد الله بن عمر
    'ابن عمر':      'عبد الله بن عمر بن الخطاب',
    # Companions/pronouns not in KASHAF as standalone primary entries
    'ابو هريره': None,
    'ابي هريره': None,
    'رسول الله': None,
    'النبي':     None,
    'ابيه':      None,    # "his father" — relative pronoun
    'ابي':       None,    # "my father" — relative pronoun
    'عائشه':     None,    # companion, not in KASHAF as primary
}

GRADE_ORDER = ['companion','reliable','mostly_reliable','weak','abandoned','fabricator','unknown']

# Arabic grade_ar terms that weren't captured by build_narrator_grades.py's keyword scan
# These are used to upgrade grade_en='unknown' entries at match time
_RELIABLE_AR  = ['ثقة', 'ثبت', 'إمام', 'متقن', 'حافظ', 'جليل', 'أتقن', 'وثقه', 'احتج به']
_MOSTLY_AR    = ['صدوق', 'لا بأس', 'مقبول', 'حسن الحديث', 'صالح']
_WEAK_AR      = ['ضعيف', 'فيه لين', 'فيه ضعف', 'لين', 'منكر']
_ABANDONED_AR = ['متروك', 'كذاب', 'وضاع', 'موضوع', 'ساقط']

def upgrade_grade(gd: dict) -> dict:
    """
    If grade_en=='unknown' but grade_ar has recognisable terms, derive a better grade.
    Returns a (possibly modified) copy.
    """
    if gd.get('grade_en', 'unknown') != 'unknown':
        return gd
    ar = gd.get('grade_ar', '')
    if not ar:
        return gd
    gd = dict(gd)   # don't mutate the original
    for marker in _ABANDONED_AR:
        if marker in ar:
            gd['grade_en'] = 'abandoned'
            gd['color']    = GRADE_COLORS['abandoned']
            return gd
    for marker in _WEAK_AR:
        if marker in ar:
            gd['grade_en'] = 'weak'
            gd['color']    = GRADE_COLORS['weak']
            return gd
    for marker in _MOSTLY_AR:
        if marker in ar:
            gd['grade_en'] = 'mostly_reliable'
            gd['color']    = GRADE_COLORS['mostly_reliable']
            return gd
    for marker in _RELIABLE_AR:
        if marker in ar:
            gd['grade_en'] = 'reliable'
            gd['color']    = GRADE_COLORS['reliable']
            return gd
    return gd

# Honorific suffixes to strip from isnad names
HONORIFIC_RE = re.compile(
    r'[\s\u0640]*(?:ـ\s*)?(?:رضى?|رضي)\s+الله\s+عن(?:هما|هم|ها|ه)'
    r'|[\s\u0640]*ـ\s*رضى?\s+الله\s+عن(?:هما|هم|ها|ه)\s*ـ'
    r'|[\s\u0640]*\u0640+\s*$'
)

def normalize(s: str) -> str:
    """Strip diacritics, normalize alef variants, tatweel, lower-case-equivalent."""
    s = DIACRITICS.sub('', s)
    s = TATWEEL.sub('', s)
    s = ALEF.sub('ا', s)
    s = s.replace('ة', 'ه')   # taa marbuta → haa  (consistent with concordance)
    s = s.strip()
    return s

def clean_isnad_name(raw: str) -> str:
    """Remove honorifics and leading/trailing noise from an isnad narrator id."""
    s = HONORIFIC_RE.sub('', raw)
    s = s.strip(' \u0640-')
    return normalize(s)

def tokens(s: str) -> list[str]:
    """Non-trivial tokens from a normalized name."""
    return [t for t in s.split() if t not in STOP_WORDS and len(t) > 1]

def primary_name(kashaf_key: str) -> str:
    """Extract the primary biographical name (before first colon)."""
    part = kashaf_key.split(':')[0]
    # Also cut at 'ويقال' / 'يقال' alternatives
    for marker in ['، ويقال', '، يقال', '،يقال']:
        part = part.split(marker)[0]
    return normalize(part.strip())

def kunya_word(name: str) -> str | None:
    """Return the word after أبو/أبي/ابو if the name starts with it, else None."""
    parts = name.split()
    if parts and parts[0] in {'ابو', 'ابي'}:
        if len(parts) >= 2:
            return parts[1]
    return None

GRADE_RANK = {g: i for i, g in enumerate(GRADE_ORDER)}

def best_grade(matches: list) -> tuple | None:
    """
    Pick the best match from a list of (key, primary_norm, grade_dict).
    Preference order: reliable > mostly_reliable > companion > weak > abandoned > unknown.
    Returns the match only if there's a clear winner (unique best grade OR single entry).
    """
    if not matches:
        return None
    if len(matches) == 1:
        return matches[0]
    ranked = sorted(matches, key=lambda x: GRADE_RANK.get(x[2].get('grade_en','unknown'), 99))
    best = ranked[0][2].get('grade_en','unknown')
    top  = [m for m in ranked if m[2].get('grade_en','unknown') == best]
    if len(top) == 1:
        return top[0]
    return None  # ambiguous at same grade level

# ─── Build KASHAF lookup structures ─────────────────────────────────────────
def build_kashaf_index(grade_lookup: dict):
    """
    Returns:
      primary_exact   : normalized_primary → list[(key, grade_dict)]
      by_first_token  : first_token        → list[(key, primary_norm, grade_dict)]
      by_kunya_word   : kunya_word         → list[(key, primary_norm, grade_dict)]
      by_laqab        : laqab_token        → list[(key, primary_norm, grade_dict)]
      all_tokens_idx  : token → set of primary_norm keys (for multi-token matching)
    """
    primary_exact   = defaultdict(list)
    by_first_token  = defaultdict(list)
    by_kunya_word   = defaultdict(list)
    by_laqab        = defaultdict(list)
    all_tokens_idx  = defaultdict(set)

    for key, gd in grade_lookup.items():
        pn   = primary_name(key)
        toks = tokens(pn)
        entry = (key, pn, gd)

        primary_exact[pn].append(entry)

        if toks:
            first = toks[0]
            by_first_token[first].append(entry)

            # Normalize أبي→أبو in kunya so genitive form matches too
            pn_kunya = pn.replace('ابي ', 'ابو ', 1) if pn.startswith('ابي ') else pn
            kw = kunya_word(pn_kunya)
            if kw:
                by_kunya_word[kw].append(entry)

            # Laqab: tokens that start with ال
            for t in toks:
                if t.startswith('ال'):
                    by_laqab[t].append(entry)

            for t in toks:
                all_tokens_idx[t].add(pn)

    return primary_exact, by_first_token, by_kunya_word, by_laqab, all_tokens_idx

def best_single(matches: list) -> tuple | None:
    """From a list of (key, primary_norm, grade_dict) return the one entry, or None if ambiguous."""
    if len(matches) == 1:
        return matches[0]
    return best_grade(matches)   # try grade-based disambiguation

def pick_best_multi(matches: list, query_tokens: list) -> tuple | None:
    """
    Pick the KASHAF entry where the most query_tokens match tokens in its primary name,
    with ties broken by grade then shortest primary name.
    Returns None if there are 0 matches or genuine ambiguity.
    """
    if not matches:
        return None
    scored = []
    qtset  = set(query_tokens)
    for key, pn, gd in matches:
        pn_toks = set(tokens(pn))
        score   = len(qtset & pn_toks)
        g_rank  = GRADE_RANK.get(gd.get('grade_en', 'unknown'), 99)
        scored.append((score, -g_rank, -len(pn), key, pn, gd))
    scored.sort(reverse=True)
    best_score = scored[0][0]
    top = [x for x in scored if x[0] == best_score]
    if len(top) == 1:
        _, _, _, key, pn, gd = top[0]
        return key, pn, gd
    # Tiebreak by grade
    best_grade_rank = top[0][1]
    top2 = [x for x in top if x[1] == best_grade_rank]
    if len(top2) == 1:
        _, _, _, key, pn, gd = top2[0]
        return key, pn, gd
    return None

# ─── Match a single isnad narrator name ─────────────────────────────────────
def match_name(raw_id: str, primary_exact, by_first_token, by_kunya_word,
               by_laqab, all_tokens_idx, grade_lookup):
    cleaned = clean_isnad_name(raw_id)
    toks    = tokens(cleaned)

    if not toks:
        return None, "empty"

    # ── Strategy 0: manual aliases ───────────────────────────────────────────
    if cleaned in MANUAL_ALIASES:
        target_pn = MANUAL_ALIASES[cleaned]
        if target_pn is None:
            return None, "manual-skip"
        hits = primary_exact.get(target_pn, [])
        if hits:
            return hits[0][2], "manual"
        # Try prefix match on the manual alias
        for pn, entries in primary_exact.items():
            if pn.startswith(target_pn[:30]):
                return entries[0][2], "manual-prefix"

    # Also check kunya-normalized form for manual aliases
    cleaned_kunya_norm = cleaned.replace('ابي ', 'ابو ', 1) if cleaned.startswith('ابي ') else cleaned
    if cleaned_kunya_norm in MANUAL_ALIASES:
        target_pn = MANUAL_ALIASES[cleaned_kunya_norm]
        if target_pn is None:
            return None, "manual-skip"
        hits = primary_exact.get(target_pn, [])
        if hits:
            return hits[0][2], "manual"

    # ── Strategy 1: exact match on normalized primary ────────────────────────
    if cleaned in primary_exact:
        hits = primary_exact[cleaned]
        if len(hits) == 1:
            return hits[0][2], "exact"
        return hits[0][2], "exact-multi"

    # ── Strategy 2: multi-token match (≥2 non-trivial tokens) ───────────────
    if len(toks) >= 2:
        candidate_pns = None
        for t in toks:
            pns_with_t = all_tokens_idx.get(t, set())
            if candidate_pns is None:
                candidate_pns = set(pns_with_t)
            else:
                candidate_pns &= pns_with_t
        if candidate_pns:
            matches = []
            for pn in candidate_pns:
                for key, pnk, gd in primary_exact.get(pn, []):
                    matches.append((key, pnk, gd))
            result = pick_best_multi(matches, toks)
            if result:
                return result[2], "multi-token"

    # ── Strategy 3: kunya match (أبي/أبو X) ─────────────────────────────────
    # Normalize genitive أبي → أبو for matching
    cleaned_norm = cleaned.replace('ابي ', 'ابو ', 1) if cleaned.startswith('ابي ') else cleaned
    kw = kunya_word(cleaned_norm)
    if kw:
        hits = by_kunya_word.get(kw, [])
        r = best_single(hits)
        if r:
            return r[2], "kunya"

    # ── Strategy 4: laqab/nisba (starts with ال) ────────────────────────────
    if len(toks) == 1 and toks[0].startswith('ال'):
        hits = by_laqab.get(toks[0], [])
        # Prefer entries where this word is the FIRST meaningful token (true laqab)
        first_hits = [(k, pn, gd) for k, pn, gd in hits
                      if tokens(pn) and tokens(pn)[0] == toks[0]]
        if first_hits:
            r = best_single(first_hits)
            if r:
                return r[2], "laqab-first"
        r = best_single(hits)
        if r:
            return r[2], "laqab"

    # ── Strategy 5: single first-name match ─────────────────────────────────
    if len(toks) == 1:
        first = toks[0]
        hits  = by_first_token.get(first, [])
        r = best_single(hits)
        if r:
            return r[2], "first-name"

    # ── Strategy 6: ابن X — "son of X" nickname ─────────────────────────────
    # e.g. "ابن عباس" → look for KASHAF primary containing both عبد الله and عباس
    if len(toks) >= 1 and cleaned.startswith('ابن '):
        rest_tok = toks[0]  # first non-trivial token after stripping ابن
        # Find primaries where rest_tok appears
        candidate_pns = all_tokens_idx.get(rest_tok, set())
        if candidate_pns:
            matches = [(k, pn, gd)
                       for pn in candidate_pns
                       for k, _, gd in primary_exact.get(pn, [])]
            r = best_grade(matches)
            if r:
                return r[2], "ibn-pattern"

    return None, "no-match"

# ─── Main ────────────────────────────────────────────────────────────────────
def main():
    print("Loading data…")
    with open(GRADES_F) as f:
        narrator_grades = json.load(f)
    with open(ISNAD_F) as f:
        isnad_graph = json.load(f)

    grade_lookup = narrator_grades["grade_lookup"]

    print(f"  KASHAF entries : {len(grade_lookup):,}")

    print("Building KASHAF index…")
    primary_exact, by_first_token, by_kunya_word, by_laqab, all_tokens_idx = \
        build_kashaf_index(grade_lookup)

    # Collect all unique narrator ids across all books
    all_ids: dict[str, dict] = {}   # id → best grade_dict found (or None)
    for book_id, book_data in isnad_graph.items():
        for node in book_data["nodes"]:
            nid = node["id"]
            if nid not in all_ids:
                all_ids[nid] = None

    print(f"  Unique narrator ids: {len(all_ids):,}")

    # Match each id
    match_results: dict[str, tuple] = {}   # id → (grade_dict | None, strategy)
    strategy_counts = defaultdict(int)

    for raw_id in all_ids:
        gd, strategy = match_name(
            raw_id,
            primary_exact, by_first_token, by_kunya_word,
            by_laqab, all_tokens_idx, grade_lookup
        )
        match_results[raw_id] = (gd, strategy)
        strategy_counts[strategy] += 1

    # ── Report ───────────────────────────────────────────────────────────────
    total    = len(all_ids)
    matched  = sum(1 for gd, _ in match_results.values() if gd is not None)
    print(f"\nMatch results: {matched}/{total} ({100*matched/total:.1f}%)")
    print("\nBy strategy:")
    for strat, cnt in sorted(strategy_counts.items(), key=lambda x: -x[1]):
        print(f"  {strat:<22} {cnt:4d}")

    # Show top-20 Bukhari narrator matches
    print("\nTop-30 Bukhari nodes → match:")
    buk_nodes = isnad_graph.get('bukhari', {}).get('nodes', [])
    for node in buk_nodes[:30]:
        nid = node['id']
        gd, strat = match_results.get(nid, (None, '?'))
        if gd:
            print(f"  ✓ {nid:<35} → {gd['grade_en']:<18} [{strat}]")
        else:
            print(f"  ✗ {nid:<35}   {strat}")

    # ── Update isnad_graph.json ───────────────────────────────────────────────
    print("\nUpdating isnad_graph.json…")
    upgrade_count = 0
    for book_id, book_data in isnad_graph.items():
        for node in book_data["nodes"]:
            nid = node["id"]
            gd, strategy = match_results.get(nid, (None, "no-match"))
            if gd is not None:
                gd = upgrade_grade(gd)
                old_grade = node.get("grade_en", "unknown")
                node["grade_en"] = gd.get("grade_en", "unknown")
                node["grade_ar"] = gd.get("grade_ar", "")
                node["color"]    = GRADE_COLORS.get(node["grade_en"], "#95a5a6")
                node["death"]    = str(gd.get("death", ""))
                node["places"]   = gd.get("places", "")
                if old_grade == "unknown" and node["grade_en"] != "unknown":
                    upgrade_count += 1
            # Nodes that remain unmatched keep their existing values

    print(f"  Grade upgrades (unknown→specific): {upgrade_count}")

    with open(ISNAD_F, "w", encoding="utf-8") as f:
        json.dump(isnad_graph, f, ensure_ascii=False, separators=(',', ':'))

    print(f"Done — isnad_graph.json updated ({ISNAD_F.stat().st_size//1024} KB)")

if __name__ == "__main__":
    main()
