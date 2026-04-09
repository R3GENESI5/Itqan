"""
merge_classical_rijal.py
========================
Merges 83,082 parsed entries from 8 classical rijal texts into
the existing narrator_unified.json database (18,298 profiles).

Strategy:
  1. Load existing profiles with their name variants (namings)
  2. Build a normalized name → profile_id index
  3. For each classical entry, try to match by normalized name
  4. If matched: enrich the profile with classical source data
  5. If not matched: create a new profile
  6. Priority for grades: Taqrib > existing AR-Sanad > Mizan > Jarh > others

Usage:
    python src/merge_classical_rijal.py              # merge all
    python src/merge_classical_rijal.py --dry-run    # preview without writing
"""

import json, re, sys
from pathlib import Path
from collections import defaultdict, Counter

ROOT = Path(__file__).resolve().parent.parent
PARSED = ROOT / "src" / "rijal_parsed"
UNIFIED = ROOT / "app" / "data" / "narrator_unified.json"
OUTPUT = ROOT / "app" / "data" / "narrator_unified.json"

# ── Normalization ────────────────────────────────────────────────────

DIACRITICS_RE = re.compile(
    r'[\u0610-\u061A\u064B-\u065F\u0670\u06D6-\u06DC'
    r'\u06DF-\u06E4\u06E7\u06E8\u06EA-\u06ED]'
)
ALEF_RE = re.compile(r'[أإآا]')


def normalize(name):
    """Normalize Arabic name for matching."""
    n = DIACRITICS_RE.sub('', name)
    n = ALEF_RE.sub('ا', n)
    n = n.replace('\u0640', '')  # tatweel
    n = re.sub(r'\s+', ' ', n).strip()
    return n


# Grade priority (lower = higher priority)
GRADE_PRIORITY = {
    'taqrib': 1,        # Ibn Hajar's final summary — most authoritative
    'mizan': 2,         # Al-Dhahabi — strong assessments
    'jarh': 3,          # Ibn Abi Hatim — early evaluation
    'thiqat': 4,        # Ibn Hibban — reliable list
    'kamil': 5,         # Ibn 'Adi — weak catalog
    'tarikh': 6,        # Al-Khatib — geographical
    'tahdhib_tahdhib': 7,  # Condensed encyclopedia
    'tahdhib_kamal': 8,    # Reference (no direct grades)
}

GRADE_COLORS = {
    'companion':       '#9b59b6',
    'reliable':        '#2ecc71',
    'mostly_reliable': '#f39c12',
    'weak':            '#e74c3c',
    'abandoned':       '#c0392b',
    'fabricator':      '#8b0000',
    'unknown':         '#95a5a6',
}

# Grade strength (for upgrade decisions)
GRADE_STRENGTH = {
    'companion': 6, 'reliable': 5, 'mostly_reliable': 4,
    'weak': 3, 'abandoned': 2, 'fabricator': 1, 'unknown': 0,
}


def main():
    dry_run = '--dry-run' in sys.argv

    # ── Load existing database ─────────────────────────────────────────
    print("Loading existing narrator database...")
    with open(UNIFIED, encoding='utf-8') as f:
        db = json.load(f)

    profiles = db['profiles']  # dict: str(id) → profile
    by_name = db['by_name']    # dict: name_variant → lookup entry
    print(f"  Existing: {len(profiles)} profiles, {len(by_name)} name variants")

    # Build normalized name → profile ID index
    name_index = {}  # normalized_name → profile_id (str)
    for name_var, entry in by_name.items():
        norm = normalize(name_var)
        if norm not in name_index:
            name_index[norm] = str(entry['id'])

    # Build fuzzy index: first N tokens of name → set of profile IDs
    # This catches matches like "أحمد بن إبراهيم بن خالد" matching
    # "أحمد بن إبراهيم بن خالد الموصلي أبو علي" in the classical text
    prefix_index = defaultdict(set)  # "token1 token2 token3" → {profile_ids}
    for name_var, entry in by_name.items():
        norm = normalize(name_var)
        tokens = norm.split()
        # Index 3-token and 4-token prefixes (skip very short names)
        if len(tokens) >= 3:
            prefix_index[' '.join(tokens[:3])].add(str(entry['id']))
        if len(tokens) >= 4:
            prefix_index[' '.join(tokens[:4])].add(str(entry['id']))
    print(f"  Fuzzy prefix index: {len(prefix_index)} prefixes")

    # ── Load parsed classical texts ────────────────────────────────────
    sources = [
        'taqrib', 'mizan', 'jarh', 'thiqat', 'kamil',
        'tarikh', 'tahdhib_tahdhib', 'tahdhib_kamal',
    ]

    all_entries = {}
    for src in sources:
        path = PARSED / f"{src}.json"
        if not path.exists():
            print(f"  [skip] {src} — not found")
            continue
        entries = json.load(open(path, encoding='utf-8'))
        all_entries[src] = entries
        print(f"  Loaded {src}: {len(entries)} entries")

    # ── Match and merge ───────────────────────────────────────────────
    print("\nMatching classical entries to existing profiles...")

    matched = 0
    new_profiles = 0
    grade_upgrades = 0
    source_additions = 0
    next_id = max(int(k) for k in profiles.keys()) + 1

    for src in sources:
        if src not in all_entries:
            continue

        src_matched = 0
        src_new = 0

        for entry in all_entries[src]:
            entry_name = normalize(entry['name'])
            profile_id = name_index.get(entry_name)

            # Fuzzy fallback: try prefix matching
            if not profile_id:
                tokens = entry_name.split()
                candidates = set()
                if len(tokens) >= 4:
                    candidates = prefix_index.get(' '.join(tokens[:4]), set())
                if not candidates and len(tokens) >= 3:
                    candidates = prefix_index.get(' '.join(tokens[:3]), set())
                # Only use if exactly one candidate (avoid ambiguity)
                if len(candidates) == 1:
                    profile_id = next(iter(candidates))

            if profile_id and profile_id in profiles:
                # ── Match found: enrich existing profile ──
                profile = profiles[profile_id]
                src_matched += 1

                # Add classical source reference
                if 'classical_sources' not in profile:
                    profile['classical_sources'] = {}
                if src not in profile['classical_sources']:
                    profile['classical_sources'][src] = {
                        'entry_id': entry.get('id'),
                        'grade_en': entry.get('grade_en', 'unknown'),
                        'grade_ar': entry.get('grade_ar', ''),
                    }
                    source_additions += 1

                # Upgrade grade if classical source is higher priority
                current_grade = profile.get('grade_en', 'unknown')
                new_grade = entry.get('grade_en', 'unknown')

                if new_grade != 'unknown' and current_grade == 'unknown':
                    # Fill empty grade
                    priority = GRADE_PRIORITY.get(src, 99)
                    profile['grade_en'] = new_grade
                    profile['grade_ar'] = entry.get('grade_ar', '')
                    profile['color'] = GRADE_COLORS.get(new_grade, '#95a5a6')
                    grade_upgrades += 1
                elif (new_grade != 'unknown' and current_grade != 'unknown'
                      and src == 'taqrib'):
                    # Taqrib overrides everything (Ibn Hajar's final word)
                    if new_grade != current_grade:
                        profile['grade_en'] = new_grade
                        profile['grade_ar'] = entry.get('grade_ar', '')
                        profile['color'] = GRADE_COLORS.get(new_grade, '#95a5a6')
                        grade_upgrades += 1

                # Fill death year if empty
                if not profile.get('death') and entry.get('death'):
                    profile['death'] = entry['death']

                # Fill tabaqat if empty (from Taqrib)
                if not profile.get('tabaqat') and entry.get('tabaqah'):
                    profile['tabaqat'] = entry['tabaqah']

            else:
                # ── No match: create new profile ──
                new_id = str(next_id)
                next_id += 1

                new_profile = {
                    'id': int(new_id),
                    'full_name': entry['name'],
                    'kunya': entry.get('kunya', ''),
                    'grade_en': entry.get('grade_en', 'unknown'),
                    'grade_ar': entry.get('grade_ar', ''),
                    'color': GRADE_COLORS.get(entry.get('grade_en', 'unknown'), '#95a5a6'),
                    'death': entry.get('death', ''),
                    'tabaqat': entry.get('tabaqah', ''),
                    'city': '',
                    'laqab': '',
                    'nasab': '',
                    'dhahabi': '',
                    'namings': [entry['name']],
                    'classical_sources': {
                        src: {
                            'entry_id': entry.get('id'),
                            'grade_en': entry.get('grade_en', 'unknown'),
                            'grade_ar': entry.get('grade_ar', ''),
                        }
                    },
                }

                profiles[new_id] = new_profile

                # Add to name index and by_name
                norm_name = normalize(entry['name'])
                name_index[norm_name] = new_id
                by_name[entry['name']] = {
                    'id': int(new_id),
                    'grade_en': new_profile['grade_en'],
                    'grade_ar': new_profile['grade_ar'],
                    'color': new_profile['color'],
                    'death': new_profile.get('death', ''),
                    'full_name': entry['name'],
                    'kunya': entry.get('kunya', ''),
                }

                src_new += 1
                new_profiles += 1

        matched += src_matched
        print(f"  {src}: {src_matched} matched, {src_new} new")

    # ── Summary ───────────────────────────────────────────────────────
    print(f"\n{'DRY RUN — ' if dry_run else ''}Summary:")
    print(f"  Matched to existing profiles: {matched}")
    print(f"  Grade upgrades: {grade_upgrades}")
    print(f"  Source cross-references added: {source_additions}")
    print(f"  New profiles created: {new_profiles}")
    print(f"  Total profiles: {len(profiles)}")
    print(f"  Total name variants: {len(by_name)}")

    # Grade distribution
    grades = Counter(p.get('grade_en', 'unknown') for p in profiles.values())
    print(f"\n  Grade distribution:")
    for g, c in grades.most_common():
        pct = 100 * c / len(profiles)
        print(f"    {g}: {c:,} ({pct:.1f}%)")

    if dry_run:
        print("\n  [DRY RUN] No files written.")
        return

    # ── Write output ──────────────────────────────────────────────────
    db['profiles'] = profiles
    db['by_name'] = by_name

    print(f"\n  Writing {OUTPUT.name}...")
    with open(OUTPUT, 'w', encoding='utf-8') as f:
        json.dump(db, f, ensure_ascii=False)
    print(f"  -> {OUTPUT.stat().st_size:,} bytes")


if __name__ == '__main__':
    main()
