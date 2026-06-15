# Unified narrator index

A cross-source index merging **every biographical entry** from the 16
narrator-keyed OpenITI books in `sources/` into one place, so a narrator can be
looked up across all the rijāl/taʿdīl/jarḥ/ikhtilāṭ/tadlīs sources at once.

Built by `build_unified_index.py` (reuses the same OpenITI parser as
`../build_openiti_books.py`).

## Files

| File | What it is |
|------|------------|
| `unified_narrator_index.csv` | **Long-form master** — one row per biographical entry (70,673 rows), with source metadata + normalized name keys + **full verbatim verdict text**. UTF-8 BOM. |
| `unified_by_narrator.csv` | Grouped on the exact normalized name (67,590 keys): which books cover each name, on what pages. The cross-source lookup. |
| `unified_narrator_index.xlsx` | Browsable: `by_narrator` sheet + `stats` sheet. RTL. |
| `multi_source_candidates.csv` | Shortlist of the 1,122 names attested in **≥2 books** — the candidate corroborations to verify (compact columns). |

## Coverage

16 of the 24 books — the ones with clean per-narrator entry structure:
Lisān al-Mīzān, al-Tārīkh al-Kabīr, al-Kāshif, al-Mughnī, Siyar, al-Ṭabaqāt
al-Kubrā, the Ḍuʿafāʾ set (ʿUqaylī, Ibn al-Jawzī, Nasāʾī, Majrūḥīn), Maʿrifat
al-Thiqāt, Ibn Shāhīn, Jāmiʿ al-Taḥṣīl, Ṭabaqāt al-Mudallisīn, al-Kawākib,
al-Ightibāṭ.

**Excluded** (not narrator-keyed — they're page/section indexed): the two ʿIlal
works, the four muṣṭalaḥ treatises, Tārīkh Ibn Maʿīn and al-Marāsīl (those two
are narrator content but the OpenITI version isn't segmented into entries). Use
their per-book files directly.

## Long-form schema

`row_id, source_slug, source_book, author, death_ah, source_type, section,
entry_no, narrator_name, raw_heading, name_norm, name_head, name_quality,
keyable, page, n_chars, text`

- `name_norm` — diacritics removed; ا/ى/ة/همزة normalized; whitespace collapsed.
- `name_head` — first 5 tokens of `name_norm` (for looser joins).
- `name_quality` — `heading` (name parsed from the entry heading, 56,381),
  `body_fallback` (heading was number-only, e.g. al-Kāshif → name taken from the
  start of the entry body, 14,260), `degenerate` (no usable name, 32).
- `source_type` — taʿdīl (thiqāt) / jarḥ (ḍuʿafāʾ) / ikhtilāṭ / tadlīs /
  biography / general — lets you weight a source's polarity.

## ⚠️ Honest caveat — this is candidate generation, NOT identity resolution

`unified_by_narrator.csv` groups rows by **exact normalized name string**. That
is a *heuristic join key*, not a resolved person:

- **Short/common names conflate distinct narrators.** e.g. `محمد بن عبد الله`
  groups 16 entries from 3 books — but those are several *different* men. A high
  `n_books` for a short name is a **collision**, not corroboration.
- Conversely, the *same* narrator written with/without a nisba or kunya lands in
  **different** keys, so real corroborations are missed.

Treat the ≥2-book rows (1,122 of them; 78 with ≥3) as **candidates to feed your
own matcher** (`dedup_narrators.py` / `match_narrator_grades.py`), and verify
before asserting that two entries are the same person. Verdict text is quoted
verbatim; **no grades are inferred.**

## Arranged by ṭabaqa (generation) — duplicate-hunting aid

To make cross-book duplicates easier to spot, narrators are also grouped by
**generation layer** (ṣaḥāba → kibār al-tābiʿīn → … → tabaʿ al-atbāʿ). Within one
generation the candidate set is small, so the same person appearing in several
books sits close together.

| File | What it is |
|------|------------|
| `unified_by_tabaqa.csv` | All 67,590 narrators, sorted by `tabaqa_order` then name. |
| `multi_source_candidates_by_tabaqa.csv` | The 1,122 ≥2-book candidates, same sort. |
| `candidates_by_tabaqa.xlsx` | One sheet per generation; ≥3-book rows highlighted. |

**How the generation is assigned** (column `tabaqa_basis`, priority order):
`arsanad:tabaqa` (Ibn Ḥajar ṭabaqa from the project's `src/arsanad_narrators.csv`)
→ `text:طبقة` (Taqrīb ordinal in the entry) → `text:وفاة` (death year parsed from
"مات سنة …" incl. Arabic numeral words) → `text:صحابي` → `arsanad~:*` (looser
first-5-token name match) → `text:تابعي`. Death years map to bands
(≤110 / ≤150 / ≤180 / ≤215 / ≤245 / >245 AH).

**⚠️ Coverage & precision.** Only **~35% of the candidates** (and ~18% overall)
get a generation — the rest are obscure narrators absent from arsanad with no
self-stated generation, parked in **`٨ غير محدد`** (still name-sorted, so exact
duplicates still cluster). The signal is heuristic: `text:صحابي` and the loose
`arsanad~` matches carry noise. **Filter/trust by `tabaqa_basis`** — `arsanad:tabaqa`
and `text:وفاة` are the most reliable; treat the rest as hints to verify.

## Deduplication (entity resolution)

`build_dedup.py` collapses entries that refer to the **same narrator** across
books — *conservatively* (precision over recall; no false merges).

| File | What it is |
|------|------------|
| `narrator_clusters.csv` | One row per resolved narrator: `canonical_name`, `arsanad_id`, `basis`, `death`, `tabaqa`, `n_books`, `variant_names` (the merged forms), `members` (book:page). |
| `entry_to_cluster.csv` | Per-entry → `cluster_id` map (join back to the long-form). |
| `duplicate_clusters.xlsx` | The **1,294 cross-book duplicates** only — the reviewable result. |

**How it merges** (every merge is gated, never on a bare name):
1. **Canonical anchor** — link each entry to an `arsanad_narrators.csv` `id` via exact
   name *or* fuzzy match (token-Jaccard ≥0.8, ≥4 tokens, death-gated). Same id ⇒ merge.
   This carries word-order / kunya-first / spelling variants (e.g. al-Ifrīqī).
2. **Name near-equality** — Jaccard ≥0.85 (≥4 tokens) within a name-core block;
   Jaccard ≥0.62 if death years agree. **Jaccard (not containment)** so a short
   generic name can't "hub" distinct people together.
3. **Hard splits** — different `arsanad_id` ⇒ never merge; death years >5 apart ⇒
   never merge.

**Results:** 70,620 entries → **68,781 clusters** (1,417 merged; 1,294 cross-book:
1,061 span 2 books … 1 spans 7). Evidence tally: 1,276 by canonical id, 712 by
name, 5 by name+death.

**⚠️ Precision-first / recall limits.** Only ~3% collapse — most entries are
genuinely distinct narrators, arsanad covers ~18k of the canonical rawīs, and
death years parse for only ~3% of entries (the disambiguator is thin). Real
duplicates with very different phrasing are **missed** rather than guessed. Each
cluster carries `basis` + `variant_names` so you can audit every merge. The
principled recall lift is the **isnad teacher/student graph** (arsanad's
`narrated_from`/`narrated_to` id-lists) — match a narrator's mentioned
teachers/students to corroborate identity. Feed clusters into `dedup_narrators.py`.

## Regenerate

```bash
python sources/build_unified_index.py      # base index
python sources/build_dedup.py              # + narrator clusters
python sources/build_tabaqa_index.py       # + generation arrangement
```
