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

## Regenerate

```bash
python sources/build_unified_index.py      # base index
python sources/build_tabaqa_index.py       # + generation arrangement
```
