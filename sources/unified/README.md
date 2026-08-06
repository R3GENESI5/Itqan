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
| `narrator_clusters.csv` | One row per resolved narrator: `canonical_name`, `canon_id`, `basis`, **`flag`**, `death`, `tabaqa`, `n_books`, `variant_names`, `members`. |
| `entry_to_cluster.csv` | Per-entry → `cluster_id` map (join back to the long-form). |
| `duplicate_clusters.xlsx` | The **3,386 cross-book duplicates** — the reviewable result; `flag=review` rows highlighted. |

**Canonical authority:** the project's **merged rijal DB** (`app/data/rijal/` — 115,735
profiles integrating **GK / Jawāmiʿ al-Kalim** + classical sources, with `death`,
`tabaqat`, `namings`, and GK `teachers`/`students` id-graph). The **blocked multi-feature
linker** (below) connects **22,236** corpus entries (≈5× what `arsanad_narrators.csv`
alone reached). To avoid the DB's relational `namings` ("أبيه", an ancestor's name)
conflating kin, the index uses each profile's `full_name` plus only namings that **start
with the person's ism**. GK (`src/kaggle_rawis.csv`, 24,326 narrators) supplies a **death
year and generation for every narrator** — via each profile's `gk_id` and by direct name match.

Dedup is fundamentally a **temporal-fix problem**: same name + same era ⇒ same person;
same name + different era ⇒ different people. Each entry gets an **era estimate** from the
sharpest signal available — **death → canonical-DB death → ṭabaqa → GK death/generation →
contemporaries/peers**. Era fixed for **17,911** entries (death 2,214 · canon 5,192 ·
ṭabaqa 7,484 · canon-tabaqa 971 · GK 305 · GK-gen 458 · peer 1,287).

**How it merges** (every merge is gated; never on a bare or common name):
1. **Canonical anchor** — link each entry to a merged-DB profile `id` with a **blocked
   multi-feature linker**: block on **(ism, father)**, then among candidates require
   `father_compat` + a shared distinctive token, and score on name-Jaccard + era +
   distinctive overlap (accept Jaccard ≥0.55, or ≥0.40 with agreeing death/era or ≥2
   shared distinctive). Same id ⇒ merge. This lower, *guarded* threshold links 56% more
   entries than the old Jaccard-0.8 rule **and** lowers the conflict rate (below).
2. **Name near-equality** — Jaccard ≥0.85 within a name-core block.
3. **Name + temporal fix** — Jaccard ≥0.55 when the two **era estimates agree** (the
   death→ṭabaqa→peer hierarchy). This is the recall lever.
4. **Isnad corroboration** — share transmitters/students within a name-core block:
   ≥2 of the curated **GK teacher/student ids** (`isnad-gk`), else ≥3 (or ≥2 at Jaccard
   ≥0.6) of the names parsed from the text ("روى عن … وعنه …"). *The GK-id path is
   precise but rarely fires — short teacher names ("الزهري") don't resolve to a unique id.*
5. **Required for 2–4:** a shared **distinctive (non-common) token** — sharing only
   ubiquitous elements (عبد/الله/محمد/أحمد…) is not identifying, so a common name
   can't "hub" distinct people (this alone cut the largest false cluster 59→10).
6. **Patronymic agreement** — same person ⇒ same father. Every merge (incl. the
   canonical-id ones) requires compatible fathers, tolerant of truncation and
   grandfather-attribution (نُسب إلى جدّه) but **not** of a father made only of common
   tokens. A final safety pass re-splits any union lacking father + distinctive links.
7. **Hard splits:** different `canon_id`; death years >5 apart; **incompatible era**;
   different ism (blocks father/son links where a father's name is a sub-chain of the son's).

> **Rejected (honest note):** a *cross-block* isnad merge (pairing entries with
> different names that share rare transmitters) was prototyped and **dropped** — it
> merged distinct people who share an isnad circle, e.g. the **brothers** al-Ḥasan &
> ʿAlī b. Ṣāliḥ b. Ḥayy. Relatives/peers share teachers, so isnad is only safe *with*
> a matching ism+father anchor.

**Results:** 70,620 entries → **65,579 clusters** (3,597 merged; **3,386 cross-book** —
2.2× the previous linker). Evidence: 4,721 canonical id · 207 name≈ · 112 name+era ·
11 isnad · 7 name+death. Largest cluster 11 (no hubs).

**Audit (`flag` column).** A grandfather/kunya-aware same-person test — does every
auto-verifiable member share an ism (chain head) and a compatible father? — flags
**18 of 3,597 merged clusters (0.5%)** as `flag=review`; the other **3,579 are `ok`**.
A separate naive patronymic test puts conflicts at **0.3%** — so the new linker **more
than doubled recall while lowering the error rate** (the guarded low-Jaccard scorer
admits true variants the old 0.8 cutoff missed, and rejects kin via ism+father+death).
**Filter `flag=ok` for high-confidence dedup**, review the 18. Earlier egregious merges
(59-entry hubs; father/son pairs like الحكم بن ظهير / إبراهيم بن الحكم) were eliminated.

**⚠️ Precision-first / recall limits.** Still conservative — most entries are
genuinely distinct narrators, and death parses for only ~3% of entries (ṭabaqa+peer
widen the temporal fix to 22%). Duplicates with very different phrasing *and* no
shared era/isnad are **missed** rather than guessed. Every merge is auditable via
`basis` + `variant_names`. Feed clusters into `dedup_narrators.py`.

## Regenerate

```bash
python sources/build_unified_index.py      # base index
python sources/build_dedup.py              # + narrator clusters
python sources/build_tabaqa_index.py       # + generation arrangement
```
