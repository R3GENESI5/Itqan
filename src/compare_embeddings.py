"""
compare_embeddings.py
=====================
Compares two embedding models for Arabic hadith retrieval:

  Model A (ours):     intfloat/multilingual-e5-small   (multilingual, instruction-tuned)
  Model B (baseline): sentence-transformers/all-MiniLM-L6-v2  (English-first)

Uses the existing FAISS index built with multilingual-e5-small together with a
fresh temporary index built from a 10,000-hadith subset encoded by MiniLM.

Outputs:
  src/embedding_comparison.json   — full per-query results

Usage:
  pip install sentence-transformers faiss-cpu numpy tqdm
  python src/compare_embeddings.py
"""

import json
import math
import random
import time
from pathlib import Path

import faiss
import numpy as np
from sentence_transformers import SentenceTransformer
from tqdm import tqdm

# ── Paths ─────────────────────────────────────────────────────────────────────
ROOT      = Path(__file__).resolve().parent.parent
SEM_DIR   = ROOT / "app" / "data" / "semantic"
FAISS_PATH = SEM_DIR / "semantic_index.faiss"
META_PATH  = SEM_DIR / "semantic_meta.json"
OUT_PATH   = ROOT / "src" / "embedding_comparison.json"

SAMPLE_SIZE = 10_000
TOP_K       = 5
RANDOM_SEED = 42

# ── Test queries ──────────────────────────────────────────────────────────────
TEST_QUERIES = [
    ("الصلاة",                  "prayer"),
    ("الصيام في رمضان",          "fasting in Ramadan"),
    ("الزكاة والصدقة",            "zakat and charity"),
    ("بر الوالدين",              "honoring parents"),
    ("الأمانة والصدق",            "honesty and trustworthiness"),
    ("التوبة والاستغفار",          "repentance and seeking forgiveness"),
    ("الجهاد في سبيل الله",        "jihad in the path of God"),
    ("حقوق الجار",               "rights of neighbors"),
    ("النية في العبادة",           "intention in worship"),
    ("الرحمة بالضعفاء",           "mercy toward the weak"),
    ("العلم وطلبه",              "knowledge and seeking it"),
    ("الموت والآخرة",             "death and the hereafter"),
]


# ── Helpers ───────────────────────────────────────────────────────────────────

def normalize(vecs: np.ndarray) -> np.ndarray:
    """L2-normalize rows in-place and return."""
    norms = np.linalg.norm(vecs, axis=1, keepdims=True)
    norms = np.where(norms == 0, 1.0, norms)
    return (vecs / norms).astype("float32")


def arabic_keyword_match(text: str, query: str) -> bool:
    """
    Simple keyword check: does the Arabic text contain any word from the query?
    Strip common Arabic diacritics before comparison.
    """
    diacritics = "ًٌٍَُِّْٰٓٔ"
    def strip_diac(s: str) -> str:
        return "".join(c for c in s if c not in diacritics)

    text_clean  = strip_diac(text)
    query_words = strip_diac(query).split()
    return any(w in text_clean for w in query_words if len(w) > 1)


def proportional_sample(meta: list, n: int, seed: int = RANDOM_SEED) -> list[int]:
    """
    Draw n indices from meta proportionally by book_id so all books are
    represented, then shuffle. Falls back to simple random sample if n >= len.
    """
    if n >= len(meta):
        return list(range(len(meta)))

    # Group indices by book
    book_to_idxs: dict[str, list[int]] = {}
    for i, entry in enumerate(meta):
        book = entry.get("book", "unknown")
        book_to_idxs.setdefault(book, []).append(i)

    rng = random.Random(seed)
    sampled: list[int] = []
    total = len(meta)
    remaining = n

    books = sorted(book_to_idxs.keys())
    for j, book in enumerate(books):
        idxs = book_to_idxs[book]
        # Proportional quota
        if j == len(books) - 1:
            quota = remaining
        else:
            quota = math.ceil(len(idxs) / total * n)
            quota = min(quota, remaining, len(idxs))

        chosen = rng.sample(idxs, min(quota, len(idxs)))
        sampled.extend(chosen)
        remaining -= len(chosen)
        if remaining <= 0:
            break

    # Trim to exactly n
    rng.shuffle(sampled)
    return sampled[:n]


# ── 1. Load metadata ──────────────────────────────────────────────────────────
print(f"\n{'='*60}")
print("Step 1/6 — Loading semantic_meta.json …")
t0 = time.time()
with open(META_PATH, encoding="utf-8") as f:
    meta: list[dict] = json.load(f)
print(f"  Loaded {len(meta):,} entries in {time.time()-t0:.1f}s")


# ── 2. Draw proportional 10k sample ──────────────────────────────────────────
print(f"\nStep 2/6 — Sampling {SAMPLE_SIZE:,} hadiths proportionally …")
sample_idxs = proportional_sample(meta, SAMPLE_SIZE)
sample_meta  = [meta[i] for i in sample_idxs]
print(f"  Sample covers {len(set(m['book'] for m in sample_meta))} books")


# ── 3. Load multilingual-e5-small FAISS index and extract sample vectors ──────
print("\nStep 3/6 — Loading multilingual-e5-small FAISS index …")
t0 = time.time()
e5_index_full = faiss.read_index(str(FAISS_PATH))
DIM = e5_index_full.d
print(f"  Full index: {e5_index_full.ntotal:,} vectors, dim={DIM}")

# Reconstruct only the 10k sample vectors
print(f"  Extracting {len(sample_idxs):,} sample vectors from e5 index …")
sample_idx_arr = np.array(sample_idxs, dtype=np.int64)

# faiss.IndexFlatIP supports reconstruct_batch
e5_sample_vecs = np.zeros((len(sample_idxs), DIM), dtype="float32")
for pos, global_idx in enumerate(tqdm(sample_idxs, desc="  Reconstruct e5 vecs", ncols=80)):
    e5_index_full.reconstruct(int(global_idx), e5_sample_vecs[pos])

# Build a sub-index for searching the 10k subset with e5 embeddings
e5_sub_index = faiss.IndexFlatIP(DIM)
e5_sub_index.add(e5_sample_vecs)
print(f"  e5 sub-index ready: {e5_sub_index.ntotal:,} vectors")


# ── 4. Load MiniLM and embed the same 10k hadiths ────────────────────────────
print("\nStep 4/6 — Loading all-MiniLM-L6-v2 …")
minilm = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")
print(f"  MiniLM dim: {minilm.get_sentence_embedding_dimension()}")

# Build passage texts for MiniLM (Arabic preferred, else English, no prefix)
def passage_for_minilm(entry: dict) -> str:
    ar = entry.get("ar", "").strip()
    en = entry.get("en", "").strip()
    return ar if ar else en

minilm_passages = [passage_for_minilm(m) for m in sample_meta]

print(f"  Embedding {len(minilm_passages):,} passages with MiniLM (CPU ~2–3 min) …")
t0 = time.time()
BATCH = 256
minilm_vecs_list = []
for i in tqdm(range(0, len(minilm_passages), BATCH), desc="  MiniLM embed", ncols=80):
    batch = minilm_passages[i : i + BATCH]
    vecs  = minilm.encode(batch, normalize_embeddings=True,
                          show_progress_bar=False, convert_to_numpy=True)
    minilm_vecs_list.append(vecs)

minilm_vecs = np.vstack(minilm_vecs_list).astype("float32")
print(f"  Done in {(time.time()-t0)/60:.1f} min. Shape: {minilm_vecs.shape}")

# Build MiniLM sub-index
minilm_index = faiss.IndexFlatIP(DIM)
minilm_index.add(minilm_vecs)
print(f"  MiniLM sub-index ready: {minilm_index.ntotal:,} vectors")


# ── 5. Load e5-small model for encoding queries ───────────────────────────────
print("\nStep 5/6 — Loading multilingual-e5-small for query encoding …")
e5_model = SentenceTransformer("intfloat/multilingual-e5-small")
print(f"  e5-small loaded (dim={e5_model.get_sentence_embedding_dimension()})")


# ── 6. Run evaluation across all 12 queries ───────────────────────────────────
print("\nStep 6/6 — Running 12 queries against both models …\n")

all_results = []

e5_top1_scores     = []
minilm_top1_scores = []
e5_arabic_hits     = []
minilm_arabic_hits = []
overlap_counts     = []

SEP = "─" * 80

for arabic_query, english_label in TEST_QUERIES:
    print(SEP)
    print(f"Query: {arabic_query}  ({english_label})")
    print()

    # ── Encode with e5-small (instruction prefix for queries)
    e5_q_text = "query: " + arabic_query
    e5_q_vec  = e5_model.encode([e5_q_text], normalize_embeddings=True,
                                 convert_to_numpy=True).astype("float32")

    # ── Encode with MiniLM (no prefix)
    minilm_q_vec = minilm.encode([arabic_query], normalize_embeddings=True,
                                  convert_to_numpy=True).astype("float32")

    # ── Search
    e5_scores,     e5_ranks     = e5_sub_index.search(e5_q_vec, TOP_K)
    minilm_scores, minilm_ranks = minilm_index.search(minilm_q_vec, TOP_K)

    e5_scores     = e5_scores[0].tolist()
    e5_ranks      = e5_ranks[0].tolist()
    minilm_scores = minilm_scores[0].tolist()
    minilm_ranks  = minilm_ranks[0].tolist()

    # ── Retrieve metadata for results
    def get_hit(rank: int, score: float) -> dict:
        m = sample_meta[rank]
        return {
            "rank":     rank,
            "score":    round(float(score), 4),
            "book":     m.get("book", ""),
            "num":      m.get("num", ""),
            "arabic":   m.get("ar", "")[:120],
            "english":  m.get("en", "")[:100],
        }

    e5_hits     = [get_hit(r, s) for r, s in zip(e5_ranks, e5_scores)]
    minilm_hits = [get_hit(r, s) for r, s in zip(minilm_ranks, minilm_scores)]

    # ── Side-by-side display
    print(f"  {'multilingual-e5-small':<38}  {'all-MiniLM-L6-v2'}")
    print(f"  {'─'*38}  {'─'*38}")
    for k in range(TOP_K):
        e = e5_hits[k]
        m = minilm_hits[k]
        e_text = (e["arabic"] or e["english"])[:60]
        m_text = (m["arabic"] or m["english"])[:60]
        print(f"  #{k+1} [{e['score']:.3f}] {e['book']}:{e['num']}")
        print(f"       {e_text}")
        print(f"       #{k+1} [{m['score']:.3f}] {m['book']}:{m['num']}")
        print(f"            {m_text}")
        print()

    # ── Compute metrics
    top1_e5     = e5_scores[0]
    top1_minilm = minilm_scores[0]
    e5_top1_scores.append(top1_e5)
    minilm_top1_scores.append(top1_minilm)

    # Arabic root match: fraction of top-5 results whose Arabic text contains query keywords
    def arabic_match_ratio(hits: list[dict]) -> float:
        matched = sum(1 for h in hits if arabic_keyword_match(h["arabic"], arabic_query))
        return matched / len(hits) if hits else 0.0

    e5_match     = arabic_match_ratio(e5_hits)
    minilm_match = arabic_match_ratio(minilm_hits)
    e5_arabic_hits.append(e5_match)
    minilm_arabic_hits.append(minilm_match)

    # Overlap@5: how many sub-index positions appear in both top-5
    e5_set     = set(e5_ranks)
    minilm_set = set(minilm_ranks)
    overlap    = len(e5_set & minilm_set)
    overlap_counts.append(overlap)

    print(f"  Metrics — top-1 score: e5={top1_e5:.3f}  MiniLM={top1_minilm:.3f}  "
          f"overlap@5={overlap}/{TOP_K}  "
          f"ArabicMatch: e5={e5_match:.0%}  MiniLM={minilm_match:.0%}")

    all_results.append({
        "query_arabic":          arabic_query,
        "query_english":         english_label,
        "e5_small": {
            "hits":              e5_hits,
            "top1_score":        round(top1_e5, 4),
            "arabic_match_at5":  round(e5_match, 4),
        },
        "minilm": {
            "hits":              minilm_hits,
            "top1_score":        round(top1_minilm, 4),
            "arabic_match_at5":  round(minilm_match, 4),
        },
        "overlap_at5":           overlap,
    })


# ── Summary ───────────────────────────────────────────────────────────────────
avg_e5_top1     = float(np.mean(e5_top1_scores))
avg_minilm_top1 = float(np.mean(minilm_top1_scores))
avg_e5_match    = float(np.mean(e5_arabic_hits))
avg_minilm_match= float(np.mean(minilm_arabic_hits))
avg_overlap     = float(np.mean(overlap_counts))

print("\n" + "=" * 60)
print(f"Model Comparison Summary ({SAMPLE_SIZE:,}-hadith subset)")
print("─" * 60)
col1 = "multilingual-e5-small"
col2 = "all-MiniLM-L6-v2"
print(f"{'Metric':<26} {col1:<24} {col2}")
print(f"{'─'*26} {'─'*24} {'─'*20}")
print(f"{'Avg top-1 score':<26} {avg_e5_top1:<24.3f} {avg_minilm_top1:.3f}")
print(f"{'Arabic root match@5':<26} {avg_e5_match:<24.1%} {avg_minilm_match:.1%}")
print(f"{'Avg overlap@5':<26} {avg_overlap:.1f}/{TOP_K}               (reference)")
print("─" * 60)

# Verdict
e5_wins  = sum(1 for a, b in zip(e5_top1_scores, minilm_top1_scores) if a > b)
min_wins = len(TEST_QUERIES) - e5_wins
print(f"\nPer-query top-1 score wins: e5-small={e5_wins}, MiniLM={min_wins}")
if avg_e5_top1 > avg_minilm_top1:
    diff_pct = (avg_e5_top1 - avg_minilm_top1) / avg_minilm_top1 * 100
    print(f"Conclusion: multilingual-e5-small scores {diff_pct:.1f}% higher on average.")
else:
    diff_pct = (avg_minilm_top1 - avg_e5_top1) / avg_e5_top1 * 100
    print(f"Conclusion: all-MiniLM-L6-v2 scores {diff_pct:.1f}% higher on average.")
print(f"Arabic keyword match: e5={avg_e5_match:.1%} vs MiniLM={avg_minilm_match:.1%}")
print("=" * 60)


# ── Save results ──────────────────────────────────────────────────────────────
output = {
    "config": {
        "sample_size":    SAMPLE_SIZE,
        "top_k":          TOP_K,
        "total_hadiths":  len(meta),
        "e5_model":       "intfloat/multilingual-e5-small",
        "minilm_model":   "sentence-transformers/all-MiniLM-L6-v2",
    },
    "summary": {
        "e5_small": {
            "avg_top1_score":        round(avg_e5_top1,  4),
            "avg_arabic_match_at5":  round(avg_e5_match, 4),
            "query_wins":            e5_wins,
        },
        "minilm": {
            "avg_top1_score":        round(avg_minilm_top1,  4),
            "avg_arabic_match_at5":  round(avg_minilm_match, 4),
            "query_wins":            min_wins,
        },
        "avg_overlap_at5":           round(avg_overlap, 2),
    },
    "queries": all_results,
}

with open(OUT_PATH, "w", encoding="utf-8") as f:
    json.dump(output, f, ensure_ascii=False, indent=2)

print(f"\nFull results saved to: {OUT_PATH}")
