"""
Al-Itqan Semantic Search — HuggingFace Space
============================================
BasilSuhail upgrade: hybrid semantic + keyword search over 87k hadiths.

Deploy:
  1. Create a new Space at huggingface.co/spaces (Gradio SDK)
  2. Upload this file as app.py
  3. Set env var HF_INDEX_REPO = "iqrossed/al-itqan-index"
  4. The Space downloads the FAISS index on first run (~250MB)

Environment variables:
  HF_INDEX_REPO   — HuggingFace repo containing semantic_index.faiss + semantic_meta.json
"""

import os, json, re
import numpy as np
import faiss
import gradio as gr
from sentence_transformers import SentenceTransformer
from huggingface_hub import hf_hub_download

# ── Load model + index ───────────────────────────────────────────────────────
print("Loading model...")
MODEL = SentenceTransformer("intfloat/multilingual-e5-small")

if os.getenv("USE_LOCAL_INDEX"):
    index_path = os.getenv("INDEX_PATH", "app/data/semantic/semantic_index.faiss")
    meta_path  = os.getenv("META_PATH",  "app/data/semantic/semantic_meta.json")
    print(f"Using local index at {index_path}")
else:
    HF_REPO = os.getenv("HF_INDEX_REPO", "iqrossed/al-itqan-index")
    print(f"Downloading index from {HF_REPO}...")
    index_path = hf_hub_download(repo_id=HF_REPO, filename="semantic_index.faiss", repo_type="dataset", token=False)
    meta_path  = hf_hub_download(repo_id=HF_REPO, filename="semantic_meta.json", repo_type="dataset", token=False)

INDEX = faiss.read_index(index_path)
META  = json.load(open(meta_path, encoding="utf-8"))
print(f"Ready — {INDEX.ntotal:,} hadiths indexed")

GRADE_LABELS = {
    "sahih":   "✅ Sahih",
    "hasan":   "🟡 Hasan",
    "daif":    "🔴 Da'if",
    "":        "⬜ Ungraded",
}

BOOK_NAMES = {
    "bukhari":                 "Sahih al-Bukhari",
    "muslim":                  "Sahih Muslim",
    "abudawud":                "Sunan Abu Dawud",
    "tirmidhi":                "Jami' at-Tirmidhi",
    "nasai":                   "Sunan an-Nasa'i",
    "ibnmajah":                "Sunan Ibn Majah",
    "ahmed":                   "Musnad Ahmad",
    "malik":                   "Muwatta Malik",
    "darimi":                  "Sunan ad-Darimi",
    "musannaf_ibnabi_shaybah": "Musannaf Ibn Abi Shaybah",
    "mishkat_almasabih":       "Mishkat al-Masabih",
    "riyad_assalihin":         "Riyad as-Salihin",
    "aladab_almufrad":         "Al-Adab Al-Mufrad",
    "bulugh_almaram":          "Bulugh al-Maram",
    "nawawi40":                "Nawawi 40",
    "qudsi40":                 "Qudsi 40",
    "shahwaliullah40":         "Shah Waliullah 40",
}

def detect_lang(text: str) -> str:
    """Detect if text is primarily Arabic."""
    arabic_chars = len(re.findall(r'[\u0600-\u06FF]', text))
    return "ar" if arabic_chars > len(text) * 0.3 else "en"

def search(query: str, top_k: int, book_filter: str, family_filter: str, grade_filter: str):
    if not query.strip():
        return "Please enter a search query."

    lang = detect_lang(query)
    prefix = "query: "
    encoded = MODEL.encode([prefix + query], normalize_embeddings=True, convert_to_numpy=True)

    # Semantic search — fetch 3× to allow for post-filtering
    scores, indices = INDEX.search(encoded.astype("float32"), top_k * 3)

    results = []
    for score, idx in zip(scores[0], indices[0]):
        if idx < 0 or idx >= len(META):
            continue
        m = META[idx]

        # Filters
        if book_filter   and book_filter   != "All" and m["book"]  != book_filter:
            continue
        if family_filter and family_filter != "All" and family_filter not in m.get("families", []):
            continue
        if grade_filter  and grade_filter  != "All":
            if grade_filter == "sahih_hasan" and m.get("grade","") not in ("sahih","hasan"):
                continue

        results.append((score, m))
        if len(results) >= top_k:
            break

    if not results:
        return "No results found for your query and filters."

    # Format output
    parts = []
    for rank, (score, m) in enumerate(results, 1):
        book_name = BOOK_NAMES.get(m["book"], m["book"])
        grade_str = GRADE_LABELS.get(m.get("grade",""), "⬜ Ungraded")
        families  = ", ".join(m.get("families", [])[:3]) or "—"
        arabic    = m.get("ar", "")
        english   = m.get("en", "")

        block = f"""### {rank}. {book_name} #{m['num']}
**Score:** {score:.3f} &nbsp;|&nbsp; **Grade:** {grade_str} &nbsp;|&nbsp; **Families:** {families}

<div dir="rtl" style="font-size:1.1rem;font-family:'Traditional Arabic',serif;line-height:1.8;background:#1a1a2e;padding:10px;border-radius:6px;margin:6px 0">{arabic[:400]}</div>

{english[:350]}{"…" if len(english) > 350 else ""}

---"""
        parts.append(block)

    header = f"**{len(results)} results** for: *{query}* (lang: {lang})\n\n"
    return header + "\n".join(parts)

# ── Interface ─────────────────────────────────────────────────────────────────
books_list   = ["All"] + sorted(BOOK_NAMES.keys())
families_list = ["All", "worship", "knowledge", "end_of_times", "statecraft",
                 "family_law", "jihad", "justice", "life_death", "time",
                 "movement_journey", "creation", "earth_sky", "mercy",
                 "patience_gratitude", "heart_soul", "guidance"]

with gr.Blocks(title="Al-Itqan Semantic Search", theme=gr.themes.Soft()) as demo:
    gr.Markdown("""
    # 🔍 Al-Itqan — Semantic Hadith Search
    Search 87,688 hadiths across 18 books in **Arabic or English**.
    Uses multilingual embeddings — finds hadiths by *meaning*, not just keywords.
    Results include Quran family tags from the root bridge.
    """)

    with gr.Row():
        query_box = gr.Textbox(
            label="Query (Arabic or English)",
            placeholder="مكارم الأخلاق  |  patience in hardship  |  رحمة الأيتام",
            lines=2,
        )
    with gr.Row():
        top_k_sl      = gr.Slider(3, 20, value=7, step=1, label="Results")
        book_dd       = gr.Dropdown(books_list,    value="All", label="Filter: Book")
        family_dd     = gr.Dropdown(families_list, value="All", label="Filter: Family")
        grade_dd      = gr.Dropdown(["All", "sahih_hasan"], value="All", label="Filter: Grade")
    submit = gr.Button("Search", variant="primary")
    output = gr.Markdown()

    submit.click(search, inputs=[query_box, top_k_sl, book_dd, family_dd, grade_dd], outputs=output)
    query_box.submit(search, inputs=[query_box, top_k_sl, book_dd, family_dd, grade_dd], outputs=output)

    gr.Markdown("""
    ---
    **Model:** `intfloat/multilingual-e5-small` (Apache-2.0) — supports Classical Arabic
    **Index:** 87,688 hadiths · 384-dim vectors · FAISS cosine similarity
    **Data:** Al-Itqan root bridge — each result tagged with Quran thematic family
    """)

demo.launch()
