"""
Al-Itqan — Hadith Q&A + Concordance Search
===========================================
Two modes accessible via a toggle:

  CONCORDANCE MODE (default — starts instantly, no models)
    • Arabic input  → exact morphological lookup in concordance.json
    • English input → yellow banner + silent FAISS semantic search (loads embedder once)
    • Returns a styled list of hadith cards with collapsible family tags

  RAG MODE (loads ~3.5GB of models on first use)
    • Any language → FAISS retrieval → Qwen2.5-1.5B generates a cited answer
    • Multi-turn conversation with full session history

Environment variables:
  USE_LOCAL_INDEX  — set to any value to skip HF download (dev / NAS mode)
  INDEX_PATH       — path to semantic_index.faiss  (default: /index/semantic_index.faiss)
  META_PATH        — path to semantic_meta.json     (default: /index/semantic_meta.json)
  CONCORDANCE_PATH — path to concordance.json       (default: /data/concordance.json)
  HF_INDEX_REPO    — HuggingFace repo id            (default: iqrossed/al-itqan-index)
"""

import os, json, re, unicodedata
from threading import Thread

import numpy as np
import faiss
import gradio as gr
import torch
from huggingface_hub import hf_hub_download

# ── Config ─────────────────────────────────────────────────────────────────────
EMBED_MODEL    = "intfloat/multilingual-e5-small"
GEN_MODEL      = "Qwen/Qwen2.5-1.5B-Instruct"
DEFAULT_TOP_K  = 10
MAX_NEW_TOKS   = 600
HISTORY_WINDOW = 3

HF_REPO          = os.getenv("HF_INDEX_REPO",    "iqrossed/al-itqan-index")
USE_LOCAL        = os.getenv("USE_LOCAL_INDEX")
INDEX_PATH       = os.getenv("INDEX_PATH",        "/index/semantic_index.faiss")
META_PATH        = os.getenv("META_PATH",         "/index/semantic_meta.json")
CONCORDANCE_PATH = os.getenv("CONCORDANCE_PATH",  "/data/concordance.json")

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

GRADE_LABELS = {
    "sahih": "✅ Sahih",
    "hasan": "🟡 Hasan",
    "daif":  "❌ Da'if",
    "":      "",
}

FAMILY_COLORS = {
    "worship":              "#4a90d9",
    "knowledge":            "#7b68ee",
    "end_of_times":         "#e74c3c",
    "fighting":             "#c0392b",
    "provision":            "#27ae60",
    "speech_communication": "#f39c12",
    "body":                 "#1abc9c",
    "time":                 "#8e44ad",
    "movement_journey":     "#2980b9",
    "statecraft":           "#d35400",
    "family_law":           "#c0392b",
}

# ── Startup: load index + concordance ─────────────────────────────────────────
print("Loading FAISS index and metadata...")
if USE_LOCAL:
    index_path        = INDEX_PATH
    meta_path         = META_PATH
    concordance_path  = CONCORDANCE_PATH
else:
    print(f"Downloading index files from {HF_REPO}...")
    index_path       = hf_hub_download(repo_id=HF_REPO, filename="semantic_index.faiss", repo_type="dataset", token=False)
    meta_path        = hf_hub_download(repo_id=HF_REPO, filename="semantic_meta.json", repo_type="dataset", token=False)
    concordance_path = hf_hub_download(repo_id=HF_REPO, filename="concordance.json", repo_type="dataset", token=False)

INDEX = faiss.read_index(index_path)
META  = json.load(open(meta_path, encoding="utf-8"))

# Build uid → metadata dict for O(1) concordance result resolution
UID_MAP = {f"{m['book']}:{m['num']}": m for m in META}
print(f"Index ready — {INDEX.ntotal:,} hadiths, {len(UID_MAP):,} in uid map")

print(f"Loading concordance from {concordance_path}...")
try:
    CONCORDANCE = json.load(open(concordance_path, encoding="utf-8"))
    print(f"Concordance ready — {len(CONCORDANCE):,} words")
except FileNotFoundError:
    CONCORDANCE = {}
    print("WARNING: concordance.json not found — concordance mode will use FAISS only")

# ── Lazy-loaded models ─────────────────────────────────────────────────────────
_embedder  = None
_tokenizer = None
_gen_model = None
_generator_ready = False

def get_embedder():
    global _embedder
    if _embedder is None:
        print("Loading embedding model (first use)...")
        from sentence_transformers import SentenceTransformer
        _embedder = SentenceTransformer(EMBED_MODEL)
        print("Embedding model ready.")
    return _embedder

def get_generator():
    global _tokenizer, _gen_model, _generator_ready
    if not _generator_ready:
        print(f"Loading generation model: {GEN_MODEL} (first use)...")
        from transformers import AutoTokenizer, AutoModelForCausalLM
        _tokenizer = AutoTokenizer.from_pretrained(GEN_MODEL)
        _gen_model  = AutoModelForCausalLM.from_pretrained(
            GEN_MODEL,
            torch_dtype=torch.float16 if torch.cuda.is_available() else torch.float32,
            device_map="auto",
        )
        _gen_model.eval()
        _generator_ready = True
        print("Generation model ready.")
    return _tokenizer, _gen_model

# ── Arabic normalization ───────────────────────────────────────────────────────
_DIACRITICS = re.compile(r'[\u0610-\u061A\u064B-\u065F\u0670\u06D6-\u06DC\u06DF-\u06E4\u06E7\u06E8\u06EA-\u06ED]')
_ALEF       = re.compile(r'[أإآٱ]')
_TAA        = re.compile(r'ة')

def normalize_arabic(text: str) -> str:
    text = _DIACRITICS.sub('', text)
    text = _ALEF.sub('ا', text)
    text = _TAA.sub('ه', text)
    return text.strip()

def arabic_tokens(text: str) -> list[str]:
    text = normalize_arabic(text)
    return [t for t in re.split(r'[\s،,؛;]+', text) if len(t) > 1]

def detect_lang(text: str) -> str:
    arabic_chars = len(re.findall(r'[\u0600-\u06FF]', text))
    return "ar" if arabic_chars > len(text) * 0.3 else "en"

# ── Concordance lookup ─────────────────────────────────────────────────────────
def concordance_search(query: str, top_k: int = DEFAULT_TOP_K) -> list[dict]:
    """
    Look up each Arabic token in concordance.json.
    Strategy: intersect all token result sets; fall back to union if empty.
    Returns list of metadata dicts.
    """
    tokens = arabic_tokens(query)
    if not tokens or not CONCORDANCE:
        return []

    sets = []
    for tok in tokens:
        norm = normalize_arabic(tok)
        if norm in CONCORDANCE:
            sets.append(set(CONCORDANCE[norm]))

    if not sets:
        return []

    # Intersection first, fall back to union
    result_uids = sets[0].intersection(*sets[1:]) if len(sets) > 1 else sets[0]
    if not result_uids:
        result_uids = sets[0].union(*sets[1:])

    results = []
    for uid in list(result_uids)[:top_k * 3]:
        m = UID_MAP.get(uid)
        if m:
            results.append(m)
        if len(results) >= top_k:
            break

    return results[:top_k]

# ── Keyword search (catches proper nouns FAISS misses) ────────────────────────
def keyword_search(query: str, top_k: int = DEFAULT_TOP_K) -> list[dict]:
    """Simple text match in English and Arabic fields. Returns ALL matches up to top_k."""
    terms = [t.lower() for t in query.split() if len(t) > 2]
    if not terms:
        return []
    results = []
    for m in META:
        en = m.get("en", "").lower()
        ar = m.get("ar", "")
        if all(t in en or t in ar for t in terms):
            results.append(m)
    return results[:top_k]

# ── FAISS retrieval ────────────────────────────────────────────────────────────
def faiss_search(query: str, top_k: int = DEFAULT_TOP_K) -> list[dict]:
    """Semantic search — loads embedder on first call. Falls back to keyword."""
    # Try keyword search first for proper nouns
    kw_hits = keyword_search(query, top_k)
    if len(kw_hits) >= 3:
        return kw_hits

    embedder = get_embedder()
    vec = embedder.encode(
        ["query: " + query],
        normalize_embeddings=True,
        convert_to_numpy=True,
    ).astype("float32")
    scores, idxs = INDEX.search(vec, top_k * 2)
    results, seen = [], set()

    # Merge keyword hits first (they're exact matches)
    for m in kw_hits:
        uid = f"{m['book']}:{m['num']}"
        seen.add(uid)
        results.append(m)

    for score, idx in zip(scores[0], idxs[0]):
        if idx < 0 or idx >= len(META):
            continue
        m = META[idx]
        uid = f"{m['book']}:{m['num']}"
        if uid in seen:
            continue
        seen.add(uid)
        results.append(m)
        if len(results) >= top_k:
            break
    return results

# ── HTML card renderer ─────────────────────────────────────────────────────────
CARD_CSS = """
<style>
.itqan-results { font-family: 'Segoe UI', sans-serif; max-width: 860px; }
.itqan-card {
  background: #fafaf8; border: 1px solid #e0ddd5; border-radius: 10px;
  padding: 16px 20px; margin-bottom: 14px;
  box-shadow: 0 1px 4px rgba(0,0,0,0.06);
}
.itqan-ref {
  font-size: .78rem; color: #1a5276; margin-bottom: 8px;
  display: flex; gap: 10px; align-items: center; font-weight: 600;
}
.itqan-grade { font-size: .72rem; padding: 2px 8px; border-radius: 4px;
  background: #eef2f7; color: #555; }
.itqan-arabic {
  font-family: 'Traditional Arabic', 'Scheherazade New', 'Amiri', serif;
  font-size: 1.2rem; line-height: 2.0; direction: rtl; text-align: right;
  color: #1a3a2a; margin-bottom: 10px; padding: 10px 14px;
  background: #f5f0e8; border-radius: 6px; border-right: 3px solid #c9a96e;
}
.itqan-english { font-size: .85rem; color: #333; line-height: 1.7; margin-bottom: 8px; }
details.itqan-families { margin-top: 6px; }
details.itqan-families summary {
  cursor: pointer; font-size: .75rem; color: #888;
  list-style: none; user-select: none;
}
details.itqan-families summary::-webkit-details-marker { display: none; }
details.itqan-families summary::before { content: '▶  '; font-size: .65rem; }
details[open].itqan-families summary::before { content: '▼  '; }
.itqan-tags { display: flex; flex-wrap: wrap; gap: 6px; margin-top: 8px; }
.itqan-tag {
  font-size: .72rem; padding: 3px 10px; border-radius: 12px;
  border: 1px solid; opacity: .9; background: #fff;
}
.itqan-banner {
  background: #fff8e1; border: 1px solid #ffe082; border-radius: 6px;
  padding: 10px 14px; margin-bottom: 14px; font-size: .82rem; color: #8d6e00;
}
.itqan-empty { color: #999; font-size: .9rem; padding: 20px 0; }
</style>
"""

def render_cards(hits: list[dict], banner: str = "") -> str:
    if not hits:
        return CARD_CSS + '<div class="itqan-results"><div class="itqan-empty">No hadiths found for this query.</div></div>'

    parts = [CARD_CSS, '<div class="itqan-results">']
    if banner:
        parts.append(f'<div class="itqan-banner">{banner}</div>')

    for m in hits:
        book_name = BOOK_NAMES.get(m.get("book", ""), m.get("book", ""))
        num       = m.get("num", "")
        grade     = GRADE_LABELS.get(m.get("grade", ""), "")
        arabic    = m.get("ar", "")
        english   = m.get("en", "")
        families  = m.get("families", [])

        tag_html = ""
        if families:
            tags = []
            for f in families:
                color = FAMILY_COLORS.get(f, "#555")
                tags.append(
                    f'<span class="itqan-tag" style="color:{color};border-color:{color}55">'
                    f'{f.replace("_", " ")}</span>'
                )
            tag_html = (
                '<details class="itqan-families">'
                '<summary>Thematic families</summary>'
                f'<div class="itqan-tags">{"".join(tags)}</div>'
                '</details>'
            )

        parts.append(f"""
<div class="itqan-card">
  <div class="itqan-ref">
    <strong>{book_name} #{num}</strong>
    {f'<span class="itqan-grade">{grade}</span>' if grade else ''}
  </div>
  <div class="itqan-arabic">{arabic}</div>
  <div class="itqan-english">{english}</div>
  {tag_html}
</div>""")

    parts.append('</div>')
    return "".join(parts)

# ── RAG helpers ────────────────────────────────────────────────────────────────
def make_retrieval_query(question: str, history: list) -> str:
    if not history or len(question.split()) >= 7:
        return question
    last_user = next(
        (m["content"] for m in reversed(history) if m["role"] == "user"), ""
    )
    return f"{last_user} {question}" if last_user else question

def build_rag_prompt(question: str, hits: list[dict], lang: str, history: list) -> str:
    tokenizer, _ = get_generator()
    context_blocks = []
    for i, m in enumerate(hits, 1):
        book_name = BOOK_NAMES.get(m.get("book", ""), m.get("book", ""))
        grade_str = GRADE_LABELS.get(m.get("grade", ""), "")
        families  = ", ".join(m.get("families", [])[:3]) or "—"
        arabic    = m.get("ar", "")[:400]
        english   = m.get("en", "")[:400]
        weak_note = " ⚠ Da'if — note this." if m.get("grade") == "daif" else ""
        context_blocks.append(
            f"[Hadith {i}] {book_name} #{m.get('num','')} {grade_str}{weak_note}\n"
            f"Families: {families}\n"
            f"Arabic: {arabic}\n"
            f"English: {english}"
        )

    context = "\n\n".join(context_blocks)

    if lang == "ar":
        system_msg = (
            "أنت عالم إسلامي متخصص في علوم الحديث. "
            "أجب بناءً على الأحاديث المقدمة فقط — لا تختلق مصادر. "
            "اذكر اسم الكتاب ورقم الحديث لكل حديث تستشهد به. "
            "إذا كان الحديث ضعيفاً فنبّه على ذلك. "
            "إذا لم تجد إجابة في الأحاديث المقدمة قل ذلك صراحة."
        )
        user_msg = f"الأحاديث:\n\n{context}\n\nالسؤال: {question}"
    else:
        system_msg = (
            "You are an Islamic scholar specializing in hadith sciences. "
            "Answer based ONLY on the provided hadiths — do not fabricate sources. "
            "Cite each hadith by book name and number. "
            "Flag da'if (weak) hadiths explicitly. "
            "If the hadiths do not answer the question, say so clearly."
        )
        user_msg = f"Relevant hadiths:\n\n{context}\n\nQuestion: {question}"

    prior = [m for m in history[-(HISTORY_WINDOW * 2):]]
    messages = [{"role": "system", "content": system_msg}]
    messages.extend(prior)
    messages.append({"role": "user", "content": user_msg})

    return tokenizer.apply_chat_template(messages, tokenize=False, add_generation_prompt=True)

def format_sources(hits: list[dict]) -> str:
    lines = ["**Sources retrieved:**"]
    for i, m in enumerate(hits, 1):
        book_name = BOOK_NAMES.get(m.get("book", ""), m.get("book", ""))
        grade     = GRADE_LABELS.get(m.get("grade", ""), "")
        families  = ", ".join(m.get("families", [])[:3]) or "—"
        snippet   = m.get("en", "")[:130]
        if len(m.get("en", "")) > 130:
            snippet += "…"
        lines.append(
            f"{i}. **{book_name} #{m.get('num','')}** {grade}  \n"
            f"   Families: {families}  \n"
            f"   {snippet}"
        )
    return "\n".join(lines)

# ── Concordance handler ────────────────────────────────────────────────────────
def handle_concordance(query: str, top_k: int):
    query = query.strip()
    if not query:
        yield gr.update(value=""), gr.update(value="")
        return

    lang   = detect_lang(query)
    banner = ""

    if lang == "en":
        # Try keyword first, then FAISS
        all_kw = keyword_search(query, top_k=9999)
        if all_kw:
            hits = all_kw[:top_k]
            banner = f"Found {len(all_kw)} hadiths matching \"{query}\" — showing {len(hits)}. Drag the slider to see more."
        else:
            banner = "⚠ No keyword matches — using semantic search."
            hits = faiss_search(query, top_k)
    else:
        hits = concordance_search(query, top_k)
        if not hits:
            banner = "⚠ No concordance matches found — showing semantic search results."
            hits = faiss_search(query, top_k)
        else:
            banner = f"Found {len(hits)} hadiths via concordance."

    html = render_cards(hits, banner)
    yield gr.update(value=html), gr.update(value="")

# ── RAG handler (streaming) ────────────────────────────────────────────────────
def handle_rag(question: str, history: list, show_sources: bool, top_k: int):
    from transformers import TextIteratorStreamer

    question = question.strip()
    if not question:
        yield history, "", history, gr.update(value="")
        return

    lang        = detect_lang(question)
    retrieval_q = make_retrieval_query(question, history)
    hits        = faiss_search(retrieval_q, top_k)

    if not hits:
        msg = "لم أجد أحاديث ذات صلة." if lang == "ar" else "No relevant hadiths found."
        new_history = history + [
            {"role": "user",      "content": question},
            {"role": "assistant", "content": msg},
        ]
        yield new_history, "", new_history, gr.update(value="")
        return

    prompt    = build_rag_prompt(question, hits, lang, history)
    tokenizer, model = get_generator()
    inputs    = tokenizer(prompt, return_tensors="pt").to(model.device)
    streamer  = TextIteratorStreamer(tokenizer, skip_prompt=True, skip_special_tokens=True)
    thread    = Thread(target=model.generate, kwargs={
        **inputs,
        "max_new_tokens": MAX_NEW_TOKS,
        "do_sample":      False,
        "streamer":       streamer,
    })
    thread.start()

    new_history = history + [
        {"role": "user",      "content": question},
        {"role": "assistant", "content": ""},
    ]
    sources_md = format_sources(hits) if show_sources else ""

    partial = ""
    for chunk in streamer:
        partial += chunk
        new_history[-1]["content"] = partial
        yield new_history, sources_md, new_history, gr.update(value="")

    thread.join()
    new_history[-1]["content"] = re.sub(r'<\|.*?\|>', '', partial).strip()
    yield new_history, sources_md, new_history, gr.update(value="")

# ── Interface ──────────────────────────────────────────────────────────────────
_CSS = """
/* ── Page base — warm dark theme matching main Itqan app ── */
body, .gradio-container { background: #0a0a0f !important; color: #e0d8c8; }
.gradio-container { max-width: 920px !important; margin: 0 auto !important; padding: 0 16px 40px; }
footer { display: none !important; }

/* ── Header ────────────────────────────────────── */
.itq-header { text-align: center; padding: 28px 0 20px; border-bottom: 1px solid #2a2420; margin-bottom: 20px; }
.itq-header-ar { font-family: 'Noto Naskh Arabic', serif; font-size: 2rem; color: #d4a855; letter-spacing: .05em; }
.itq-header-en { font-size: .85rem; color: #8a8070; margin-top: 4px; }
.itq-header-stats { font-size: .78rem; color: #666; margin-top: 6px; }

/* ── Tabs ───────────────────────────────────────── */
.tab-nav button { background: transparent !important; color: #8a8070 !important;
  border: none !important; border-bottom: 2px solid transparent !important;
  border-radius: 0 !important; font-size: .9rem; padding: 8px 20px !important; }
.tab-nav button.selected { color: #d4a855 !important; border-bottom-color: #d4a855 !important; }

/* ── Search bar ─────────────────────────────────── */
.itq-search-row { display: flex; gap: 8px; align-items: flex-start; margin-bottom: 10px; }
textarea, input[type=text] {
  background: #1a1612 !important; border: 1px solid #333 !important;
  color: #e0d8c8 !important; border-radius: 8px !important; font-size: 1rem;
}
textarea:focus, input:focus { border-color: #d4a855 !important; outline: none !important; box-shadow: none !important; }
button.primary { background: #d4a855 !important; color: #0a0a0f !important;
  border: none !important; border-radius: 8px !important; font-weight: 600; }
button.primary:hover { background: #e0be82 !important; }

/* ── Arabic keyboard ────────────────────────────── */
.ar-kb-wrap { background: #131318; border: 1px solid #2a2420; border-radius: 8px; padding: 10px 12px; margin-bottom: 12px; }
.ar-kb-label { font-size: .72rem; color: #666; margin-bottom: 6px; }
.ar-kb { display: flex; flex-wrap: wrap; gap: 4px; direction: rtl; }
.ar-kb button {
  font-size: 1.1rem; padding: 5px 9px; border: 1px solid #333; border-radius: 5px;
  background: #1a1612; color: #d0c8b0; cursor: pointer; font-family: 'Noto Naskh Arabic', serif;
  min-width: 34px; text-align: center; transition: background .12s, border-color .12s;
}
.ar-kb button:hover { background: #2a2218; border-color: #d4a855; color: #e8d8a0; }

/* ── Slider ─────────────────────────────────────── */
input[type=range] { accent-color: #d4a855; }
label { color: #8a8070 !important; font-size: .8rem !important; }

/* ── Chatbot ─────────────────────────────────────── */
.message-wrap { background: #131318 !important; }
.message.user div { background: #2a2218 !important; color: #e0d8c8 !important; border-radius: 8px !important; }
.message.bot div { background: #1a1612 !important; color: #e0d8c8 !important; border-radius: 8px !important; }

/* ── Results cards ─────────────────────────────── */
.itqan-card { background: #1a1612; border: 1px solid #2a2420; border-radius: 8px; padding: 14px; margin-bottom: 10px; }
.itqan-card:hover { border-color: #d4a855; }
.itqan-ref { color: #d4a855; font-weight: 600; font-size: .85rem; }
.itqan-grade-sahih { color: #4aaa70; font-size: .75rem; }
.itqan-grade-daif { color: #e74c3c; font-size: .75rem; }
.itqan-arabic { direction: rtl; font-family: 'Noto Naskh Arabic', serif; font-size: 1.05rem; line-height: 1.9; color: #e0d8c8; margin: 8px 0; }
.itqan-english { color: #a09888; font-size: .88rem; line-height: 1.6; }
.itqan-families { display: flex; gap: 4px; flex-wrap: wrap; margin-top: 6px; }
.itqan-fam-tag { font-size: .68rem; color: #8a8070; background: #1e1a16; border: 1px solid #2a2420; border-radius: 10px; padding: 1px 8px; }

/* ── Disclaimer ─────────────────────────────────── */
.itq-footer { font-size: .75rem; color: #555; text-align: center; margin-top: 24px; border-top: 1px solid #2a2420; padding-top: 14px; }
"""

_AR_KB_HTML = """
<div class="ar-kb-wrap">
<div class="ar-kb-label">Arabic keyboard — click to insert</div>
<div class="ar-kb" id="arKeyboard">
  <button onclick="insertAr('ا')">ا</button><button onclick="insertAr('ب')">ب</button>
  <button onclick="insertAr('ت')">ت</button><button onclick="insertAr('ث')">ث</button>
  <button onclick="insertAr('ج')">ج</button><button onclick="insertAr('ح')">ح</button>
  <button onclick="insertAr('خ')">خ</button><button onclick="insertAr('د')">د</button>
  <button onclick="insertAr('ذ')">ذ</button><button onclick="insertAr('ر')">ر</button>
  <button onclick="insertAr('ز')">ز</button><button onclick="insertAr('س')">س</button>
  <button onclick="insertAr('ش')">ش</button><button onclick="insertAr('ص')">ص</button>
  <button onclick="insertAr('ض')">ض</button><button onclick="insertAr('ط')">ط</button>
  <button onclick="insertAr('ظ')">ظ</button><button onclick="insertAr('ع')">ع</button>
  <button onclick="insertAr('غ')">غ</button><button onclick="insertAr('ف')">ف</button>
  <button onclick="insertAr('ق')">ق</button><button onclick="insertAr('ك')">ك</button>
  <button onclick="insertAr('ل')">ل</button><button onclick="insertAr('م')">م</button>
  <button onclick="insertAr('ن')">ن</button><button onclick="insertAr('ه')">ه</button>
  <button onclick="insertAr('و')">و</button><button onclick="insertAr('ي')">ي</button>
  <button onclick="insertAr('ء')">ء</button><button onclick="insertAr('ة')">ة</button>
  <button onclick="insertAr('ى')">ى</button><button onclick="insertAr('ئ')">ئ</button>
  <button onclick="insertAr('ؤ')">ؤ</button><button onclick="insertAr('إ')">إ</button>
  <button onclick="insertAr('أ')">أ</button><button onclick="insertAr('آ')">آ</button>
  <button onclick="insertAr(' ')" style="min-width:72px">space</button>
  <button onclick="insertAr('back')" style="min-width:46px">&#x232B;</button>
</div>
<script>
function insertAr(ch) {
  const ta = document.querySelector('.itq-search textarea') || document.querySelector('textarea');
  if (!ta) return;
  ta.focus();
  if (ch === 'back') { ta.value = ta.value.slice(0, -1); }
  else { ta.value += ch; }
  ta.dispatchEvent(new Event('input', {bubbles: true}));
}
</script>
</div>
"""

with gr.Blocks(title="Itqan AI — الإتقان", css=_CSS) as demo:

    gr.HTML("""
    <div class="itq-header">
      <div class="itq-header-ar">الإتقان — Itqan AI</div>
      <div class="itq-header-en">Hadith Search &amp; Q&amp;A</div>
      <div class="itq-header-stats">112,221 hadiths · 18 books · Arabic concordance · semantic search · AI Q&amp;A</div>
    </div>
    """)

    state = gr.State([])

    with gr.Tabs() as tabs:

        # ── Tab 1: Concordance ─────────────────────────────────────────────────
        with gr.Tab("Concordance  ⊕"):
            gr.HTML('<div style="font-size:.8rem;color:#666;margin-bottom:10px">Arabic root search · keyword search · up to 500 results</div>')
            with gr.Row(elem_classes="itq-search"):
                query_box = gr.Textbox(
                    placeholder="رحمة — or type English keywords…",
                    lines=1, show_label=False, scale=5,
                )
                search_btn = gr.Button("Search", variant="primary", scale=1)

            top_k_sl = gr.Slider(minimum=5, maximum=500, value=10, step=5,
                                  label="Results to show")
            concordance_out = gr.HTML()

        # ── Tab 2: RAG Chat ────────────────────────────────────────────────────
        with gr.Tab("RAG Chat  ✦"):
            gr.HTML('<div style="font-size:.8rem;color:#666;margin-bottom:10px">Ask any question · AI reads relevant hadiths · cites sources · first query loads model (~3 min)</div>')
            chatbot   = gr.Chatbot(height=420, show_label=False)
            with gr.Row():
                rag_box   = gr.Textbox(placeholder="What does hadith say about…", lines=1,
                                        show_label=False, scale=5)
                rag_btn   = gr.Button("Ask", variant="primary", scale=1)
            with gr.Row():
                show_src  = gr.Checkbox(value=True, label="Show retrieved hadiths")
                rag_top_k = gr.Slider(minimum=3, maximum=20, value=10, step=1, label="Hadiths to retrieve")
                clear_btn = gr.Button("Clear", size="sm")
            sources = gr.Markdown()

    gr.HTML('<div class="itq-footer">Weak (da\'if) hadiths are flagged with ❌ &nbsp;·&nbsp; Always verify rulings with a qualified scholar.</div>')

    # ── Concordance submit ─────────────────────────────────────────────────────
    def do_concordance(query, top_k):
        for html, _ in handle_concordance(query, top_k):
            yield html

    search_btn.click(do_concordance, inputs=[query_box, top_k_sl], outputs=[concordance_out])
    query_box.submit(do_concordance, inputs=[query_box, top_k_sl], outputs=[concordance_out])

    # ── RAG submit ─────────────────────────────────────────────────────────────
    def do_rag(question, history, show_sources, top_k):
        for h, s, st, _ in handle_rag(question, history, show_sources, top_k):
            yield h, s, st, gr.update(value="")

    rag_btn.click(do_rag, inputs=[rag_box, state, show_src, rag_top_k],
                  outputs=[chatbot, sources, state, rag_box])
    rag_box.submit(do_rag, inputs=[rag_box, state, show_src, rag_top_k],
                   outputs=[chatbot, sources, state, rag_box])
    clear_btn.click(lambda: ([], [], ""), outputs=[chatbot, state, sources])

demo.launch()
