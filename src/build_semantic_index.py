"""
build_semantic_index.py  —  BasilSuhail upgrade
================================================
Embeds all 87,688 hadiths with a multilingual sentence-transformer
and builds a FAISS index for semantic search.

Model: intfloat/multilingual-e5-small  (117MB, Apache-2.0)
  - Supports Arabic natively (trained on 100 languages)
  - 384-dim vectors, cosine similarity
  - Runs on CPU in ~60–90 min for 87k hadiths
  - On GPU (Colab T4): ~8 min

Outputs (upload these to your HuggingFace model repo):
  app/data/semantic/semantic_index.faiss   — FAISS index
  app/data/semantic/semantic_meta.json     — id → metadata mapping

Usage:
  pip install sentence-transformers faiss-cpu
  python src/build_semantic_index.py
  # Then upload outputs to HuggingFace Hub:
  # huggingface-cli upload your-org/al-itqan-index app/data/semantic/ .
"""

import json, os, time
from pathlib import Path

ROOT    = Path(__file__).resolve().parent.parent
DATA    = ROOT / 'app' / 'data'
OUT_DIR = DATA / 'semantic'
OUT_DIR.mkdir(exist_ok=True)

# ── 1. Load sentence-transformer ─────────────────────────────────────────────
print('Loading multilingual-e5-small...')
from sentence_transformers import SentenceTransformer
import numpy as np
import faiss

MODEL_NAME = 'intfloat/multilingual-e5-small'
model = SentenceTransformer(MODEL_NAME)
DIM = model.get_sentence_embedding_dimension()
print(f'  Model: {MODEL_NAME}  dim={DIM}')

# ── 2. Load bridge for family tags ───────────────────────────────────────────
print('Loading bridge for family tags...')
bridge = json.load(open(DATA / 'quran_hadith_bridge.json', encoding='utf-8'))

# Build: hadith_id → [family_keys]
hadith_to_families = {}
fam_data = json.load(open(DATA / 'family_corpus.json', encoding='utf-8'))
for fam_key, fd in fam_data.items():
    for hid in fd.get('hadith_ids', []):
        hadith_to_families.setdefault(hid, []).append(fam_key)

print(f'  {len(hadith_to_families):,} hadiths tagged with families')

# ── 3. Collect all hadiths ───────────────────────────────────────────────────
print('Collecting hadiths...')
records = []   # {id, text, meta}

for book_dir in sorted((DATA / 'sunni').iterdir()):
    if not book_dir.is_dir(): continue
    idx = book_dir / 'index.json'
    if not idx.exists(): continue
    chapters = json.load(open(idx, encoding='utf-8'))
    book_id  = book_dir.name
    for ch in chapters:
        ch_file = book_dir / ch['file']
        if not ch_file.exists(): continue
        for h in json.load(open(ch_file, encoding='utf-8')):
            hid     = f"{book_id}:{h.get('idInBook','')}"
            arabic  = h.get('arabic', '').strip()
            en      = h.get('english', {})
            english = ''
            if isinstance(en, dict):
                english = (en.get('narrator','') + ' ' + en.get('text','')).strip()
            elif isinstance(en, str):
                english = en.strip()

            if not arabic and not english:
                continue

            # multilingual-e5 instruction prefix for retrieval
            # Use Arabic for Arabic text, English otherwise
            text_for_embed = (
                'passage: ' + arabic if arabic
                else 'passage: ' + english
            )

            records.append({
                'id':        hid,
                'book':      book_id,
                'hadith_num': h.get('idInBook',''),
                'arabic':    arabic[:500],
                'english':   english[:400],
                'families':  hadith_to_families.get(hid, [])[:5],
                'grade':     h.get('grade', ''),
                'text':      text_for_embed,
            })

print(f'  {len(records):,} hadiths collected')

# ── 4. Embed in batches ──────────────────────────────────────────────────────
print('Embedding (this takes 60–90 min on CPU, ~8 min on GPU)...')
BATCH = 256
texts   = [r['text'] for r in records]
all_vecs = []
t0 = time.time()

for i in range(0, len(texts), BATCH):
    batch = texts[i:i+BATCH]
    vecs  = model.encode(batch, normalize_embeddings=True,
                         show_progress_bar=False, convert_to_numpy=True)
    all_vecs.append(vecs)
    if (i // BATCH) % 20 == 0:
        elapsed = time.time() - t0
        done    = i + len(batch)
        eta     = elapsed / done * (len(texts) - done) if done else 0
        print(f'  {done:6,}/{len(texts):,}  {elapsed/60:.1f}m elapsed  ETA {eta/60:.1f}m')

embeddings = np.vstack(all_vecs).astype('float32')
print(f'  Embeddings shape: {embeddings.shape}')

# ── 5. Build FAISS index ──────────────────────────────────────────────────────
print('Building FAISS index (IndexFlatIP = exact cosine on normalized vecs)...')
index = faiss.IndexFlatIP(DIM)
index.add(embeddings)
print(f'  Index size: {index.ntotal:,} vectors')

faiss.write_index(index, str(OUT_DIR / 'semantic_index.faiss'))
print(f'  Saved semantic_index.faiss')

# ── 6. Save metadata ─────────────────────────────────────────────────────────
meta = [
    {
        'id':       r['id'],
        'book':     r['book'],
        'num':      r['hadith_num'],
        'ar':       r['arabic'],
        'en':       r['english'],
        'families': r['families'],
        'grade':    r['grade'],
    }
    for r in records
]
meta_path = OUT_DIR / 'semantic_meta.json'
with open(meta_path, 'w', encoding='utf-8') as f:
    json.dump(meta, f, ensure_ascii=False, separators=(',',':'))
sz = meta_path.stat().st_size // 1024 // 1024
print(f'  Saved semantic_meta.json ({sz} MB, {len(meta):,} entries)')

print('\n✓ Done. Next steps:')
print('  1. Upload to HuggingFace Hub:')
print('     huggingface-cli upload YOUR_ORG/al-itqan-index app/data/semantic/ .')
print('  2. Deploy hf_spaces/search/app.py as a HuggingFace Space')
print('  3. Set HF_REPO_ID env var in the Space to YOUR_ORG/al-itqan-index')
