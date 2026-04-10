---
title: Al-Itqan Hadith Q&A
emoji: 📖
colorFrom: green
colorTo: blue
sdk: gradio
sdk_version: "5.23.3"
app_file: app.py
pinned: false
license: apache-2.0
---

# Al-Itqan — Hadith Q&A (RAG)

Ask questions about Islamic topics in Arabic or English.
Answers are grounded in 112,221 hadiths across 18 books — sources always cited, no hallucination.

Researched, compiled & developed by [Ali Bin Shahid](https://www.linkedin.com/in/alibinshahid/)

## How it works

1. Your question is encoded with `intfloat/multilingual-e5-small`
2. FAISS retrieves the most semantically relevant hadiths from 112,221 indexed entries
3. `Qwen2.5-1.5B-Instruct` generates a cited answer from those hadiths only

## Models

| Role | Model | License |
|------|-------|---------|
| Embeddings | `intfloat/multilingual-e5-small` | Apache-2.0 |
| Generation | `Qwen/Qwen2.5-1.5B-Instruct` | Apache-2.0 |

## Limitation

The model answers **only** from retrieved hadiths. If no relevant hadith is found
it will say so rather than fabricate. Always verify rulings with a qualified scholar.

## Data

Part of the [Itqan](https://github.com/R3GENESI5/Itqan) project:
- 112,221 hadiths across 18 Sunni books (including full Musnad Ahmad, Arnaut edition)
- 1,590 shared Arabic roots generating 1,528,346 Quran-Hadith links
- 115,112 narrator profiles with jarh wa ta'dil
- 39 thematic families from classical lexicography
- FAISS index hosted at [iqrossed/al-itqan-index](https://huggingface.co/datasets/iqrossed/al-itqan-index)
