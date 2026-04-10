---
title: Al-Itqan Semantic Hadith Search
emoji: 🔍
colorFrom: indigo
colorTo: purple
sdk: gradio
sdk_version: "5.23.3"
app_file: app.py
pinned: false
license: apache-2.0
---

# Al-Itqan — Semantic Hadith Search

Search **112,221 hadiths** across 18 books by *meaning*, not just keywords.
Supports Arabic and English queries using multilingual embeddings.

Researched, compiled & developed by [Ali Bin Shahid](https://www.linkedin.com/in/alibinshahid/)

## Features

- Arabic + English query support (auto-detected)
- Filter by book, thematic family, or grade (Sahih/Hasan)
- Results include Arabic text, English translation, grade, and Quran family tags
- ~300ms search latency on CPU

## Model

`intfloat/multilingual-e5-small` — Apache-2.0, 117MB, 100-language support including Classical Arabic.

## Data

Part of the [Itqan](https://github.com/R3GENESI5/Itqan) project:
- 112,221 hadiths across 18 Sunni books (including full Musnad Ahmad, Arnaut edition)
- 1,590 shared Arabic roots generating 1,528,346 Quran-Hadith links
- 115,112 narrator profiles with jarh wa ta'dil
- 39 thematic families from classical lexicography
- FAISS index hosted at [iqrossed/al-itqan-index](https://huggingface.co/datasets/iqrossed/al-itqan-index)
