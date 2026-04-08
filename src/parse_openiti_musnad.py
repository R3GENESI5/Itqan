"""
parse_openiti_musnad.py
========================
Parses Musnad Ahmad from OpenITI mARkdown format into the app's JSON structure.

Source: github.com/OpenITI/0250AH/.../0241IbnHanbal.Musnad.Shamela0025794-ara1.mARkdown
Edition: Shu'ayb al-Arna'ut (Mu'assasat al-Risalah, 2001)

Usage:
    python src/parse_openiti_musnad.py
"""

import json, re, os
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
INPUT = ROOT / "musnad_openiti.txt"
OUT_DIR = ROOT / "app" / "data" / "sunni" / "ahmed"

def parse():
    text = open(INPUT, encoding="utf-8").read()

    # Split into lines for processing
    lines = text.split("\n")

    current_volume = ""
    current_chapter = ""
    current_hadith_num = None
    current_hadith_lines = []

    chapters = {}  # chapter_key → {name_ar, name_en, hadiths: []}
    chapter_order = []

    def flush_hadith():
        nonlocal current_hadith_num, current_hadith_lines
        if current_hadith_num is None:
            return
        arabic = " ".join(current_hadith_lines).strip()
        # Clean OpenITI markers
        arabic = re.sub(r"~~", "", arabic)
        arabic = re.sub(r"PageV\d+P\d+", "", arabic)
        arabic = re.sub(r"\s+", " ", arabic).strip()

        if not arabic or len(arabic) < 20:
            current_hadith_num = None
            current_hadith_lines = []
            return

        ch_key = current_chapter or current_volume or "unknown"
        if ch_key not in chapters:
            chapters[ch_key] = {
                "name_ar": ch_key,
                "name_en": "",
                "hadiths": [],
            }
            chapter_order.append(ch_key)

        chapters[ch_key]["hadiths"].append({
            "hadithNumber": current_hadith_num,
            "arabic": arabic,
        })

        current_hadith_num = None
        current_hadith_lines = []

    for line in lines:
        line = line.rstrip()

        # Skip metadata
        if line.startswith("#META#") or line.startswith("######"):
            continue

        # Volume header: ### | ...
        m = re.match(r"^### \| (.+)", line)
        if m:
            flush_hadith()
            current_volume = m.group(1).strip()
            continue

        # Chapter header: ### || ...
        m = re.match(r"^### \|\| (.+)", line)
        if m:
            flush_hadith()
            current_chapter = m.group(1).strip()
            continue

        # Sub-chapter: ### ||| ...
        m = re.match(r"^### \|\|\| (.+)", line)
        if m:
            flush_hadith()
            current_chapter = m.group(1).strip()
            continue

        # Hadith start: # NUMBER - text...
        m = re.match(r"^# (\d+) - (.+)", line)
        if m:
            flush_hadith()
            current_hadith_num = int(m.group(1))
            current_hadith_lines = [m.group(2)]
            continue

        # Continuation line (may start with # for non-hadith content, or ~~ for continuation)
        if current_hadith_num is not None:
            # Lines starting with # but no number = section headers, flush
            if re.match(r"^# [^\d]", line) and not line.startswith("# بسم"):
                flush_hadith()
                continue
            # Continuation text
            cleaned = line.lstrip("~").lstrip()
            if cleaned:
                current_hadith_lines.append(cleaned)

    flush_hadith()

    # Save in app format
    if OUT_DIR.exists():
        for old in OUT_DIR.glob("*.json"):
            old.unlink()
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    index = []
    total = 0

    for i, ch_key in enumerate(chapter_order):
        ch_data = chapters[ch_key]
        hadiths = ch_data["hadiths"]
        if not hadiths:
            continue

        filename = f"{i + 1}.json"
        formatted = []
        for j, h in enumerate(hadiths):
            formatted.append({
                "idInBook": j + 1,
                "hadithNumber": h["hadithNumber"],
                "arabic": h["arabic"],
                "english": {"narrator": "", "text": ""},
            })

        with open(OUT_DIR / filename, "w", encoding="utf-8") as f:
            json.dump(formatted, f, ensure_ascii=False, separators=(",", ":"))

        index.append({
            "file": filename,
            "name_en": ch_data["name_en"] or ch_key,
            "name_ar": ch_data["name_ar"],
            "count": len(formatted),
        })
        total += len(formatted)

    with open(OUT_DIR / "index.json", "w", encoding="utf-8") as f:
        json.dump(index, f, ensure_ascii=False, indent=2)

    print(f"Parsed {total:,} hadiths across {len(index)} chapters")
    print(f"Saved to {OUT_DIR}/")
    for ch in index[:5]:
        print(f"  {ch['file']:10s} {ch['count']:>5} {ch['name_ar'][:50]}")
    if len(index) > 5:
        print(f"  ... and {len(index) - 5} more chapters")

if __name__ == "__main__":
    parse()
