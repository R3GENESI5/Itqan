"""
Hadith Data Pipeline
====================
Downloads and processes hadith data from:
  1. AhmedBaset/hadith-json (GitHub) — 17 Sunni books, Arabic + English
  2. meeAtif/hadith_datasets (HuggingFace) — grading for 6 Sunni books
  3. MohammedArab1/ThaqalaynAPI (GitHub) — Shia hadith (Al-Kafi + others)

Outputs structured JSON files under app/data/ for the web app.

Usage:
    pip install requests datasets tqdm
    python src/download_data.py
    python src/download_data.py --only sunni
    python src/download_data.py --only shia
    python src/download_data.py --only grades
    python src/download_data.py --build-search-index
"""

import argparse
import json
import os
import time
from pathlib import Path

import requests
from tqdm import tqdm

# ── Config ────────────────────────────────────────────────────────────────────

ROOT       = Path(__file__).parent.parent
DATA_SUNNI = ROOT / "app" / "data" / "sunni"
DATA_SHIA  = ROOT / "app" / "data" / "shia"

# GitHub raw base URLs
SUNNI_BASE = "https://raw.githubusercontent.com/AhmedBaset/hadith-json/main/db"
SHIA_BASE  = "https://raw.githubusercontent.com/MohammedArab1/ThaqalaynAPI/main/V1/ThaqalaynData"

# Map: app book_id → AhmedBaset by_chapter subfolder
# These folders contain numbered files (1.json, 2.json, ...) with chapter names
SUNNI_BOOKS = {
    # The 9 Books
    "bukhari":             "the_9_books/bukhari",
    "muslim":              "the_9_books/muslim",
    "abudawud":            "the_9_books/abudawud",
    "tirmidhi":            "the_9_books/tirmidhi",
    "nasai":               "the_9_books/nasai",
    "ibnmajah":            "the_9_books/ibnmajah",
    "ahmed":               "the_9_books/ahmed",
    "malik":               "the_9_books/malik",
    "darimi":              "the_9_books/darimi",
    # Forties (single file in forties/ folder)
    "nawawi40":            "forties/nawawi40",
    "qudsi40":             "forties/qudsi40",
    "shahwaliullah40":     "forties/shahwaliullah40",
    # Other books
    "riyad_assalihin":     "other_books/riyad_assalihin",
    "aladab_almufrad":     "other_books/aladab_almufrad",
    "bulugh_almaram":      "other_books/bulugh_almaram",
    "mishkat_almasabih":   "other_books/mishkat_almasabih",
    "shamail_muhammadiyah":"other_books/shamail_muhammadiyah",
}

# Map: app book_id → ThaqalaynAPI JSON filename
SHIA_BOOKS = {
    "alkafi-1":               "Al-Kafi-Volume-1-Kulayni.json",
    "alkafi-2":               "Al-Kafi-Volume-2-Kulayni.json",
    "alkafi-3":               "Al-Kafi-Volume-3-Kulayni.json",
    "alkafi-4":               "Al-Kafi-Volume-4-Kulayni.json",
    "alkafi-5":               "Al-Kafi-Volume-5-Kulayni.json",
    "alkafi-6":               "Al-Kafi-Volume-6-Kulayni.json",
    "alkafi-7":               "Al-Kafi-Volume-7-Kulayni.json",
    "alkafi-8":               "Al-Kafi-Volume-8-Kulayni.json",
    "al-amali-mufid":         "Al-Amali-Mufid.json",
    "al-amali-saduq":         "Al-Amali-Saduq.json",
    "al-khisal":              "Al-Khisal-Saduq.json",
    "al-tawhid":              "Al-Tawhid-Saduq.json",
    "maani-al-akhbar":        "Maani-al-Akhbar-Saduq.json",
    "uyun-rida-1":            "Uyun-akhbar-al-Rida-Volume-1-Saduq.json",
    "uyun-rida-2":            "Uyun-akhbar-al-Rida-Volume-2-Saduq.json",
    "kamil-al-ziyarat":       "Kamil-al-Ziyarat-Qummi.json",
    "kitab-al-ghayba-numani": "Kitab-al-Ghayba-Numani.json",
    "kitab-al-ghayba-tusi":   "Kitab-al-Ghayba-Tusi.json",
}

# Books that have grading from meeAtif dataset
GRADED_BOOKS = {"abudawud", "tirmidhi", "nasai", "ibnmajah"}

# meeAtif HuggingFace book name → our book_id
HF_BOOK_MAP = {
    "Jami' at-Tirmidhi":  "tirmidhi",
    "Sunan Abu Dawud":    "abudawud",
    "Sunan an-Nasa'i":    "nasai",
    "Sunan Ibn Majah":    "ibnmajah",
    "Sahih al-Bukhari":   "bukhari",
    "Sahih Muslim":       "muslim",
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def fetch_json(url: str, retries: int = 3) -> dict | list | None:
    for attempt in range(retries):
        try:
            r = requests.get(url, timeout=30)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
            else:
                print(f"  ✗ Failed: {url} — {e}")
                return None


def write_json(path: Path, data, indent: int = None):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, separators=(",", ":") if indent is None else None, indent=indent)


def normalize_grade(raw: str) -> str:
    """Simplify grade strings: 'Sahih (Darussalam)' → 'Sahih (Darussalam)'  (keep as-is)."""
    return (raw or "").strip()


# ── Sunni pipeline ────────────────────────────────────────────────────────────

def fetch_chapter_file(book_id: str, subfolder: str, ch_num: int) -> dict | None:
    """Fetch a single by_chapter file. Returns None on 404."""
    url = f"{SUNNI_BASE}/by_chapter/{subfolder}/{ch_num}.json"
    try:
        r = requests.get(url, timeout=30)
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return r.json()
    except Exception:
        return None


def process_chapter_file(ch_data: dict) -> tuple[dict, list]:
    """
    Parse a by_chapter file into (chapter_meta, clean_hadiths).
    Structure: { chapter: {arabic, english}, hadiths: [...], metadata: {...} }
    """
    ch = ch_data.get("chapter") or {}
    name_ar = ch.get("arabic") or ch_data.get("metadata", {}).get("arabic", {}).get("introduction", "")
    name_en = ch.get("english") or ch_data.get("metadata", {}).get("english", {}).get("introduction", "")

    clean = []
    for h in ch_data.get("hadiths", []):
        eng = h.get("english", {})
        clean.append({
            "id":       h.get("id"),
            "idInBook": h.get("idInBook"),
            "arabic":   h.get("arabic", ""),
            "english": {
                "narrator": eng.get("narrator", "") if isinstance(eng, dict) else "",
                "text":     eng.get("text", "")     if isinstance(eng, dict) else str(eng),
            },
        })
    return {"name_ar": name_ar, "name_en": name_en}, clean


def download_sunni():
    print("\n── Downloading Sunni books (by_chapter) ────────────")
    for book_id, subfolder in tqdm(SUNNI_BOOKS.items(), desc="Books"):
        print(f"\n  → {book_id}")
        out_dir = DATA_SUNNI / book_id
        out_dir.mkdir(parents=True, exist_ok=True)

        chapter_index = []
        total_hadiths = 0

        # Forties use a single "all.json" file
        if subfolder.startswith("forties/"):
            url = f"{SUNNI_BASE}/by_chapter/{subfolder}/all.json"
            raw = fetch_json(url)
            if raw is None:
                print(f"    ✗ Failed to fetch {subfolder}/all.json")
                continue
            meta, clean = process_chapter_file(raw)
            write_json(out_dir / "1.json", clean)
            chapter_index.append({"file": "1.json", "name_ar": meta["name_ar"],
                                   "name_en": meta["name_en"], "count": len(clean)})
            total_hadiths = len(clean)
        else:
            # Numbered chapter files: fetch until 404
            ch_num = 1
            while True:
                raw = fetch_chapter_file(book_id, subfolder, ch_num)
                if raw is None:
                    break
                meta, clean = process_chapter_file(raw)
                filename = f"{ch_num}.json"
                write_json(out_dir / filename, clean)
                chapter_index.append({
                    "file":    filename,
                    "name_ar": meta["name_ar"],
                    "name_en": meta["name_en"],
                    "count":   len(clean),
                })
                total_hadiths += len(clean)
                ch_num += 1

        if not chapter_index:
            print(f"    ✗ No chapters fetched")
            continue

        write_json(out_dir / "index.json", chapter_index, indent=2)
        print(f"    ✓ {total_hadiths} hadiths, {len(chapter_index)} chapters")

    print("\n  ✓ Sunni books done.")


# ── Grades pipeline ───────────────────────────────────────────────────────────

def download_grades():
    """
    Fetches grading data from meeAtif/hadith_datasets on HuggingFace.
    Writes app/data/sunni/{book_id}/grades.json: { "idInBook": "grade string" }
    """
    print("\n── Downloading grading data ────────────────────────")
    try:
        from datasets import load_dataset
    except ImportError:
        print("  ✗ Install 'datasets': pip install datasets")
        return

    print("  Fetching meeAtif/hadith_datasets from HuggingFace…")
    try:
        ds = load_dataset("meeAtif/hadith_datasets", split="train")
    except Exception as e:
        print(f"  ✗ Failed to load dataset: {e}")
        return

    grade_data: dict[str, dict] = {}

    for row in tqdm(ds, desc="Processing grades"):
        book_name = row.get("Book", "")
        book_id   = HF_BOOK_MAP.get(book_name)
        if not book_id:
            continue

        # Extract hadith number from In-book_reference e.g. "Book 1, Hadith 3"
        ref = row.get("In-book_reference", "")
        hadith_num = None
        if "Hadith" in ref:
            try:
                hadith_num = int(ref.split("Hadith")[-1].strip().split()[0])
            except ValueError:
                pass

        if hadith_num is None:
            continue

        grade = normalize_grade(row.get("Grade", ""))
        if not grade:
            continue

        grade_data.setdefault(book_id, {})[str(hadith_num)] = grade

    for book_id, grades in grade_data.items():
        out_path = DATA_SUNNI / book_id / "grades.json"
        write_json(out_path, grades)
        print(f"  ✓ {book_id}: {len(grades)} grades saved")

    print("\n  ✓ Grades done.")


# ── Shia pipeline ─────────────────────────────────────────────────────────────

def process_shia_book(book_id: str, raw_data: dict | list) -> list:
    """
    Processes Thaqalayn flat-list JSON into per-category chapter files.
    Each item has: id, category, categoryId, chapter, englishText, arabicText,
                   majlisiGrading, behdudiGrading, mohseniGrading, URL
    Groups by categoryId → category name as chapter.
    """
    out_dir = DATA_SHIA / book_id
    out_dir.mkdir(parents=True, exist_ok=True)

    # Normalize to flat list
    if isinstance(raw_data, list):
        flat = raw_data
    else:
        flat = (raw_data.get("hadiths") or raw_data.get("Hadiths")
                or raw_data.get("data") or [])

    if not flat:
        print(f"  ✗ No hadiths found in {book_id}")
        return []

    # Group by categoryId (preserving order)
    from collections import OrderedDict
    categories: OrderedDict[int, dict] = OrderedDict()
    for h in flat:
        cid = h.get("categoryId") or h.get("chapterId") or 0
        if cid not in categories:
            categories[cid] = {
                "name_en": h.get("category") or h.get("chapter") or f"Section {cid}",
                "name_ar": "",
                "hadiths": [],
            }
        categories[cid]["hadiths"].append(h)

    chapter_index = []
    for file_idx, (cid, cat) in enumerate(categories.items()):
        clean_hadiths = []
        for h in cat["hadiths"]:
            # Pick best available grading
            grading = (h.get("majlisiGrading") or h.get("behdudiGrading")
                       or h.get("mohseniGrading") or "")
            clean_hadiths.append({
                "id":              h.get("id"),
                "arabic":          h.get("arabicText") or h.get("arabic") or "",
                "english":         h.get("englishText") or h.get("english") or "",
                "chapter":         h.get("chapter") or "",
                "majlisiGrading":  h.get("majlisiGrading") or "",
                "behdudiGrading":  h.get("behdudiGrading") or "",
                "mohseniGrading":  h.get("mohseniGrading") or "",
                "grade":           grading,
                "url":             h.get("URL") or "",
            })

        filename = f"{file_idx + 1}.json"
        write_json(out_dir / filename, clean_hadiths)

        chapter_index.append({
            "file":    filename,
            "name_ar": cat["name_ar"],
            "name_en": cat["name_en"],
            "count":   len(clean_hadiths),
        })

    write_json(out_dir / "index.json", chapter_index, indent=2)
    return chapter_index


def download_shia():
    print("\n── Downloading Shia books ──────────────────────────")
    for book_id, filename in tqdm(SHIA_BOOKS.items(), desc="Books"):
        url = f"{SHIA_BASE}/{filename}"
        print(f"\n  → {book_id}")
        raw = fetch_json(url)
        if raw is None:
            continue

        idx = process_shia_book(book_id, raw)
        total = sum(ch["count"] for ch in idx)
        print(f"    ✓ {total} hadiths, {len(idx)} chapters")

    print("\n  ✓ Shia books done.")


# ── Search index builder ──────────────────────────────────────────────────────

def build_search_index():
    """
    Builds a flat search index from all downloaded hadith files.
    Writes app/data/search_index.json and app/data/shia_search_index.json
    """
    print("\n── Building search indexes ─────────────────────────")

    # Load books metadata
    books_path = ROOT / "app" / "data" / "books.json"
    with open(books_path, encoding="utf-8") as f:
        books_meta = json.load(f)

    sunni_books = (
        books_meta["sunni"]["the_9_books"]
        + books_meta["sunni"]["forties"]
        + books_meta["sunni"]["other_books"]
    )
    book_lookup = {b["id"]: b for b in sunni_books}

    # ── Sunni index
    sunni_index = []
    for book_id in SUNNI_BOOKS:
        idx_path = DATA_SUNNI / book_id / "index.json"
        if not idx_path.exists():
            continue
        with open(idx_path, encoding="utf-8") as f:
            chapters = json.load(f)

        meta = book_lookup.get(book_id, {})

        # Load grades if available
        grades = {}
        grade_path = DATA_SUNNI / book_id / "grades.json"
        if grade_path.exists():
            with open(grade_path, encoding="utf-8") as f:
                grades = json.load(f)

        for ch_idx, ch in enumerate(chapters):
            ch_file = DATA_SUNNI / book_id / ch["file"]
            if not ch_file.exists():
                continue
            with open(ch_file, encoding="utf-8") as f:
                hadiths = json.load(f)
            for h in hadiths:
                id_in_book = str(h.get("idInBook", ""))
                sunni_index.append({
                    "bookId":    book_id,
                    "bookNameEn": meta.get("name_en", book_id),
                    "bookNameAr": meta.get("name_ar", ""),
                    "chapterIdx": ch_idx,
                    "chapterEn":  ch.get("name_en", ""),
                    "idInBook":   h.get("idInBook"),
                    "arabic":     h.get("arabic", ""),
                    "narrator":   h.get("english", {}).get("narrator", ""),
                    "text":       h.get("english", {}).get("text", ""),
                    "grade":      grades.get(id_in_book),
                })

    write_json(ROOT / "app" / "data" / "search_index.json", sunni_index)
    print(f"  ✓ Sunni search index: {len(sunni_index):,} hadiths")

    # ── Shia index
    shia_index = []
    shia_meta  = {b["id"]: b for b in books_meta["shia"]}
    for book_id in SHIA_BOOKS:
        idx_path = DATA_SHIA / book_id / "index.json"
        if not idx_path.exists():
            continue
        with open(idx_path, encoding="utf-8") as f:
            chapters = json.load(f)

        meta = shia_meta.get(book_id, {})
        for ch_idx, ch in enumerate(chapters):
            ch_file = DATA_SHIA / book_id / ch["file"]
            if not ch_file.exists():
                continue
            with open(ch_file, encoding="utf-8") as f:
                hadiths = json.load(f)
            for h in hadiths:
                shia_index.append({
                    "bookId":     book_id,
                    "bookNameEn": meta.get("name_en", book_id),
                    "chapterIdx": ch_idx,
                    "chapterEn":  ch.get("name_en", ""),
                    "id":         h.get("id"),
                    "arabic":     h.get("arabic", ""),
                    "chapter":    h.get("chapter", ""),
                    "text":       h.get("english", ""),
                    "grade":      h.get("grade") or h.get("majlisiGrading") or "",
                })

    write_json(ROOT / "app" / "data" / "shia_search_index.json", shia_index)
    print(f"  ✓ Shia search index: {len(shia_index):,} hadiths")
    print("\n  ✓ Search indexes done.")


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Hadith data pipeline")
    parser.add_argument("--only",  choices=["sunni", "shia", "grades"], help="Run only one stage")
    parser.add_argument("--build-search-index", action="store_true", help="Build search indexes from existing data")
    args = parser.parse_args()

    if args.build_search_index:
        build_search_index()
        return

    if args.only == "sunni":
        download_sunni()
    elif args.only == "shia":
        download_shia()
    elif args.only == "grades":
        download_grades()
    else:
        download_sunni()
        download_shia()
        download_grades()
        build_search_index()

    print("\n✓ All done! Data is in app/data/")
    print("  Run a local server: cd app && python -m http.server 8000")


if __name__ == "__main__":
    main()
