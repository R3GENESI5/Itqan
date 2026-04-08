"""
scrape_musnad_ahmad.py
======================
Scrapes Musnad Ahmad (~28,199 hadiths) from sunnah.com and saves in
the app's data format under app/data/sunni/ahmed/.

No API key needed — scrapes the public HTML pages.
Rate-limited to be respectful (0.3s between requests).

Usage:
    python src/scrape_musnad_ahmad.py
    python src/scrape_musnad_ahmad.py --start 1 --end 100   # test range
"""

import json, re, os, sys, time, argparse
from pathlib import Path
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parent.parent
OUT_DIR = ROOT / "app" / "data" / "sunni" / "ahmed"

RATE_DELAY = 0.5  # seconds between requests
MAX_HADITH = 28200
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Itqan-Research-Bot/1.0 (academic hadith concordance project)"
})

def fetch_hadith(num):
    """Fetch a single hadith from sunnah.com/ahmad:{num}"""
    url = f"https://sunnah.com/ahmad:{num}"
    for attempt in range(3):
        try:
            resp = SESSION.get(url, timeout=20)
            if resp.status_code == 404:
                return None  # hadith doesn't exist
            if resp.status_code == 429:
                # Rate limited — back off
                time.sleep(5 * (attempt + 1))
                continue
            resp.raise_for_status()
            time.sleep(RATE_DELAY)
            return parse_hadith_page(resp.text, num)
        except requests.RequestException as e:
            if attempt < 2:
                time.sleep(3 * (attempt + 1))
            else:
                print(f"  FAILED hadith {num}: {e}", flush=True)
                return None

def parse_hadith_page(html, num):
    """Extract hadith data from sunnah.com HTML."""
    soup = BeautifulSoup(html, "html.parser")

    # Arabic text
    arabic_div = soup.find("div", class_="arabic_hadith_full") or soup.find("div", class_="hadith_narrated")
    arabic = ""
    if arabic_div:
        arabic = arabic_div.get_text(strip=True)

    # English text
    english_div = soup.find("div", class_="text_details") or soup.find("div", class_="english_hadith_full")
    narrator = ""
    english_text = ""
    if english_div:
        # Narrator is usually in a <p> or first text block
        narr_span = english_div.find("span", class_="hadith_narrated")
        if narr_span:
            narrator = narr_span.get_text(strip=True)
        # Full text
        full = english_div.get_text(strip=True)
        if narrator and full.startswith(narrator):
            english_text = full[len(narrator):].strip()
        else:
            english_text = full

    # Grade — try multiple selectors
    grade = ""
    for sel in [("td", "text_details"), ("td", None), ("div", "hadith_grade")]:
        tag, cls = sel
        candidates = soup.find_all(tag, class_=cls) if cls else soup.find_all(tag)
        for el in candidates:
            text = el.get_text(strip=True)
            if any(g in text.lower() for g in ["sahih", "hasan", "da'if", "daif", "weak", "darussalam"]):
                grade = text
                break
        if grade:
            break

    # Book/chapter info
    book_num = ""
    book_name_en = ""
    book_name_ar = ""
    chapter_div = soup.find("div", class_="book_page_english_name")
    if chapter_div:
        book_name_en = chapter_div.get_text(strip=True)
    chapter_div_ar = soup.find("div", class_="book_page_arabic_name")
    if chapter_div_ar:
        book_name_ar = chapter_div_ar.get_text(strip=True)

    # Book number from dedicated div
    book_num_div = soup.find("div", class_="book_page_number")
    if book_num_div:
        book_num = book_num_div.get_text(strip=True)
    else:
        # Fallback: parse from reference text
        ref_tds = soup.find_all("td")
        for td in ref_tds:
            text = td.get_text(strip=True)
            m = re.search(r"Book\s+(\d+)", text)
            if m:
                book_num = m.group(1)
                break

    if not arabic and not english_text:
        return None

    return {
        "hadithNumber": num,
        "book_num": book_num,
        "book_name_en": book_name_en,
        "book_name_ar": book_name_ar,
        "arabic": arabic,
        "english": {
            "narrator": narrator,
            "text": english_text,
        },
        "grade": grade,
    }

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", type=int, default=1)
    parser.add_argument("--end", type=int, default=MAX_HADITH)
    args = parser.parse_args()

    WORKERS = 3
    print(f"Scraping Musnad Ahmad hadiths {args.start} to {args.end} ({WORKERS} workers)...")

    # Group by book_num (chapter)
    chapters = defaultdict(list)
    results = {}

    nums = list(range(args.start, args.end + 1))
    done = 0

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = {pool.submit(fetch_hadith, n): n for n in nums}
        for future in as_completed(futures):
            num = futures[future]
            try:
                hadith = future.result()
                if hadith:
                    results[num] = hadith
            except Exception as e:
                print(f"  ERROR hadith {num}: {e}")
            done += 1
            if done % 500 == 0:
                print(f"  {done:>6}/{len(nums)} fetched, {len(results):,} found")

    # Sort into chapters in order
    for num in sorted(results.keys()):
        h = results[num]
        ch = h["book_num"] or "0"
        chapters[ch].append(h)

    total = sum(len(v) for v in chapters.values())
    print(f"\nScraped {total:,} hadiths across {len(chapters)} chapters")

    # Save in app format — clean old files first
    if OUT_DIR.exists():
        for old in OUT_DIR.glob("*.json"):
            old.unlink()
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    index = []
    for ch_num in sorted(chapters.keys(), key=lambda x: int(x) if x.isdigit() else 999):
        hadiths = chapters[ch_num]
        if not hadiths:
            continue

        # Determine chapter name from first hadith
        ch_name_en = hadiths[0].get("book_name_en", f"Chapter {ch_num}")
        ch_name_ar = hadiths[0].get("book_name_ar", "")

        # Format hadiths for our app
        formatted = []
        for i, h in enumerate(hadiths):
            formatted.append({
                "idInBook": i + 1,
                "hadithNumber": h["hadithNumber"],
                "arabic": h["arabic"],
                "english": h["english"],
                "grade": h["grade"],
            })

        filename = f"{ch_num}.json"
        with open(OUT_DIR / filename, "w", encoding="utf-8") as f:
            json.dump(formatted, f, ensure_ascii=False, indent=None)

        index.append({
            "file": filename,
            "name_en": ch_name_en,
            "name_ar": ch_name_ar,
            "count": len(formatted),
        })

    with open(OUT_DIR / "index.json", "w", encoding="utf-8") as f:
        json.dump(index, f, ensure_ascii=False, indent=2)

    print(f"Saved to {OUT_DIR}/")
    print(f"  {len(index)} chapter files + index.json")
    print(f"  {total:,} total hadiths")

if __name__ == "__main__":
    main()
