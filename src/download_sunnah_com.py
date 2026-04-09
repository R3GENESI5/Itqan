"""
download_sunnah_com.py
======================
Downloads hadith collections from the sunnah.com API and saves them in the
app's data format under app/data/sunni/{book_id}/.

API docs: https://sunnah.com/developers
Base URL: https://api.sunnah.com/v1/

Usage:
    python src/download_sunnah_com.py --apikey YOUR_KEY
    python src/download_sunnah_com.py --apikey YOUR_KEY --books ahmed
    python src/download_sunnah_com.py --apikey YOUR_KEY --books ahmed,bukhari
    python src/download_sunnah_com.py --apikey YOUR_KEY --books ahmed --force
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any

import requests
from tqdm import tqdm

# ── Constants ─────────────────────────────────────────────────────────────────

API_BASE = "https://api.sunnah.com/v1"
PAGE_LIMIT = 50          # hadiths per page (API max is 50)
RATE_DELAY = 0.2         # seconds between requests
RETRY_ATTEMPTS = 3
RETRY_BACKOFF = 2.0      # seconds; doubles on each retry

ROOT = Path(__file__).parent.parent
DATA_SUNNI = ROOT / "app" / "data" / "sunni"

# sunnah.com collection name → our app book_id
# The collection name is also used as the directory name by default,
# but this map lets us rename if needed.
COLLECTION_MAP: dict[str, str] = {
    "ahmad":            "ahmed",
    "bukhari":          "bukhari",
    "muslim":           "muslim",
    "abudawud":         "abudawud",
    "tirmidhi":         "tirmidhi",
    "nasai":            "nasai",
    "ibnmajah":         "ibnmajah",
    "malik":            "malik",
    "darimi":           "darimi",
    # Forties
    "nawawi40":         "nawawi40",
    "qudsi40":          "qudsi40",
    # Other books
    "riyad_assalihin":  "riyad_assalihin",
    "adab_mufrad":      "aladab_almufrad",
    "bulugh_almaram":   "bulugh_almaram",
    "mishkat":          "mishkat_almasabih",
    "shamail":          "shamail_muhammadiyah",
}

# When the user passes a book argument, look it up by either the sunnah.com
# collection name or our app book_id so both spellings work.
def _resolve_collection_name(user_arg: str) -> str:
    """Return the sunnah.com collection name for a user-supplied argument.

    Accepts either the sunnah.com collection name (e.g. "ahmad") or the
    app book_id (e.g. "ahmed").  If no mapping is found the argument is
    returned as-is so that uncommon collections still work.
    """
    # Direct hit: user supplied the sunnah.com name
    if user_arg in COLLECTION_MAP:
        return user_arg
    # Reverse lookup: user supplied our app book_id
    for col_name, book_id in COLLECTION_MAP.items():
        if book_id == user_arg:
            return col_name
    # Unknown — pass through verbatim (collection name == book_id)
    return user_arg


def _book_id_for(collection_name: str) -> str:
    """Return the app book_id for a sunnah.com collection name."""
    return COLLECTION_MAP.get(collection_name, collection_name)


# ── HTTP helpers ──────────────────────────────────────────────────────────────

class SunnahClient:
    """Thin wrapper around requests with auth, rate-limiting, and retries."""

    def __init__(self, api_key: str) -> None:
        self._session = requests.Session()
        self._session.headers.update({
            "X-API-Key": api_key,
            "Accept": "application/json",
        })

    def get(self, path: str, **params: Any) -> dict:
        """GET {API_BASE}{path} with query params; returns parsed JSON dict."""
        url = f"{API_BASE}{path}"
        for attempt in range(RETRY_ATTEMPTS):
            try:
                resp = self._session.get(url, params=params or None, timeout=30)
                resp.raise_for_status()
                time.sleep(RATE_DELAY)
                return resp.json()
            except requests.HTTPError as exc:
                status = exc.response.status_code if exc.response is not None else 0
                if status == 429:
                    wait = RETRY_BACKOFF * (2 ** attempt)
                    tqdm.write(f"  Rate limited — sleeping {wait:.0f}s …")
                    time.sleep(wait)
                elif status == 401:
                    print(
                        "\nERROR: 401 Unauthorized. "
                        "Check your API key (register at https://sunnah.com/developers).",
                        file=sys.stderr,
                    )
                    raise SystemExit(1)
                elif attempt < RETRY_ATTEMPTS - 1:
                    time.sleep(RETRY_BACKOFF * (attempt + 1))
                else:
                    raise
            except requests.RequestException:
                if attempt < RETRY_ATTEMPTS - 1:
                    time.sleep(RETRY_BACKOFF * (attempt + 1))
                else:
                    raise
        # Should not be reachable but satisfies type checkers
        raise RuntimeError(f"Failed to GET {url} after {RETRY_ATTEMPTS} attempts")


# ── Data conversion ───────────────────────────────────────────────────────────

def _extract_hadith(raw: dict) -> dict:
    """Convert a sunnah.com hadith object into the app's format.

    App format:
        {
            "id":       <hadithNumber as int, or string if non-numeric>,
            "idInBook": <same>,
            "arabic":   "...",
            "english":  {"narrator": "...", "text": "..."}
        }
    """
    # hadithNumber may be "1", "1a", etc.
    hadith_number = raw.get("hadithNumber", "")
    try:
        numeric_id = int(hadith_number)
    except (ValueError, TypeError):
        numeric_id = hadith_number  # keep as string for non-numeric IDs

    arabic_text = ""
    narrator = ""
    english_text = ""

    for lang_block in raw.get("hadith", []):
        lang = (lang_block.get("lang") or "").lower()
        body = (lang_block.get("body") or "").strip()
        if lang == "ar":
            arabic_text = body
        elif lang == "en":
            english_text = body
            narrator = (lang_block.get("narrator") or "").strip()

    return {
        "id":       numeric_id,
        "idInBook": numeric_id,
        "arabic":   arabic_text,
        "english": {
            "narrator": narrator,
            "text":     english_text,
        },
    }


# ── Core download logic ───────────────────────────────────────────────────────

def fetch_books(client: SunnahClient, collection: str) -> list[dict]:
    """Return the list of book (chapter) objects for a collection."""
    data = client.get(f"/collections/{collection}/books", limit=500)
    return data.get("data", [])


def fetch_hadiths_for_book(
    client: SunnahClient,
    collection: str,
    book_number: str,
) -> list[dict]:
    """Paginate through all hadiths for one book, return converted list."""
    hadiths: list[dict] = []
    page = 1
    while True:
        data = client.get(
            f"/collections/{collection}/books/{book_number}/hadiths",
            limit=PAGE_LIMIT,
            page=page,
        )
        batch = data.get("data", [])
        for raw in batch:
            hadiths.append(_extract_hadith(raw))

        total = data.get("total", 0)
        fetched = len(hadiths)

        if fetched >= total or not data.get("next"):
            break
        page += 1

    return hadiths


def download_collection(
    client: SunnahClient,
    collection: str,
    force: bool = False,
) -> int:
    """Download one collection; return total hadiths saved."""
    book_id = _book_id_for(collection)
    out_dir = DATA_SUNNI / book_id
    out_dir.mkdir(parents=True, exist_ok=True)

    index_path = out_dir / "index.json"

    # Load existing index so we can skip already-downloaded chapters
    existing_index: list[dict] = []
    existing_files: set[str] = set()
    if index_path.exists() and not force:
        try:
            existing_index = json.loads(index_path.read_text(encoding="utf-8"))
            existing_files = {entry["file"] for entry in existing_index}
        except (json.JSONDecodeError, KeyError):
            existing_index = []
            existing_files = set()

    print(f"\nFetching book list for '{collection}' …")
    books = fetch_books(client, collection)
    if not books:
        print(f"  WARNING: No books returned for collection '{collection}'. "
              "Check the collection name.")
        return 0

    total_chapters = len(books)
    print(f"  Found {total_chapters} chapters.")

    new_index: list[dict] = list(existing_index)  # preserve already-saved entries
    # Build a lookup of existing index entries by file name for dedup
    existing_by_file: dict[str, dict] = {e["file"]: e for e in existing_index}

    grand_total = sum(e["count"] for e in existing_index)  # already on disk

    # Iterate with a tqdm progress bar over chapters
    chapter_bar = tqdm(
        enumerate(books, start=1),
        total=total_chapters,
        desc=f"{book_id}",
        unit="ch",
        leave=True,
    )

    for seq_idx, book_obj in chapter_bar:
        file_name = f"{seq_idx}.json"
        dest_path = out_dir / file_name

        # The API uses 'bookNumber' as the paginated endpoint key
        book_number = str(book_obj.get("bookNumber") or book_obj.get("book_number", seq_idx))
        name_ar = (book_obj.get("book") or {}).get("ar", "") or ""
        name_en = (book_obj.get("book") or {}).get("en", "") or ""

        # Skip if already downloaded and not forcing
        if not force and dest_path.exists() and file_name in existing_files:
            chapter_bar.set_postfix_str(f"skip {file_name}")
            continue

        chapter_bar.set_postfix_str(f"ch {seq_idx}/{total_chapters}")

        try:
            hadiths = fetch_hadiths_for_book(client, collection, book_number)
        except Exception as exc:
            tqdm.write(f"  ERROR fetching {collection}/books/{book_number}: {exc}")
            continue

        count = len(hadiths)

        # Write chapter data file
        with open(dest_path, "w", encoding="utf-8") as fh:
            json.dump(hadiths, fh, ensure_ascii=False, separators=(",", ":"))

        grand_total += count
        tqdm.write(
            f"  {book_id}: chapter {seq_idx}/{total_chapters} "
            f"({count} hadiths), total so far: {grand_total}"
        )

        # Update index entry (overwrite if re-downloading)
        entry = {
            "file":    file_name,
            "name_ar": name_ar,
            "name_en": name_en,
            "count":   count,
        }
        if file_name in existing_by_file:
            # Replace the old entry in-place
            for i, e in enumerate(new_index):
                if e["file"] == file_name:
                    new_index[i] = entry
                    break
        else:
            new_index.append(entry)
            existing_by_file[file_name] = entry

        # Re-sort index by sequential file number before writing
        new_index.sort(key=lambda e: int(Path(e["file"]).stem))

        # Persist index after every chapter so progress is not lost on crash
        with open(index_path, "w", encoding="utf-8") as fh:
            json.dump(new_index, fh, ensure_ascii=False, indent=2)

    return grand_total


# ── Entry point ───────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download hadith collections from sunnah.com API.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--apikey",
        required=True,
        metavar="KEY",
        help="Your sunnah.com API key (register at https://sunnah.com/developers)",
    )
    parser.add_argument(
        "--books",
        default="ahmed",
        metavar="BOOKS",
        help=(
            "Comma-separated list of collections to download. "
            "Accepts sunnah.com collection names (e.g. 'ahmad') or app book IDs "
            "(e.g. 'ahmed'). Default: ahmed"
        ),
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing chapter files (default: skip already-downloaded chapters)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    client = SunnahClient(args.apikey)

    requested = [b.strip() for b in args.books.split(",") if b.strip()]
    collections = [_resolve_collection_name(b) for b in requested]

    print(f"Collections to download: {', '.join(collections)}")
    print(f"Output directory: {DATA_SUNNI}")
    print(f"Force overwrite: {args.force}")

    overall_total = 0
    for collection in collections:
        total = download_collection(client, collection, force=args.force)
        overall_total += total

    print(f"\nTotal hadiths downloaded: {overall_total}")
    print("Run `python src/build_semantic_index.py` to rebuild the FAISS index")


if __name__ == "__main__":
    main()
