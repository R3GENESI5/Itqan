"""
download_openiti_rijal.py
=========================
Downloads classical rijal (narrator criticism) texts from OpenITI GitHub.
These are the primary sources for Phase 14: structured narrator parsing.

Texts downloaded:
  1. Tahdhib al-Kamal (al-Mizzi, d.742)      — Six Books narrator encyclopedia
  2. Mizan al-I'tidal (al-Dhahabi, d.748)     — Critical narrator assessments
  3. Al-Jarh wa al-Ta'dil (Ibn Abi Hatim, d.327) — Reliability evaluations
  4. Al-Thiqat (Ibn Hibban, d.354)             — Reliable narrator list
  5. Al-Kamil fi Du'afa (Ibn 'Adi, d.365)      — Weak narrator catalog
  6. Tarikh Baghdad (al-Khatib, d.463)         — Baghdad scholar biographies
  7. Tahdhib al-Tahdhib (Ibn Hajar, d.852)     — Condensed encyclopedia
  8. Taqrib al-Tahdhib (Ibn Hajar, d.852)      — Grading manual

Usage:
    python src/download_openiti_rijal.py
"""

import os, sys, urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "src" / "rijal_raw"
OUT.mkdir(exist_ok=True)

BASE = "https://raw.githubusercontent.com/OpenITI"

TEXTS = [
    {
        "id": "tahdhib_kamal",
        "title": "Tahdhib al-Kamal (al-Mizzi)",
        "url": f"{BASE}/0750AH/master/data/0742Mizzi/0742Mizzi.TahdhibKamal/0742Mizzi.TahdhibKamal.Shamela0003722-ara1.mARkdown",
        "filename": "tahdhib_kamal.txt",
    },
    {
        "id": "mizan_itidal",
        "title": "Mizan al-I'tidal (al-Dhahabi)",
        "url": f"{BASE}/0750AH/master/data/0748Dhahabi/0748Dhahabi.MizanIctidal/0748Dhahabi.MizanIctidal.JK001293BK1-ara1.mARkdown",
        "filename": "mizan_itidal.txt",
    },
    {
        "id": "jarh_tadil",
        "title": "Al-Jarh wa al-Ta'dil (Ibn Abi Hatim)",
        "url": f"{BASE}/0350AH/master/data/0327IbnAbiHatimRazi/0327IbnAbiHatimRazi.JarhWaTacdil/0327IbnAbiHatimRazi.JarhWaTacdil.Shamela0002170-ara1.completed",
        "filename": "jarh_tadil.txt",
    },
    {
        "id": "thiqat",
        "title": "Al-Thiqat (Ibn Hibban)",
        "url": f"{BASE}/0375AH/master/data/0354IbnHibbanBusti/0354IbnHibbanBusti.Thiqat/0354IbnHibbanBusti.Thiqat.Shamela0005816-ara1.completed",
        "filename": "thiqat.txt",
    },
    {
        "id": "kamil_duafa",
        "title": "Al-Kamil fi Du'afa (Ibn 'Adi)",
        "url": f"{BASE}/0375AH/master/data/0365IbnCadiJurjani/0365IbnCadiJurjani.KamilFiDucafa/0365IbnCadiJurjani.KamilFiDucafa.Shamela0012579-ara1.mARkdown",
        "filename": "kamil_duafa.txt",
    },
    {
        "id": "tarikh_baghdad",
        "title": "Tarikh Baghdad (al-Khatib al-Baghdadi)",
        "url": f"{BASE}/0475AH/master/data/0463KhatibBaghdadi/0463KhatibBaghdadi.TarikhBaghdad/0463KhatibBaghdadi.TarikhBaghdad.Shamela0000736-ara2.mARkdown",
        "filename": "tarikh_baghdad.txt",
    },
    {
        "id": "tahdhib_tahdhib",
        "title": "Tahdhib al-Tahdhib (Ibn Hajar)",
        "url": f"{BASE}/0875AH/master/data/0852IbnHajarCasqalani/0852IbnHajarCasqalani.TahdhibTahdhib/0852IbnHajarCasqalani.TahdhibTahdhib.JK000134-ara1.mARkdown",
        "filename": "tahdhib_tahdhib.txt",
    },
    {
        "id": "taqrib_tahdhib",
        "title": "Taqrib al-Tahdhib (Ibn Hajar)",
        "url": f"{BASE}/0875AH/master/data/0852IbnHajarCasqalani/0852IbnHajarCasqalani.TaqribTahdhib/0852IbnHajarCasqalani.TaqribTahdhib.JK000121-ara1.completed",
        "filename": "taqrib_tahdhib.txt",
    },
]


def download(text):
    dest = OUT / text["filename"]
    if dest.exists():
        size = dest.stat().st_size
        print(f"  [skip] {text['title']} — already downloaded ({size:,} bytes)")
        return True

    print(f"  [download] {text['title']}...")
    try:
        urllib.request.urlretrieve(text["url"], dest)
        size = dest.stat().st_size
        print(f"    -> {size:,} bytes saved to {dest.name}")
        return True
    except Exception as e:
        print(f"    [ERROR] {e}")
        return False


if __name__ == "__main__":
    print("Downloading OpenITI rijal texts...\n")
    ok, fail = 0, 0
    for t in TEXTS:
        if download(t):
            ok += 1
        else:
            fail += 1
    print(f"\nDone: {ok} downloaded, {fail} failed.")
