"""
generate_charts.py — Generate individual chart PNGs for README and paper.
Uses matplotlib with Itqan dark theme. One chart per image.

Usage:
    python src/generate_charts.py
"""

import json, os
from pathlib import Path
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / 'app' / 'data'
OUT  = ROOT / 'docs' / 'screenshots'
OUT.mkdir(parents=True, exist_ok=True)

# Itqan dark theme
BG = '#0a0a0f'
CARD_BG = '#1a1612'
GOLD = '#d4a855'
TEXT = '#e0d8c8'
MUTED = '#8a8070'
GREEN = '#4aaa70'
RED = '#e74c3c'
BLUE = '#5dade2'
ORANGE = '#f39c12'

plt.rcParams.update({
    'figure.facecolor': BG,
    'axes.facecolor': CARD_BG,
    'axes.edgecolor': '#333',
    'axes.labelcolor': MUTED,
    'xtick.color': MUTED,
    'ytick.color': MUTED,
    'text.color': TEXT,
    'font.size': 10,
    'figure.dpi': 150,
})

def load(name):
    return json.load(open(DATA / name, encoding='utf-8'))

print('Loading data...')
bridge = load('quran_hadith_bridge.json')
fam = load('family_corpus.json')
wensinck = load('wensinck.json')

# Count books
book_counts = {}
for book_dir in sorted((DATA / 'sunni').iterdir()):
    if not book_dir.is_dir(): continue
    idx = book_dir / 'index.json'
    if not idx.exists(): continue
    total = 0
    for ch in json.load(open(idx, encoding='utf-8')):
        cf = book_dir / ch['file']
        if cf.exists():
            total += len(json.load(open(cf, encoding='utf-8')))
    book_counts[book_dir.name] = total

# ── Chart 1: 39 Thematic Families ────────────────────────────────────────────
print('Chart 1: Families...')
fam_sorted = sorted(fam.items(), key=lambda x: -x[1]['hadith_count'])
names = [f['meaning'].split(',')[0][:30] for _, f in fam_sorted]
counts = [f['hadith_count'] for _, f in fam_sorted]

fig, ax = plt.subplots(figsize=(10, 8))
bars = ax.barh(range(len(names)), counts, color=BLUE, height=0.7)
ax.set_yticks(range(len(names)))
ax.set_yticklabels(names, fontsize=7)
ax.invert_yaxis()
ax.set_xlabel('Hadiths', color=MUTED)
ax.set_title('39 Thematic Families — Hadith Coverage', color=GOLD, fontsize=13, fontweight='bold')
ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f'{int(x):,}'))
plt.tight_layout()
plt.savefig(OUT / 'chart-families.png', facecolor=BG)
plt.close()

# ── Chart 2: Per-Book Coverage ────────────────────────────────────────────────
print('Chart 2: Per-book...')
books_sorted = sorted(book_counts.items(), key=lambda x: -x[1])
bnames = [b[0].replace('_', ' ').title()[:20] for b in books_sorted]
bcounts = [b[1] for b in books_sorted]

fig, ax = plt.subplots(figsize=(10, 5))
bars = ax.bar(range(len(bnames)), bcounts, color=GREEN, width=0.7)
# Highlight Ahmad
for i, (name, _) in enumerate(books_sorted):
    if name == 'ahmed':
        bars[i].set_color(GOLD)
ax.set_xticks(range(len(bnames)))
ax.set_xticklabels(bnames, rotation=45, ha='right', fontsize=7)
ax.set_ylabel('Hadiths', color=MUTED)
ax.set_title('Per-Book Hadith Count (112,221 total)', color=GOLD, fontsize=13, fontweight='bold')
ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f'{int(x):,}'))
plt.tight_layout()
plt.savefig(OUT / 'chart-per-book.png', facecolor=BG)
plt.close()

# ── Chart 3: Root Coverage — Dual Stemmer ─────────────────────────────────────
print('Chart 3: Dual stemmer...')
total_roots = len(bridge)
camel_connected = 1336  # before Wensinck
wensinck_recovered = 254
truly_zero = 61

fig, ax = plt.subplots(figsize=(8, 5))
categories = ['CAMeL Connected\n(primary stemmer)', 'Wensinck Recovered\n(light stemmer fallback)', 'Quran-Only\n(both methods agree: zero)']
values = [camel_connected, wensinck_recovered, truly_zero]
colors = [GREEN, GOLD, RED]
bars = ax.bar(categories, values, color=colors, width=0.6)
for bar, val in zip(bars, values):
    ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 15,
            f'{val:,}', ha='center', color=TEXT, fontsize=11, fontweight='bold')
ax.set_ylabel('Quranic Roots', color=MUTED)
ax.set_title(f'Dual-Stemmer Root Resolution — {total_roots} Quranic Roots → 96.3% Connected',
             color=GOLD, fontsize=12, fontweight='bold')
ax.set_ylim(0, max(values) * 1.15)
plt.tight_layout()
plt.savefig(OUT / 'chart-dual-stemmer.png', facecolor=BG)
plt.close()

# ── Chart 4: Quran Frequency vs Hadith Count ──────────────────────────────────
print('Chart 4: Quran vs Hadith scatter...')
x_vals, y_vals = [], []
for root, d in bridge.items():
    if d['hadith_count'] > 0 and d.get('frequency_quran', 0) > 0:
        x_vals.append(d['frequency_quran'])
        y_vals.append(d['hadith_count'])

fig, ax = plt.subplots(figsize=(8, 6))
ax.scatter(x_vals, y_vals, s=12, alpha=0.5, color=BLUE, edgecolors='none')
ax.set_xlabel('Quran Frequency (ayahs)', color=MUTED)
ax.set_ylabel('Hadith Count', color=MUTED)
ax.set_title('Quran Frequency vs Hadith Coverage (1,590 roots)', color=GOLD, fontsize=12, fontweight='bold')
ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f'{int(x):,}'))
ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f'{int(x):,}'))
plt.tight_layout()
plt.savefig(OUT / 'chart-quran-vs-hadith.png', facecolor=BG)
plt.close()

# ── Chart 5: Version Growth Timeline ──────────────────────────────────────────
print('Chart 5: Version timeline...')
versions = ['v1.0', 'v1.0.1', 'v1.1', 'v1.2', 'v1.3', 'v1.4', 'v1.5', 'v1.6']
hadiths = [49000, 87056, 87056, 87056, 112221, 112221, 112221, 112221]
roots = [0, 384016, 384016, 384016, 1326229, 1326229, 1326229, 1528346]
narrators = [0, 0, 0, 0, 0, 18298, 65391, 65391]

fig, ax1 = plt.subplots(figsize=(10, 5))
ax1.plot(versions, hadiths, 'o-', color=BLUE, linewidth=2, markersize=6, label='Hadiths')
ax1.set_ylabel('Hadiths', color=BLUE)
ax1.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f'{int(x/1000)}k'))

ax2 = ax1.twinx()
ax2.plot(versions, roots, 's-', color=GOLD, linewidth=2, markersize=6, label='Root Links')
ax2.set_ylabel('Root Links', color=GOLD)
ax2.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f'{x/1e6:.1f}M'))

ax3 = ax1.twinx()
ax3.spines['right'].set_position(('outward', 60))
ax3.plot(versions, narrators, '^-', color=GREEN, linewidth=2, markersize=6, label='Narrator Profiles')
ax3.set_ylabel('Narrators', color=GREEN)
ax3.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f'{int(x/1000)}k'))

ax1.set_title('Itqan Growth: v1.0 → v1.6', color=GOLD, fontsize=13, fontweight='bold')
lines1, labels1 = ax1.get_legend_handles_labels()
lines2, labels2 = ax2.get_legend_handles_labels()
lines3, labels3 = ax3.get_legend_handles_labels()
ax1.legend(lines1+lines2+lines3, labels1+labels2+labels3, loc='upper left',
           facecolor=CARD_BG, edgecolor='#333', labelcolor=TEXT)
plt.tight_layout()
plt.savefig(OUT / 'chart-version-growth.png', facecolor=BG)
plt.close()

print(f'\n✓ Generated 5 charts in {OUT}/')
for f in sorted(OUT.glob('chart-*.png')):
    print(f'  {f.name} ({f.stat().st_size // 1024} KB)')
