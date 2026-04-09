"""
visualize_concordance.py — Visual audit of concordance, bridge, and families.
Generates: app/concordance_audit.html  (self-contained Chart.js dashboard)
"""

import json, re
from pathlib import Path
from collections import defaultdict

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / 'app' / 'data'
OUT  = ROOT / 'app' / 'concordance_audit.html'

def load(name): return json.load(open(DATA / name, encoding='utf-8'))

print('Loading data...')
conc   = load('concordance.json')
wd     = load('word_defs_v2.json')
bridge = load('quran_hadith_bridge.json')
fam    = load('family_corpus.json')

# ── 1. Family coverage ──────────────────────────────────────────────────────
families_sorted = sorted(fam.items(), key=lambda x: -x[1]['hadith_count'])
fam_labels  = [f['meaning'][:35] + '…' if len(f['meaning']) > 35 else f['meaning']
               for _, f in families_sorted]
fam_hadiths = [f['hadith_count'] for _, f in families_sorted]
fam_ayahs   = [f['ayah_count']   for _, f in families_sorted]
fam_keys    = [k for k, _ in families_sorted]

# Color new families differently
fam_colors = ['#e74c3c' if k in ('end_of_times','jihad','statecraft','family_law')
              else '#3498db' for k in fam_keys]

# ── 2. Top 40 roots by hadith count ─────────────────────────────────────────
roots_sorted = sorted(bridge.items(), key=lambda x: -x[1]['hadith_count'])
top_roots = [(r, d) for r, d in roots_sorted if d['hadith_count'] > 0][:40]
root_labels  = [r for r, _ in top_roots]
root_hadiths = [d['hadith_count'] for _, d in top_roots]
root_ayahs   = [d['ayah_count']   for _, d in top_roots]
root_meanings = [d['definitions'].get('quran_meaning','')[:40] for _, d in top_roots]

# ── 3. Per-book concordance coverage ────────────────────────────────────────
import glob, os
book_hadith_counts = {}
for book_dir in Path(DATA / 'sunni').iterdir():
    if not book_dir.is_dir(): continue
    idx = book_dir / 'index.json'
    if not idx.exists(): continue
    chapters = json.load(open(idx))
    total = 0
    for ch in chapters:
        f = book_dir / ch['file']
        if f.exists():
            total += len(json.load(open(f)))
    book_hadith_counts[book_dir.name] = total

book_conc_entries = defaultdict(int)
for word, ids in conc.items():
    for hid in ids:
        book = hid.split(':')[0]
        book_conc_entries[book] += 1

books_sorted = sorted(book_hadith_counts.keys(),
                      key=lambda b: -book_hadith_counts[b])
book_labels  = books_sorted
book_total   = [book_hadith_counts[b] for b in books_sorted]
book_indexed = [book_conc_entries.get(b, 0) for b in books_sorted]
book_ratio   = [round(book_conc_entries.get(b,0) / book_hadith_counts[b], 1)
                if book_hadith_counts[b] else 0 for b in books_sorted]

# ── 4. Word frequency distribution (log-scale histogram) ────────────────────
freq_buckets = defaultdict(int)
for word, ids in conc.items():
    n = len(ids)
    if n < 5:       freq_buckets['1–4'] += 1
    elif n < 20:    freq_buckets['5–19'] += 1
    elif n < 50:    freq_buckets['20–49'] += 1
    elif n < 100:   freq_buckets['50–99'] += 1
    elif n < 250:   freq_buckets['100–249'] += 1
    elif n < 500:   freq_buckets['250–499'] += 1
    elif n < 1000:  freq_buckets['500–999'] += 1
    elif n < 2000:  freq_buckets['1000–1999'] += 1
    else:           freq_buckets['2000 (cap)'] += 1

hist_labels = ['1–4','5–19','20–49','50–99','100–249','250–499','500–999','1000–1999','2000 (cap)']
hist_values = [freq_buckets[l] for l in hist_labels]

# ── 5. Quran freq vs Hadith count scatter (top 200 roots) ───────────────────
scatter_data = []
for r, d in bridge.items():
    if d['hadith_count'] > 0 and d['frequency_quran'] > 0:
        scatter_data.append({
            'x': d['frequency_quran'],
            'y': d['hadith_count'],
            'r': r,
            'm': d['definitions'].get('quran_meaning','')[:30],
        })
scatter_data.sort(key=lambda x: -(x['x'] * x['y']))
scatter_top = scatter_data[:120]

# ── 6. Key roots spot-check table ───────────────────────────────────────────
SPOT_ROOTS = [
    ('صلو','prayer'),('زكو','zakat'),('صوم','fasting'),('حجج','hajj'),
    ('أمن','faith'),('يوم','day/Qiyama'),('فتن','fitnah'),('أمر','command'),
    ('ولي','guardian'),('أرض','earth'),('وقي','taqwa'),('خلف','caliphate'),
    ('ملك','sovereignty'),('قضي','judiciary'),('طلق','divorce'),('ورث','inheritance'),
    ('نكح','marriage'),('جهد','striving'),('شهد','martyrdom'),('فتح','conquest'),
    ('علم','knowledge'),('رحم','mercy'),('قبر','grave'),('بعث','resurrection'),
    ('نفخ','trumpet'),
]
spot_rows = []
for root, topic in SPOT_ROOTS:
    if root in bridge:
        d = bridge[root]
        books = sorted(d['book_breakdown'].items(), key=lambda x: -x[1])[:2]
        top_books = ', '.join(f'{b}({n})' for b,n in books)
        spot_rows.append({
            'root': root, 'topic': topic,
            'ayahs': d['ayah_count'],
            'hadiths': d['hadith_count'],
            'top_books': top_books,
            'ok': d['hadith_count'] > 0,
        })
    else:
        spot_rows.append({'root': root, 'topic': topic, 'ayahs': 0, 'hadiths': 0,
                          'top_books': '—', 'ok': False})

# ── Build HTML ───────────────────────────────────────────────────────────────
def js_list(lst):
    return json.dumps(lst, ensure_ascii=False)

spot_html = ''.join(
    f'<tr class="{"ok" if r["ok"] else "zero"}">'
    f'<td class="ar">{r["root"]}</td>'
    f'<td>{r["topic"]}</td>'
    f'<td>{r["ayahs"]:,}</td>'
    f'<td><strong>{r["hadiths"]:,}</strong></td>'
    f'<td class="books">{r["top_books"]}</td></tr>'
    for r in spot_rows
)

html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Al-Itqan — Concordance Audit</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.4/dist/chart.umd.min.js"></script>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: 'Segoe UI', sans-serif; background: #0f0f1a; color: #e0e0e0; padding: 24px; }}
  h1 {{ color: #f0c060; font-size: 1.6rem; margin-bottom: 4px; }}
  .subtitle {{ color: #888; font-size: .9rem; margin-bottom: 28px; }}
  .grid {{ display: grid; gap: 24px; }}
  .grid-2 {{ grid-template-columns: 1fr 1fr; }}
  .grid-3 {{ grid-template-columns: 1fr 1fr 1fr; }}
  .card {{ background: #1a1a2e; border-radius: 10px; padding: 20px; border: 1px solid #2a2a4a; }}
  .card h2 {{ font-size: 1rem; color: #7eb3f0; margin-bottom: 14px; border-bottom: 1px solid #2a2a4a; padding-bottom: 8px; }}
  .stat-row {{ display: flex; gap: 16px; flex-wrap: wrap; margin-bottom: 24px; }}
  .stat {{ background: #1a1a2e; border: 1px solid #2a2a4a; border-radius: 8px; padding: 14px 20px; flex: 1; min-width: 140px; }}
  .stat .num {{ font-size: 1.7rem; font-weight: 700; color: #f0c060; }}
  .stat .lbl {{ font-size: .75rem; color: #888; margin-top: 2px; }}
  canvas {{ max-height: 360px; }}
  table {{ width: 100%; border-collapse: collapse; font-size: .82rem; }}
  th {{ background: #12122a; color: #7eb3f0; padding: 7px 10px; text-align: left; font-weight: 600; }}
  td {{ padding: 6px 10px; border-bottom: 1px solid #1e1e3a; }}
  tr.ok td {{ color: #d0f0d0; }}
  tr.zero td {{ color: #f08080; }}
  tr.zero td strong {{ color: #f05050; }}
  .ar {{ font-size: 1.1rem; font-family: 'Traditional Arabic', serif; direction: rtl; }}.books {{ color: #aaa; font-size: .75rem; }}
  @media (max-width: 900px) {{ .grid-2, .grid-3 {{ grid-template-columns: 1fr; }} }}
</style>
</head>
<body>
<h1>Al-Itqan — Concordance & Bridge Audit</h1>
<div class="subtitle">Verification dashboard for the Quran ↔ Hadith knowledge bridge</div>

<div class="stat-row">
  <div class="stat"><div class="num">{len(conc):,}</div><div class="lbl">Words in concordance</div></div>
  <div class="stat"><div class="num">{sum(len(v) for v in conc.values()):,}</div><div class="lbl">Total concordance links</div></div>
  <div class="stat"><div class="num">{sum(d['hadith_count'] for d in bridge.values()):,}</div><div class="lbl">Quran↔Hadith root links</div></div>
  <div class="stat"><div class="num">{sum(1 for d in bridge.values() if d['hadith_count']>0):,}</div><div class="lbl">Roots with hadith coverage</div></div>
  <div class="stat"><div class="num">{len(fam):,}</div><div class="lbl">Thematic families</div></div>
  <div class="stat"><div class="num">{sum(book_hadith_counts.values()):,}</div><div class="lbl">Hadith corpus size</div></div>
</div>

<div class="grid grid-2">

  <!-- Chart 1: Family coverage -->
  <div class="card">
    <h2>📚 39 Thematic Families — Hadith Coverage <span style="color:#e74c3c;font-weight:normal;font-size:.8rem">■ new family</span></h2>
    <canvas id="c1"></canvas>
  </div>

  <!-- Chart 2: Top 40 roots by hadiths -->
  <div class="card">
    <h2>🌿 Top 40 Roots — Hadith Count</h2>
    <canvas id="c2"></canvas>
  </div>

  <!-- Chart 3: Per-book coverage -->
  <div class="card">
    <h2>📖 Per-Book Concordance Coverage (entries / hadiths)</h2>
    <canvas id="c3"></canvas>
  </div>

  <!-- Chart 4: Word frequency distribution -->
  <div class="card">
    <h2>📊 Concordance Word Frequency Distribution</h2>
    <canvas id="c4"></canvas>
  </div>

</div>

<div class="grid" style="grid-template-columns:1fr 1.4fr; margin-top:24px;">

  <!-- Chart 5: Scatter Quran vs Hadith -->
  <div class="card">
    <h2>🔗 Quran Frequency vs Hadith Coverage (top 120 roots)</h2>
    <canvas id="c5" style="max-height:340px;"></canvas>
  </div>

  <!-- Table: Spot-check -->
  <div class="card">
    <h2>✅ Key Islamic Roots — Spot Check</h2>
    <table>
      <thead><tr><th>Root</th><th>Topic</th><th>Ayahs</th><th>Hadiths</th><th>Top books</th></tr></thead>
      <tbody>{spot_html}</tbody>
    </table>
  </div>

</div>

<script>
const C = (id, cfg) => new Chart(document.getElementById(id).getContext('2d'), cfg);
Chart.defaults.color = '#aaa';
Chart.defaults.borderColor = '#2a2a4a';

// 1. Family bar chart
C('c1', {{
  type: 'bar',
  data: {{
    labels: {js_list(fam_labels)},
    datasets: [{{
      label: 'Hadiths',
      data: {js_list(fam_hadiths)},
      backgroundColor: {js_list(fam_colors)},
      borderRadius: 3,
    }}]
  }},
  options: {{
    indexAxis: 'y',
    responsive: true,
    plugins: {{ legend: {{ display: false }} }},
    scales: {{
      x: {{ grid: {{ color: '#2a2a4a' }} }},
      y: {{ ticks: {{ font: {{ size: 10 }} }}, grid: {{ color: '#2a2a4a' }} }}
    }}
  }}
}});

// 2. Top roots bar
C('c2', {{
  type: 'bar',
  data: {{
    labels: {js_list(root_labels)},
    datasets: [
      {{ label: 'Hadiths', data: {js_list(root_hadiths)}, backgroundColor: '#3498db', borderRadius: 2 }},
      {{ label: 'Ayahs',   data: {js_list(root_ayahs)},   backgroundColor: '#f39c12', borderRadius: 2 }},
    ]
  }},
  options: {{
    responsive: true,
    plugins: {{
      legend: {{ position: 'top' }},
      tooltip: {{
        callbacks: {{
          afterLabel: (ctx) => {{
            const meanings = {js_list(root_meanings)};
            return meanings[ctx.dataIndex] || '';
          }}
        }}
      }}
    }},
    scales: {{
      x: {{ ticks: {{ font: {{ size: 10 }} }}, grid: {{ color: '#2a2a4a' }} }},
      y: {{ grid: {{ color: '#2a2a4a' }} }}
    }}
  }}
}});

// 3. Per-book coverage
C('c3', {{
  type: 'bar',
  data: {{
    labels: {js_list(book_labels)},
    datasets: [
      {{ label: 'Total Hadiths', data: {js_list(book_total)},   backgroundColor: '#2ecc71', borderRadius: 2 }},
      {{ label: 'Conc. Entries', data: {js_list(book_indexed)}, backgroundColor: '#9b59b6', borderRadius: 2 }},
    ]
  }},
  options: {{
    responsive: true,
    plugins: {{ legend: {{ position: 'top' }} }},
    scales: {{
      x: {{ ticks: {{ font: {{ size: 10 }} }}, grid: {{ color: '#2a2a4a' }} }},
      y: {{ grid: {{ color: '#2a2a4a' }} }}
    }}
  }}
}});

// 4. Word frequency histogram
C('c4', {{
  type: 'bar',
  data: {{
    labels: {js_list(hist_labels)},
    datasets: [{{
      label: 'Number of words',
      data: {js_list(hist_values)},
      backgroundColor: '#e67e22',
      borderRadius: 4,
    }}]
  }},
  options: {{
    responsive: true,
    plugins: {{ legend: {{ display: false }} }},
    scales: {{
      x: {{ grid: {{ color: '#2a2a4a' }} }},
      y: {{ grid: {{ color: '#2a2a4a' }}, title: {{ display: true, text: 'Number of words' }} }}
    }}
  }}
}});

// 5. Scatter
const scatterData = {js_list([{'x': d['x'], 'y': d['y'], 'r': d['r'], 'm': d['m']} for d in scatter_top])};
C('c5', {{
  type: 'scatter',
  data: {{
    datasets: [{{
      label: 'Root',
      data: scatterData.map(d => ({{ x: d.x, y: d.y }})),
      backgroundColor: 'rgba(52,152,219,0.6)',
      pointRadius: 5,
    }}]
  }},
  options: {{
    responsive: true,
    plugins: {{
      legend: {{ display: false }},
      tooltip: {{
        callbacks: {{
          label: (ctx) => {{
            const d = scatterData[ctx.dataIndex];
            return `${{d.r}} — ${{d.m}} | Quran:${{ctx.parsed.x}} Had:${{ctx.parsed.y}}`;
          }}
        }}
      }}
    }},
    scales: {{
      x: {{ title: {{ display: true, text: 'Quran Frequency' }}, grid: {{ color: '#2a2a4a' }} }},
      y: {{ title: {{ display: true, text: 'Hadith Count' }}, grid: {{ color: '#2a2a4a' }} }}
    }}
  }}
}});
</script>
</body>
</html>"""

OUT.write_text(html, encoding='utf-8')
print(f'✓ Saved to {OUT}')
print(f'  Open: file:///{str(OUT).replace(chr(92), "/")}')
