// Itqan Narrator DB Viewer — vanilla JS, lazy-loaded sharded JSON
(() => {
'use strict';

const PAGE_SIZE = 50;

// Columnar index: row = [pid, name, gradeCode, freq, books, tc, sc, shard, flagMask]
const COL = { pid: 0, name: 1, grade: 2, freq: 3, books: 4, tc: 5, sc: 6, shard: 7, flags: 8 };

const state = {
  meta: null,
  index: [],
  filtered: [],
  shardCache: new Map(),
  redirects: null,
  audit: null,
  page: 0,
  sortKey: 'freq',
  sortDir: -1,
  searchTerm: '',
  gradeFilter: '',
  flagFilter: '',
  hidePlaceholders: true,
  hideAbandoned: true,
  gradeByCode: {},   // code → name
  flagByCode: {},    // bit → name
};

// ----- Utilities -----
const $ = (sel, root = document) => root.querySelector(sel);
const $$ = (sel, root = document) => [...root.querySelectorAll(sel)];
const fmt = n => (n || 0).toLocaleString();
const el = (tag, cls, text) => { const e = document.createElement(tag); if (cls) e.className = cls; if (text !== undefined) e.textContent = text; return e; };

const DIACR = /[\u064b-\u0652]/g;
function norm(s) {
  if (!s) return '';
  return s.replace(DIACR, '').replace(/[أإآا]/g, 'ا').replace(/ة/g, 'ه').replace(/\u0640/g, '').replace(/ى/g, 'ي').trim();
}

function gradeName(code) { return state.gradeByCode[code] || 'missing'; }
function flagNames(mask) {
  const out = [];
  for (const [name, bit] of Object.entries(state.meta.flag_codes)) {
    if (mask & bit) out.push(name);
  }
  return out;
}

// ----- Data loading -----
async function loadJSON(path) {
  const r = await fetch(path);
  if (!r.ok) throw new Error(`${path}: ${r.status}`);
  return r.json();
}

async function loadShard(shardNum) {
  if (state.shardCache.has(shardNum)) return state.shardCache.get(shardNum);
  const data = await loadJSON(`data/shards/shard_${String(shardNum).padStart(3, '0')}.json`);
  state.shardCache.set(shardNum, data);
  return data;
}

async function boot() {
  state.meta = await loadJSON('data/meta.json');
  for (const [n, c] of Object.entries(state.meta.grade_codes)) state.gradeByCode[c] = n || 'missing';
  state.index = await loadJSON('data/index.json');
  state.redirects = await loadJSON('data/redirects.json');
  state.audit = await loadJSON('data/audit_flags.json');
  renderHeader();
  populateGradeFilter();
  populateFlagFilter();
  applyFilter();
  renderDashboard();
  bindUI();
  handleHashChange();
}

function renderHeader() {
  $('#meta-version').textContent = `v${state.meta.version} · ${fmt(state.meta.alive)} narrators (redirects + abandoned hidden; use search or flag-filter to access)`;
}

// ----- Dashboard -----
function renderDashboard() {
  const sg = $('#stats-grid');
  sg.innerHTML = '';
  const cards = [
    ['Total profiles', fmt(state.meta.total_profiles)],
    ['Browsable (alive)', fmt(state.meta.alive)],
    ['Redirects', fmt(state.meta.redirects)],
    ['Shards', state.meta.n_shards],
    ['Generated', state.meta.generated],
  ];
  for (const [label, val] of cards) {
    const c = el('div', 'stat-card');
    c.appendChild(el('div', 'label', label));
    c.appendChild(el('div', 'val', val));
    sg.appendChild(c);
  }
  const gt = $('#grade-table tbody');
  gt.innerHTML = '';
  const grades = Object.entries(state.meta.grade_counts).sort((a,b) => b[1] - a[1]);
  for (const [codeStr, count] of grades) {
    const code = Number(codeStr);
    const gName = gradeName(code);
    const tr = document.createElement('tr');
    const td1 = el('td');
    const a = el('a', '', '');
    a.href = '#browse';
    a.onclick = () => { state.gradeFilter = String(code); $('#filter-grade').value = String(code); applyFilter(); showView('browse'); };
    a.appendChild(el('span', `grade ${gName}`, gName));
    td1.appendChild(a);
    tr.appendChild(td1);
    tr.appendChild(el('td', '', fmt(count)));
    gt.appendChild(tr);
  }
  const ft = $('#flag-table tbody');
  ft.innerHTML = '';
  const flags = Object.entries(state.meta.audit_flag_counts).sort((a,b) => b[1] - a[1]);
  for (const [f, c] of flags) {
    const tr = document.createElement('tr');
    const td1 = el('td');
    const a = el('a', '', f);
    a.href = '#audit';
    a.onclick = () => { showView('audit'); renderAudit(f); };
    td1.appendChild(a);
    tr.appendChild(td1);
    tr.appendChild(el('td', '', fmt(c)));
    ft.appendChild(tr);
  }
}

// ----- Browse filters -----
function populateGradeFilter() {
  const sel = $('#filter-grade');
  const codes = Object.entries(state.meta.grade_counts).sort((a,b) => b[1] - a[1]);
  for (const [codeStr] of codes) {
    const o = document.createElement('option');
    o.value = codeStr;
    o.textContent = gradeName(Number(codeStr));
    sel.appendChild(o);
  }
}
function populateFlagFilter() {
  const sel = $('#filter-flag');
  for (const f of Object.keys(state.meta.audit_flag_counts)) {
    const o = document.createElement('option'); o.value = f; o.textContent = f; sel.appendChild(o);
  }
}

function applyFilter() {
  const q = norm(state.searchTerm);
  const g = state.gradeFilter;
  const f = state.flagFilter;
  const flagPids = f ? new Set(state.audit[f] || []) : null;
  const KINSHIP_BIT = state.meta.flag_codes.kinship_placeholder || 128;
  const ABANDONED_BIT = state.meta.flag_codes.abandoned || 1;
  state.filtered = state.index.filter(e => {
    const fmask = e[COL.flags];
    if (state.hidePlaceholders && (fmask & KINSHIP_BIT)) return false;
    if (state.hideAbandoned && (fmask & ABANDONED_BIT)) return false;
    if (g !== '' && e[COL.grade] !== Number(g)) return false;
    if (flagPids && !flagPids.has(e[COL.pid])) return false;
    if (q && !norm(e[COL.name]).includes(q) && !norm(e[COL.pid]).includes(q)) return false;
    return true;
  });
  const kToCol = { rank: -1, name: COL.name, grade: COL.grade, freq: COL.freq, books: COL.books, teachers: COL.tc, students: COL.sc };
  const c = kToCol[state.sortKey];
  if (c >= 0) {
    const dir = state.sortDir;
    state.filtered.sort((a, b) => {
      const va = a[c], vb = b[c];
      if (typeof va === 'number') return (va - vb) * dir;
      return String(va || '').localeCompare(String(vb || '')) * dir;
    });
  }
  state.page = 0;
  renderBrowse();
}

function renderBrowse() {
  const tbody = $('#browse-tbody');
  tbody.innerHTML = '';
  const start = state.page * PAGE_SIZE;
  const rows = state.filtered.slice(start, start + PAGE_SIZE);
  rows.forEach((e, i) => {
    const tr = document.createElement('tr');
    tr.onclick = () => showProfile(e[COL.pid]);
    tr.appendChild(el('td', 'num', start + i + 1));
    tr.appendChild(el('td', 'name', e[COL.name]));
    const tdg = el('td');
    const gName = gradeName(e[COL.grade]);
    tdg.appendChild(el('span', `grade ${gName}`, gName));
    tr.appendChild(tdg);
    tr.appendChild(el('td', 'num', fmt(e[COL.freq])));
    tr.appendChild(el('td', 'num', fmt(e[COL.books])));
    tr.appendChild(el('td', 'num', fmt(e[COL.tc])));
    tr.appendChild(el('td', 'num', fmt(e[COL.sc])));
    const tdf = el('td');
    flagNames(e[COL.flags]).forEach(fl => tdf.appendChild(el('span', `flag ${fl}`, fl)));
    tr.appendChild(tdf);
    tbody.appendChild(tr);
  });
  $('#result-count').textContent = `${fmt(state.filtered.length)} results`;
  $('#page-info').textContent = `Page ${state.page + 1} of ${Math.max(1, Math.ceil(state.filtered.length / PAGE_SIZE))}`;
  $('#prev-page').disabled = state.page === 0;
  $('#next-page').disabled = (state.page + 1) * PAGE_SIZE >= state.filtered.length;
}

// ----- Profile view -----
async function showProfile(pid) {
  showView('profile');
  const c = $('#profile-content');
  c.innerHTML = 'Loading…';
  let displayPid = pid;
  let redirectNote = null;
  if (state.redirects[pid]) {
    displayPid = state.redirects[pid];
    redirectNote = `This profile was redirected from "${pid}"`;
  }
  const idxEntry = state.index.find(e => e[COL.pid] === displayPid);
  if (!idxEntry) { c.textContent = `Profile not found: ${displayPid}`; return; }
  const shard = await loadShard(idxEntry[COL.shard]);
  const p = shard[displayPid];
  if (!p) { c.textContent = `Profile in shard not found: ${displayPid}`; return; }
  c.innerHTML = '';
  renderProfile(c, displayPid, p, redirectNote);
}

function renderProfile(container, pid, p, redirectNote) {
  const header = el('div', 'profile-header');
  header.appendChild(el('h2', '', p.full_name || pid));
  const subline = el('div', 'subline');
  const pidLine = el('span'); pidLine.innerHTML = `pid <strong>${pid}</strong>`; subline.appendChild(pidLine);
  if (p.grade_en) subline.appendChild(el('span', `grade ${p.grade_en}`, p.grade_en));
  const freqSpan = el('span'); freqSpan.innerHTML = `freq <strong>${fmt(p.frequency || 0)}</strong>`; subline.appendChild(freqSpan);
  const booksSpan = el('span'); booksSpan.innerHTML = `books <strong>${fmt(p.book_count || 0)}</strong>`; subline.appendChild(booksSpan);
  const tcSpan = el('span'); tcSpan.innerHTML = `teachers <strong>${fmt(p.teacher_count || 0)}</strong>`; subline.appendChild(tcSpan);
  const scSpan = el('span'); scSpan.innerHTML = `students <strong>${fmt(p.student_count || 0)}</strong>`; subline.appendChild(scSpan);
  header.appendChild(subline);
  container.appendChild(header);

  if (redirectNote) container.appendChild(el('div', 'flag-banner warn', redirectNote));
  if (p._abandoned) container.appendChild(el('div', 'flag-banner bad', 'ABANDONED — never cited in any chain (freq=0). Import ghost.'));
  if (p._ambiguous_companion_candidate) container.appendChild(el('div', 'flag-banner warn', 'AMBIGUOUS — may conflate multiple identities. Needs chain-context disambiguation.'));
  const fp = p.field_provenance || {};
  const ci = fp.classical_identity;
  if (ci && ci.status === 'CONFLATED') container.appendChild(el('div', 'flag-banner bad', `CONFLATED — ${ci.reason || 'multiple real people share this string form'}`));
  else if (ci && ci.canonical_name) container.appendChild(el('div', 'flag-banner ok', `Classical identity: ${ci.canonical_name}${ci.death_year ? ` (d. ${ci.death_year} AH)` : ''}. ${ci.note || ''}`));

  const grid = el('div', 'profile-grid');
  container.appendChild(grid);
  const left = el('div'), right = el('div');

  // Track rendered fields to dump the rest at the end as raw JSON
  const renderedFields = new Set(['_redirect_to', '_redirect_reason', '_redirect_date',
    'frequency','book_count','teacher_count','student_count','teachers','students',
    'sample_chains','top_books','chain_position','field_provenance']);

  const addSection = (title, fields, target) => {
    const box = el('div', 'section-box');
    box.appendChild(el('h3', '', title));
    let added = 0;
    for (const f of fields) {
      renderedFields.add(f);
      const v = p[f];
      if (v === undefined || v === null || v === '') continue;
      if (Array.isArray(v) && v.length === 0) continue;
      if (typeof v === 'object' && !Array.isArray(v) && Object.keys(v).length === 0) continue;
      box.appendChild(kv(f, v));
      added++;
    }
    if (added > 0) target.appendChild(box);
  };

  // IDENTITY
  addSection('Identity', [
    'full_name','norm_name','full_name_diacritics','alt_name','namings',
    'kunya','laqab','nisba','full_nasab','en_name','gender','is_woman',
    'full_name_archived','norm_name_archived',
  ], left);

  // ERA & LOCATION
  addSection('Era & Location', [
    'birth','death','birth_year','death_year','birth_gregorian','death_gregorian',
    'death_range','death_range_note','death_range_confidence',
    'tabaqat','tabaqat_num',
    'city','places','birth_place','death_place','area_of_interest',
  ], left);

  // GRADE
  addSection('Grade & Mudallis', [
    'grade_en','grade_ar','grade_source','scholar_opinions',
    'is_mudallis','mudallis_rank','mudallis_source',
    'is_companion','companion_evidence','companion_verified','companion_verification_note',
    'companion_checks','companion_suspect','companion_suspect_reason',
    'alias_canonical','alias_overlap',
  ], left);

  // GRADING DERIVATION — trace grade back to its evidence sources
  {
    const gd = el('div', 'section-box');
    gd.appendChild(el('h3', '', 'Grading derivation'));
    let added = 0;
    const row = (k, v) => { gd.appendChild(kv(k, v)); added++; };
    if (p.grade_en) row('final grade', p.grade_en + (p.grade_ar ? `  (${p.grade_ar})` : ''));
    if (p.grade_source) row('grade_source', p.grade_source);
    if (Array.isArray(p.scholar_opinions) && p.scholar_opinions.length) {
      row('scholar_opinions_count', p.scholar_opinions.length);
      const scholars = [...new Set(p.scholar_opinions.map(o => o.scholar || o.source).filter(Boolean))];
      if (scholars.length) row('scholars_consulted', scholars.slice(0, 10).join(' · ') + (scholars.length > 10 ? ` +${scholars.length-10}` : ''));
    }
    if (Array.isArray(p.dorar_results) && p.dorar_results.length) {
      row('dorar_commentary_count', p.dorar_results.length);
      const mohdiths = [...new Set(p.dorar_results.map(r => r.mohdith).filter(Boolean))];
      if (mohdiths.length) row('dorar_mohdiths', mohdiths.slice(0, 6).join(' · '));
    }
    if (p.book_sources) {
      const bs = typeof p.book_sources === 'object' ? Object.keys(p.book_sources) : [];
      if (bs.length) row('book_sources', bs.join(' · '));
    }
    if (p.isnad_evidence) {
      const ie = p.isnad_evidence;
      row('isnad_tally', `sahih=${ie.sahih||0} hasan=${ie.hasan||0} daif=${ie.daif||0} / ${ie.total||0}`);
    }
    if (p.companion_verified !== undefined) row('companion_verified', String(p.companion_verified));
    if (p.companion_verification_note) row('companion_note', p.companion_verification_note);
    if (Array.isArray(p.companion_checks) && p.companion_checks.length) row('companion_checks', p.companion_checks.length + ' checks');
    const fp2 = p.field_provenance || {};
    for (const k of Object.keys(fp2)) {
      if (k.startsWith('grade_') || k.includes('upgrade') || k.includes('demote') || k.includes('thiqa') || k.includes('mononym') || k === 'classical_identity') {
        const v = fp2[k];
        row('prov.' + k, typeof v === 'string' ? v : JSON.stringify(v).slice(0, 200));
      }
    }
    if (added > 0) left.insertBefore(gd, left.children[3] || null);
  }
  ['dorar_results'].forEach(f => renderedFields.add(f));

  // FAMILY
  addSection('Family', [
    'parents','children','siblings','spouse','family_notes','tags',
  ], left);

  // EXTERNAL IDs & SOURCES
  addSection('External sources', [
    'gk_rawy_id','locked_db_pid','kaggle_idx','source','source_book','book_sources',
    'fath_albari_mentions','hawramani_entries','hawramani_book_count','_import_confidence',
  ], left);

  if (p.chain_position) {
    const cp = el('div', 'section-box');
    cp.appendChild(el('h3', '', 'Chain position'));
    const { first=0, middle=0, last=0 } = p.chain_position;
    const total = first + middle + last;
    cp.appendChild(kv('terminal (end)', `${last} (${total ? ((100*last/total)|0) : 0}%)`));
    cp.appendChild(kv('middle', `${middle} (${total ? ((100*middle/total)|0) : 0}%)`));
    cp.appendChild(kv('first (compiler-side)', `${first} (${total ? ((100*first/total)|0) : 0}%)`));
    cp.appendChild(kv('total', total));
    if (p.isnad_tree_count) cp.appendChild(kv('isnad_tree_count', p.isnad_tree_count));
    left.appendChild(cp);
  }
  renderedFields.add('isnad_tree_count');

  // AUDIT NOTE — prominent banner for past corrections
  if (p.audit_note) {
    const an = el('div', 'flag-banner ok');
    an.style.fontStyle = 'italic';
    an.textContent = `AUDIT: ${p.audit_note}`;
    container.insertBefore(an, grid);
  }
  renderedFields.add('audit_note');

  // BIOGRAPHY — long classical text, own section on left
  if (p.biography) {
    const b = el('div', 'section-box');
    b.appendChild(el('h3', '', 'Classical biography'));
    const val = el('div');
    val.style.fontFamily = '"Amiri","Traditional Arabic",serif'; val.style.fontSize = '15px';
    val.style.direction = 'rtl'; val.style.textAlign = 'right'; val.style.lineHeight = '1.8';
    val.style.maxHeight = '400px'; val.style.overflowY = 'auto';
    val.style.padding = '8px'; val.style.background = 'var(--bg)'; val.style.borderRadius = '4px';
    val.textContent = String(p.biography);
    b.appendChild(val);
    left.appendChild(b);
  }
  renderedFields.add('biography');

  // SCHOLAR OPINIONS — jarh wa ta'dil per-scholar
  if (p.scholar_opinions && Array.isArray(p.scholar_opinions) && p.scholar_opinions.length > 0) {
    const so = el('div', 'section-box');
    const h = el('h3', '', 'Scholar opinions (jarh wa ta\'dil) ');
    h.appendChild(el('span', 'count', `(${p.scholar_opinions.length})`));
    so.appendChild(h);
    for (const op of p.scholar_opinions.slice(0, 12)) {
      const li = el('div', 'list-item');
      li.style.flexDirection = 'column'; li.style.alignItems = 'flex-start';
      const sch = el('div'); sch.style.fontWeight = '600'; sch.style.fontFamily = '"Amiri",serif';
      sch.style.fontSize = '14px'; sch.style.direction = 'rtl';
      sch.textContent = op.scholar || op.source || '(scholar)';
      li.appendChild(sch);
      const opn = el('div'); opn.style.fontFamily = '"Amiri",serif';
      opn.style.fontSize = '14px'; opn.style.direction = 'rtl'; opn.style.marginTop = '2px';
      opn.style.color = 'var(--text-dim)';
      opn.textContent = op.opinion || op.text || op.grade || '';
      li.appendChild(opn);
      if (op.source && op.source !== op.scholar) {
        const src = el('div'); src.style.fontSize = '10px'; src.style.color = 'var(--text-dim)';
        src.style.marginTop = '2px'; src.textContent = `source: ${op.source}`;
        li.appendChild(src);
      }
      so.appendChild(li);
    }
    if (p.scholar_opinions.length > 12) {
      so.appendChild(el('div', 'hint', `…and ${p.scholar_opinions.length - 12} more opinions`));
    }
    left.appendChild(so);
  }
  renderedFields.add('scholar_opinions');

  // FLAGS
  const flagFields = ['_abandoned','_kinship_placeholder','_ambiguous_companion_candidate',
    '__parse_artifact','cross_book_conflict','merged_from','merge_count'];
  const hasFlags = flagFields.some(f => p[f] !== undefined && p[f] !== null && p[f] !== false);
  if (hasFlags) {
    const fb = el('div', 'section-box');
    fb.appendChild(el('h3', '', 'Flags & merge history'));
    for (const f of flagFields) {
      renderedFields.add(f);
      const v = p[f];
      if (v === undefined || v === null || v === false) continue;
      if (Array.isArray(v) && v.length === 0) continue;
      fb.appendChild(kv(f, v));
    }
    left.appendChild(fb);
  } else {
    flagFields.forEach(f => renderedFields.add(f));
  }

  if (p.top_books && p.top_books.length) {
    const tb = el('div', 'section-box');
    const h = el('h3', '', `Top books `); h.appendChild(el('span', 'count', `(${p.top_books.length})`));
    tb.appendChild(h);
    for (const b of p.top_books.slice(0, 15)) {
      const li = el('div', 'list-item');
      li.appendChild(el('span', 'name', b.book));
      li.appendChild(el('span', 'count', fmt(b.count)));
      tb.appendChild(li);
    }
    left.appendChild(tb);
  }

  if ((p.hawramani_urls && p.hawramani_urls.length) || (p.hawramani_source_books && p.hawramani_source_books.length)) {
    const hw = el('div', 'section-box');
    hw.appendChild(el('h3', '', 'Hawramani attestation'));
    (p.hawramani_urls || []).slice(0, 3).forEach(u => {
      const li = el('div', 'list-item');
      const a = el('a', '', u); a.href = u; a.target = '_blank';
      li.appendChild(a); hw.appendChild(li);
    });
    (p.hawramani_source_books || []).slice(0, 10).forEach(b => hw.appendChild(el('div', 'list-item', b.substring(0, 120))));
    if (p.hawramani_result) hw.appendChild(kv('hawramani_result', p.hawramani_result));
    left.appendChild(hw);
  }
  ['hawramani_urls','hawramani_source_books','hawramani_result'].forEach(f => renderedFields.add(f));
  renderedFields.add('isnad_evidence'); renderedFields.add('dorar_results');

  if (p.isnad_evidence) {
    const ie = el('div', 'section-box');
    ie.appendChild(el('h3', '', 'Isnad evidence'));
    const { sahih=0, hasan=0, daif=0, total=0, sources=[] } = p.isnad_evidence;
    ie.appendChild(kv('sahih', sahih)); ie.appendChild(kv('hasan', hasan)); ie.appendChild(kv('daif', daif)); ie.appendChild(kv('total', total));
    if (sources.length) ie.appendChild(kv('sources', sources.join(', ')));
    left.appendChild(ie);
  }
  if (p.dorar_results && p.dorar_results.length) {
    const dr = el('div', 'section-box');
    const h = el('h3', '', 'Dorar commentary '); h.appendChild(el('span', 'count', `(${p.dorar_results.length})`));
    dr.appendChild(h);
    for (const r of p.dorar_results.slice(0, 5)) {
      const li = el('div', 'list-item');
      li.appendChild(el('span', 'name', `${r.mohdith || ''}: ${(r.grade || '').substring(0, 80)}`));
      li.appendChild(el('span', 'count', r.binary || ''));
      dr.appendChild(li);
    }
    left.appendChild(dr);
  }
  grid.appendChild(left);

  if (p.teachers && p.teachers.length) {
    const t = el('div', 'section-box');
    const h = el('h3', '', 'Teachers (narrated FROM) '); h.appendChild(el('span', 'count', `(${p.teachers.length})`));
    t.appendChild(h);
    for (const te of p.teachers.slice(0, 25)) {
      const li = el('div', 'list-item');
      const nm = el('span', 'name', te.name);
      nm.onclick = ev => { ev.stopPropagation(); showProfile(te.name); };
      li.appendChild(nm);
      li.appendChild(el('span', 'count', fmt(te.count)));
      t.appendChild(li);
    }
    right.appendChild(t);
  }
  if (p.students && p.students.length) {
    const s = el('div', 'section-box');
    const h = el('h3', '', 'Students (narrated FROM this) '); h.appendChild(el('span', 'count', `(${p.students.length})`));
    s.appendChild(h);
    for (const st of p.students.slice(0, 25)) {
      const li = el('div', 'list-item');
      const nm = el('span', 'name', st.name);
      nm.onclick = ev => { ev.stopPropagation(); showProfile(st.name); };
      li.appendChild(nm);
      li.appendChild(el('span', 'count', fmt(st.count)));
      s.appendChild(li);
    }
    right.appendChild(s);
  }
  if (p.sample_chains && p.sample_chains.length) {
    const sc = el('div', 'section-box');
    const h = el('h3', '', 'Sample chains '); h.appendChild(el('span', 'count', `(${p.sample_chains.length})`));
    sc.appendChild(h);
    for (const cs of p.sample_chains) {
      const box = el('div', 'chain-sample');
      box.appendChild(el('div', 'book', cs.book || ''));
      const txt = el('div', 'chain-text');
      (cs.chain || []).forEach((nm, i) => {
        if (i > 0) txt.appendChild(document.createTextNode(' ← '));
        const ns = el('span', nm === p.full_name ? 'me' : '', nm);
        ns.onclick = () => showProfile(nm);
        ns.style.cursor = 'pointer';
        txt.appendChild(ns);
      });
      box.appendChild(txt);
      sc.appendChild(box);
    }
    right.appendChild(sc);
  }

  // HADITHS NARRATED — reverse index lookup from narrator_hadiths shard
  renderHadithsNarrated(right, pid, p).catch(e => console.error('hadiths:', e));
  if (p.field_provenance && Object.keys(p.field_provenance).length) {
    const pr = el('div', 'section-box');
    pr.appendChild(el('h3', '', 'Provenance (field_provenance)'));
    const entries = Object.entries(p.field_provenance);
    // Sort: put pre_kinship_resolve snapshots at bottom (noisy)
    entries.sort((a, b) => {
      const aOld = a[0].includes('pre_') ? 1 : 0;
      const bOld = b[0].includes('pre_') ? 1 : 0;
      return aOld - bOld;
    });
    for (const [field, prov] of entries) {
      const pf = el('div', 'prov-field');
      pf.appendChild(el('span', 'field-name', field));
      const pre = el('pre');
      pre.textContent = typeof prov === 'string' ? prov : JSON.stringify(prov, null, 2);
      pf.appendChild(pre);
      pr.appendChild(pf);
    }
    right.appendChild(pr);
  }

  // RAW JSON — everything else we didn't explicitly render.
  // This ensures the viewer shows EVERY field present on the profile.
  const remaining = {};
  for (const [k, v] of Object.entries(p)) {
    if (renderedFields.has(k)) continue;
    if (v === undefined || v === null || v === '') continue;
    if (Array.isArray(v) && v.length === 0) continue;
    if (typeof v === 'object' && !Array.isArray(v) && Object.keys(v).length === 0) continue;
    remaining[k] = v;
  }
  if (Object.keys(remaining).length > 0) {
    const rb = el('div', 'section-box');
    const h = el('h3', '', 'Other fields '); h.appendChild(el('span', 'count', `(${Object.keys(remaining).length})`));
    rb.appendChild(h);
    for (const [k, v] of Object.entries(remaining)) {
      rb.appendChild(kv(k, v));
    }
    right.appendChild(rb);
  }

  grid.appendChild(right);
}

// ---- Hadith reverse-index loaders ----
const hadithIndexCache = new Map();  // shard_num -> {pid: [[bid,idx,pos]]}
const hadithBookCache = new Map();   // book_id -> {book_id, name_ar, hadiths:[{n,c,m}]}
let hadithBooksMeta = null;          // book_id -> name_ar map

async function loadHadithBooksMeta() {
  if (hadithBooksMeta) return hadithBooksMeta;
  try { hadithBooksMeta = await loadJSON('data/hadith_books.json'); }
  catch(e) { hadithBooksMeta = {}; }
  return hadithBooksMeta;
}
async function loadHadithIndexShard(shard) {
  if (hadithIndexCache.has(shard)) return hadithIndexCache.get(shard);
  try {
    const d = await loadJSON(`data/narrator_hadiths/shard_${String(shard).padStart(3,'0')}.json`);
    hadithIndexCache.set(shard, d);
    return d;
  } catch(e) { hadithIndexCache.set(shard, {}); return {}; }
}
async function loadHadithBook(bid) {
  if (hadithBookCache.has(bid)) return hadithBookCache.get(bid);
  const d = await loadJSON(`data/hadiths/book_${bid}.json`);
  hadithBookCache.set(bid, d);
  return d;
}

async function renderHadithsNarrated(container, pid, profile) {
  // Find which shard this pid is in
  const indexRow = state.index.find(r => r[COL.pid] === pid);
  if (!indexRow) return;
  const shard = indexRow[COL.shard];
  const [books, idx] = await Promise.all([loadHadithBooksMeta(), loadHadithIndexShard(shard)]);
  const refs = idx[pid];
  if (!refs || !refs.length) return;

  const box = el('div', 'section-box');
  const head = el('h3', '', 'Hadiths narrated ');
  head.appendChild(el('span', 'count', `(${fmt(refs.length)}${refs.length >= 2000 ? '+ capped' : ''})`));
  box.appendChild(head);

  // Book-count summary
  const byBook = new Map();
  for (const [bid] of refs) byBook.set(bid, (byBook.get(bid) || 0) + 1);
  const topBooks = [...byBook.entries()].sort((a,b) => b[1]-a[1]).slice(0, 8);
  const sum = el('div', 'hint');
  sum.style.marginBottom = '8px';
  sum.textContent = `Across ${byBook.size} books. Top: ` +
    topBooks.map(([b,c]) => `${books[b] || 'book '+b} (${c})`).join(' · ');
  box.appendChild(sum);

  // Paged list — 20 per page
  const state2 = { page: 0, perPage: 20 };
  const listWrap = el('div');
  box.appendChild(listWrap);
  const pagerWrap = el('div', 'pager');
  box.appendChild(pagerWrap);

  const render = async () => {
    listWrap.innerHTML = '';
    const start = state2.page * state2.perPage;
    const slice = refs.slice(start, start + state2.perPage);
    for (const [bid, hidx, pos] of slice) {
      const row = el('div', 'hadith-row');
      row.style.borderTop = '1px solid var(--border)';
      row.style.padding = '6px 0';
      const hdr = el('div');
      hdr.style.display = 'flex'; hdr.style.justifyContent = 'space-between';
      hdr.style.alignItems = 'baseline'; hdr.style.gap = '8px';
      const bk = el('span'); bk.style.fontFamily = '"Amiri",serif'; bk.style.fontSize = '13px';
      bk.style.direction = 'rtl'; bk.textContent = books[bid] || `book ${bid}`;
      hdr.appendChild(bk);
      const meta2 = el('span', 'hint'); meta2.textContent = `chain pos ${pos}`;
      hdr.appendChild(meta2);
      row.appendChild(hdr);
      const body = el('div');
      body.style.fontFamily = '"Amiri","Traditional Arabic",serif';
      body.style.fontSize = '14px'; body.style.direction = 'rtl'; body.style.textAlign = 'right';
      body.style.marginTop = '4px'; body.style.lineHeight = '1.7';
      body.textContent = 'loading…';
      row.appendChild(body);
      listWrap.appendChild(row);
      // lazy fill matn + chain
      loadHadithBook(bid).then(bk => {
        const h = (bk.hadiths || [])[hidx];
        if (!h) { body.textContent = '(not found)'; return; }
        body.innerHTML = '';
        const num = el('div'); num.style.fontSize = '11px'; num.style.color = 'var(--text-dim)';
        num.style.direction = 'ltr'; num.textContent = `#${h.n}`;
        body.appendChild(num);
        // chain with clickable names, current narrator highlighted
        const chain = el('div');
        chain.style.fontSize = '12px'; chain.style.color = 'var(--text-dim)'; chain.style.margin = '2px 0 4px';
        (h.c || []).forEach((nm, i) => {
          if (i > 0) chain.appendChild(document.createTextNode(' ← '));
          const n = norm(nm);
          const isMe = n === norm(profile.full_name || '') || n === norm(pid);
          const span = el('span', isMe ? 'me' : '', nm);
          span.style.cursor = 'pointer';
          if (isMe) { span.style.fontWeight = '700'; span.style.color = 'var(--accent)'; }
          span.onclick = () => showProfile(nm);
          chain.appendChild(span);
        });
        body.appendChild(chain);
        const matn = el('div');
        matn.textContent = h.m || '(no matn)';
        body.appendChild(matn);
      }).catch(e => { body.textContent = '(load error)'; });
    }
    // pager
    pagerWrap.innerHTML = '';
    const nPages = Math.ceil(refs.length / state2.perPage);
    const info = el('span', 'hint', `page ${state2.page+1} / ${nPages} · showing ${start+1}-${Math.min(start+state2.perPage, refs.length)} of ${fmt(refs.length)}`);
    pagerWrap.appendChild(info);
    if (state2.page > 0) {
      const prev = el('button', '', '« prev'); prev.onclick = () => { state2.page--; render(); };
      pagerWrap.appendChild(prev);
    }
    if (state2.page < nPages - 1) {
      const next = el('button', '', 'next »'); next.onclick = () => { state2.page++; render(); };
      pagerWrap.appendChild(next);
    }
  };
  render();
  container.appendChild(box);
}

function kv(k, v) {
  const d = el('div', 'kv');
  d.appendChild(el('span', 'k', k.replace(/_/g, ' ') + ': '));
  const isObj = typeof v === 'object' && v !== null;
  if (isObj && Array.isArray(v) && v.length > 0 && typeof v[0] === 'object') {
    // List of dicts — expand inline as sub-items
    const wrap = document.createElement('div');
    wrap.style.marginTop = '4px';
    for (const item of v.slice(0, 12)) {
      const sub = el('div', 'prov-field');
      sub.style.fontSize = '11px'; sub.style.padding = '4px 6px';
      const pre = el('pre'); pre.textContent = JSON.stringify(item, null, 1);
      sub.appendChild(pre);
      wrap.appendChild(sub);
    }
    if (v.length > 12) wrap.appendChild(el('div', 'hint', `…and ${v.length - 12} more`));
    d.appendChild(wrap);
    return d;
  }
  if (isObj && !Array.isArray(v)) {
    // Nested object — pretty-print
    const pre = el('pre');
    pre.style.margin = '4px 0 0 0'; pre.style.fontSize = '11px';
    pre.style.background = 'var(--bg)'; pre.style.padding = '6px 8px'; pre.style.borderRadius = '3px';
    pre.textContent = JSON.stringify(v, null, 2);
    d.appendChild(pre);
    return d;
  }
  const val = el('span', 'v');
  const s = Array.isArray(v) ? v.map(x => typeof x === 'object' ? JSON.stringify(x) : String(x)).join(', ') : String(v);
  val.textContent = s.length > 500 ? s.substring(0, 500) + '…' : s;
  if (/^[\x00-\x7f]+$/.test(s)) val.classList.add('en');
  d.appendChild(val);
  return d;
}

// ----- Chain grader -----
async function gradeChain() {
  const input = $('#chain-input').value.trim();
  if (!input) return;
  const raw = input.split(/\n|→|\bعن\b|\bحدثنا\b|\bأخبرنا\b|\bسمعت\b|,/).map(s => s.trim()).filter(Boolean);
  const result = $('#chain-result');
  result.innerHTML = '';
  const rows = raw.map(nm => {
    const n = norm(nm);
    const hit = state.index.find(e => norm(e[COL.name]) === n || norm(e[COL.pid]) === n);
    return { input: nm, entry: hit };
  });
  // Weakest-link via grade code strength
  const STRENGTH = { 1: 5, 2: 5, 3: 4, 4: 3, 5: 2, 6: 2, 7: 1, 8: 0, 9: 0, 10: -1, 0: -1 };
  let minStrength = 10, minIdx = -1, hasUnknown = false;
  rows.forEach((r, i) => {
    if (!r.entry) { hasUnknown = true; return; }
    const s = STRENGTH[r.entry[COL.grade]];
    if (s == null || s < 0) { hasUnknown = true; return; }
    if (s < minStrength) { minStrength = s; minIdx = i; }
  });
  let verdict = 'unknown', verdictText = 'Could not grade (unresolved narrator or unknown grade)';
  if (!hasUnknown) {
    if (minStrength >= 3) { verdict = 'sahih'; verdictText = 'Ṣaḥīḥ — weakest link is reliable or better'; }
    else if (minStrength >= 2) { verdict = 'hasan'; verdictText = 'Ḥasan — weakest link is acceptable'; }
    else if (minStrength >= 1) { verdict = 'daif'; verdictText = 'Ḍaʿīf — weakest link is weak'; }
    else { verdict = 'mawdu'; verdictText = 'Mawḍūʿ — weakest link is abandoned/fabricator'; }
  }
  result.appendChild(el('div', `chain-verdict ${verdict}`, verdictText));
  const box = el('div', 'section-box');
  rows.forEach((r, i) => {
    const row = el('div', `chain-narrator-row ${i === minIdx ? 'weakest' : ''}`);
    row.appendChild(el('span', 'idx', i + 1));
    const nm = el('span', 'nm', r.input);
    if (r.entry) {
      nm.onclick = () => showProfile(r.entry[COL.pid]);
      nm.style.cursor = 'pointer';
      nm.style.textDecoration = 'underline';
    }
    row.appendChild(nm);
    const g = r.entry ? gradeName(r.entry[COL.grade]) : null;
    row.appendChild(el('span', `grade ${g || 'unknown'}`, g || 'NOT FOUND'));
    box.appendChild(row);
  });
  result.appendChild(box);
}

// ----- Audit view -----
function renderAudit(focusFlag) {
  const c = $('#audit-content');
  c.innerHTML = '';
  const entries = Object.entries(state.audit).sort((a, b) => b[1].length - a[1].length);
  for (const [flag, pids] of entries) {
    const g = el('div', 'audit-group');
    if (flag === focusFlag) g.style.borderColor = 'var(--warn)';
    const h = el('h3');
    h.appendChild(el('span', '', flag));
    h.appendChild(el('span', 'count', `${fmt(pids.length)} profiles`));
    g.appendChild(h);
    const list = el('div', 'pid-list');
    for (const pid of pids.slice(0, 200)) {
      const chip = el('span', 'pid-chip', pid);
      chip.onclick = () => showProfile(pid);
      list.appendChild(chip);
    }
    if (pids.length > 200) list.appendChild(el('span', 'count', `… and ${fmt(pids.length - 200)} more`));
    g.appendChild(list);
    c.appendChild(g);
  }
}

// ----- Nav -----
function showView(name) {
  $$('.view').forEach(v => v.classList.add('hidden'));
  const sel = $(`#view-${name}`); if (sel) sel.classList.remove('hidden');
  $$('nav a.tab').forEach(a => a.classList.toggle('active', a.dataset.view === name));
  if (name === 'audit') renderAudit();
  location.hash = name;
}
function handleHashChange() {
  const h = (location.hash || '#browse').substring(1);
  if (['dashboard','browse','chain','audit'].includes(h)) showView(h);
  else showView('browse');
}
function bindUI() {
  $$('nav a.tab').forEach(a => a.addEventListener('click', e => { e.preventDefault(); showView(a.dataset.view); }));
  $('#search').addEventListener('input', e => { state.searchTerm = e.target.value; applyFilter(); });
  $('#filter-grade').addEventListener('change', e => { state.gradeFilter = e.target.value; applyFilter(); });
  $('#filter-flag').addEventListener('change', e => { state.flagFilter = e.target.value; applyFilter(); });
  $('#hide-placeholders').addEventListener('change', e => { state.hidePlaceholders = e.target.checked; applyFilter(); });
  $('#hide-abandoned').addEventListener('change', e => { state.hideAbandoned = e.target.checked; applyFilter(); });
  $('#prev-page').addEventListener('click', () => { if (state.page > 0) { state.page--; renderBrowse(); window.scrollTo(0, 0); } });
  $('#next-page').addEventListener('click', () => { state.page++; renderBrowse(); window.scrollTo(0, 0); });
  $$('th[data-sort]').forEach(th => th.addEventListener('click', () => {
    const k = th.dataset.sort;
    if (state.sortKey === k) state.sortDir *= -1;
    else { state.sortKey = k; state.sortDir = (['freq','books','teachers','students'].includes(k)) ? -1 : 1; }
    applyFilter();
  }));
  $('#chain-grade-btn').addEventListener('click', gradeChain);
  window.addEventListener('hashchange', handleHashChange);
}

boot().catch(e => {
  document.body.innerHTML = `<pre style="padding:20px;color:red">Boot error: ${e.message}

Ensure you opened index.html via a local server. File://  does not work for fetch().
Run: python -m http.server 8000
Then open: http://localhost:8000</pre>`;
});
})();
