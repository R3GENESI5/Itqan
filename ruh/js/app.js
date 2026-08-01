/* ── Ruh al-Ma'ani reader ──────────────────────────────
   Arabic text of al-Alusi's tafsir with hover translation.
   Word glosses come from the offline Buckwalter analysis
   (data/lex.json + per-section `lex`); clicking a word with
   a Qur'anic root opens the same root panel as the Quran app,
   backed by ../quran/data/*.
   ───────────────────────────────────────────────────── */

const Ruh = {
    // Data
    index: null,
    lex: {},                  // normalised word → "gloss" | "gloss|root"
    rootSenses: {},           // root → short English sense
    secCache: {},
    current: null,            // { s, a1 } of the loaded section

    // Root panel data (loaded on first root click)
    rootsIndex: null,
    mufradat: null,
    families: null,
    versesText: null,
    surahList: null,
    rootDataLoading: null,
    selectedRoot: null,
    showAllVerses: false,
    PREVIEW_LIMIT: 25,

    // Prefs
    glossOn: true,
    darkMode: false,
    isMobile: false,
    TEXT_SCALES: [1, 1.15, 1.3, 1.5],
    textScaleIdx: 0,

    QURAN_DATA: '../quran/data',
    $: (id) => document.getElementById(id),

    // Arabic orthography
    RE_SPLIT: /([ء-ْٰٱ]+)/,
    RE_DIAC: /[ً-ْٰـ]/g,
    RE_VOWEL: /[ً-ْٰ]/,

    norm(w) {
        return w.replace(this.RE_DIAC, '').replace(/ٱ/g, 'ا');
    },

    // ── Init ───────────────────────────────────────
    async init() {
        try {
            const [index, lex, roots] = await Promise.all([
                fetch('data/index.json').then(r => r.json()),
                fetch('data/lex.json').then(r => r.json()),
                fetch('data/roots.json').then(r => r.json()).catch(() => ({})),
            ]);
            this.index = index;
            this.lex = lex;
            this.rootSenses = roots;

            this.restorePrefs();
            this.setupUI();
            this.renderStats();
            this.$('loading-overlay').classList.add('hidden');
            this.handleHash();
        } catch (err) {
            console.error('Init failed:', err);
            document.querySelector('.loader-text').textContent = 'خطأ في التحميل';
        }
    },

    restorePrefs() {
        this.checkMobile();
        this.darkMode = localStorage.getItem('qbq-dark-mode') === 'true';
        this.updateThemeIcon();

        this.glossOn = localStorage.getItem('ruh-gloss') !== 'false';
        document.body.classList.toggle('gloss-on', this.glossOn);
        this.$('gloss-toggle').classList.toggle('active', this.glossOn);

        const saved = parseInt(localStorage.getItem('qbq-text-scale') || '0');
        if (saved > 0 && saved < this.TEXT_SCALES.length) {
            this.textScaleIdx = saved;
            document.documentElement.style.setProperty('--text-scale', this.TEXT_SCALES[saved]);
            this.$('text-scale-btn').classList.add('active');
        }
    },

    checkMobile() {
        this.isMobile = window.matchMedia('(max-width: 768px)').matches;
    },

    renderStats() {
        const s = this.index.stats || {};
        const fmt = n => n >= 1e6 ? (n / 1e6).toFixed(1) + 'M' : n >= 1e3 ? Math.round(n / 1e3) + 'k' : n;
        this.$('rw-stats').innerHTML =
            `<div><b>${fmt(s.pages || 0)}</b><span>Pages</span></div>` +
            `<div><b>${fmt(s.tokens || 0)}</b><span>Words</span></div>` +
            `<div><b>${fmt(s.glossed_types || 0)}</b><span>Glossed forms</span></div>` +
            `<div><b>${Math.round((s.token_coverage || 0) * 100)}%</b><span>Coverage</span></div>` +
            `<div><b>${fmt(s.sections || 0)}</b><span>Passages</span></div>`;
    },

    // ── UI ─────────────────────────────────────────
    setupUI() {
        window.addEventListener('resize', () => this.checkMobile());

        const surahSel = this.$('surah-select');
        surahSel.innerHTML = '<option value="">— السورة —</option>' +
            this.index.surahs.filter(s => s.sec.length)
                .map(s => `<option value="${s.n}">${s.n}. ${s.ar}</option>`).join('');
        surahSel.addEventListener('change', () => {
            const n = parseInt(surahSel.value);
            if (n) {
                const first = this.index.surahs[n - 1].sec[0];
                location.hash = this.hashFor(n, first.a1, 0);
            }
        });

        this.$('sec-select').addEventListener('change', (e) => {
            if (this.current) location.hash = this.hashFor(this.current.s, +e.target.value, 0);
        });

        this.$('prev-sec').addEventListener('click', () => this.step(-1));
        this.$('next-sec').addEventListener('click', () => this.step(1));
        this.$('nav-prev').addEventListener('click', () => this.step(-1));
        this.$('nav-next').addEventListener('click', () => this.step(1));

        // Gloss toggle
        this.$('gloss-toggle').addEventListener('click', () => {
            this.glossOn = !this.glossOn;
            document.body.classList.toggle('gloss-on', this.glossOn);
            this.$('gloss-toggle').classList.toggle('active', this.glossOn);
            localStorage.setItem('ruh-gloss', this.glossOn);
            if (!this.glossOn) this.hideTip();
        });

        // Dark mode
        this.$('theme-toggle').addEventListener('click', () => {
            this.darkMode = !this.darkMode;
            document.documentElement.classList.toggle('dark-mode', this.darkMode);
            localStorage.setItem('qbq-dark-mode', this.darkMode);
            this.updateThemeIcon();
        });

        // Text scale
        this.$('text-scale-btn').addEventListener('click', () => {
            this.textScaleIdx = (this.textScaleIdx + 1) % this.TEXT_SCALES.length;
            document.documentElement.style.setProperty('--text-scale', this.TEXT_SCALES[this.textScaleIdx]);
            this.$('text-scale-btn').classList.toggle('active', this.textScaleIdx > 0);
            localStorage.setItem('qbq-text-scale', this.textScaleIdx);
        });

        // Word interaction — delegated, the body has tens of thousands of spans
        const body = this.$('sec-body');
        body.addEventListener('mouseover', (e) => {
            const w = e.target.closest('.w');
            if (w && this.glossOn && w.dataset.g) this.showTip(w);
        });
        body.addEventListener('mouseout', (e) => {
            if (e.target.closest('.w')) this.hideTip();
        });
        body.addEventListener('click', (e) => {
            const w = e.target.closest('.w');
            if (w && w.dataset.r) this.onWordClick(w.dataset.r, w);
        });

        // Panel
        this.$('close-panel').addEventListener('click', () => this.closePanel());
        this.$('panel-backdrop').addEventListener('click', () => this.closePanel());
        this.$('show-more-btn').addEventListener('click', () => {
            this.showAllVerses = true;
            this.renderConnectedVerses(this.selectedRoot);
            this.$('show-more-btn').style.display = 'none';
        });
        this.$('root-panel').addEventListener('click', (e) => {
            const toggle = e.target.closest('.section-toggle');
            if (!toggle) return;
            const section = toggle.closest('.panel-section');
            const chevron = toggle.querySelector('.chevron');
            section.classList.toggle('collapsed');
            if (chevron) chevron.textContent = section.classList.contains('collapsed') ? '▸' : '▾';
        });

        // Swipe-to-dismiss the mobile bottom drawer
        const panel = this.$('root-panel');
        panel.addEventListener('touchstart', (e) => {
            if (!this.isMobile) return;
            if (!e.target.closest('.drawer-handle, .panel-header')) return;
            this._drag = { startY: e.touches[0].clientY, currentY: e.touches[0].clientY };
            panel.style.transition = 'none';
        }, { passive: true });

        panel.addEventListener('touchmove', (e) => {
            if (!this._drag) return;
            this._drag.currentY = e.touches[0].clientY;
            const dy = Math.max(0, this._drag.currentY - this._drag.startY);
            panel.style.transform = `translateY(${dy}px)`;
        }, { passive: true });

        panel.addEventListener('touchend', () => {
            if (!this._drag) return;
            const dy = this._drag.currentY - this._drag.startY;
            this._drag = null;
            panel.style.transition = '';
            if (dy > 100) this.closePanel();
            else panel.style.transform = 'translateY(0)';
        }, { passive: true });

        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape') this.closePanel();
            if (e.target.closest('select, input')) return;
            if (e.key === 'ArrowLeft') { e.preventDefault(); this.step(1); }
            if (e.key === 'ArrowRight') { e.preventDefault(); this.step(-1); }
        });

        window.addEventListener('hashchange', () => this.handleHash());
    },

    updateThemeIcon() {
        this.$('theme-toggle').textContent = this.darkMode ? '☀' : '☽';
    },

    // ── Routing ────────────────────────────────────
    handleHash() {
        const hash = decodeURIComponent(location.hash.slice(1));
        if (!hash) {
            this.$('ruh-welcome').style.display = 'flex';
            this.$('sec-header').style.display = 'none';
            this.$('sec-nav').style.display = 'none';
            this.$('sec-body').innerHTML = '';
            return;
        }
        if (hash === 'front') return this.loadFront();

        // #surah:ayah  — or #surah:ayah:chunk for a page deeper into a passage
        const [sRaw, aRaw, kRaw] = hash.split(':');
        const s = parseInt(sRaw);
        if (!(s >= 1 && s <= 114)) return;
        const ayah = parseInt(aRaw) || 1;
        const sec = this.findSection(s, ayah);
        if (!sec) return;
        this.loadSection(s, sec.a1, parseInt(kRaw) || 0);
    },

    /** Canonical hash for a position; chunk 0 is left implicit. */
    hashFor(s, a1, k) {
        return k ? `${s}:${a1}:${k}` : `${s}:${a1}`;
    },

    /** The passage covering `ayah`.
     *
     * Some of Shamela's range labels are short of the text they actually
     * head — An-Nahl's "1 إلى 50" node runs to about ayah 87 — so when no
     * range formally covers the ayah, fall back to the last passage that
     * *starts* at or before it rather than skipping ahead to the next one.
     */
    findSection(surah, ayah) {
        const list = this.index.surahs[surah - 1]?.sec || [];
        if (!list.length) return null;
        const exact = list.find(x => ayah >= x.a1 && ayah <= x.a2);
        if (exact) return exact;
        let before = null;
        for (const x of list) if (x.a1 <= ayah) before = x;
        return before || list[0];
    },

    step(dir) {
        if (!this.current) {
            // Reading the introduction — forward means the start of the book
            if (dir > 0) location.hash = this.hashFor(1, this.index.surahs[0].sec[0].a1, 0);
            return;
        }
        const { s, a1, k } = this.current;
        const list = this.index.surahs[s - 1].sec;
        const i = list.findIndex(x => x.a1 === a1);

        // Move within the passage first, then on to the next passage
        const nk = k + dir;
        if (nk >= 0 && nk < (list[i].c || 1)) {
            location.hash = this.hashFor(s, a1, nk);
            return;
        }
        const next = list[i + dir];
        if (next) {
            location.hash = this.hashFor(s, next.a1, dir > 0 ? 0 : (next.c || 1) - 1);
            return;
        }
        // Roll over into the neighbouring surah
        for (let n = s + dir; n >= 1 && n <= 114; n += dir) {
            const l = this.index.surahs[n - 1].sec;
            if (!l.length) continue;
            const t = dir > 0 ? l[0] : l[l.length - 1];
            location.hash = this.hashFor(n, t.a1, dir > 0 ? 0 : (t.c || 1) - 1);
            return;
        }
    },

    // ── Loading ────────────────────────────────────
    async fetchSection(name) {
        if (!this.secCache[name]) {
            const r = await fetch(`data/sec/${name}.json`);
            if (!r.ok) throw new Error(`section ${name}: ${r.status}`);
            this.secCache[name] = await r.json();
        }
        return this.secCache[name];
    },

    async loadSection(s, a1, k = 0) {
        let data;
        try {
            data = await this.fetchSection(`${s}-${a1}-${k}`);
        } catch (err) {
            console.error(err);
            this.showError(`تعذر تحميل هذا المقطع — could not load ${s}:${a1}`);
            return;
        }
        this.current = { s, a1, k };
        Object.assign(this.lex, data.lex || {});

        const info = this.index.surahs[s - 1];
        const range = data.a1 === data.a2
            ? `الآية ${data.a1}`
            : `الآيات ${data.a1} — ${data.a2}`;
        const part = data.of > 1 ? ` · ${data.k + 1}/${data.of}` : '';

        this.$('ruh-welcome').style.display = 'none';
        this.$('sec-header').style.display = '';
        this.$('sec-nav').style.display = 'flex';
        this.$('sec-surah').textContent = `سورة ${info.ar}`;
        this.$('sec-range').textContent = range + part;
        this.$('sec-info').textContent =
            `${info.en} ${data.a1}${data.a2 !== data.a1 ? '–' + data.a2 : ''}${part}`;
        this.$('surah-select').value = s;
        this.$('sec-select').style.display = '';
        this.renderSecSelect(s, a1);
        this.renderPages(data.pages);

        const list = info.sec;
        const i = list.findIndex(x => x.a1 === a1);
        const atStart = s === 1 && i <= 0 && k === 0;
        const atEnd = s === 114 && i >= list.length - 1 && k >= (list[i].c || 1) - 1;
        this.$('nav-prev').disabled = this.$('prev-sec').disabled = atStart;
        this.$('nav-next').disabled = this.$('next-sec').disabled = atEnd;

        window.scrollTo(0, 0);
        if (this.selectedRoot) this.highlightRoot(this.selectedRoot);
        this.prefetchNeighbours(s, a1, k);
    },

    prefetchNeighbours(s, a1, k) {
        const list = this.index.surahs[s - 1].sec;
        const i = list.findIndex(x => x.a1 === a1);
        const names = [];
        if (k + 1 < (list[i].c || 1)) names.push(`${s}-${a1}-${k + 1}`);
        else if (list[i + 1]) names.push(`${s}-${list[i + 1].a1}-0`);
        if (k > 0) names.push(`${s}-${a1}-${k - 1}`);
        names.forEach(n => this.fetchSection(n).catch(() => {}));
    },

    async loadFront() {
        const r = await fetch('data/front.json');
        const data = await r.json();
        this.current = null;
        this.$('ruh-welcome').style.display = 'none';
        this.$('sec-header').style.display = '';
        this.$('sec-nav').style.display = 'none';
        this.$('sec-surah').textContent = 'خطبة المفسر';
        this.$('sec-range').textContent = "The author's introduction";
        this.$('sec-info').textContent = 'Introduction';
        // No passage context here — the range selector has nothing to show
        this.$('sec-select').style.display = 'none';
        this.$('surah-select').value = '';
        this.$('prev-sec').disabled = true;
        this.$('next-sec').disabled = false;
        this.renderPages(data.pages);
        window.scrollTo(0, 0);
    },

    showError(msg) {
        this.$('ruh-welcome').style.display = 'none';
        this.$('sec-header').style.display = 'none';
        this.$('sec-nav').style.display = 'none';
        this.$('sec-body').innerHTML =
            `<p class="para" style="text-align:center;opacity:.7">${this.esc(msg)}</p>`;
    },

    // ── Rendering ──────────────────────────────────
    renderPages(pages) {
        const out = [];
        pages.forEach(pg => {
            if (pg.v != null) {
                out.push(`<div class="pg-mark">vol. ${pg.v} &middot; p. ${pg.p}</div>`);
            }
            pg.t.forEach(para => out.push(`<p class="para">${this.markup(para)}</p>`));
        });
        this.$('sec-body').innerHTML = out.join('');
    },

    /** Wrap every Arabic word in a span carrying its gloss and root. */
    markup(text) {
        const parts = text.split(this.RE_SPLIT);
        // First pass: which tokens are vocalised (i.e. quoted Qur'an)?
        const voc = parts.map((p, i) => i % 2 === 1 && this.RE_VOWEL.test(p));
        const out = [];
        for (let i = 0; i < parts.length; i++) {
            const p = parts[i];
            if (i % 2 === 0) { out.push(this.esc(p)); continue; }

            const entry = this.lex[this.norm(p)];
            let cls = 'w';
            // A lone vocalised word is usually just a pointed technical term;
            // a run of them is a Qur'anic quotation.
            if (voc[i] && (voc[i - 2] || voc[i + 2])) cls += ' qq';

            let attrs = '';
            if (entry) {
                const bar = entry.lastIndexOf('|');
                const gloss = bar >= 0 ? entry.slice(0, bar) : entry;
                const root = bar >= 0 ? entry.slice(bar + 1) : '';
                attrs = ` data-g="${this.esc(gloss)}"`;
                if (root) attrs += ` data-r="${this.esc(root)}"`;
            }
            out.push(`<span class="${cls}"${attrs}>${this.esc(p)}</span>`);
        }
        return out.join('');
    },

    esc(s) {
        return s.replace(/[&<>"]/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));
    },

    renderSecSelect(s, a1) {
        const list = this.index.surahs[s - 1].sec;
        this.$('sec-select').innerHTML = list.map(x =>
            `<option value="${x.a1}"${x.a1 === a1 ? ' selected' : ''}>` +
            `${x.a1 === x.a2 ? 'آية ' + x.a1 : 'آيات ' + x.a1 + '–' + x.a2}</option>`
        ).join('');
    },

    // ── Tooltip ────────────────────────────────────
    showTip(el) {
        const tip = this.$('tip');
        // The word's own meaning leads, in gold; what its root means follows.
        tip.querySelector('.tip-gloss').textContent = el.dataset.g;
        const rootEl = tip.querySelector('.tip-root');
        const root = el.dataset.r;
        if (root) {
            const sense = this.rootSenses[root];
            rootEl.innerHTML =
                `<span class="tr-letters">${this.esc(root.split('').join(' '))}</span>` +
                (sense ? `<span class="tr-sense">${this.esc(sense)}</span>` : '') +
                `<small>click for root</small>`;
            rootEl.hidden = false;
        } else {
            rootEl.hidden = true;
        }
        tip.hidden = false;

        const r = el.getBoundingClientRect();
        const t = tip.getBoundingClientRect();
        let left = r.left + r.width / 2 - t.width / 2;
        left = Math.max(8, Math.min(left, window.innerWidth - t.width - 8));
        let top = r.top - t.height - 8;
        if (top < 8) top = r.bottom + 8;
        tip.style.left = `${left}px`;
        tip.style.top = `${top}px`;
    },

    hideTip() { this.$('tip').hidden = true; },

    // ── Root panel ─────────────────────────────────
    async loadRootData() {
        if (this.rootsIndex) return;
        if (!this.rootDataLoading) {
            const d = this.QURAN_DATA;
            this.rootDataLoading = Promise.all([
                fetch(`${d}/roots_index.json`).then(r => r.json()),
                fetch(`${d}/mufradat.json`).then(r => r.json()),
                fetch(`${d}/families.json`).then(r => r.json()),
                fetch(`${d}/verses_text.json`).then(r => r.json()),
                fetch(`${d}/surah_list.json`).then(r => r.json()),
            ]).then(([ri, mu, fa, vt, sl]) => {
                this.rootsIndex = ri; this.mufradat = mu;
                this.families = fa; this.versesText = vt; this.surahList = sl;
            });
        }
        return this.rootDataLoading;
    },

    async onWordClick(root, el) {
        this.hideTip();
        this.selectedRoot = root;
        this.showAllVerses = false;
        document.querySelectorAll('.w-selected').forEach(x => x.classList.remove('w-selected'));
        el.classList.add('w-selected');
        this.highlightRoot(root);
        this.openPanelShell(root);
        await this.loadRootData();
        this.renderPanel(root);
    },

    highlightRoot(root) {
        document.querySelectorAll('.w-related').forEach(x => x.classList.remove('w-related'));
        document.querySelectorAll(`.w[data-r="${CSS.escape(root)}"]`).forEach(x => {
            if (!x.classList.contains('w-selected')) x.classList.add('w-related');
        });
    },

    openPanelShell(root) {
        this.$('root-display').textContent = root.split('').join(' ');
        this.$('root-buckwalter').textContent = '';
        this.$('root-gloss').textContent = '';
        this.$('root-meaning').textContent = 'جارٍ التحميل…';
        this.$('root-meaning').style.display = 'block';
        this.$('root-frequency').textContent = '';
        this.$('mufradat-section').style.display = 'none';
        this.$('root-family-section').style.display = 'none';
        this.$('connected-verses').innerHTML = '';
        this.$('show-more-btn').style.display = 'none';

        this.$('root-panel').classList.add('panel-visible');
        document.body.classList.add('panel-open');
        if (this.isMobile) {
            const b = this.$('panel-backdrop');
            b.style.display = 'block';
            requestAnimationFrame(() => b.classList.add('visible'));
            document.body.style.overflow = 'hidden';
        }
    },

    renderPanel(root) {
        const data = this.rootsIndex[root];
        if (!data) {
            this.$('root-meaning').textContent = 'لا توجد بيانات لهذا الجذر';
            return;
        }
        this.$('root-buckwalter').textContent = data.b || '';
        this.$('root-gloss').textContent = this.extractGloss(data.m || '');
        this.$('root-meaning').textContent = data.m || '';
        this.$('root-meaning').style.display = data.m ? 'block' : 'none';
        this.$('root-frequency').textContent = `Appears in ${data.f} verses across the Quran`;

        this.renderMufradat(root);
        this.renderFamilyInfo(root, data.fam ? (Array.isArray(data.fam) ? data.fam : [data.fam]) : []);
        this.renderConnectedVerses(root);

        ['mufradat-section', 'root-family-section'].forEach(id => {
            const sec = this.$(id);
            sec.classList.add('collapsed');
            const ch = sec.querySelector('.chevron');
            if (ch) ch.textContent = '▸';
        });
    },

    renderMufradat(root) {
        const section = this.$('mufradat-section');
        const entry = this.mufradat[root];
        if (!entry) { section.style.display = 'none'; return; }
        section.style.display = 'block';
        this.$('mufradat-root').textContent = entry.r !== root ? `${entry.r} (${root})` : entry.r;
        this.$('mufradat-text').textContent = entry.t;
    },

    renderFamilyInfo(root, familyIds) {
        const section = this.$('root-family-section');
        if (!familyIds.length) { section.style.display = 'none'; return; }
        section.style.display = 'block';
        const container = this.$('root-family-info');
        container.innerHTML = '';

        familyIds.forEach(fid => {
            const fam = this.families[fid];
            if (!fam) return;
            const div = document.createElement('div');
            div.style.marginBottom = '12px';

            const badge = document.createElement('span');
            badge.className = 'family-badge';
            badge.textContent = fam.name_ar;
            div.appendChild(badge);

            const meaning = document.createElement('div');
            meaning.className = 'family-meaning';
            meaning.textContent = fam.meaning;
            div.appendChild(meaning);

            const chips = document.createElement('div');
            chips.className = 'family-roots';
            fam.roots.forEach(r => {
                const chip = document.createElement('span');
                chip.className = 'family-root-chip';
                if (r === root) chip.classList.add('active');
                chip.textContent = r;
                chip.addEventListener('click', () => {
                    this.selectedRoot = r;
                    this.showAllVerses = false;
                    this.renderPanel(r);
                    this.$('root-display').textContent = r.split('').join(' ');
                    this.highlightRoot(r);
                });
                chips.appendChild(chip);
            });
            div.appendChild(chips);
            container.appendChild(div);
        });
    },

    renderConnectedVerses(root) {
        const data = this.rootsIndex[root];
        if (!data) return;
        const container = this.$('connected-verses');
        container.innerHTML = '';

        const verseKeys = data.v || [];
        const shown = verseKeys.slice(0, this.showAllVerses ? verseKeys.length : this.PREVIEW_LIMIT);

        const groups = {};
        shown.forEach(vk => {
            const n = parseInt(vk.split(':')[0]);
            (groups[n] = groups[n] || []).push(vk);
        });

        Object.keys(groups).sort((a, b) => a - b).forEach(n => {
            const info = this.surahList[n - 1];
            const group = document.createElement('div');
            group.className = 'connected-group';

            const header = document.createElement('div');
            header.className = 'connected-group-header';
            header.textContent = `${info.name_en} (${info.name_ar})`;
            group.appendChild(header);

            groups[n].forEach(vk => {
                const a = document.createElement('a');
                a.className = 'connected-verse';
                // Read the verse in the Quran app, where the full root
                // apparatus (furuq, co-occurrence, hadith) already lives.
                a.href = `../quran/index.html#${vk}`;

                const key = document.createElement('span');
                key.className = 'connected-verse-key';
                key.textContent = vk;

                const txt = document.createElement('div');
                txt.className = 'connected-verse-text';
                txt.textContent = this.versesText[vk] || '';

                a.appendChild(key);
                a.appendChild(txt);
                group.appendChild(a);
            });
            container.appendChild(group);
        });

        const titleEl = this.$('connected-header').querySelector('.section-title');
        if (titleEl) titleEl.textContent = `الآيات المتصلة — ${verseKeys.length} Connected Verses`;

        const btn = this.$('show-more-btn');
        if (verseKeys.length > this.PREVIEW_LIMIT && !this.showAllVerses) {
            btn.style.display = 'block';
            btn.textContent = `عرض الكل (${verseKeys.length} آية)`;
        } else {
            btn.style.display = 'none';
        }
    },

    closePanel() {
        const panel = this.$('root-panel');
        panel.classList.remove('panel-visible');
        panel.style.transform = '';
        panel.style.transition = '';
        document.body.classList.remove('panel-open');
        const b = this.$('panel-backdrop');
        b.classList.remove('visible');
        setTimeout(() => { b.style.display = 'none'; }, 300);
        document.body.style.overflow = '';
        document.querySelectorAll('.w-selected, .w-related')
            .forEach(x => x.classList.remove('w-selected', 'w-related'));
        this.selectedRoot = null;
    },

    extractGloss(meaning) {
        if (!meaning) return '';
        const m = meaning.replace(/\s+/g, ' ');
        const patterns = [
            /primarily means[:\s]+"([^"]+)"/i,
            /primarily means[:\s]+to ([^.,;]+)/i,
            /primarily means[:\s]+([^.]+)/i,
            /means[:\s]+"([^"]+)"/i,
            /means[:\s]+to ([^.,;]+)/i,
            /centers on ([^.]+)/i,
            /signifies ([^.]+)/i,
        ];
        for (const pat of patterns) {
            const match = m.match(pat);
            if (match) {
                let g = match[1].trim().replace(/["“”]/g, '');
                return g.length > 60 ? g.slice(0, 57) + '…' : g;
            }
        }
        const first = m.split(/[.!]/)[0];
        return first.length > 60 ? first.slice(0, 57) + '…' : first;
    },
};

document.addEventListener('DOMContentLoaded', () => Ruh.init());
