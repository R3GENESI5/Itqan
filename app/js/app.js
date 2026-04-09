/* ── Hadith App ─────────────────────────────────────────────────────────── */
const App = {

    // ── State ─────────────────────────────────────────────────────────────────
    books:            null,
    currentBookId:    null,
    currentBookMeta:  null,
    currentChapterIdx: 0,
    chapterCache:     {},   // "bukhari:3" → [...hadiths]
    chapterIndex:     {},   // "bukhari"   → [{name_ar, name_en, count, file}]
    gradeMap:         {},   // "bukhari:idInBook" → "Sahih…"
    wordDefs:         null, // norm → {r, g, s, n}
    connections:      null, // "bukhari:1" → [{id, t1, t2, t3, score}]
    narratorIndex:    null, // narrator name → {total, books, topics, grade_profile, ...}
    rootsLexicon:     null, // root_ar → {definition_en, summary_en, buckwalter}

    // Root filter (from ?root= URL param)
    rootFilter:   null,     // {root, ids: {bookId: Set(idInBook)}} or null
    quranRootsIndex: null,  // root → {b, m, f, v: [ayah refs], fam}

    // Settings
    showArabic:   true,
    showNarrator: true,
    darkMode:     false,
    textScale:    1,        // 1 | 1.15 | 1.3

    // Search
    searchDebounce:      null,
    searchIndex:         [],
    searchIndexLoaded:   false,
    searchIndexLoading:  false,

    // Active items
    activeHadith:    null,
    activeWord:      null,

    isMobile: false,

    $: id => document.getElementById(id),

    // ── Arabic helpers ────────────────────────────────────────────────────────
    DIACRITICS: /[\u0610-\u061A\u064B-\u065F\u0670\u06D6-\u06DC\u06DF-\u06E4\u06E7\u06E8\u06EA-\u06ED]/g,

    stripDiacritics(t)  { return t.replace(this.DIACRITICS, ''); },

    normalizeArabic(w) {
        w = this.stripDiacritics(w);
        return w.replace(/[أإآ]/g,'ا').replace(/ة/g,'ه').replace(/ى/g,'ي');
    },

    // ── Init ─────────────────────────────────────────────────────────────────
    async init() {
        this.checkMobile();
        window.addEventListener('resize', () => this.checkMobile());

        this.showArabic   = localStorage.getItem('hadith-arabic')   !== 'false';
        this.showNarrator = localStorage.getItem('hadith-narrator')  !== 'false';
        this.darkMode     = localStorage.getItem('hadith-dark')      === 'true';
        this.textScale    = parseFloat(localStorage.getItem('hadith-scale') || '1');

        this.applySettings();

        try {
            this.books = await fetch('data/books.json').then(r => r.json());
            this.renderSidebar();
            this.setupUI();

            // Root filter from ?root= param takes priority over hash
            const urlRoot = new URLSearchParams(location.search).get('root');
            if (urlRoot) {
                await this.applyRootFilter(urlRoot);
            } else {
                this.handleHash();
            }

            this.$('loading-overlay').classList.add('hidden');

            // Hash change listener for back/forward navigation
            window.addEventListener('hashchange', () => {
                if (!this.rootFilter) this.handleHash();
            });

            // Background loads
            this.loadWordDefs();
            this.loadConnections();
            this.buildSearchIndex();
        } catch (err) {
            console.error('Init failed:', err);
            document.querySelector('.loader-text').textContent = 'Failed to load. Please refresh.';
        }
    },

    checkMobile() {
        this.isMobile = window.matchMedia('(max-width: 640px)').matches;
    },

    async loadWordDefs() {
        // Try v2 (CAMeL) first, fall back to v1
        try {
            this.wordDefs = await fetch('data/word_defs_v2.json').then(r => { if (!r.ok) throw 0; return r.json(); });
            this.wordDefsVersion = 2;
        } catch {
            try {
                this.wordDefs = await fetch('data/word_defs.json').then(r => r.json());
                this.wordDefsVersion = 1;
            } catch { this.wordDefs = {}; this.wordDefsVersion = 0; }
        }
        // Hadiths were rendered before this loaded — re-render now so word spans get defs/highlights
        if (this._lastHadiths) this.renderHadiths(this._lastHadiths);
    },

    async loadConnections() {
        try {
            this.connections = await fetch('data/hadith_connections.json').then(r => r.json());
        } catch { this.connections = {}; }
    },

    async loadNarratorIndex() {
        if (this.narratorIndex) return;
        try {
            this.narratorIndex = await fetch('data/narrator_index.json').then(r => r.json());
        } catch { this.narratorIndex = {}; }
    },

    async loadRootsLexicon() {
        if (this.rootsLexicon) return;
        try {
            this.rootsLexicon = await fetch('data/roots_lexicon.json').then(r => r.json());
        } catch { this.rootsLexicon = {}; }
    },

    // ── Library Map ───────────────────────────────────────────────────────────
    async renderLibraryMap() {
        const groups = [
            { key: 'the_9_books', elId: 'lm-9books' },
            { key: 'forties',     elId: 'lm-forties' },
            { key: 'other_books', elId: 'lm-other' },
        ];

        // Pre-load all chapter indexes to get counts (sequential to avoid overwhelming dev server)
        const allBooks = [
            ...this.books.sunni.the_9_books,
            ...this.books.sunni.forties,
            ...this.books.sunni.other_books,
        ];
        for (const b of allBooks) await this.loadChapterIndex(b.id);

        // Find max hadith count for relative bar sizing
        const counts = allBooks.map(b => {
            const idx = this.chapterIndex[b.id] || [];
            return idx.reduce((s, ch) => s + (ch.count || 0), 0);
        });
        const maxCount = Math.max(...counts, 1);

        groups.forEach(({ key, elId }) => {
            const el = this.$(elId);
            if (!el) return;
            el.innerHTML = '';
            (this.books.sunni[key] || []).forEach(book => {
                const idx    = this.chapterIndex[book.id] || [];
                const total  = idx.reduce((s, ch) => s + (ch.count || 0), 0);
                const chapters = idx.length;
                const pct    = Math.round((total / maxCount) * 100);
                const diedLabel = book.died ? `d. ${book.died} AH` : '';

                const card = document.createElement('div');
                card.className = 'lm-card' + (this.currentBookId === book.id ? ' active' : '');
                card.dataset.bookId = book.id;
                card.innerHTML = `
                    <div class="lm-card-ar">${this.escHtml(book.name_ar)}</div>
                    <div class="lm-card-en">${this.escHtml(book.name_en)}</div>
                    <div class="lm-card-author">${this.escHtml(book.author)}${diedLabel ? ' · ' + diedLabel : ''}</div>
                    <div class="lm-bar"><div class="lm-bar-fill" style="width:0%"></div></div>
                    <div class="lm-card-stats">
                        <div class="lm-stat"><span class="lm-stat-n">${total.toLocaleString()}</span><span class="lm-stat-l">Hadiths</span></div>
                        <div class="lm-stat"><span class="lm-stat-n">${chapters}</span><span class="lm-stat-l">Chapters</span></div>
                        ${book.graded ? '<div class="lm-stat"><span class="lm-stat-n" style="color:var(--grade-sahih)">✓</span><span class="lm-stat-l">Graded</span></div>' : ''}
                    </div>
                `;
                card.addEventListener('click', () => {
                    this.hideMap();
                    this.selectBook(book.id).then(() => this.loadChapter(0));
                });
                card.dataset.pct = pct;
                el.appendChild(card);
            });
        });

        // Animate all bars after the DOM is fully painted
        setTimeout(() => {
            document.querySelectorAll('.lm-card[data-pct]').forEach(card => {
                const bar = card.querySelector('.lm-bar-fill');
                if (bar) bar.style.width = card.dataset.pct + '%';
            });
        }, 50);
    },

    showMap() {
        this._mapVisible = true;
        this.$('library-map').style.display = '';
        this.$('hadith-list').style.display  = 'none';
        this.$('welcome').style.display      = 'none';
        this.$('reader-header').style.display = 'none';
        this.$('map-toggle').classList.add('active');
        if (!this._mapRendered) {
            this._mapRendered = true;
            this.renderLibraryMap();
        } else {
            // Refresh active state
            document.querySelectorAll('.lm-card').forEach(c =>
                c.classList.toggle('active', c.dataset.bookId === this.currentBookId));
        }
    },

    hideMap() {
        this._mapVisible = false;
        this.$('library-map').style.display = 'none';
        this.$('map-toggle').classList.remove('active');
        // Restore hadith list or welcome
        if (this.currentBookId) {
            this.$('hadith-list').style.display = '';
            if (this.currentChapterIdx != null) this.$('reader-header').style.display = '';
        } else {
            this.$('welcome').style.display = '';
        }
    },

    // ── Sidebar ───────────────────────────────────────────────────────────────
    renderSidebar() {
        const groups = [
            { key: 'the_9_books', elId: 'books-9' },
            { key: 'forties',     elId: 'books-forties' },
            { key: 'other_books', elId: 'books-other' },
        ];
        groups.forEach(({ key, elId }) => {
            const ul = this.$(elId);
            ul.innerHTML = '';
            (this.books.sunni[key] || []).forEach(book => {
                const filterCount = this.rootFilter?.ids[book.id]?._total ?? null;
                // Dim books with no matches when filter is active
                const li = document.createElement('li');
                li.className = 'book-item' + (this.rootFilter && !filterCount ? ' rf-dimmed' : '');
                li.dataset.bookId = book.id;
                const countBadge = filterCount !== null
                    ? `<span class="book-filter-count">${filterCount}</span>`
                    : '';
                li.innerHTML = `
                    <div class="book-item-inner">
                        <span class="book-name-ar">${book.name_ar}</span>
                        <span class="book-name-en">${book.name_en}</span>
                    </div>
                    ${countBadge}
                    ${book.graded ? '<span class="book-grade-dot" title="Grading available"></span>' : ''}
                `;
                li.addEventListener('click', () => this.selectBook(book.id));
                ul.appendChild(li);
            });
        });
    },

    // ── Book selection ────────────────────────────────────────────────────────
    async selectBook(bookId) {
        document.querySelectorAll('.book-item').forEach(el =>
            el.classList.toggle('active', el.dataset.bookId === bookId));

        this.currentBookId   = bookId;
        this.currentBookMeta = this.findBook(bookId);

        await this.loadChapterIndex(bookId);
        this.renderChapterPanel();
        location.hash = bookId;

        if (this.isMobile) this.$('chapter-panel').classList.remove('hidden');
    },

    findBook(id) {
        return [
            ...this.books.sunni.the_9_books,
            ...this.books.sunni.forties,
            ...this.books.sunni.other_books,
        ].find(b => b.id === id) || null;
    },

    // ── Chapter index ─────────────────────────────────────────────────────────
    async loadChapterIndex(bookId) {
        if (this.chapterIndex[bookId]) return;
        try {
            this.chapterIndex[bookId] = await fetch(`data/sunni/${bookId}/index.json`).then(r => r.json());
            if (this.currentBookMeta?.graded) await this.loadGrades(bookId);
        } catch {
            console.warn('No chapter index for', bookId);
            this.chapterIndex[bookId] = [];
        }
    },

    async loadGrades(bookId) {
        if (this.gradeMap[bookId + '_loaded']) return;
        try {
            const data = await fetch(`data/sunni/${bookId}/grades.json`).then(r => r.json());
            Object.entries(data).forEach(([id, grade]) => {
                this.gradeMap[`${bookId}:${id}`] = grade;
            });
            this.gradeMap[bookId + '_loaded'] = true;
        } catch {}
    },

    // ── Chapter panel ─────────────────────────────────────────────────────────
    renderChapterPanel() {
        const meta     = this.currentBookMeta;
        const chapters = this.chapterIndex[this.currentBookId] || [];
        const cp       = this.$('chapter-panel');

        this.$('chapter-book-title').textContent = meta?.name_ar || meta?.name_en || '';
        cp.classList.remove('hidden');

        const ul = this.$('chapter-list');
        ul.innerHTML = '';

        if (chapters.length === 0) {
            ul.innerHTML = '<li style="padding:16px;color:var(--text-muted);font-size:.82rem">No chapters found.<br>Run the data pipeline first.</li>';
            return;
        }

        chapters.forEach((ch, idx) => {
            const li = document.createElement('li');
            li.className = 'chapter-item';
            li.dataset.idx = idx;
            li.innerHTML = `
                ${ch.count ? `<span class="ci-count">${ch.count}</span>` : ''}
                <span class="ci-ar">${ch.name_ar || ''}</span>
                <span class="ci-en">${ch.name_en || `Chapter ${idx + 1}`}</span>
            `;
            li.addEventListener('click', () => this.loadChapter(idx));
            ul.appendChild(li);
        });

        this.populateChapterSelect(chapters);
    },

    populateChapterSelect(chapters) {
        const sel = this.$('chapter-select');
        sel.innerHTML = '';
        chapters.forEach((ch, idx) => {
            const opt = document.createElement('option');
            opt.value = idx;
            opt.textContent = ch.name_en || `Chapter ${idx + 1}`;
            sel.appendChild(opt);
        });
    },

    // ── Load chapter ──────────────────────────────────────────────────────────
    async loadChapter(chapterIdx) {
        const bookId   = this.currentBookId;
        const chapters = this.chapterIndex[bookId] || [];
        if (!chapters[chapterIdx]) return;

        const ch = chapters[chapterIdx];
        this.currentChapterIdx = chapterIdx;

        document.querySelectorAll('.chapter-item').forEach(el =>
            el.classList.toggle('active', parseInt(el.dataset.idx) === chapterIdx));

        document.querySelector('.chapter-item.active')?.scrollIntoView({ block: 'nearest' });

        this.$('chapter-select').value = chapterIdx;

        const meta = this.currentBookMeta;
        this.$('rh-book-ar').textContent    = meta?.name_ar || '';
        this.$('rh-book-en').textContent    = meta?.name_en || '';
        this.$('rh-chapter-ar').textContent = ch.name_ar || '';
        this.$('rh-chapter-en').textContent = ch.name_en || '';
        this.$('reader-header').style.display = 'flex';

        this.$('prev-chapter').disabled = chapterIdx === 0;
        this.$('next-chapter').disabled = chapterIdx === chapters.length - 1;

        this.$('welcome').style.display = 'none';

        if (this.isMobile) this.$('chapter-panel').classList.add('hidden');

        const cacheKey = `${bookId}:${chapterIdx}`;
        if (!this.chapterCache[cacheKey]) {
            this.$('hadith-list').innerHTML = '<div style="padding:20px;color:var(--text-muted);font-size:.82rem;text-align:center">Loading…</div>';
            try {
                const file = ch.file || `${chapterIdx + 1}.json`;
                this.chapterCache[cacheKey] = await fetch(`data/sunni/${bookId}/${file}`).then(r => r.json());
            } catch {
                this.$('hadith-list').innerHTML = '<div style="padding:20px;color:var(--text-muted);font-size:.82rem;text-align:center">Data not available.<br>Run <code>src/download_data.py</code> first.</div>';
                return;
            }
        }

        this.renderHadiths(this.chapterCache[cacheKey]);
        location.hash = `${bookId}/${chapterIdx}`;
    },

    // ── Render hadiths ────────────────────────────────────────────────────────
    renderHadiths(hadiths) {
        this._lastHadiths = hadiths;   // saved so wordDefs re-render can replay
        const list = this.$('hadith-list');
        list.style.display = '';
        list.innerHTML = '';

        // Apply root filter if active — match by chapter + idInBook
        if (this.rootFilter && hadiths?.length) {
            const bookFilter = this.rootFilter.ids[this.currentBookId];
            const chapterSet = bookFilter?.[this.currentChapterIdx];
            if (chapterSet) {
                hadiths = hadiths.filter(h => chapterSet.has(h.idInBook));
            } else if (bookFilter) {
                hadiths = [];  // book has matches but not this chapter
            } else {
                hadiths = [];  // book has no matches at all
            }
        }

        if (!hadiths?.length) {
            const msg = this.rootFilter
                ? 'No connected hadiths in this chapter.'
                : 'No hadiths in this chapter.';
            list.innerHTML = `<div style="padding:24px;color:var(--text-muted);text-align:center">${msg}</div>`;
            return;
        }

        const frag = document.createDocumentFragment();
        hadiths.forEach(h => {
            const grade = this.gradeMap[`${this.currentBookId}:${h.idInBook}`] || h.grade || null;
            const card  = document.createElement('div');
            card.className = 'hadith-card';
            card.dataset.id = h.idInBook;

            const hasIsnad   = !!(h.english?.narrator);
            const arabicHtml = this.showArabic && h.arabic
                ? `<div class="hc-arabic" dir="rtl">${this.renderArabicWords(h.arabic, hasIsnad)}</div>`
                : '';
            const narratorHtml = this.showNarrator && h.english?.narrator
                ? `<div class="hc-narrator">${this.renderNarratorHtml(h.english.narrator)}</div>`
                : '';

            card.innerHTML = `
                <div class="hc-meta">
                    <span class="hc-num" data-hadith-id="${h.idInBook}">#${h.idInBook}</span>
                    ${grade ? `<span class="grade-badge ${this.gradeClass(grade)}">${this.gradeLabel(grade)}</span>` : ''}
                </div>
                ${arabicHtml}
                ${narratorHtml}
                <div class="hc-english">${this.escHtml(h.english?.text || '')}</div>
            `;

            // Hadith number click → detail panel
            card.querySelector('.hc-num').addEventListener('click', (e) => {
                e.stopPropagation();
                this.openHadithPanel(h, grade);
            });

            // Card click → detail panel
            card.addEventListener('click', () => this.openHadithPanel(h, grade));

            // Word clicks (delegated)
            card.querySelector('.hc-arabic')?.addEventListener('click', (e) => {
                const span = e.target.closest('.arabic-word.has-def');
                if (span) {
                    e.stopPropagation();
                    this.openWordPanel(span.dataset.word, span.dataset.def);
                }
            });

            // Narrator link click
            card.querySelector('.narrator-link')?.addEventListener('click', (e) => {
                e.stopPropagation();
                this.openRawiPanel(e.target.dataset.name);
            });

            frag.appendChild(card);
        });

        list.appendChild(frag);
        list.scrollTop = 0;
    },

    // ── Arabic word rendering ─────────────────────────────────────────────────
    renderArabicWords(arabic, hasIsnad = false) {
        if (!arabic) return '';
        const defs = this.wordDefs;
        const activeRoot = this.rootFilter ? this.normalizeArabic(this.rootFilter.root) : null;

        const tokens = arabic.split(/(\s+)/);
        return tokens.map(token => {
            if (!token.trim()) return token;

            // Arabic letters only (U+0621–U+063A, U+0641–U+064A)
            const arabicOnly = token.replace(/[^\u0621-\u063A\u0641-\u064A]/g, '');
            if (!arabicOnly) return this.escHtml(token);

            if (defs) {
                const norm = this.normalizeArabic(arabicOnly);
                const def  = defs[norm];
                if (def) {
                    // word_defs.json: g=gloss, r=root_ar, s=buckwalter, n=freq
                    const gloss = (def.g || def.s || '').slice(0, 80).replace(/"/g, '&quot;');
                    const safeToken = this.escHtml(token);
                    const safeNorm  = this.escHtml(norm);
                    const isRootMatch = activeRoot && def.r
                        && this.normalizeArabic(def.r) === activeRoot;
                    const cls = 'arabic-word has-def' + (isRootMatch ? ' root-hl' : '');
                    return `<span class="${cls}" data-word="${safeNorm}" data-def="${gloss}">${safeToken}</span>`;
                }
            }
            return `<span class="arabic-word">${this.escHtml(token)}</span>`;
        }).join('');
    },

    // ── Word panel ────────────────────────────────────────────────────────────
    async openWordPanel(normWord, gloss) {
        const def = this.wordDefs?.[normWord];
        if (!def) return;

        this.activeWord = normWord;

        // ── Header: show the clicked word + root chip ──────────────────────
        this.$('wp-word-ar').textContent = normWord;
        this.$('wp-root-chip').innerHTML = def.r
            ? `<span class="chip-label">root</span>${this.escHtml(def.r)}` + (def.s ? ` <span class="chip-label">${this.escHtml(def.s)}</span>` : '')
            : '';

        // ── ① المعنى — Meaning ────────────────────────────────────────────
        this.$('wp-gloss').textContent = def.g || '';

        // Morphology chips (v2 only)
        const chips = [];
        if (def.pos)   chips.push(`<span class="morph-chip">${def.pos}</span>`);
        if (def.form)  chips.push(`<span class="morph-chip">Form ${def.form}</span>`);
        if (def.voice) chips.push(`<span class="morph-chip">${def.voice}</span>`);
        if (def.asp)   chips.push(`<span class="morph-chip">${def.asp}</span>`);
        if (def.lem)   chips.push(`<span class="morph-chip morph-lem">lemma: ${def.lem}</span>`);
        this.$('wp-morph').innerHTML = chips.length ? `<div class="wp-morph">${chips.join('')}</div>` : '';

        this.$('wp-freq').textContent = def.n
            ? `Appears ~${Number(def.n).toLocaleString()} times in corpus`
            : '';

        // Lane's full definition — lazy
        this.$('wp-definition').textContent = '';
        this.loadRootsLexicon().then(() => {
            if (this.activeWord !== normWord) return; // user already pivoted
            const entry = this.rootsLexicon?.[def.r];
            this.$('wp-definition').textContent = entry?.definition_en || entry?.summary_en || '';
        });

        // Ensure المعنى section is open
        this.$('wp-meaning-section').classList.remove('collapsed');
        this.$('wp-meaning-toggle').querySelector('.chevron').textContent = '▾';

        // ── ② الأسرة — Root Family ────────────────────────────────────────
        this.renderRootFamily(normWord, def.r);

        // ── ③ المصاحبة — reset, mark uncomputed ──────────────────────────
        const cooccurSection = this.$('wp-cooccur-section');
        cooccurSection.classList.add('collapsed');
        cooccurSection.dataset.computed = '';
        this.$('wp-cooccur-toggle').querySelector('.chevron').textContent = '▸';
        this.$('wp-cooccur-list').innerHTML = '';

        // ── ④ الأحاديث — concordance list ────────────────────────────────
        this.renderWordHadiths(normWord);

        // Open panel
        this.closePanels();
        this.$('word-panel').classList.add('panel-open');
        this.$('panel-backdrop').classList.add('visible');
    },

    // ── Root Family ────────────────────────────────────────────────────────
    buildRootFamilyIndex() {
        if (this._rootFamilies || !this.wordDefs) return;
        this._rootFamilies = {};
        for (const [word, def] of Object.entries(this.wordDefs)) {
            const r = def.r;
            if (!r) continue;
            if (!this._rootFamilies[r]) this._rootFamilies[r] = [];
            this._rootFamilies[r].push({ word, freq: def.n || 0, gloss: def.g || '' });
        }
        for (const fam of Object.values(this._rootFamilies)) {
            fam.sort((a, b) => b.freq - a.freq);
        }
    },

    renderRootFamily(normWord, root) {
        this.buildRootFamilyIndex();
        const family  = this._rootFamilies?.[root] || [];
        const section = this.$('wp-family-section');
        const countEl = this.$('wp-family-count');

        if (family.length <= 1) { section.style.display = 'none'; return; }
        section.style.display = '';
        section.classList.remove('collapsed');
        this.$('wp-family-toggle').querySelector('.chevron').textContent = '▾';
        if (countEl) countEl.textContent = `${family.length} forms`;

        this.$('wp-family-list').innerHTML = family.map(({ word, freq, gloss }) => {
            const cur = word === normWord;
            return `<div class="wp-family-word${cur ? ' current' : ''}" data-word="${this.escHtml(word)}" title="${this.escHtml(gloss)}">
                <span class="wpf-ar">${this.escHtml(word)}</span>
                <span class="wpf-freq">${freq > 0 ? '×' + Number(freq).toLocaleString() : ''}</span>
            </div>`;
        }).join('');

        this.$('wp-family-list').querySelectorAll('.wp-family-word:not(.current)').forEach(el => {
            el.addEventListener('click', () => {
                const w = el.dataset.word;
                const d = this.wordDefs?.[w];
                if (d) this.openWordPanel(w, d.g || '');
            });
        });
    },

    // ── Co-occurrence (computed lazily when section expanded) ──────────────
    async computeCoOccurrence(normWord) {
        const myIds = await this.fetchConcordance(normWord);
        if (!myIds.length || !this._concordance) return [];

        // Cap to 200 to keep iteration fast
        const mySet = new Set(myIds.slice(0, 200));
        const scores = {};
        for (const [word, ids] of Object.entries(this._concordance)) {
            if (word === normWord) continue;
            if (ids.length > 400 || ids.length < 2) continue; // skip ultra-common and hapax
            let overlap = 0;
            for (const id of ids) {
                if (mySet.has(id)) { overlap++; if (overlap >= 2) break; }
            }
            // Count fully
            if (overlap >= 2) {
                overlap = 0;
                for (const id of ids) if (mySet.has(id)) overlap++;
                scores[word] = overlap;
            }
        }
        return Object.entries(scores)
            .sort((a, b) => b[1] - a[1])
            .slice(0, 18)
            .map(([word, count]) => ({ word, count, gloss: this.wordDefs?.[word]?.g || '' }));
    },

    async renderCoOccurrence(normWord) {
        const list = this.$('wp-cooccur-list');
        list.innerHTML = '<div class="whi-loading">Computing…</div>';
        const results = await this.computeCoOccurrence(normWord);
        if (!results.length) {
            list.innerHTML = '<div style="color:var(--text-muted);font-size:.78rem;padding:6px 0">Not enough data.</div>';
            return;
        }
        const maxCount = results[0].count;
        list.innerHTML = results.map(({ word, count, gloss }) =>
            `<span class="wp-cooccur-tag" data-word="${this.escHtml(word)}" title="${this.escHtml(gloss)}">
                <span class="wpc-ar">${this.escHtml(word)}</span>
                <span class="wpc-count" style="opacity:${.4 + .6*(count/maxCount)}">${count}</span>
            </span>`
        ).join('');
        list.querySelectorAll('.wp-cooccur-tag').forEach(el => {
            el.addEventListener('click', () => {
                const w = el.dataset.word;
                const d = this.wordDefs?.[w];
                if (d) this.openWordPanel(w, d.g || '');
            });
        });
    },

    async fetchConcordance(normWord) {
        // Lazy-fetch just the entry for this word from concordance.json
        // We can't load 9MB upfront, so fetch the whole file once and cache it
        if (!this._concordance) {
            if (this._concordanceLoading) {
                // Wait for in-progress load
                await new Promise(r => { this._concordanceWaiters = this._concordanceWaiters || []; this._concordanceWaiters.push(r); });
            } else {
                this._concordanceLoading = true;
                try {
                    this._concordance = await fetch('data/concordance.json').then(r => r.json());
                } catch { this._concordance = {}; }
                this._concordanceLoading = false;
                (this._concordanceWaiters || []).forEach(r => r());
                this._concordanceWaiters = [];
            }
        }
        return this._concordance[normWord] || [];
    },

    // ── Fetch a single hadith by bookId + idInBook (reusable helper) ─────────
    async fetchHadithById(bookId, idInBook) {
        // 1. Try chapter cache first
        for (const [key, hadiths] of Object.entries(this.chapterCache)) {
            if (!key.startsWith(bookId + ':')) continue;
            const h = hadiths.find(x => String(x.idInBook) === String(idInBook));
            if (h) return h;
        }
        // 2. Load chapter index if needed
        try {
            if (!this.chapterIndex[bookId]) {
                this.chapterIndex[bookId] = await fetch(`data/sunni/${bookId}/index.json`).then(r => r.json());
            }
            const chapters = this.chapterIndex[bookId];
            let cum = 0, chIdx = 0;
            for (let i = 0; i < chapters.length; i++) {
                cum += (chapters[i].count || 0);
                if (parseInt(idInBook) <= cum) { chIdx = i; break; }
                if (i === chapters.length - 1) chIdx = i;
            }
            const cacheKey = `${bookId}:${chIdx}`;
            if (!this.chapterCache[cacheKey]) {
                const ch = chapters[chIdx];
                this.chapterCache[cacheKey] = await fetch(`data/sunni/${bookId}/${ch.file || `${chIdx+1}.json`}`).then(r => r.json());
            }
            return this.chapterCache[cacheKey]?.find(x => String(x.idInBook) === String(idInBook)) || null;
        } catch { return null; }
    },

    // ── Highlight one word in Arabic text (no tooltips — safe in any container) ─
    highlightWordInArabic(arabic, normWord) {
        if (!arabic || !normWord) return this.escHtml(arabic || '');
        return arabic.split(/(\s+)/).map(token => {
            if (!token.trim()) return this.escHtml(token);
            const arabicOnly = token.replace(/[^\u0621-\u063A\u0641-\u064A]/g, '');
            if (!arabicOnly) return this.escHtml(token);
            if (this.normalizeArabic(arabicOnly) === normWord) {
                return `<mark class="word-hit">${this.escHtml(token)}</mark>`;
            }
            return this.escHtml(token);
        }).join('');
    },

    async renderWordHadiths(normWord) {
        const section = this.$('wp-hadiths-section');
        const list    = this.$('wp-hadiths-list');
        const toggle  = this.$('wp-hadiths-toggle');

        section.style.display = '';
        list.innerHTML = '<div class="whi-loading">Loading concordance…</div>';

        const hadithIds = await this.fetchConcordance(normWord);

        if (!hadithIds.length) {
            section.style.display = 'none';
            return;
        }

        const total   = hadithIds.length;
        const toShow  = hadithIds.slice(0, 30);
        const countEl = this.$('wp-hadiths-count');
        if (countEl) countEl.textContent = total.toLocaleString();

        // Render skeleton placeholders immediately so panel opens fast
        list.innerHTML = toShow.map(hid => {
            const [bookId, idInBook] = hid.split(':');
            const bookName = this.findBook(bookId)?.name_en || bookId;
            return `<div class="wp-hadith-item" data-book="${bookId}" data-id="${idInBook}">
                <div class="whi-meta">${this.escHtml(bookName)} #${idInBook}</div>
                <div class="whi-arabic" dir="rtl"></div>
                <div class="whi-text whi-loading">Loading…</div>
            </div>`;
        }).join('');

        if (total > 30) {
            list.insertAdjacentHTML('beforeend',
                `<div class="whi-more">Showing 30 of ${total.toLocaleString()} — click any to open full hadith</div>`);
        }

        // Attach click handlers now (before fetch — user might click immediately)
        list.querySelectorAll('.wp-hadith-item[data-book]').forEach(el => {
            el.addEventListener('click', () => this.openHadithFromConcordance(el.dataset.book, el.dataset.id));
        });

        // Fetch each hadith and fill in text
        for (const hid of toShow) {
            const [bookId, idInBook] = hid.split(':');
            const el = list.querySelector(`.wp-hadith-item[data-book="${bookId}"][data-id="${idInBook}"]`);
            if (!el) continue;

            const h = await this.fetchHadithById(bookId, idInBook);
            if (!h) {
                el.querySelector('.whi-text').textContent = '';
                el.querySelector('.whi-text').classList.remove('whi-loading');
                continue;
            }

            // Arabic: show first ~120 chars with the searched word highlighted
            const arabicFull = h.arabic || '';
            const shortAr = arabicFull.length > 140
                ? arabicFull.slice(0, 140) + '…'
                : arabicFull;
            el.querySelector('.whi-arabic').innerHTML = this.highlightWordInArabic(shortAr, normWord);

            // English: brief snippet
            const eng = (h.english?.text || '').slice(0, 100);
            const textEl = el.querySelector('.whi-text');
            textEl.textContent = eng + (eng.length === 100 ? '…' : '');
            textEl.classList.remove('whi-loading');
        }
    },

    // ── Hadith detail panel ───────────────────────────────────────────────────
    openHadithPanel(h, grade) {
        this.activeHadith = h;
        const meta     = this.currentBookMeta;
        const chapters = this.chapterIndex[this.currentBookId] || [];
        const ch       = chapters[this.currentChapterIdx];
        const hadithId = `${this.currentBookId}:${h.idInBook}`;

        this.$('hp-ref').textContent = [meta?.name_en, ch?.name_en, `#${h.idInBook}`].filter(Boolean).join(' — ');

        // Update URL for deep linking
        history.replaceState(null, '', `#${this.currentBookId}/${this.currentChapterIdx}/${h.idInBook}`);

        this.$('hp-grade').innerHTML = grade
            ? `<span class="grade-badge ${this.gradeClass(grade)}">${grade}</span>`
            : '';

        // Render Arabic with word spans in the panel too
        const arabicEl = this.$('hp-arabic');
        arabicEl.innerHTML = this.renderArabicWords(h.arabic || '', !!(h.english?.narrator));
        arabicEl.addEventListener('click', (e) => {
            const span = e.target.closest('.arabic-word.has-def');
            if (span) this.openWordPanel(span.dataset.word, span.dataset.def);
        });

        this.$('hp-narrator').innerHTML = h.english?.narrator
            ? this.renderNarratorHtml(h.english.narrator) : '';
        this.$('hp-narrator').style.display = h.english?.narrator ? '' : 'none';
        // Re-attach narrator click in panel
        this.$('hp-narrator').querySelector('.narrator-link')?.addEventListener('click', (e) => {
            this.openRawiPanel(e.target.dataset.name);
        });
        this.$('hp-english').textContent = h.english?.text || '';

        // Connections — new format is [{id, t1, t2, t3, score}] or old format ["book:id"]
        const connSection = this.$('hp-connections-section');
        const connList    = this.$('hp-connections-list');
        const rawConns    = this.connections?.[hadithId] || [];
        // Normalise: old string format → new object format
        const connObjs = rawConns.map(c =>
            typeof c === 'string' ? { id: c, t1: [], t2: [], t3: [] } : c
        );

        if (connObjs.length) {
            connSection.style.display = '';
            connList.innerHTML = '';
            this.renderConnections(connObjs, connList);
        } else {
            connSection.style.display = 'none';
        }

        this.closePanels();
        this.$('hadith-panel').classList.add('panel-open');
        this.$('panel-backdrop').classList.add('visible');
    },

    renderConnections(connObjs, container) {
        container.innerHTML = '<div style="color:var(--text-muted);font-size:.78rem;padding:8px 0">Loading…</div>';

        const promises = connObjs.slice(0, 8).map(async (conn) => {
            const cid = conn.id;
            const [bookId, idInBook] = cid.split(':');

            // 1. Check cache first
            for (const [key, hadiths] of Object.entries(this.chapterCache)) {
                if (!key.startsWith(bookId + ':')) continue;
                const h = hadiths.find(x => String(x.idInBook) === idInBook);
                if (h) return { bookId, idInBook, h };
            }

            // 2. Load book index if needed, find the right chapter, fetch it
            try {
                if (!this.chapterIndex[bookId]) {
                    const idx = await fetch(`data/sunni/${bookId}/index.json`).then(r => r.json());
                    this.chapterIndex[bookId] = idx;
                }
                const chapters = this.chapterIndex[bookId];
                // Find the chapter that contains this hadith id
                // chapters have cumulative counts — use binary search by hadith id range
                let targetChIdx = 0;
                let cumulative = 0;
                for (let i = 0; i < chapters.length; i++) {
                    cumulative += (chapters[i].count || 0);
                    if (parseInt(idInBook) <= cumulative) { targetChIdx = i; break; }
                    if (i === chapters.length - 1) targetChIdx = i;
                }
                const ch = chapters[targetChIdx];
                const file = ch.file || `${targetChIdx + 1}.json`;
                const cacheKey = `${bookId}:${targetChIdx}`;
                if (!this.chapterCache[cacheKey]) {
                    const data = await fetch(`data/sunni/${bookId}/${file}`).then(r => r.json());
                    this.chapterCache[cacheKey] = data;
                }
                const h = this.chapterCache[cacheKey].find(x => String(x.idInBook) === idInBook);
                if (h) return { bookId, idInBook, h };
            } catch {}

            return { bookId, idInBook, h: null, conn };
        });

        Promise.all(promises).then(items => {
            container.innerHTML = items.map(({ bookId, idInBook, h, conn }) => {
                const meta   = this.findBook(bookId);
                const tiers  = this.renderTierBadges(conn);
                if (!h) {
                    return `<div class="conn-item">
                        <div class="conn-meta">${this.escHtml(meta?.name_en || bookId)} #${idInBook}</div>
                        ${tiers}
                        <div class="conn-text" style="color:var(--text-muted);font-size:.75rem;font-style:italic">Not cached — open book to view</div>
                    </div>`;
                }
                return `<div class="conn-item" data-book="${bookId}" data-id="${idInBook}">
                    <div class="conn-meta">${this.escHtml(meta?.name_en || bookId)} #${idInBook}</div>
                    ${tiers}
                    <div class="conn-ar" dir="rtl">${this.escHtml((h.arabic || '').slice(0, 100))}…</div>
                    <div class="conn-text">${this.escHtml((h.english?.text || '').slice(0, 140))}…</div>
                </div>`;
            }).join('');
        });
    },

    renderTierBadges(conn) {
        if (!conn) return '';
        const badges = [];
        if (conn.t1?.length) badges.push(`<span class="conn-tier t1">Matn: ${conn.t1.slice(0,2).join(', ')}</span>`);
        if (conn.t2?.length) badges.push(`<span class="conn-tier t2">${conn.t2.join(', ')}</span>`);
        if (conn.t3?.length) badges.push(`<span class="conn-tier t3">${conn.t3.join(', ')}</span>`);
        return badges.length ? `<div class="conn-tiers">${badges.join('')}</div>` : '';
    },

    // ── Open hadith from concordance without losing word panel ───────────────
    async openHadithFromConcordance(bookId, idInBook) {
        const h = await this.fetchHadithById(bookId, idInBook);

        if (!h) {
            // Fallback: navigate to book
            this.closePanels();
            this.selectBook(bookId);
            return;
        }

        // Get grade
        await this.loadChapterIndex(bookId);
        if (this.findBook(bookId)?.graded) await this.loadGrades(bookId);
        const grade = this.gradeMap[`${bookId}:${idInBook}`] || h.grade || null;

        // Temporarily set context so openHadithPanel works correctly
        const prevBookId  = this.currentBookId;
        const prevBookMeta = this.currentBookMeta;
        const prevChIdx   = this.currentChapterIdx;
        this.currentBookId   = bookId;
        this.currentBookMeta = this.findBook(bookId);

        // Find the chapter for display
        const chapters = this.chapterIndex[bookId] || [];
        let chIdx = 0;
        let cumulative = 0;
        for (let i = 0; i < chapters.length; i++) {
            cumulative += (chapters[i].count || 0);
            if (parseInt(idInBook) <= cumulative) { chIdx = i; break; }
        }
        this.currentChapterIdx = chIdx;

        // Open hadith panel — but add a back button to return to word panel
        this.$('word-panel').classList.remove('panel-open'); // slide word panel away first
        this.openHadithPanel(h, grade);

        // Add "← Back to word" button in hadith panel header
        let backBtn = this.$('hp-back-btn');
        if (!backBtn) {
            backBtn = document.createElement('button');
            backBtn.id = 'hp-back-btn';
            backBtn.className = 'hp-back-btn';
            backBtn.textContent = '← Word';
            this.$('hp-ref').parentNode.insertBefore(backBtn, this.$('hp-ref'));
        }
        backBtn.style.display = 'inline-flex';
        backBtn.onclick = () => {
            // Restore previous context, go back to word panel
            this.currentBookId   = prevBookId;
            this.currentBookMeta = prevBookMeta;
            this.currentChapterIdx = prevChIdx;
            this.$('hadith-panel').classList.remove('panel-open');
            this.$('word-panel').classList.add('panel-open');
            backBtn.style.display = 'none';
        };
    },

    closePanels() {
        this.$('word-panel').classList.remove('panel-open');
        this.$('hadith-panel').classList.remove('panel-open');
        this.$('rawi-panel').classList.remove('panel-open');
        this.$('panel-backdrop').classList.remove('visible');
    },

    // ── Narrator rendering ────────────────────────────────────────────────────
    renderNarratorHtml(narratorStr) {
        if (!narratorStr) return '';
        // Extract name part: "Narrated Abu Hurairah:" → make "Abu Hurairah" a link
        const m = narratorStr.match(/^(Narrated\s+)(.+?)(:.*)?$/);
        if (m) {
            const name = m[2].trim();
            return `${this.escHtml(m[1])}<span class="narrator-link" data-name="${this.escHtml(name)}">${this.escHtml(name)}</span>${this.escHtml(m[3] || '')}`;
        }
        // Fallback: wrap whole string
        const name = narratorStr.split(':')[0].trim();
        return `<span class="narrator-link" data-name="${this.escHtml(name)}">${this.escHtml(narratorStr)}</span>`;
    },

    // ── Rawi panel ────────────────────────────────────────────────────────────
    async openRawiPanel(narratorName) {
        await this.loadNarratorIndex();
        const data = this.narratorIndex?.[narratorName];

        this.$('rp-name').textContent = narratorName;

        if (!data) {
            this.$('rp-stats').innerHTML = `<div style="color:var(--text-muted);font-size:.78rem">No profile data for this narrator.</div>`;
            this.$('rp-topics-section').style.display  = 'none';
            this.$('rp-grades-section').style.display  = 'none';
            this.$('rp-books-section').style.display   = 'none';
            this.$('rp-hadiths-section').style.display = 'none';
            this.closePanels();
            this.$('rawi-panel').classList.add('panel-open');
            this.$('panel-backdrop').classList.add('visible');
            return;
        }

        // Stats row
        const topBook  = Object.entries(data.books || {}).sort((a,b) => b[1]-a[1])[0];
        const topBookMeta = topBook ? this.findBook(topBook[0]) : null;
        this.$('rp-stats').innerHTML = `
            <div class="rp-stat"><span class="rp-stat-n">${data.total?.toLocaleString()}</span><span class="rp-stat-l">Hadiths</span></div>
            <div class="rp-stat"><span class="rp-stat-n">${Object.keys(data.books || {}).length}</span><span class="rp-stat-l">Books</span></div>
            <div class="rp-stat"><span class="rp-stat-n">${data.unique_count || 0}</span><span class="rp-stat-l">Unique</span></div>
        `;

        // Topic fingerprint
        const topics = data.topics || {};
        const topicEntries = Object.entries(topics).sort((a,b) => b[1]-a[1]);
        if (topicEntries.length) {
            this.$('rp-topics-section').style.display = '';
            const maxPct = topicEntries[0][1];
            this.$('rp-topics').innerHTML = topicEntries.map(([topic, pct]) => `
                <div class="rp-topic-row">
                    <span class="rp-topic-label">${this.escHtml(topic)}</span>
                    <div class="rp-topic-bar-wrap">
                        <div class="rp-topic-bar" style="width:${Math.round(pct/maxPct*100)}%"></div>
                    </div>
                    <span class="rp-topic-pct">${pct}%</span>
                </div>
            `).join('');
        } else {
            this.$('rp-topics-section').style.display = 'none';
        }

        // Grade profile
        const gradeProfile = data.grade_profile || {};
        const gradeEntries = Object.entries(gradeProfile);
        if (gradeEntries.length) {
            this.$('rp-grades-section').style.display = '';
            this.$('rp-grades').innerHTML = gradeEntries.map(([g, pct]) => `
                <div class="rp-grade-pill ${g}">
                    <span class="rp-grade-pct">${pct}%</span>
                    <span>${g.charAt(0).toUpperCase() + g.slice(1)}</span>
                </div>
            `).join('');
        } else {
            this.$('rp-grades-section').style.display = 'none';
        }

        // Books distribution
        const books = data.books || {};
        const bookEntries = Object.entries(books).sort((a,b) => b[1]-a[1]);
        if (bookEntries.length) {
            this.$('rp-books-section').style.display = '';
            this.$('rp-books').innerHTML = bookEntries.map(([bookId, count]) => {
                const meta = this.findBook(bookId);
                return `<div class="rp-book-row">
                    <span class="rp-book-name">${this.escHtml(meta?.name_en || bookId)}</span>
                    <span class="rp-book-count">${count}</span>
                </div>`;
            }).join('');
        } else {
            this.$('rp-books-section').style.display = 'none';
        }

        // Hadith list (sample)
        const hadithIds = data.hadith_ids || [];
        if (hadithIds.length) {
            this.$('rp-hadiths-section').style.display = '';
            this.$('rp-hadiths-list').innerHTML = hadithIds.slice(0, 30).map(hid => {
                const [bookId, idInBook] = hid.split(':');
                const meta = this.findBook(bookId);
                // Try to get text from cache
                let text = '';
                for (const [key, hadiths] of Object.entries(this.chapterCache)) {
                    if (!key.startsWith(bookId + ':')) continue;
                    const h = hadiths.find(x => String(x.idInBook) === idInBook);
                    if (h) { text = h.english?.text || ''; break; }
                }
                return `<div class="rp-hadith-item" data-book="${bookId}" data-id="${idInBook}">
                    <div class="rp-hi-meta">${this.escHtml(meta?.name_en || bookId)} #${idInBook}</div>
                    <div class="rp-hi-text">${text ? this.escHtml(text.slice(0,130)) + '…' : '<em style="color:var(--text-muted)">Open book to load text</em>'}</div>
                </div>`;
            }).join('');

            // Click to navigate
            this.$('rp-hadiths-list').querySelectorAll('.rp-hadith-item').forEach(el => {
                el.addEventListener('click', () => {
                    const bookId = el.dataset.book;
                    const idInBook = el.dataset.id;
                    this.closePanels();
                    this.selectBook(bookId); // navigate to the book
                });
            });
        } else {
            this.$('rp-hadiths-section').style.display = 'none';
        }

        this.closePanels();
        this.$('rawi-panel').classList.add('panel-open');
        this.$('panel-backdrop').classList.add('visible');
    },

    // ── Grade helpers ─────────────────────────────────────────────────────────
    gradeClass(grade) {
        if (!grade) return '';
        const g = grade.toLowerCase();
        if (g.includes('sahih'))                                      return 'grade-sahih';
        if (g.includes('hasan'))                                      return 'grade-hasan';
        if (g.includes("da'if") || g.includes('daif') || g.includes('weak')) return 'grade-daif';
        return 'grade-other';
    },

    gradeLabel(grade) {
        if (!grade) return '';
        const g = grade.toLowerCase();
        if (g.includes('sahih'))                                      return 'Sahih';
        if (g.includes('hasan'))                                      return 'Hasan';
        if (g.includes("da'if") || g.includes('daif') || g.includes('weak')) return "Da'if";
        return grade.split('(')[0].trim();
    },

    // ── Search ────────────────────────────────────────────────────────────────
    async buildSearchIndex() {
        this.searchIndexLoading = true;
        try {
            this.searchIndex      = await fetch('data/search_index.json').then(r => r.json());
            this.searchIndexLoaded = true;
        } catch {
            this.searchIndexLoaded = false;
        }
        this.searchIndexLoading = false;
    },

    doSearch(query) {
        const q       = query.trim().toLowerCase();
        if (!q || q.length < 2) return [];
        const isAr    = /[\u0600-\u06FF]/.test(q);
        const pool    = this.searchIndexLoaded ? this.searchIndex : this.buildLocalSearchPool();
        const results = [];

        for (const h of pool) {
            if (results.length >= 40) break;
            const hay = isAr
                ? (h.arabic || '')
                : `${h.narrator || ''} ${h.text || ''}`.toLowerCase();
            if (hay.includes(isAr ? query.trim() : q)) results.push(h);
        }
        return results;
    },

    buildLocalSearchPool() {
        const pool = [];
        Object.entries(this.chapterCache).forEach(([key, hadiths]) => {
            const [bookId, chIdx] = key.split(':');
            const meta = this.findBook(bookId);
            hadiths.forEach(h => pool.push({
                bookId,
                bookNameEn: meta?.name_en || bookId,
                chapterIdx: parseInt(chIdx),
                idInBook:   h.idInBook,
                arabic:     h.arabic || '',
                narrator:   h.english?.narrator || '',
                text:       h.english?.text || '',
                grade:      this.gradeMap[`${bookId}:${h.idInBook}`] || h.grade || null,
            }));
        });
        return pool;
    },

    renderSearchResults(results, query) {
        const panel = this.$('search-results-panel');
        if (!results.length) {
            panel.innerHTML = `<div class="search-no-results">No results for "<strong>${this.escHtml(query)}</strong>"</div>`;
            panel.style.display = 'block';
            return;
        }

        const isAr = /[\u0600-\u06FF]/.test(query);
        panel.innerHTML = results.map(h => {
            const snippet = isAr
                ? this.highlight(h.arabic?.slice(0, 120) || '', query, true)
                : this.highlight((h.narrator ? h.narrator + ' — ' : '') + (h.text?.slice(0, 160) || ''), query.toLowerCase());
            return `
                <div class="search-result-item"
                     data-book="${h.bookId}"
                     data-chapter="${h.chapterIdx}"
                     data-hadith="${h.idInBook}">
                    <div class="sri-meta">
                        <span>${this.escHtml(h.bookNameEn)}</span>
                        ${h.grade ? `<span class="grade-badge ${this.gradeClass(h.grade)} sri-grade">${this.gradeLabel(h.grade)}</span>` : ''}
                        <span>#${h.idInBook}</span>
                    </div>
                    <div class="sri-en">${snippet}</div>
                    ${isAr ? '' : `<div class="sri-ar">${this.escHtml((h.arabic || '').slice(0, 80))}</div>`}
                </div>
            `;
        }).join('');

        panel.style.display = 'block';

        panel.querySelectorAll('.search-result-item').forEach(el => {
            el.addEventListener('click', () => {
                const bookId   = el.dataset.book;
                const chIdx    = parseInt(el.dataset.chapter);
                const hadithId = parseInt(el.dataset.hadith);
                this.closeSearch();
                this.selectBook(bookId).then(() => this.loadChapter(chIdx)).then(() => {
                    setTimeout(() => {
                        document.querySelectorAll('.hadith-card').forEach(c => {
                            if (parseInt(c.dataset.id) === hadithId) {
                                c.scrollIntoView({ behavior: 'smooth', block: 'center' });
                                c.style.transition = 'background .3s';
                                c.style.background = 'var(--gold-light)';
                                setTimeout(() => c.style.background = '', 1500);
                            }
                        });
                    }, 300);
                });
            });
        });
    },

    closeSearch() {
        this.$('search-results-panel').style.display = 'none';
        this.$('search-input').value = '';
        this.$('search-clear').style.display = 'none';
    },

    highlight(text, query, isArabic = false) {
        if (!query) return this.escHtml(text);
        const safe  = this.escHtml(text);
        const safeQ = this.escHtml(query.trim()).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
        try {
            return safe.replace(new RegExp(safeQ, isArabic ? 'g' : 'gi'), m => `<mark>${m}</mark>`);
        } catch { return safe; }
    },

    // ── UI Setup ──────────────────────────────────────────────────────────────
    setupUI() {
        // Library Map
        this.$('map-toggle').addEventListener('click', () => {
            this._mapVisible ? this.hideMap() : this.showMap();
        });

        // Theme
        this.$('theme-toggle').addEventListener('click', () => {
            this.darkMode = !this.darkMode;
            document.documentElement.classList.toggle('dark-mode', this.darkMode);
            localStorage.setItem('hadith-dark', this.darkMode);
        });

        // Arabic
        const arBtn = this.$('arabic-toggle');
        arBtn.addEventListener('click', () => {
            this.showArabic = !this.showArabic;
            arBtn.classList.toggle('active', this.showArabic);
            localStorage.setItem('hadith-arabic', this.showArabic);
            document.querySelectorAll('.hc-arabic').forEach(el =>
                el.classList.toggle('hidden', !this.showArabic));
        });

        // Narrator
        const narBtn = this.$('narrator-toggle');
        narBtn.addEventListener('click', () => {
            this.showNarrator = !this.showNarrator;
            narBtn.classList.toggle('active', this.showNarrator);
            localStorage.setItem('hadith-narrator', this.showNarrator);
            document.querySelectorAll('.hc-narrator').forEach(el =>
                el.classList.toggle('hidden', !this.showNarrator));
        });

        // Text scale
        const SCALES = [1, 1.15, 1.3];
        this.$('text-scale-btn').addEventListener('click', () => {
            const idx = SCALES.indexOf(this.textScale);
            this.textScale = SCALES[(idx + 1) % SCALES.length];
            localStorage.setItem('hadith-scale', this.textScale);
            document.documentElement.style.setProperty('--text-scale', this.textScale);
        });
        document.documentElement.style.setProperty('--text-scale', this.textScale);

        // Search
        const searchInput = this.$('search-input');
        const clearBtn    = this.$('search-clear');

        searchInput.addEventListener('input', () => {
            const val = searchInput.value;
            clearBtn.style.display = val ? 'block' : 'none';
            clearTimeout(this.searchDebounce);
            if (val.length < 2) {
                this.$('search-results-panel').style.display = 'none';
                return;
            }
            this.$('search-results-panel').innerHTML = '<div class="search-loading">Searching…</div>';
            this.$('search-results-panel').style.display = 'block';
            this.searchDebounce = setTimeout(() => {
                this.renderSearchResults(this.doSearch(val), val);
            }, 250);
        });

        clearBtn.addEventListener('click', () => this.closeSearch());

        document.addEventListener('click', (e) => {
            if (!e.target.closest('.header-search')) {
                this.$('search-results-panel').style.display = 'none';
            }
        });

        // Chapter panel
        this.$('chapter-panel-close').addEventListener('click', () =>
            this.$('chapter-panel').classList.add('hidden'));

        // Prev/next chapter
        this.$('prev-chapter').addEventListener('click', () => {
            if (this.currentChapterIdx > 0) this.loadChapter(this.currentChapterIdx - 1);
        });
        this.$('next-chapter').addEventListener('click', () => {
            const chs = this.chapterIndex[this.currentBookId] || [];
            if (this.currentChapterIdx < chs.length - 1)
                this.loadChapter(this.currentChapterIdx + 1);
        });
        this.$('chapter-select').addEventListener('change', (e) =>
            this.loadChapter(parseInt(e.target.value)));

        // Panel closes
        this.$('close-word-panel').addEventListener('click',   () => this.closePanels());
        this.$('close-hadith-panel').addEventListener('click', () => this.closePanels());
        this.$('close-rawi-panel').addEventListener('click',   () => this.closePanels());
        this.$('panel-backdrop').addEventListener('click',     () => this.closePanels());

        // Accordion toggles — word panel
        this.setupAccordion('wp-meaning-toggle',  'wp-meaning-section');
        this.setupAccordion('wp-family-toggle',   'wp-family-section');
        this.setupAccordion('wp-cooccur-toggle',  'wp-cooccur-section');
        this.setupAccordion('wp-hadiths-toggle',  'wp-hadiths-section');
        // Accordion toggles — hadith + rawi panels
        this.setupAccordion('hp-connections-toggle', 'hp-connections-section');
        this.setupAccordion('rp-topics-toggle',      'rp-topics-section');
        this.setupAccordion('rp-grades-toggle',      'rp-grades-section');
        this.setupAccordion('rp-books-toggle',       'rp-books-section');
        this.setupAccordion('rp-hadiths-toggle',     'rp-hadiths-section');

        // Lazy co-occurrence: compute the first time المصاحبة is expanded
        this.$('wp-cooccur-toggle').addEventListener('click', () => {
            const section = this.$('wp-cooccur-section');
            if (!section.classList.contains('collapsed') && !section.dataset.computed) {
                section.dataset.computed = '1';
                this.renderCoOccurrence(this.activeWord);
            }
        });

        // Copy / Share in hadith panel
        this.$('hp-copy').addEventListener('click', () => {
            const h    = this.activeHadith;
            const meta = this.currentBookMeta;
            if (!h) return;
            const text = [
                meta?.name_en ? `[${meta.name_en}] #${h.idInBook}` : '',
                h.arabic || '',
                h.english?.narrator || '',
                h.english?.text || '',
            ].filter(Boolean).join('\n\n');
            navigator.clipboard?.writeText(text).then(() => {
                this.$('hp-copy').textContent = '✓ Copied';
                setTimeout(() => this.$('hp-copy').textContent = '⧉ Copy', 1500);
            });
        });

        this.$('hp-share').addEventListener('click', () => {
            const h = this.activeHadith;
            if (!h) return;
            const url = `${location.origin}${location.pathname}#${this.currentBookId}/${this.currentChapterIdx}/${h.idInBook}`;
            navigator.clipboard?.writeText(url);
            this.$('hp-share').textContent = '✓ Copied';
            setTimeout(() => this.$('hp-share').textContent = '🔗 Share', 1500);
        });

        this.$('hp-open').addEventListener('click', () => {
            const h = this.activeHadith;
            if (!h) return;
            window.open(`hadith.html?book=${this.currentBookId}&id=${h.idInBook}`, '_blank');
        });

        // Keyboard
        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape') {
                this.closePanels();
                this.closeSearch();
                this.$('chapter-panel').classList.add('hidden');
            }
            if (e.key === '/' && document.activeElement !== searchInput) {
                e.preventDefault();
                searchInput.focus();
            }
        });

        // Mobile swipe-to-dismiss panels
        this.setupSwipeDismiss('word-panel');
        this.setupSwipeDismiss('hadith-panel');
        this.setupSwipeDismiss('rawi-panel');
    },

    setupAccordion(toggleId, sectionId) {
        const toggle  = this.$(toggleId);
        const section = this.$(sectionId);
        if (!toggle || !section) return;

        toggle.addEventListener('click', () => {
            const collapsed = section.classList.toggle('collapsed');
            toggle.querySelector('.chevron').textContent = collapsed ? '▸' : '▾';
        });
    },

    setupSwipeDismiss(panelId) {
        const panel = this.$(panelId);
        if (!panel) return;
        let startY = 0;
        panel.addEventListener('touchstart', e => { startY = e.touches[0].clientY; }, { passive: true });
        panel.addEventListener('touchend', e => {
            const dy = e.changedTouches[0].clientY - startY;
            if (dy > 80) this.closePanels();  // swipe down 80px+ → close
        }, { passive: true });
    },

    applySettings() {
        document.documentElement.classList.toggle('dark-mode', this.darkMode);
        this.$('arabic-toggle')?.classList.toggle('active', this.showArabic);
        this.$('narrator-toggle')?.classList.toggle('active', this.showNarrator);
    },

    // ── Root filter (from Quran bil-Quran) ───────────────────────────────────
    async applyRootFilter(root) {
        // Fetch only this root's file — tiny, fast, no 3.8MB download
        // Format: {book: {chapter_idx: [idInBook, ...]}}
        let byBook;
        try {
            const enc = encodeURIComponent(root);
            const raw = await fetch(`data/bridge_ids/${enc}.json`).then(r => r.json());
            byBook = {};
            for (const [book, chapters] of Object.entries(raw)) {
                byBook[book] = { _total: 0 };
                for (const [ch, ids] of Object.entries(chapters)) {
                    byBook[book][ch] = new Set(ids);
                    byBook[book]._total += ids.length;
                }
            }
        } catch (e) {
            console.warn('Root filter not found for:', root, e);
            return;
        }
        if (!byBook || !Object.keys(byBook).length) return;

        this.rootFilter = { root, ids: byBook };
        this.renderSidebar();        // re-render with count badges
        this.renderRootFilterBanner();

        // Auto-open first book that has matches, starting at a chapter with hits
        const allBooks = [
            ...this.books.sunni.the_9_books,
            ...this.books.sunni.forties,
            ...this.books.sunni.other_books,
        ];
        const firstBook = allBooks.find(b => byBook[b.id]);
        if (firstBook) {
            await this.selectBook(firstBook.id);
            // Find first chapter with matches
            const bookFilter = byBook[firstBook.id];
            const firstCh = Object.keys(bookFilter).find(k => k !== '_total' && bookFilter[k].size > 0);
            await this.loadChapter(firstCh !== undefined ? parseInt(firstCh) : 0);
        }
    },

    clearRootFilter() {
        this.rootFilter = null;
        this.$('root-filter-banner').style.display = 'none';
        this.renderSidebar();
        history.replaceState(null, '', location.pathname + location.hash);
        // Re-render hadith list without the filter
        if (this._lastHadiths) this.renderHadiths(this._lastHadiths);
    },

    renderRootFilterBanner() {
        const { root, ids } = this.rootFilter;
        const total = Object.values(ids).reduce((s, book) => s + (book._total || 0), 0);
        const letters = root.split('').join(' ');
        const banner = this.$('root-filter-banner');
        banner.innerHTML = `
            <span class="rfb-root" dir="rtl">${letters}</span>
            <span class="rfb-count">${total.toLocaleString()} connected hadiths</span>
            <button class="rfb-quran" onclick="App.toggleQuranBridge()">📖 Quran verses</button>
            <button class="rfb-clear" onclick="App.clearRootFilter()">✕ clear filter</button>
        `;
        banner.style.display = 'flex';
        this.loadQuranBridge(root);
    },

    async loadQuranBridge(root) {
        if (!this.quranRootsIndex) {
            try {
                this.quranRootsIndex = await fetch('../quran/data/roots_index.json').then(r => r.json());
            } catch { this.quranRootsIndex = {}; }
        }
        const entry = this.quranRootsIndex[root];
        const panel = this.$('quran-bridge-panel');
        if (!entry || !entry.v || !entry.v.length) {
            panel.style.display = 'none';
            return;
        }
        const verses = entry.v;
        const meaning = entry.m || '';
        const family = entry.fam || '';
        panel.innerHTML = `
            <div class="qbp-header">
                <span class="qbp-title">آيات القرآن — ${verses.length} Quran verses with this root</span>
                ${family ? `<span class="qbp-family">${family}</span>` : ''}
            </div>
            ${meaning ? `<div class="qbp-meaning">${this.escHtml(meaning)}</div>` : ''}
            <div class="qbp-verses">${verses.map(v => {
                const [s, a] = v.split(':');
                return `<a href="../quran/index.html#${s}" class="qbp-verse" title="Surah ${s}, Ayah ${a}">${v}</a>`;
            }).join('')}</div>
        `;
        panel.style.display = 'none'; // starts collapsed, toggled by button
    },

    toggleQuranBridge() {
        const panel = this.$('quran-bridge-panel');
        const isHidden = panel.style.display === 'none';
        panel.style.display = isHidden ? 'block' : 'none';
    },

    // ── Hash routing ──────────────────────────────────────────────────────────
    handleHash() {
        const hash = location.hash.slice(1);
        if (!hash) return;
        const parts = hash.split('/');
        const [bookId, chIdx, hadithId] = parts;
        if (!bookId) return;

        // Skip if we're already on this book+chapter (prevents reload loop)
        const targetCh = chIdx !== undefined ? parseInt(chIdx) : 0;
        if (bookId === this.currentBookId && targetCh === this.currentChapterIdx && !hadithId) {
            return;
        }

        if (bookId !== this.currentBookId) {
            this.selectBook(bookId).then(() => {
                if (chIdx !== undefined) {
                    this.loadChapter(targetCh).then(() => {
                        if (hadithId) this.scrollToHadith(hadithId);
                    });
                }
            });
        } else if (targetCh !== this.currentChapterIdx) {
            this.loadChapter(targetCh).then(() => {
                if (hadithId) this.scrollToHadith(hadithId);
            });
        } else if (hadithId) {
            this.scrollToHadith(hadithId);
        }
    },

    scrollToHadith(idInBook) {
        requestAnimationFrame(() => {
            const card = document.querySelector(`.hadith-card[data-id="${idInBook}"]`);
            if (!card) return;
            card.scrollIntoView({ behavior: 'smooth', block: 'center' });
            card.classList.add('hadith-highlight');
            setTimeout(() => card.classList.remove('hadith-highlight'), 3000);
        });
    },

    // ── Utility ───────────────────────────────────────────────────────────────
    escHtml(str) {
        if (!str) return '';
        return String(str)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;');
    },
};

document.addEventListener('DOMContentLoaded', () => App.init());
