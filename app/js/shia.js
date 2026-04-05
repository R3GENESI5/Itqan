/* ── Shia Hadith App ───────────────────────────────── */
const ShiaApp = {

    // ── State ─────────────────────────────────────────
    books: null,
    currentBookId: null,
    currentBookMeta: null,
    currentChapterIdx: 0,
    chapterCache: {},      // { "alkafi-1:3": [...hadiths] }
    chapterIndex: {},      // { "alkafi-1": [{id,name_ar,name_en,count},...] }

    // Settings
    showArabic: true,
    showChain: true,
    darkMode: false,

    // Search
    searchDebounce: null,
    searchIndexLoaded: false,
    searchIndex: [],

    activeHadith: null,
    isMobile: false,

    // Shia book groupings (mapped from books.json shia array)
    KAFI_IDS:   ['alkafi-1','alkafi-2','alkafi-3','alkafi-4','alkafi-5','alkafi-6','alkafi-7','alkafi-8'],
    SADUQ_IDS:  ['al-amali-saduq','al-khisal','al-tawhid','maani-al-akhbar','uyun-rida-1','uyun-rida-2'],

    $: (id) => document.getElementById(id),

    // ── Init ──────────────────────────────────────────
    async init() {
        this.checkMobile();
        window.addEventListener('resize', () => this.checkMobile());

        this.showArabic = localStorage.getItem('hadith-arabic')   !== 'false';
        this.showChain  = localStorage.getItem('hadith-narrator')  !== 'false';
        this.darkMode   = localStorage.getItem('hadith-dark')      === 'true';
        this.applySettings();

        try {
            this.books = await fetch('data/books.json').then(r => r.json());
            this.renderSidebar();
            this.setupUI();
            this.handleHash();
            this.$('loading-overlay').classList.add('hidden');
            this.buildSearchIndex();
        } catch (err) {
            console.error('Shia init failed:', err);
            document.querySelector('.loader-text').textContent = 'Failed to load. Please refresh.';
        }
    },

    checkMobile() {
        this.isMobile = window.matchMedia('(max-width: 640px)').matches;
    },

    // ── Sidebar ───────────────────────────────────────
    renderSidebar() {
        const shiaBooks = this.books.shia;

        const groups = [
            { ids: this.KAFI_IDS,  elId: 'books-kafi' },
            { ids: this.SADUQ_IDS, elId: 'books-saduq' },
            { ids: null,           elId: 'books-other-shia' },  // everything else
        ];

        const usedIds = new Set([...this.KAFI_IDS, ...this.SADUQ_IDS]);

        groups.forEach(({ ids, elId }) => {
            const ul = this.$(elId);
            ul.innerHTML = '';
            const subset = ids
                ? shiaBooks.filter(b => ids.includes(b.id))
                : shiaBooks.filter(b => !usedIds.has(b.id));

            subset.forEach(book => {
                const li = document.createElement('li');
                li.className = 'book-item';
                li.dataset.bookId = book.id;
                li.innerHTML = `
                    <div class="book-item-inner">
                        <span class="book-name-ar">${book.name_ar}</span>
                        <span class="book-name-en">${book.name_en}</span>
                    </div>
                `;
                li.addEventListener('click', () => this.selectBook(book.id));
                ul.appendChild(li);
            });
        });
    },

    // ── Book selection ────────────────────────────────
    async selectBook(bookId) {
        document.querySelectorAll('.book-item').forEach(el => {
            el.classList.toggle('active', el.dataset.bookId === bookId);
        });

        this.currentBookId = bookId;
        this.currentBookMeta = this.books.shia.find(b => b.id === bookId) || null;

        await this.loadChapterIndex(bookId);
        this.renderChapterPanel();
        location.hash = bookId;

        if (this.isMobile) {
            this.$('chapter-panel').classList.remove('hidden');
        }
    },

    // ── Chapter index ─────────────────────────────────
    async loadChapterIndex(bookId) {
        if (this.chapterIndex[bookId]) return;
        try {
            const data = await fetch(`data/shia/${bookId}/index.json`).then(r => r.json());
            this.chapterIndex[bookId] = data;
        } catch {
            console.warn('No chapter index for', bookId, '— run download_data.py');
            this.chapterIndex[bookId] = [];
        }
    },

    // ── Chapter panel ─────────────────────────────────
    renderChapterPanel() {
        const meta = this.currentBookMeta;
        const chapters = this.chapterIndex[this.currentBookId] || [];
        const cp = this.$('chapter-panel');

        this.$('chapter-book-title').textContent = meta?.name_ar || meta?.name_en || '';
        cp.classList.remove('hidden');

        const ul = this.$('chapter-list');
        ul.innerHTML = '';

        if (chapters.length === 0) {
            ul.innerHTML = '<li style="padding:16px;color:var(--text-muted);font-size:0.82rem">No chapters found.<br>Run the data pipeline first.</li>';
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

    // ── Load chapter ──────────────────────────────────
    async loadChapter(chapterIdx) {
        const bookId = this.currentBookId;
        const chapters = this.chapterIndex[bookId] || [];
        if (!chapters[chapterIdx]) return;

        const ch = chapters[chapterIdx];
        this.currentChapterIdx = chapterIdx;

        document.querySelectorAll('.chapter-item').forEach(el => {
            el.classList.toggle('active', parseInt(el.dataset.idx) === chapterIdx);
        });
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
            this.$('hadith-list').innerHTML = '<div style="padding:20px;color:var(--text-muted);font-size:0.82rem;text-align:center">Loading…</div>';
            try {
                const data = await fetch(`data/shia/${bookId}/${ch.file || chapterIdx + 1 + '.json'}`).then(r => r.json());
                this.chapterCache[cacheKey] = data;
            } catch {
                this.$('hadith-list').innerHTML = '<div style="padding:20px;color:var(--text-muted);font-size:0.82rem;text-align:center">Data not available.<br>Run <code>src/download_data.py</code> first.</div>';
                return;
            }
        }

        this.renderHadiths(this.chapterCache[cacheKey]);
        location.hash = `${bookId}/${chapterIdx}`;
    },

    // ── Render hadiths ────────────────────────────────
    renderHadiths(hadiths) {
        const list = this.$('hadith-list');
        list.innerHTML = '';

        if (!hadiths || hadiths.length === 0) {
            list.innerHTML = '<div style="padding:24px;color:var(--text-muted);text-align:center">No hadiths in this chapter.</div>';
            return;
        }

        const frag = document.createDocumentFragment();
        hadiths.forEach(h => {
            const card = document.createElement('div');
            card.className = 'hadith-card';
            card.dataset.id = h.id || h.idInBook || '';

            // Thaqalayn data structure: { id, arabic, english, chain, chapter, book }
            const arabic  = h.arabic  || h.arabicText  || '';
            const english = h.english || h.englishText || '';
            const chain   = h.chain   || h.isnad       || '';

            const grade = h.grade || h.majlisiGrading || '';
            card.innerHTML = `
                <div class="hc-meta">
                    <span class="hc-num">#${h.id || h.idInBook || ''}</span>
                    ${grade ? `<span class="grade-badge grade-other" title="Majlisi: ${this.escHtml(h.majlisiGrading||'')} | Behdudi: ${this.escHtml(h.behdudiGrading||'')} | Mohseni: ${this.escHtml(h.mohseniGrading||'')}">${this.escHtml(grade.length > 20 ? grade.slice(0,20)+'…' : grade)}</span>` : ''}
                    ${h.chapter ? `<span style="font-size:0.7rem;color:var(--text-muted)">${this.escHtml(h.chapter)}</span>` : ''}
                </div>
                ${this.showArabic && arabic
                    ? `<div class="hc-arabic">${this.escHtml(arabic)}</div>`
                    : ''}
                ${this.showChain && chain
                    ? `<div class="hc-chain">${this.escHtml(chain)}</div>`
                    : ''}
                <div class="hc-english">${this.escHtml(english)}</div>
            `;

            card.addEventListener('click', () => this.openDetail(h));
            frag.appendChild(card);
        });

        list.appendChild(frag);
        list.scrollTop = 0;
    },

    // ── Detail panel ──────────────────────────────────
    openDetail(h) {
        this.activeHadith = h;
        const meta = this.currentBookMeta;
        const chapters = this.chapterIndex[this.currentBookId] || [];
        const ch = chapters[this.currentChapterIdx];

        const arabic  = h.arabic  || h.arabicText  || '';
        const english = h.english || h.englishText || '';
        const chain   = h.chain   || h.isnad       || '';

        this.$('dp-ref').textContent     = `${meta?.name_en || ''} — ${ch?.name_en || ''} — #${h.id || h.idInBook || ''}`;
        this.$('dp-arabic').textContent  = arabic;
        this.$('dp-chain').textContent   = chain;
        this.$('dp-english').textContent = english;

        // Show grading breakdown
        const gradeEl = this.$('dp-grade');
        if (gradeEl) {
            const grades = [
                h.majlisiGrading ? `Majlisi: ${h.majlisiGrading}` : '',
                h.behdudiGrading ? `Behdudi: ${h.behdudiGrading}` : '',
                h.mohseniGrading ? `Mohseni: ${h.mohseniGrading}` : '',
            ].filter(Boolean);
            gradeEl.innerHTML = grades.length
                ? grades.map(g => `<span class="grade-badge grade-other">${this.escHtml(g)}</span>`).join(' ')
                : '';
        }

        this.$('detail-panel').classList.add('panel-open');
        this.$('panel-backdrop').classList.add('visible');
    },

    closeDetail() {
        this.$('detail-panel').classList.remove('panel-open');
        this.$('panel-backdrop').classList.remove('visible');
        this.activeHadith = null;
    },

    // ── Search ────────────────────────────────────────
    async buildSearchIndex() {
        try {
            const idx = await fetch('data/shia_search_index.json').then(r => r.json());
            this.searchIndex = idx;
            this.searchIndexLoaded = true;
        } catch { this.searchIndexLoaded = false; }
    },

    doSearch(query) {
        const q = query.trim().toLowerCase();
        if (!q || q.length < 2) return [];

        const isArabic = /[\u0600-\u06FF]/.test(q);
        const results = [];
        const pool = this.searchIndexLoaded ? this.searchIndex : this.buildLocalPool();

        for (const h of pool) {
            if (results.length >= 40) break;
            const hay = isArabic
                ? (h.arabic || '')
                : `${h.chain || ''} ${h.text || ''}`.toLowerCase();
            if (hay.includes(isArabic ? query.trim() : q)) results.push(h);
        }
        return results;
    },

    buildLocalPool() {
        const pool = [];
        Object.entries(this.chapterCache).forEach(([key, hadiths]) => {
            const [bookId] = key.split(':');
            const meta = this.books.shia.find(b => b.id === bookId);
            hadiths.forEach(h => {
                pool.push({
                    bookId,
                    bookNameEn: meta?.name_en || bookId,
                    chapterIdx: parseInt(key.split(':')[1]),
                    id: h.id || h.idInBook,
                    arabic: h.arabic || h.arabicText || '',
                    chain: h.chain || h.isnad || '',
                    text: h.english || h.englishText || '',
                });
            });
        });
        return pool;
    },

    renderSearchResults(results, query) {
        const panel = this.$('search-results-panel');
        if (results.length === 0) {
            panel.innerHTML = `<div class="search-no-results">No results for "<strong>${this.escHtml(query)}</strong>"</div>`;
            panel.style.display = 'block';
            return;
        }
        const q = query.trim().toLowerCase();
        const isArabic = /[\u0600-\u06FF]/.test(query);
        panel.innerHTML = results.map(h => `
            <div class="search-result-item"
                 data-book="${h.bookId}" data-chapter="${h.chapterIdx}" data-hadith="${h.id}">
                <div class="sri-meta">
                    <span>${this.escHtml(h.bookNameEn)}</span>
                    <span>#${h.id}</span>
                </div>
                <div class="sri-en">${this.highlight(
                    (h.chain ? h.chain + ' — ' : '') + (h.text || '').slice(0, 160), q)}</div>
                ${isArabic ? '' : `<div class="sri-ar">${this.escHtml((h.arabic||'').slice(0,80))}</div>`}
            </div>
        `).join('');
        panel.style.display = 'block';

        panel.querySelectorAll('.search-result-item').forEach(el => {
            el.addEventListener('click', () => {
                const bookId  = el.dataset.book;
                const chIdx   = parseInt(el.dataset.chapter);
                const hId     = parseInt(el.dataset.hadith);
                this.closeSearch();
                this.selectBook(bookId).then(() => this.loadChapter(chIdx)).then(() => {
                    setTimeout(() => {
                        document.querySelectorAll('.hadith-card').forEach(c => {
                            if (parseInt(c.dataset.id) === hId) {
                                c.scrollIntoView({ behavior: 'smooth', block: 'center' });
                                c.style.transition = 'background 0.3s';
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

    highlight(text, query) {
        if (!query) return this.escHtml(text);
        const safe  = this.escHtml(text);
        const safeQ = this.escHtml(query.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'));
        try { return safe.replace(new RegExp(safeQ, 'gi'), m => `<mark>${m}</mark>`); }
        catch { return safe; }
    },

    // ── UI Setup ──────────────────────────────────────
    setupUI() {
        this.$('theme-toggle').addEventListener('click', () => {
            this.darkMode = !this.darkMode;
            document.documentElement.classList.toggle('dark-mode', this.darkMode);
            localStorage.setItem('hadith-dark', this.darkMode);
        });

        const arBtn = this.$('arabic-toggle');
        arBtn.addEventListener('click', () => {
            this.showArabic = !this.showArabic;
            arBtn.classList.toggle('active', this.showArabic);
            localStorage.setItem('hadith-arabic', this.showArabic);
            document.querySelectorAll('.hc-arabic').forEach(el => el.classList.toggle('hidden', !this.showArabic));
        });

        const chainBtn = this.$('chain-toggle');
        chainBtn.addEventListener('click', () => {
            this.showChain = !this.showChain;
            chainBtn.classList.toggle('active', this.showChain);
            localStorage.setItem('hadith-narrator', this.showChain);
            document.querySelectorAll('.hc-chain').forEach(el => el.classList.toggle('hidden', !this.showChain));
        });

        // Search
        const inp  = this.$('search-input');
        const clr  = this.$('search-clear');
        inp.addEventListener('input', () => {
            clr.style.display = inp.value ? 'block' : 'none';
            clearTimeout(this.searchDebounce);
            if (inp.value.length < 2) { this.$('search-results-panel').style.display = 'none'; return; }
            this.$('search-results-panel').innerHTML = '<div class="search-loading">Searching…</div>';
            this.$('search-results-panel').style.display = 'block';
            this.searchDebounce = setTimeout(() => this.renderSearchResults(this.doSearch(inp.value), inp.value), 250);
        });
        clr.addEventListener('click', () => this.closeSearch());
        document.addEventListener('click', e => {
            if (!e.target.closest('.header-search')) this.$('search-results-panel').style.display = 'none';
        });

        this.$('chapter-panel-close').addEventListener('click', () => this.$('chapter-panel').classList.add('hidden'));

        this.$('prev-chapter').addEventListener('click', () => {
            if (this.currentChapterIdx > 0) this.loadChapter(this.currentChapterIdx - 1);
        });
        this.$('next-chapter').addEventListener('click', () => {
            const chs = this.chapterIndex[this.currentBookId] || [];
            if (this.currentChapterIdx < chs.length - 1) this.loadChapter(this.currentChapterIdx + 1);
        });
        this.$('chapter-select').addEventListener('change', e => this.loadChapter(parseInt(e.target.value)));

        this.$('close-detail').addEventListener('click', () => this.closeDetail());
        this.$('panel-backdrop').addEventListener('click', () => this.closeDetail());

        this.$('dp-copy').addEventListener('click', () => {
            if (!this.activeHadith) return;
            const h = this.activeHadith;
            const meta = this.currentBookMeta;
            const text = [
                meta?.name_en ? `[${meta.name_en}]` : '',
                h.arabic || h.arabicText || '',
                h.chain || h.isnad || '',
                h.english || h.englishText || '',
            ].filter(Boolean).join('\n\n');
            navigator.clipboard?.writeText(text).then(() => {
                this.$('dp-copy').textContent = '✓ Copied';
                setTimeout(() => this.$('dp-copy').textContent = '⧉ Copy', 1500);
            });
        });

        this.$('dp-share').addEventListener('click', () => {
            const url = `${location.origin}${location.pathname}#${this.currentBookId}/${this.currentChapterIdx}`;
            navigator.clipboard?.writeText(url);
            this.$('dp-share').textContent = '✓ Link Copied';
            setTimeout(() => this.$('dp-share').textContent = '⇗ Share', 1500);
        });

        document.addEventListener('keydown', e => {
            if (e.key === 'Escape') { this.closeDetail(); this.closeSearch(); }
            if (e.key === '/' && document.activeElement !== this.$('search-input')) {
                e.preventDefault();
                this.$('search-input').focus();
            }
        });
    },

    applySettings() {
        document.documentElement.classList.toggle('dark-mode', this.darkMode);
        this.$('arabic-toggle')?.classList.toggle('active', this.showArabic);
        this.$('chain-toggle')?.classList.toggle('active', this.showChain);
    },

    handleHash() {
        const hash = location.hash.slice(1);
        if (!hash) return;
        const [bookId, chIdx] = hash.split('/');
        if (bookId) this.selectBook(bookId).then(() => {
            if (chIdx !== undefined) this.loadChapter(parseInt(chIdx));
        });
    },

    escHtml(str) {
        if (!str) return '';
        return String(str)
            .replace(/&/g, '&amp;').replace(/</g, '&lt;')
            .replace(/>/g, '&gt;').replace(/"/g, '&quot;');
    },
};

document.addEventListener('DOMContentLoaded', () => ShiaApp.init());
