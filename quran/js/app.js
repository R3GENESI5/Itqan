/* ── Quran bil-Quran App ─────────────────────────── */

const App = {
    // State
    surahList: [],
    rootsIndex: {},
    families: {},
    versesText: {},
    mufradat: {},
    furuq: [],
    hadithBridge: null,
    currentSurah: 1,
    surahCache: {},
    selectedRoot: null,
    showAllVerses: false,
    PREVIEW_LIMIT: 25,

    // Mobile
    isMobile: false,
    _drawerDrag: null,

    // Dark mode
    darkMode: false,

    // Root search
    searchIndex: [],
    searchOpen: false,
    searchHighlight: -1,
    searchDebounce: null,
    searchMode: 'roots',   // 'roots' or 'furuq'

    // Verse → roots reverse index (for co-occurrence)
    verseRoots: {},         // { "1:1": Set(["سمو","أله","رحم"]), ... }

    // Thematic layer
    verseFamilies: {},          // { "1:1": Set(["earth_sky"]), ... }
    THEME_GROUPS: {
        creation: 0, knowledge: 0, guidance: 0, revelation: 0,
        prophets_messengers: 0, covenant_promise: 0,
        truth_falsehood: 1, justice: 1, patience_gratitude: 1,
        pride_humility: 1, repentance_return: 1, purity: 1,
        life_death: 2, paradise: 2, hellfire: 2,
        punishment: 2, mercy: 2, fear_hope: 2,
        earth_sky: 3, water: 3, plants_agriculture: 3,
        animals: 3, light_darkness: 3, time: 3,
        worship: 4, speech_communication: 4, heart_soul: 4,
        family_kinship: 4, body: 4, movement_journey: 4,
        fighting: 5, wealth: 5, deception_hypocrisy: 5,
        provision: 5, seeing_hearing: 5,
    },
    GROUP_NAMES: ['الإلهيات', 'الأخلاق', 'الآخرة', 'الطبيعة', 'الإنسان', 'المجتمع'],
    GROUP_NAMES_EN: ['Theology', 'Ethics', 'Eschatology', 'Nature', 'Human', 'Society'],

    // Translation
    translationData: null,
    wbwData: null,
    translationLoaded: false,
    showTranslation: false,

    // Transliteration
    translitData: null,
    translitLoaded: false,
    showTranslit: false,

    // Text scale
    TEXT_SCALES: [1, 1.15, 1.3, 1.5],
    textScaleIdx: 0,

    // Tafsir
    TAFSIR_API: 'https://api.quran.com/api/v4',
    tafsirCache: {},          // { "tafsirId:verseKey": html }
    localTafsirCache: {},     // { "ibn-kathir-ur:3": { "3:1": {...}, ... } }
    activeTafsir: 16,         // default: Muyassar (concise)
    TAFSIRS: {
        // Arabic (API)
        16: 'التفسير الميسّر',
        14: 'ابن كثير',
        91: 'السعدي',
        94: 'البغوي',
        15: 'الطبري',
        90: 'القرطبي',
        93: 'الوسيط — الطنطاوي',
        // Urdu (local)
        'ibn-kathir-ur': 'ابن کثیر (اردو)',
        'bayan-ul-quran': 'بیان القرآن — تھانوی',
        // English (API)
        169: 'Ibn Kathir (English)',
        168: "Ma'ariful Qur'an (English)",
    },
    TAFSIR_GROUPS: {
        'عربي': [16, 14, 91, 94, 15, 90, 93],
        'اردو': ['ibn-kathir-ur', 'bayan-ul-quran'],
        'English': [169, 168],
    },
    LOCAL_TAFSIRS: new Set(['ibn-kathir-ur', 'bayan-ul-quran']),

    // DOM refs
    $: (id) => document.getElementById(id),

    // ── Init ───────────────────────────────────────
    async init() {
        try {
            const [surahList, rootsIndex, families, versesText, mufradat, furuq] = await Promise.all([
                fetch('data/surah_list.json').then(r => r.json()),
                fetch('data/roots_index.json').then(r => r.json()),
                fetch('data/families.json').then(r => r.json()),
                fetch('data/verses_text.json').then(r => r.json()),
                fetch('data/mufradat.json').then(r => r.json()),
                fetch('data/furuq.json').then(r => r.json()),
            ]);

            this.surahList = surahList;
            this.rootsIndex = rootsIndex;
            this.families = families;
            this.versesText = versesText;
            this.mufradat = mufradat;
            this.furuq = furuq;

            this.buildVerseFamilies();
            this.buildVerseRootsIndex();
            this.buildSearchIndex();
            this.setupUI();
            this.handleHash();

            this.$('loading-overlay').classList.add('hidden');

            // Always load translation data (WBW tooltips are always visible)
            // Verse-level translation only shows when toggled on
            if (localStorage.getItem('qbq-show-translation') === 'true') {
                this.showTranslation = true;
                this.$('translation-toggle').classList.add('active');
            }
            if (localStorage.getItem('qbq-show-translit') === 'true') {
                this.showTranslit = true;
                this.$('translit-toggle').classList.add('active');
            }
            // Restore text scale
            const savedScale = parseInt(localStorage.getItem('qbq-text-scale') || '0');
            if (savedScale > 0 && savedScale < this.TEXT_SCALES.length) {
                this.textScaleIdx = savedScale;
                document.documentElement.style.setProperty('--text-scale', this.TEXT_SCALES[savedScale]);
                this.$('text-scale-btn').classList.add('active');
            }
            Promise.all([
                this.loadTranslation(),
                this.showTranslit ? this.loadTranslit() : Promise.resolve(),
            ]).then(() => this.renderSurah());
        } catch (err) {
            console.error('Init failed:', err);
            document.querySelector('.loader-text').textContent = 'خطأ في التحميل';
        }
    },

    // ── UI Setup ───────────────────────────────────
    checkMobile() {
        this.isMobile = window.matchMedia('(max-width: 768px)').matches;
    },

    setupUI() {
        // Mobile detection
        this.checkMobile();
        window.addEventListener('resize', () => this.checkMobile());

        // Populate surah selector
        const select = this.$('surah-select');
        this.surahList.forEach(s => {
            const opt = document.createElement('option');
            opt.value = s.number;
            opt.textContent = `${s.number}. ${s.name_ar}`;
            select.appendChild(opt);
        });

        select.addEventListener('change', () => {
            this.loadSurah(parseInt(select.value));
        });

        // Nav buttons
        this.$('prev-surah').addEventListener('click', () => {
            if (this.currentSurah > 1) this.loadSurah(this.currentSurah - 1);
        });
        this.$('next-surah').addEventListener('click', () => {
            if (this.currentSurah < 114) this.loadSurah(this.currentSurah + 1);
        });

        // Close panel
        this.$('close-panel').addEventListener('click', () => this.closePanel());

        // Backdrop click to dismiss
        this.$('panel-backdrop').addEventListener('click', () => this.closePanel());

        // ── Swipe-to-dismiss (mobile bottom drawer) ──
        const panel = this.$('root-panel');

        panel.addEventListener('touchstart', (e) => {
            if (!this.isMobile) return;
            const target = e.target.closest('.drawer-handle, .panel-header');
            if (!target) return;
            this._drawerDrag = { startY: e.touches[0].clientY, currentY: e.touches[0].clientY };
            panel.style.transition = 'none';
        }, { passive: true });

        panel.addEventListener('touchmove', (e) => {
            if (!this._drawerDrag) return;
            this._drawerDrag.currentY = e.touches[0].clientY;
            const dy = Math.max(0, this._drawerDrag.currentY - this._drawerDrag.startY);
            panel.style.transform = `translateY(${dy}px)`;
        }, { passive: true });

        panel.addEventListener('touchend', () => {
            if (!this._drawerDrag) return;
            const dy = this._drawerDrag.currentY - this._drawerDrag.startY;
            this._drawerDrag = null;
            panel.style.transition = '';
            if (dy > 100) {
                this.closePanel();
            } else {
                panel.style.transform = 'translateY(0)';
            }
        }, { passive: true });

        // Show more button
        this.$('show-more-btn').addEventListener('click', () => {
            this.showAllVerses = true;
            this.renderConnectedVerses(this.selectedRoot);
            this.$('show-more-btn').style.display = 'none';
        });

        // Keyboard nav
        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape') {
                if (this.searchOpen) { this.toggleSearch(); return; }
                this.closePanel();
            }
            if (e.target.closest('.root-search-input')) return;
            if (e.key === 'ArrowLeft' && !e.target.closest('select')) {
                e.preventDefault();
                if (this.currentSurah < 114) this.loadSurah(this.currentSurah + 1);
            }
            if (e.key === 'ArrowRight' && !e.target.closest('select')) {
                e.preventDefault();
                if (this.currentSurah > 1) this.loadSurah(this.currentSurah - 1);
            }
        });

        // Dark mode toggle
        this.darkMode = localStorage.getItem('qbq-dark-mode') === 'true';
        this.updateThemeIcon();
        this.$('theme-toggle').addEventListener('click', () => {
            this.darkMode = !this.darkMode;
            document.documentElement.classList.toggle('dark-mode', this.darkMode);
            localStorage.setItem('qbq-dark-mode', this.darkMode);
            this.updateThemeIcon();
        });

        // Translation toggle
        this.$('translation-toggle').addEventListener('click', async () => {
            if (!this.translationLoaded) {
                this.$('translation-toggle').textContent = '···';
                await this.loadTranslation();
            }
            this.showTranslation = !this.showTranslation;
            this.$('translation-toggle').textContent = 'En';
            this.$('translation-toggle').classList.toggle('active', this.showTranslation);
            localStorage.setItem('qbq-show-translation', this.showTranslation);
            this.renderSurah();
        });

        // Transliteration toggle
        this.$('translit-toggle').addEventListener('click', async () => {
            if (!this.translitLoaded) {
                this.$('translit-toggle').textContent = '···';
                await this.loadTranslit();
            }
            this.showTranslit = !this.showTranslit;
            this.$('translit-toggle').textContent = 'T';
            this.$('translit-toggle').classList.toggle('active', this.showTranslit);
            localStorage.setItem('qbq-show-translit', this.showTranslit);
            this.renderSurah();
        });

        // Text scale toggle — cycles through sizes, wraps back to default
        this.$('text-scale-btn').addEventListener('click', () => {
            this.textScaleIdx = (this.textScaleIdx + 1) % this.TEXT_SCALES.length;
            const scale = this.TEXT_SCALES[this.textScaleIdx];
            document.documentElement.style.setProperty('--text-scale', scale);
            this.$('text-scale-btn').classList.toggle('active', this.textScaleIdx > 0);
            localStorage.setItem('qbq-text-scale', this.textScaleIdx);
        });

        // Accordion toggle for panel sections
        this.$('root-panel').addEventListener('click', (e) => {
            const toggle = e.target.closest('.section-toggle');
            if (!toggle) return;
            const section = toggle.closest('.panel-section');
            if (!section) return;
            const chevron = toggle.querySelector('.chevron');
            section.classList.toggle('collapsed');
            chevron.textContent = section.classList.contains('collapsed') ? '\u25B8' : '\u25BE';
        });

        // Root search
        this.$('root-search-btn').addEventListener('click', () => this.toggleSearch());

        // Search mode toggle (roots ↔ furuq)
        this.$('search-mode-btn').addEventListener('click', () => {
            this.searchMode = this.searchMode === 'roots' ? 'furuq' : 'roots';
            const btn = this.$('search-mode-btn');
            const input = this.$('root-search-input');
            if (this.searchMode === 'furuq') {
                btn.textContent = 'فروق';
                btn.classList.add('furuq-mode');
                input.placeholder = 'ابحث في الفروق اللغوية...';
            } else {
                btn.textContent = 'جذر';
                btn.classList.remove('furuq-mode');
                input.placeholder = 'جذر، معنى، buckwalter...';
            }
            // Re-run current query in new mode
            const q = input.value.trim();
            if (q) this.onSearchInput(q);
        });

        this.$('root-search-input').addEventListener('input', (e) => {
            clearTimeout(this.searchDebounce);
            this.searchDebounce = setTimeout(() => this.onSearchInput(e.target.value), 100);
        });

        this.$('root-search-input').addEventListener('keydown', (e) => {
            const sel = this.searchMode === 'furuq' ? 'li.furuq-item' : 'li[data-root]';
            const items = this.$('root-search-results').querySelectorAll(sel);
            if (e.key === 'ArrowDown') {
                e.preventDefault();
                this.searchHighlight = Math.min(this.searchHighlight + 1, items.length - 1);
                this.updateSearchHighlight();
                if (items[this.searchHighlight]) items[this.searchHighlight].scrollIntoView({ block: 'nearest' });
            } else if (e.key === 'ArrowUp') {
                e.preventDefault();
                this.searchHighlight = Math.max(this.searchHighlight - 1, 0);
                this.updateSearchHighlight();
                if (items[this.searchHighlight]) items[this.searchHighlight].scrollIntoView({ block: 'nearest' });
            } else if (e.key === 'Enter' && this.searchHighlight >= 0 && items[this.searchHighlight]) {
                e.preventDefault();
                if (this.searchMode === 'furuq') {
                    items[this.searchHighlight].classList.toggle('expanded');
                } else {
                    this.onSearchSelect(items[this.searchHighlight].dataset.root);
                }
            } else if (e.key === 'Escape') {
                e.preventDefault();
                this.toggleSearch();
            }
        });

        // Close search on outside click
        document.addEventListener('click', (e) => {
            if (this.searchOpen && !e.target.closest('#root-search-wrap')) {
                this.toggleSearch();
            }
        });

        // Hash navigation
        window.addEventListener('hashchange', () => this.handleHash());
    },

    updateThemeIcon() {
        this.$('theme-toggle').textContent = this.darkMode ? '☀' : '☽';
    },

    handleHash() {
        if (this._navigating) return;
        const hash = location.hash.slice(1);
        if (!hash) {
            // Show welcome screen instead of auto-loading Fatiha
            const welcome = document.getElementById('quran-welcome');
            if (welcome) welcome.style.display = 'flex';
            return;
        }
        const parts = hash.split(':');
        const surah = parseInt(parts[0]);
        if (surah >= 1 && surah <= 114) {
            this.loadSurah(surah, true, !!parts[1]).then(() => {
                if (parts[1]) this.scrollToVerse(`${surah}:${parts[1]}`);
            });
        }
    },

    // ── Data Loading ───────────────────────────────
    async loadSurah(num, skipHash, skipScroll) {
        // Hide welcome screen and show surah header when any surah loads
        const welcome = document.getElementById('quran-welcome');
        if (welcome) welcome.style.display = 'none';
        const surahHeader = document.getElementById('surah-header');
        if (surahHeader) surahHeader.style.display = '';

        if (!this.surahCache[num]) {
            const resp = await fetch(`data/surahs/${num}.json`);
            this.surahCache[num] = await resp.json();
        }
        this.currentSurah = num;
        this.renderSurah();
        if (!skipHash) location.hash = `${num}`;

        // Update selector
        this.$('surah-select').value = num;

        // Update nav buttons
        this.$('prev-surah').disabled = num <= 1;
        this.$('next-surah').disabled = num >= 114;

        // Update info
        const info = this.surahList[num - 1];
        this.$('surah-info').textContent =
            `${info.verses_count} verses · ${info.revelation_place === 'makkah' ? 'Makki' : 'Madani'}`;

        // Update themes link with current surah
        const themesLink = document.querySelector('.themes-link');
        if (themesLink) themesLink.href = `themes.html?surah=${num}`;

        if (!skipScroll) window.scrollTo(0, 0);
    },

    // ── Rendering ──────────────────────────────────
    renderSurah() {
        const data = this.surahCache[this.currentSurah];
        const info = this.surahList[this.currentSurah - 1];

        // Surah header
        this.$('surah-name-ar').textContent = `سورة ${info.name_ar}`;
        this.$('surah-name-en').textContent = `${info.name_en} — ${info.name_translation}`;

        // Verses
        const container = this.$('verses');
        container.innerHTML = '';

        data.forEach(verse => {
            const el = document.createElement('div');
            el.className = 'verse';
            el.id = `v-${verse.k}`;

            // Verse text with clickable words + inline verse number
            const textEl = document.createElement('div');
            textEl.className = 'verse-text';

            // Quranic pause/sajdah markers — present in word arrays but absent from WBW data
            const QUR_MARKERS = 'ۖۗۘۙۚۛۜ۞۩';
            let wbwIdx = 0; // separate counter: only advances for real words, not markers

            verse.words.forEach((w, i) => {
                const span = document.createElement('span');
                span.className = 'word';
                span.textContent = w.t;
                span.dataset.index = i;

                const isMarker = w.t.trim().length === 1 && QUR_MARKERS.includes(w.t.trim());

                if (w.r) {
                    span.classList.add('word-has-root');
                    span.dataset.root = w.r;
                    span.addEventListener('click', () => this.onWordClick(w.r, span));
                }

                // Word-by-word tooltip (always shown when data is loaded)
                // Uses wbwIdx (skips markers) instead of i to stay aligned
                if (this.wbwData && !isMarker) {
                    const wbwVerse = this.wbwData[verse.k];
                    if (wbwVerse && wbwVerse[wbwIdx]) {
                        span.dataset.wbw = wbwVerse[wbwIdx];
                        span.classList.add('word-has-wbw');
                    }
                    wbwIdx++;
                }

                textEl.appendChild(span);

                // Space between words
                if (i < verse.words.length - 1) {
                    textEl.appendChild(document.createTextNode(' '));
                }
            });

            // Inline verse number at end (clickable for tafsir)
            const numEl = document.createElement('span');
            numEl.className = 'verse-num';
            numEl.textContent = verse.a;
            numEl.title = 'اضغط لعرض التفسير';
            numEl.addEventListener('click', (e) => {
                e.stopPropagation();
                this.toggleTafsir(verse.k, el);
            });
            textEl.appendChild(document.createTextNode(' '));
            textEl.appendChild(numEl);

            el.appendChild(textEl);

            // Thematic indicators (colored dots per macro-group)
            const fams = this.verseFamilies[verse.k];
            if (fams && fams.size > 0) {
                const themeRow = document.createElement('div');
                themeRow.className = 'verse-themes';
                const groupsSeen = new Set();
                for (const fid of fams) {
                    const gIdx = this.THEME_GROUPS[fid];
                    if (gIdx !== undefined && !groupsSeen.has(gIdx)) {
                        groupsSeen.add(gIdx);
                        const dot = document.createElement('span');
                        dot.className = 'theme-dot';
                        dot.dataset.group = gIdx;
                        dot.title = `${this.GROUP_NAMES[gIdx]} — ${this.GROUP_NAMES_EN[gIdx]}`;
                        themeRow.appendChild(dot);
                    }
                }
                el.appendChild(themeRow);
            }

            // Translation row
            if (this.showTranslation && this.translationData) {
                const transText = this.translationData[verse.k];
                if (transText) {
                    const transEl = document.createElement('div');
                    transEl.className = 'verse-translation';
                    transEl.textContent = transText;
                    el.appendChild(transEl);
                }
            }

            // Transliteration row
            if (this.showTranslit && this.translitData) {
                const tlText = this.translitData[verse.k];
                if (tlText) {
                    const tlEl = document.createElement('div');
                    tlEl.className = 'verse-transliteration';
                    tlEl.innerHTML = tlText;  // HTML with <u>/<b> tags
                    el.appendChild(tlEl);
                }
            }

            container.appendChild(el);
        });

        // Surah theme summary bar
        this.renderSurahThemes();
        this.renderSurahInsight();

        // Re-highlight if root is selected
        if (this.selectedRoot) {
            this.highlightRoot(this.selectedRoot);
        }
    },

    // ── Word Click ─────────────────────────────────
    onWordClick(root, wordEl) {
        this.selectedRoot = root;
        this.showAllVerses = false;

        // Clear previous selection
        document.querySelectorAll('.word-selected').forEach(el => {
            el.classList.remove('word-selected');
        });
        wordEl.classList.add('word-selected');

        // Highlight all words with this root
        this.highlightRoot(root);

        // Show panel
        this.openPanel(root);
    },

    highlightRoot(root) {
        // Clear old highlights
        document.querySelectorAll('.word-highlighted').forEach(el => {
            el.classList.remove('word-highlighted');
        });

        // Highlight words with matching root
        document.querySelectorAll(`.word-has-root[data-root="${root}"]`).forEach(el => {
            if (!el.classList.contains('word-selected')) {
                el.classList.add('word-highlighted');
            }
        });
    },

    // ── Panel ──────────────────────────────────────
    openPanel(root) {
        const data = this.rootsIndex[root];
        if (!data) return;

        const panel = this.$('root-panel');

        // Root display
        // Show individual letters separated by dots (traditional Arabic root display)
        const letters = root.split('');
        this.$('root-display').textContent = letters.join(' ');
        this.$('root-buckwalter').textContent = data.b || '';
        this.$('root-gloss').textContent = this.extractGloss(data.m || '');
        this.$('root-meaning').textContent = data.m || 'No meaning available';
        this.$('root-meaning').style.display = data.m ? 'block' : 'none';
        this.$('root-frequency').textContent = `Appears in ${data.f} verses across the Quran`;

        // Makki / Madani revelation breakdown
        this.renderRevelation(data);

        // Mufradat (classical lexicon)
        this.renderMufradat(root);

        // Furuq (linguistic distinctions)
        this.renderFuruq(root);

        // Family info
        const fam = data.fam ? (Array.isArray(data.fam) ? data.fam : [data.fam]) : [];
        this.renderFamilyInfo(root, fam);

        // Co-occurring roots
        this.renderCooccurrence(root);

        // Connected verses
        this.renderConnectedVerses(root);

        // Connected hadiths
        this.renderHadithBridge(root);

        // Reset accordion: collapse all except connected verses
        ['mufradat-section', 'furuq-section', 'root-family-section', 'cooccur-section'].forEach(id => {
            const sec = this.$(id);
            sec.classList.add('collapsed');
            const ch = sec.querySelector('.chevron');
            if (ch) ch.textContent = '\u25B8';
        });
        const connSec = this.$('connected-section');
        if (connSec) {
            connSec.classList.remove('collapsed');
            const ch = connSec.querySelector('.chevron');
            if (ch) ch.textContent = '\u25BE';
        }

        // Show panel
        panel.classList.add('panel-visible');
        document.body.classList.add('panel-open');

        // Mobile: show backdrop + lock body scroll
        if (this.isMobile) {
            const backdrop = this.$('panel-backdrop');
            backdrop.style.display = 'block';
            requestAnimationFrame(() => backdrop.classList.add('visible'));
            document.body.style.overflow = 'hidden';
        }
    },

    renderRevelation(data) {
        const el = this.$('root-revelation');
        const verses = data.v || [];
        if (verses.length === 0) { el.innerHTML = ''; return; }

        // Build surah→place lookup from surahList
        const placeMap = {};
        for (const s of this.surahList) placeMap[s.number] = s.revelation_place;

        let makki = 0, madani = 0;
        const seen = new Set();
        for (const vk of verses) {
            const sn = parseInt(vk.split(':')[0], 10);
            if (!seen.has(sn + ':' + vk)) {
                if (placeMap[sn] === 'makkah') makki++;
                else madani++;
            }
        }
        const total = makki + madani;
        const mPct = Math.round((makki / total) * 100);
        const dPct = 100 - mPct;

        // Determine dominant label
        let label = '';
        if (mPct >= 85) label = 'Mostly Makki';
        else if (dPct >= 85) label = 'Mostly Madani';
        else if (mPct >= 60) label = 'Leans Makki';
        else if (dPct >= 60) label = 'Leans Madani';
        else label = 'Mixed';

        el.innerHTML =
            `<span class="rev-label makki">Makki ${mPct}%</span>` +
            `<span class="rev-bar"><span class="rev-makki" style="width:${mPct}%"></span>` +
            `<span class="rev-madani" style="width:${dPct}%"></span></span>` +
            `<span class="rev-label madani">${dPct}% Madani</span>`;
    },

    renderMufradat(root) {
        const section = this.$('mufradat-section');
        const entry = this.mufradat[root];

        if (!entry) {
            section.style.display = 'none';
            this._mufradatVerses = null;
            return;
        }

        section.style.display = 'block';

        // Store cited verses for cross-referencing in connected verses
        this._mufradatVerses = entry.v ? new Set(entry.v) : null;

        // Original root heading from al-Raghib
        this.$('mufradat-root').textContent = entry.r !== root
            ? `${entry.r} (${root})`
            : entry.r;

        // Definition text
        this.$('mufradat-text').textContent = entry.t;

        // Verse references with cross-reference stats
        const versesEl = this.$('mufradat-verses');
        versesEl.innerHTML = '';

        if (entry.v && entry.v.length) {
            const rootData = this.rootsIndex[root];
            const totalVerses = rootData ? (rootData.v ? rootData.v.length : rootData.f || 0) : 0;

            const label = document.createElement('span');
            label.className = 'mufradat-verses-label';
            label.textContent = `الآيات المذكورة — al-Raghib cited ${entry.vc} of ${totalVerses} verses`;
            versesEl.appendChild(label);

            const chips = document.createElement('div');
            chips.className = 'mufradat-verse-chips';
            entry.v.forEach(vk => {
                const chip = document.createElement('a');
                chip.className = 'mufradat-verse-chip';
                chip.href = `#${vk}`;
                chip.textContent = vk;
                chip.addEventListener('click', (e) => {
                    e.preventDefault();
                    this.navigateToVerse(vk);
                });
                chips.appendChild(chip);
            });
            if (entry.vc > entry.v.length) {
                const more = document.createElement('span');
                more.className = 'mufradat-verse-more';
                more.textContent = `+${entry.vc - entry.v.length}`;
                chips.appendChild(more);
            }
            versesEl.appendChild(chips);
        }
    },

    renderFuruq(root) {
        const section = this.$('furuq-section');
        const list = this.$('furuq-list');
        list.innerHTML = '';

        // Search for pairs where either word contains or matches the root letters
        const rootLetters = root.split('');
        const matches = this.furuq.filter(pair => {
            return this.rootInWord(rootLetters, pair.a) || this.rootInWord(rootLetters, pair.b);
        });

        if (!matches.length) {
            section.style.display = 'none';
            return;
        }

        section.style.display = 'block';

        matches.slice(0, 5).forEach(pair => {
            const card = document.createElement('div');
            card.className = 'furuq-card';

            const header = document.createElement('div');
            header.className = 'furuq-pair';
            header.textContent = `${pair.a} — ${pair.b}`;

            const text = document.createElement('div');
            text.className = 'furuq-text';
            text.textContent = pair.t.length > 200
                ? pair.t.slice(0, 200) + '…'
                : pair.t;

            card.appendChild(header);
            card.appendChild(text);
            list.appendChild(card);
        });

        if (matches.length > 5) {
            const more = document.createElement('div');
            more.className = 'furuq-more';
            more.textContent = `+${matches.length - 5} more distinctions`;
            list.appendChild(more);
        }
    },

    rootInWord(rootLetters, word) {
        if (!word) return false;
        // Check if root letters appear in order within the word
        let idx = 0;
        for (const ch of word) {
            if (ch === rootLetters[idx]) {
                idx++;
                if (idx === rootLetters.length) return true;
            }
        }
        return false;
    },

    closePanel() {
        const panel = this.$('root-panel');
        panel.classList.remove('panel-visible');
        panel.style.transform = '';
        panel.style.transition = '';
        document.body.classList.remove('panel-open');

        // Hide backdrop + unlock body scroll
        const backdrop = this.$('panel-backdrop');
        backdrop.classList.remove('visible');
        setTimeout(() => { backdrop.style.display = 'none'; }, 300);
        document.body.style.overflow = '';

        // Clear highlights
        document.querySelectorAll('.word-selected, .word-highlighted').forEach(el => {
            el.classList.remove('word-selected', 'word-highlighted');
        });
        this.selectedRoot = null;
    },

    renderFamilyInfo(root, familyIds) {
        const section = this.$('root-family-section');

        if (!familyIds.length) {
            section.style.display = 'none';
            return;
        }

        section.style.display = 'block';
        const container = this.$('root-family-info');
        container.innerHTML = '';

        familyIds.forEach(fid => {
            const fam = this.families[fid];
            if (!fam) return;

            const div = document.createElement('div');
            div.style.marginBottom = '12px';

            // Badge
            const badge = document.createElement('span');
            badge.className = 'family-badge';
            badge.textContent = fam.name_ar;
            div.appendChild(badge);

            // Meaning
            const meaning = document.createElement('div');
            meaning.className = 'family-meaning';
            meaning.textContent = fam.meaning;
            div.appendChild(meaning);

            // Related roots
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
                    this.openPanel(r);
                    this.highlightRoot(r);
                });
                chips.appendChild(chip);
            });

            div.appendChild(chips);
            container.appendChild(div);
        });
    },

    renderCooccurrence(root) {
        const section = this.$('cooccur-section');
        const list = this.$('cooccur-list');
        list.innerHTML = '';

        const coroots = this.getCooccurringRoots(root);
        if (!coroots.length) {
            section.style.display = 'none';
            return;
        }

        section.style.display = 'block';
        const totalVerses = this.rootsIndex[root]?.v?.length || 1;

        coroots.forEach(cr => {
            const item = document.createElement('div');
            item.className = 'cooccur-item';

            const pct = Math.round((cr.shared / totalVerses) * 100);

            // Clickable root chip
            const rootEl = document.createElement('a');
            rootEl.className = 'cooccur-root';
            rootEl.href = '#';
            rootEl.textContent = cr.root.split('').join(' ');
            rootEl.addEventListener('click', (e) => {
                e.preventDefault();
                this.selectedRoot = cr.root;
                this.showAllVerses = false;
                this.openPanel(cr.root);
                this.highlightRoot(cr.root);
            });

            // Info line
            const info = document.createElement('div');
            info.className = 'cooccur-info';
            const bwSpan = cr.bw ? `<span class="cooccur-bw">${cr.bw}</span> ` : '';
            info.innerHTML = `${bwSpan}<span class="cooccur-meaning">${cr.meaning}</span>`;

            // Bar
            const bar = document.createElement('div');
            bar.className = 'cooccur-bar-wrap';
            bar.innerHTML = `<div class="cooccur-bar" style="width:${pct}%"></div>` +
                `<span class="cooccur-stat">${cr.shared} shared · ${pct}%</span>`;

            item.appendChild(rootEl);
            item.appendChild(info);
            item.appendChild(bar);
            list.appendChild(item);
        });
    },

    renderConnectedVerses(root) {
        const data = this.rootsIndex[root];
        if (!data) return;

        const container = this.$('connected-verses');
        container.innerHTML = '';

        const verseKeys = data.v;
        const limit = this.showAllVerses ? verseKeys.length : this.PREVIEW_LIMIT;
        const shown = verseKeys.slice(0, limit);

        // Group by surah
        const groups = {};
        shown.forEach(vk => {
            const surahNum = parseInt(vk.split(':')[0]);
            if (!groups[surahNum]) groups[surahNum] = [];
            groups[surahNum].push(vk);
        });

        Object.keys(groups).sort((a, b) => a - b).forEach(surahNum => {
            const surahInfo = this.surahList[surahNum - 1];
            const group = document.createElement('div');
            group.className = 'connected-group';

            const header = document.createElement('div');
            header.className = 'connected-group-header';
            header.textContent = `${surahInfo.name_en} (${surahInfo.name_ar})`;
            group.appendChild(header);

            groups[surahNum].forEach(vk => {
                const text = this.versesText[vk] || '';
                const verseEl = document.createElement('a');
                verseEl.className = 'connected-verse';
                verseEl.href = `#${vk}`;

                // Mark verses cited by al-Raghib in Mufradat
                if (this._mufradatVerses && this._mufradatVerses.has(vk)) {
                    verseEl.classList.add('raghib-cited');
                }

                const keyEl = document.createElement('span');
                keyEl.className = 'connected-verse-key';
                keyEl.textContent = vk;

                const textEl = document.createElement('div');
                textEl.className = 'connected-verse-text';
                textEl.textContent = text;

                verseEl.appendChild(keyEl);
                verseEl.appendChild(textEl);

                verseEl.addEventListener('click', (e) => {
                    e.preventDefault();
                    this.navigateToVerse(vk);
                });

                group.appendChild(verseEl);
            });

            container.appendChild(group);
        });

        // Update header count (target .section-title to preserve chevron + desc)
        const titleEl = this.$('connected-header').querySelector('.section-title');
        if (titleEl) titleEl.textContent = `الآيات المتصلة — ${verseKeys.length} Connected Verses`;

        // Show more button
        const btn = this.$('show-more-btn');
        if (verseKeys.length > this.PREVIEW_LIMIT && !this.showAllVerses) {
            btn.style.display = 'block';
            btn.textContent = `عرض الكل (${verseKeys.length} آية)`;
        } else {
            btn.style.display = 'none';
        }
    },

    async loadHadithBridge() {
        if (this.hadithBridge) return;
        try {
            const r = await fetch('hadith-data/hadith_bridge_summary.json');
            this.hadithBridge = await r.json();
        } catch (e) {
            this.hadithBridge = {};
        }
    },

    async renderHadithBridge(root) {
        const section = this.$('hadith-section');
        const container = this.$('hadith-bridge-content');
        section.style.display = 'none';
        container.innerHTML = '';

        await this.loadHadithBridge();

        const entry = this.hadithBridge[root];
        if (!entry || !entry.n) return;

        section.style.display = 'block';

        // Header count
        const titleEl = section.querySelector('.section-title');
        if (titleEl) titleEl.textContent = `الأحاديث المتصلة — ${entry.n} Connected Hadiths`;

        // Book breakdown
        if (entry.b) {
            const sorted = Object.entries(entry.b).sort((a, b) => b[1] - a[1]);
            const total = entry.n;
            sorted.forEach(([book, count]) => {
                const row = document.createElement('div');
                row.className = 'hadith-book-row';

                const label = document.createElement('span');
                label.className = 'hadith-book-name';
                label.textContent = book;

                const badge = document.createElement('span');
                badge.className = 'hadith-count-badge';
                badge.textContent = count;

                const bar = document.createElement('div');
                bar.className = 'hadith-book-bar';
                bar.style.width = Math.round((count / total) * 100) + '%';

                row.appendChild(label);
                row.appendChild(badge);
                container.appendChild(row);
                container.appendChild(bar);
            });
        }

        // Link to hadith reader
        const link = document.createElement('a');
        link.className = 'hadith-reader-link';
        const enc = encodeURIComponent(root);
        link.href = `../app/?root=${enc}`;
        link.target = '_blank';
        link.textContent = `← بحث في ${entry.n} حديث`;
        container.appendChild(link);
    },

    // ── Verse Families (thematic computation) ──────
    buildVerseFamilies() {
        const vf = {};
        for (const [root, data] of Object.entries(this.rootsIndex)) {
            if (!data.fam) continue;
            const fams = Array.isArray(data.fam) ? data.fam : [data.fam];
            if (!fams.length) continue;
            for (const vk of data.v) {
                if (!vf[vk]) vf[vk] = new Set();
                for (const f of fams) {
                    if (this.families[f]) vf[vk].add(f);
                }
            }
        }
        this.verseFamilies = vf;

        // Pre-compute Quran-wide family rates (for distinctiveness scoring)
        const totalVerses = Object.keys(this.versesText).length || 6236;
        const globalCounts = {};
        for (const fams of Object.values(vf)) {
            for (const f of fams) globalCounts[f] = (globalCounts[f] || 0) + 1;
        }
        this.globalFamilyRates = {};
        for (const [f, c] of Object.entries(globalCounts)) {
            this.globalFamilyRates[f] = c / totalVerses;
        }
    },

    renderSurahThemes() {
        const bar = this.$('surah-themes');
        const surahData = this.surahCache[this.currentSurah];
        if (!surahData) { bar.style.display = 'none'; return; }

        const familyCounts = {};
        surahData.forEach(verse => {
            const fams = this.verseFamilies[verse.k];
            if (!fams) return;
            for (const f of fams) {
                familyCounts[f] = (familyCounts[f] || 0) + 1;
            }
        });

        const sorted = Object.entries(familyCounts)
            .sort((a, b) => b[1] - a[1])
            .slice(0, 8);

        if (!sorted.length) { bar.style.display = 'none'; return; }

        bar.style.display = 'flex';
        bar.innerHTML = '';

        sorted.forEach(([fid, count]) => {
            const fam = this.families[fid];
            const gIdx = this.THEME_GROUPS[fid];
            const chip = document.createElement('span');
            chip.className = 'surah-theme-chip';
            chip.style.borderColor = `var(--group-${gIdx})`;
            chip.style.color = `var(--group-${gIdx})`;
            chip.textContent = fam.name_ar;
            chip.title = `${fam.meaning} (${count} verses)`;
            bar.appendChild(chip);
        });
    },

    // ── Surah Thematic Insight ─────────────────────
    renderSurahInsight() {
        const el = this.$('surah-insight');
        const surahData = this.surahCache[this.currentSurah];
        if (!surahData || !this.globalFamilyRates) { el.style.display = 'none'; return; }

        const totalV = surahData.length;

        // Count families in this surah
        const famCounts = {};
        surahData.forEach(verse => {
            const fams = this.verseFamilies[verse.k];
            if (!fams) return;
            for (const f of fams) famCounts[f] = (famCounts[f] || 0) + 1;
        });

        if (!Object.keys(famCounts).length) { el.style.display = 'none'; return; }

        // Aggregate by macro-group (count unique verses per group, not family hits)
        const groupVerses = [new Set(), new Set(), new Set(), new Set(), new Set(), new Set()];
        surahData.forEach(verse => {
            const fams = this.verseFamilies[verse.k];
            if (!fams) return;
            for (const f of fams) {
                const g = this.THEME_GROUPS[f];
                if (g !== undefined) groupVerses[g].add(verse.k);
            }
        });
        const groupTotals = groupVerses.map(s => s.size);
        const maxGroupIdx = groupTotals.indexOf(Math.max(...groupTotals));
        const groupPct = Math.round((groupTotals[maxGroupIdx] / totalV) * 100);

        // Find distinctive families: surah rate vs Quran-wide rate
        const distinctive = [];
        for (const [f, c] of Object.entries(famCounts)) {
            const surahRate = c / totalV;
            const globalRate = this.globalFamilyRates[f] || 0.01;
            const ratio = surahRate / globalRate;
            if (ratio > 1.5 && c >= 3) {
                distinctive.push({ id: f, ratio, count: c });
            }
        }
        distinctive.sort((a, b) => b.ratio - a.ratio);

        // Build insight text
        const dominantGroup = this.GROUP_NAMES[maxGroupIdx];
        const dominantGroupEn = this.GROUP_NAMES_EN[maxGroupIdx];

        let html = `<span class="insight-label">السمة الغالبة: ${dominantGroup}</span>`;
        html += `<span dir="ltr" style="display:block">Primary register: <strong>${dominantGroupEn}</strong> — ${groupPct}% of verses</span>`;

        if (distinctive.length > 0) {
            const topDist = distinctive.slice(0, 3).map(d => {
                const fam = this.families[d.id];
                return `${fam.name_ar} (${d.ratio.toFixed(1)}×)`;
            });
            html += `<div class="insight-distinctive">المميّز: ${topDist.join(' · ')}</div>`;
        }

        el.innerHTML = html;
        el.style.display = 'block';
    },

    // ── Translation ─────────────────────────────────
    async loadTranslation() {
        if (this.translationLoaded) return;
        try {
            const [trans, wbw] = await Promise.all([
                fetch('data/translations/en.sahih.json').then(r => r.json()),
                fetch('data/translations/wbw.en.json').then(r => r.json()),
            ]);
            this.translationData = trans;
            this.wbwData = wbw;
            this.translationLoaded = true;
        } catch (err) {
            console.error('Translation load failed:', err);
        }
    },

    // ── Transliteration ────────────────────────────
    async loadTranslit() {
        if (this.translitLoaded) return;
        try {
            this.translitData = await fetch('data/translations/transliteration.json').then(r => r.json());
            this.translitLoaded = true;
        } catch (err) {
            console.error('Transliteration load failed:', err);
        }
    },

    // ── Tafsir ──────────────────────────────────────
    sanitizeTafsir(html) {
        const tmp = document.createElement('div');
        tmp.innerHTML = html;
        // Strip all tags except safe ones; keep safe attributes
        const allowedTags = new Set(['P', 'BR', 'B', 'STRONG', 'I', 'EM', 'SPAN', 'DIV']);
        const allowedAttrs = new Set(['lang', 'dir', 'class']);
        const walk = (node) => {
            [...node.childNodes].forEach(child => {
                if (child.nodeType === 1) { // Element
                    if (!allowedTags.has(child.tagName)) {
                        child.replaceWith(document.createTextNode(child.textContent));
                    } else {
                        // Keep only safe attributes
                        [...child.attributes].forEach(a => {
                            if (!allowedAttrs.has(a.name)) child.removeAttribute(a.name);
                        });
                        walk(child);
                    }
                }
            });
        };
        walk(tmp);
        return tmp.innerHTML;
    },

    async fetchTafsir(tafsirId, verseKey) {
        const cacheKey = `${tafsirId}:${verseKey}`;
        if (this.tafsirCache[cacheKey]) return this.tafsirCache[cacheKey];

        let raw;
        if (this.LOCAL_TAFSIRS.has(tafsirId)) {
            // Local tafsir — load per-surah file, cache entire surah
            const surah = verseKey.split(':')[0];
            const surahCacheKey = `${tafsirId}:${surah}`;
            if (!this.localTafsirCache[surahCacheKey]) {
                const resp = await fetch(`data/tafsirs/${tafsirId}/${surah}.json`);
                if (!resp.ok) throw new Error(`Local tafsir ${resp.status}`);
                this.localTafsirCache[surahCacheKey] = await resp.json();
            }
            const entry = this.localTafsirCache[surahCacheKey][verseKey];
            raw = entry?.text || '';
        } else {
            // API tafsir (Arabic + English)
            const [surah, ayah] = verseKey.split(':');
            const url = `${this.TAFSIR_API}/tafsirs/${tafsirId}/by_ayah/${surah}:${ayah}`;
            const resp = await fetch(url);
            if (!resp.ok) throw new Error(`API ${resp.status}`);
            const data = await resp.json();
            raw = data.tafsir?.text || '';
        }

        const clean = this.sanitizeTafsir(raw);
        this.tafsirCache[cacheKey] = clean;
        return clean;
    },

    toggleTafsir(verseKey, verseEl) {
        const existing = verseEl.querySelector('.tafsir-block');
        const numEl = verseEl.querySelector('.verse-num');

        if (existing) {
            // Close
            existing.classList.remove('open');
            numEl.classList.remove('tafsir-open');
            setTimeout(() => existing.remove(), 350);
            return;
        }

        // Open — create block
        const block = document.createElement('div');
        block.className = 'tafsir-block';
        block.innerHTML = `<div class="tafsir-inner"><div class="tafsir-loading">جارٍ تحميل التفسير...</div></div>`;
        verseEl.appendChild(block);
        numEl.classList.add('tafsir-open');

        // Trigger animation
        requestAnimationFrame(() => block.classList.add('open'));

        this.loadTafsirContent(verseKey, block);
    },

    async loadTafsirContent(verseKey, block) {
        try {
            const html = await this.fetchTafsir(this.activeTafsir, verseKey);
            this.renderTafsirBlock(verseKey, block, html);
        } catch (err) {
            const inner = block.querySelector('.tafsir-inner');
            inner.innerHTML = `
                <div class="tafsir-error">
                    تعذر تحميل التفسير
                    <br>
                    <button class="tafsir-retry" data-vk="${verseKey}">إعادة المحاولة</button>
                </div>`;
            inner.querySelector('.tafsir-retry').addEventListener('click', () => {
                inner.innerHTML = `<div class="tafsir-loading">جارٍ تحميل التفسير...</div>`;
                this.loadTafsirContent(verseKey, block);
            });
        }
    },

    // Determine text direction for a tafsir
    tafsirDir(id) {
        return this.TAFSIR_GROUPS['English']?.includes(typeof id === 'string' ? id : +id) ? 'ltr' : 'rtl';
    },

    renderTafsirBlock(verseKey, block, html) {
        const inner = block.querySelector('.tafsir-inner');
        const dir = this.tafsirDir(this.activeTafsir);

        // Build grouped selector options
        const opts = Object.entries(this.TAFSIR_GROUPS)
            .map(([group, ids]) => {
                const options = ids.map(id =>
                    `<option value="${id}"${String(id) === String(this.activeTafsir) ? ' selected' : ''}>${this.TAFSIRS[id]}</option>`
                ).join('');
                return `<optgroup label="${group}">${options}</optgroup>`;
            }).join('');

        inner.innerHTML = `
            <div class="tafsir-toolbar">
                <span class="tafsir-label">التفسير</span>
                <select class="tafsir-select">${opts}</select>
            </div>
            <div class="tafsir-text" dir="${dir}">${html || '<em>لا يتوفر تفسير لهذه الآية</em>'}</div>`;

        // Tafsir switch handler
        inner.querySelector('.tafsir-select').addEventListener('change', async (e) => {
            const val = e.target.value;
            const newId = isNaN(val) ? val : parseInt(val);
            this.activeTafsir = newId;
            const textEl = inner.querySelector('.tafsir-text');
            textEl.dir = this.tafsirDir(newId);
            textEl.innerHTML = `<div class="tafsir-loading">جارٍ تحميل التفسير...</div>`;
            try {
                const newHtml = await this.fetchTafsir(newId, verseKey);
                textEl.innerHTML = newHtml || '<em>لا يتوفر تفسير لهذه الآية</em>';
            } catch {
                textEl.innerHTML = `
                    <div class="tafsir-error">
                        تعذر تحميل التفسير
                        <br>
                        <button class="tafsir-retry">إعادة المحاولة</button>
                    </div>`;
                textEl.querySelector('.tafsir-retry').addEventListener('click', async () => {
                    textEl.innerHTML = `<div class="tafsir-loading">جارٍ تحميل التفسير...</div>`;
                    try {
                        const retryHtml = await this.fetchTafsir(newId, verseKey);
                        textEl.innerHTML = retryHtml || '<em>لا يتوفر تفسير لهذه الآية</em>';
                    } catch {
                        textEl.innerHTML = `<div class="tafsir-error">تعذر تحميل التفسير</div>`;
                    }
                });
            }
        });
    },

    // ── Root Search ──────────────────────────────────
    buildSearchIndex() {
        this.searchIndex = [];
        for (const [root, data] of Object.entries(this.rootsIndex)) {
            this.searchIndex.push({
                root,
                bw: (data.b || '').toLowerCase(),
                meaning: (data.m || '').toLowerCase(),
                freq: data.f || (data.v ? data.v.length : 0),
            });
        }
        // Pre-sort by frequency descending
        this.searchIndex.sort((a, b) => b.freq - a.freq);
    },

    // Build reverse index: verse → set of roots (for co-occurrence)
    buildVerseRootsIndex() {
        this.verseRoots = {};
        for (const [root, data] of Object.entries(this.rootsIndex)) {
            const verses = data.v || [];
            for (const vk of verses) {
                if (!this.verseRoots[vk]) this.verseRoots[vk] = new Set();
                this.verseRoots[vk].add(root);
            }
        }
    },

    // Compute top co-occurring roots for a given root
    getCooccurringRoots(root, topN = 8) {
        const data = this.rootsIndex[root];
        if (!data || !data.v) return [];

        const counts = {};
        for (const vk of data.v) {
            const siblings = this.verseRoots[vk];
            if (!siblings) continue;
            for (const r of siblings) {
                if (r !== root) counts[r] = (counts[r] || 0) + 1;
            }
        }

        // Filter: must share at least 2 verses and be < 90% overlap
        // (excludes أله which co-occurs with everything)
        const totalVerses = data.v.length;
        return Object.entries(counts)
            .filter(([, c]) => c >= 2 && c / totalVerses < 0.9)
            .sort((a, b) => b[1] - a[1])
            .slice(0, topN)
            .map(([r, c]) => ({
                root: r,
                shared: c,
                meaning: (this.rootsIndex[r]?.m || '').slice(0, 50),
                bw: this.rootsIndex[r]?.b || '',
                total: this.rootsIndex[r]?.v?.length || 0,
            }));
    },

    toggleSearch() {
        const input = this.$('root-search-input');
        const btn = this.$('root-search-btn');
        const modeBtn = this.$('search-mode-btn');
        this.searchOpen = !this.searchOpen;

        if (this.searchOpen) {
            input.style.display = 'block';
            modeBtn.style.display = 'inline-block';
            btn.classList.add('active');
            input.value = '';
            input.focus();
        } else {
            input.style.display = 'none';
            modeBtn.style.display = 'none';
            btn.classList.remove('active');
            this.$('root-search-results').style.display = 'none';
            this.searchHighlight = -1;
        }
    },

    onSearchInput(query) {
        const results = this.$('root-search-results');
        query = query.trim();

        if (!query) {
            results.style.display = 'none';
            this.searchHighlight = -1;
            return;
        }

        if (this.searchMode === 'furuq') {
            this.onFuruqSearch(query);
            return;
        }

        const isArabic = /[\u0600-\u06FF]/.test(query);
        const lq = query.toLowerCase();

        let matches;
        if (isArabic) {
            matches = this.searchIndex.filter(e => e.root.includes(query));
        } else {
            matches = this.searchIndex.filter(e =>
                e.bw.includes(lq) || e.meaning.includes(lq)
            );
        }

        matches = matches.slice(0, 15);
        this.searchHighlight = -1;
        this.renderSearchResults(matches, query);
    },

    onFuruqSearch(query) {
        const ul = this.$('root-search-results');
        const matches = this.furuq.filter(pair =>
            pair.a.includes(query) || pair.b.includes(query) || pair.t.includes(query)
        ).slice(0, 20);

        this.searchHighlight = -1;
        ul.innerHTML = '';

        if (!matches.length) {
            ul.innerHTML = '<li class="sr-empty">لا توجد فروق — No distinctions found</li>';
            ul.style.display = 'block';
            return;
        }

        matches.forEach((pair, i) => {
            const li = document.createElement('li');
            li.className = 'furuq-item';
            li.dataset.index = i;

            const header = document.createElement('div');
            header.className = 'furuq-pair-header';
            header.innerHTML = `<span>${pair.a}</span><span class="furuq-vs">vs</span><span>${pair.b}</span>`;

            const preview = document.createElement('div');
            preview.className = 'furuq-text-preview';
            preview.textContent = pair.t;

            const hint = document.createElement('div');
            hint.className = 'furuq-expand-hint';
            hint.textContent = 'click to expand';

            li.appendChild(header);
            li.appendChild(preview);
            li.appendChild(hint);

            li.addEventListener('click', () => li.classList.toggle('expanded'));
            li.addEventListener('mouseenter', () => {
                this.searchHighlight = i;
                this.updateSearchHighlight();
            });

            ul.appendChild(li);
        });

        ul.style.display = 'block';
    },

    renderSearchResults(matches, query) {
        const ul = this.$('root-search-results');
        ul.innerHTML = '';

        if (!matches.length) {
            ul.innerHTML = '<li class="sr-empty">No roots found</li>';
            ul.style.display = 'block';
            return;
        }

        matches.forEach((m, i) => {
            const li = document.createElement('li');
            li.dataset.index = i;
            li.dataset.root = m.root;

            const rootSpan = document.createElement('span');
            rootSpan.className = 'sr-root';
            rootSpan.textContent = m.root.split('').join(' ');

            const info = document.createElement('div');
            info.className = 'sr-info';

            const bw = document.createElement('div');
            bw.className = 'sr-bw';
            bw.textContent = m.bw;

            const meaning = document.createElement('div');
            meaning.className = 'sr-meaning';
            const data = this.rootsIndex[m.root];
            meaning.textContent = (data.m || '').slice(0, 80);

            info.appendChild(bw);
            info.appendChild(meaning);

            const count = document.createElement('span');
            count.className = 'sr-count';
            count.textContent = `${m.freq}v`;

            li.appendChild(rootSpan);
            li.appendChild(info);
            li.appendChild(count);

            li.addEventListener('click', () => this.onSearchSelect(m.root));
            li.addEventListener('mouseenter', () => {
                this.searchHighlight = i;
                this.updateSearchHighlight();
            });

            ul.appendChild(li);
        });

        ul.style.display = 'block';
    },

    updateSearchHighlight() {
        const sel = this.searchMode === 'furuq' ? 'li.furuq-item' : 'li[data-root]';
        const items = this.$('root-search-results').querySelectorAll(sel);
        items.forEach((li, i) => {
            li.classList.toggle('active', i === this.searchHighlight);
        });
    },

    onSearchSelect(root) {
        // Close search
        this.searchOpen = false;
        this.$('root-search-input').style.display = 'none';
        this.$('root-search-btn').classList.remove('active');
        this.$('root-search-results').style.display = 'none';
        this.searchHighlight = -1;

        // Open the root panel (reuses existing logic)
        this.selectedRoot = root;
        this.showAllVerses = false;
        this.openPanel(root);
        this.highlightRoot(root);
    },

    // ── Navigation ─────────────────────────────────
    async navigateToVerse(verseKey) {
        const [surah, ayah] = verseKey.split(':').map(Number);
        const crossSurah = surah !== this.currentSurah;
        this._navigating = true;
        await this.loadSurah(surah, true, true);
        location.hash = verseKey;

        // Wait for browser layout after large DOM render before scrolling
        await new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r)));
        this.scrollToVerse(verseKey, crossSurah);
        this._navigating = false;

        // Re-highlight current root in new surah
        if (this.selectedRoot) {
            this.highlightRoot(this.selectedRoot);
        }
    },

    scrollToVerse(verseKey, instant) {
        const el = document.getElementById(`v-${verseKey}`);
        if (el) {
            el.scrollIntoView({ behavior: instant ? 'instant' : 'smooth', block: 'center' });
            // Flash effect
            el.style.transition = 'background 0.3s';
            el.style.background = 'var(--gold-light)';
            setTimeout(() => { el.style.background = ''; }, 1500);
        }
    },

    extractGloss(meaning) {
        if (!meaning) return '';
        // Try to extract short gloss from meaning patterns:
        // "means to X" / "means \"X\"" / "primarily means X" / "centers on X"
        let m = meaning.replace(/\s+/g, ' ');
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
                let gloss = match[1].trim().replace(/["""]/g, '');
                if (gloss.length > 60) gloss = gloss.substring(0, 57) + '…';
                return gloss;
            }
        }
        // Fallback: first sentence, truncated
        const first = m.split(/[.!]/)[0];
        if (first.length > 60) return first.substring(0, 57) + '…';
        return first;
    },
};

// ── Start ──────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => App.init());
