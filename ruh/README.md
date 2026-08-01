# روح المعاني — Ruh al-Ma'ani

The complete Arabic text of Mahmud al-Alusi's tafsir (d. 1270 AH / 1854 CE),
with hover translation on every word and click-through to the Qur'anic root
apparatus already in `/quran/`.

Open at `ruh/index.html#<surah>:<ayah>` — e.g. `#29:2` — or browse with the
surah / passage selectors.

## How it works

**Text.** Scraped page by page from [al-Maktaba al-Shamela book 22835](https://shamela.ws/book/22835)
(Dar al-Kutub al-Ilmiyya edition, 16 vols, 6,473 pages). Shamela's own table of
contents brackets each passage by ayah range — `[سورة النور (٢٤) : الآيات ٢٩ إلى ٣٣]` —
which is what makes `#24:29` resolve to the right passage. Consecutive pages
sharing a section title are grouped into one reading unit.

**Glosses.** Every distinct word form in the book is analysed offline with the
Buckwalter/AraMorph morphological analyser, which segments it into
prefix + stem + suffix and returns an English gloss for each part — so
`والكفان` reads "and + the + palm of the hand + two". Three things improve on
the raw analyser output:

1. *Verb form.* Buckwalter marks form I — the plain triliteral — by carrying
   the imperfect vowel in the lemma id (`xaraj-u_1`, `jAz-u_1`, `kafar-i_1`),
   while derived forms never do (`xar~aj_1`, `>axoraj_1`, `kaf~ar_1`). Form I
   is the unmarked default, and preferring it is what stops `يجوز` reading as
   "marry off" instead of "be permissible", `خرج` as "oust" instead of "go
   out", and `كفروا` as "atone" instead of "disbelieve".

   The preference stops at verbs. Promoting form I above *nouns* as well was
   tempting and much worse: `اسم` became "poison" rather than "name", `رجل`
   "walk" rather than "man", `قلب` "overthrow" rather than "heart". Between a
   form I verb and a noun, corpus usage is the better judge.
2. *Corpus usage.* Each word form spreads its token count across the lemmas
   that could produce it, giving every lemma weight proportional to how often
   its forms actually occur. Counting only *unambiguous* forms looked stricter
   but was badly biased — a lemma whose every surface form happens to be
   ambiguous scored zero, which is precisely how جاز lost يجوز to the far rarer
   جوّز.
3. *Closed-class table* (`src/ruh/function_words.py`). Particles, prepositions
   and pronouns are finite, are the most frequent words on the page, and are
   exactly where a context-free analyser fails — `وقد` is equally well read as
   "and + already" or the verb "to ignite". These ~1,000 forms are composed
   mechanically from a small base rather than guessed.
4. *Genre vocabulary* (`src/ruh/tafsir_terms.py`). The authorities al-Alusi
   cites and the technical vocabulary of tafsir and grammar are two more closed
   sets that a modern-MSA dictionary has no entries for.
5. *Root derivation from the lemma*, never from the inflected surface form.
   Stemming the surface yields plausible-looking wrong roots (`والكفان` → كفن).
   A root is only attached when it exists in `/quran/data/roots_index.json`, so
   every clickable root has a real entry behind it.

**Two senses, not one.** Arabic script drops the vowels that would tell `ظهر`
("appear") from `ظَهْر` ("back"), and nothing in an isolated word can settle it.
Where the runner-up reading is genuinely different it is shown after a slash —
"back; spine / appear; emerge" — so the sentence decides rather than the
ranking being confidently wrong.

Coverage is about **98.8% of running words**. The remainder is mostly proper
names, poetry, and rare classical vocabulary — those words simply render
without a tooltip.

**Qur'anic quotations** are not tagged in the data. Al-Alusi's own prose is
unvocalised while quoted revelation is fully vocalised, so a run of two or more
vocalised words is detected at render time and coloured green.

## Data layout

| File | Contents |
|---|---|
| `data/index.json` | Surah list → passages (ayah range, volume, page), plus corpus stats |
| `data/lex.json` | Core lexicon: the most frequent word forms, `"gloss"` or `"gloss\|root"` |
| `data/sec/<surah>-<a1>.json` | One passage: its pages and paragraphs, plus a `lex` of the rarer forms it uses |
| `data/front.json` | خطبة المفسر — the author's introduction |

Splitting the lexicon this way means the common ~90% of words load once and the
long tail arrives with the passage that needs it, so no page waits on a
multi-megabyte dictionary.

## Rebuilding

```bash
python3 src/ruh/scrape.py                 # → pages.jsonl (resumable)
python3 src/ruh/build_data.py ruh/data    # → index.json, lex.json, sec/*.json
```

`scrape.py` is resumable: re-running it fetches only the pages missing from
`pages.jsonl`. `build_data.py` needs `pyaramorph` and `nltk`.

## Sources and licensing

- **Text**: روح المعاني في تفسير القرآن العظيم والسبع المثاني, Mahmud al-Alusi
  (d. 1270 AH). The work itself is public domain; the digital edition is from
  al-Maktaba al-Shamela.
- **Glosses**: derived from the Buckwalter Arabic Morphological Analyzer
  dictionaries, © 2002 QAMUS LLC and the Trustees of the University of
  Pennsylvania, released under **GPL-2.0**.

> **Note:** the rest of Itqan is MIT-licensed. Because the gloss data in
> `data/lex.json` and the `lex` blocks of `data/sec/*.json` is derived from
> GPL-2.0 dictionaries, those files carry GPL-2.0 rather than MIT. The reader
> code (`index.html`, `css/`, `js/`) is MIT like the rest of the project.
