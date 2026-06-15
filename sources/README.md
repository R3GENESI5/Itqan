# `sources/` — OpenITI classical rijāl / ḥadīth corpus

24 classical narrator-criticism, ʿilal, and muṣṭalaḥ texts pulled from the
**OpenITI** corpus (all corrected, non-OCR primary versions). Each book folder
contains:

- `<slug>.mARkdown` — primary OpenITI text (canonical, structured: `### $` entry
  markers, `PageVxxPyyy` page refs, `#META#` header). **Use for ingestion.**
- `<slug>_clean.txt` — readable plain-text.
- `<slug>.xlsx` — structured workbook (one row per narrator, or per page/section
  for treatises) + a `معلومات` provenance sheet.
- `README.md` — per-book provenance.

Regenerate everything from the primary files with `python sources/build_openiti_books.py`.
The same texts are wired into `src/download_openiti_rijal.py` (downloads into the
gitignored `src/rijal_raw/`).

> Text is reproduced **verbatim** from the OpenITI primary editions. The xlsx files
> do **not** infer grades — critic statements are quoted as-is, pages as `ج/ص`.

## Index

| Folder | Book | Author (d. AH) | xlsx rows | structure |
|--------|------|----------------|-----------|-----------|
| `kawakib_nayyirat` | al-Kawākib al-Nayyirāt (ikhtilāṭ) | Ibn al-Kayyāl (939) | 121 | per-narrator |
| `lisan_mizan_ibnhajar` | Lisān al-Mīzān | Ibn Ḥajar (852) | 15,521 | per-narrator |
| `tarikh_kabir_bukhari` | al-Tārīkh al-Kabīr | al-Bukhārī (256) | 13,958 | per-narrator |
| `kashif_dhahabi` | al-Kāshif | al-Dhahabī (748) | 8,267 | per-narrator |
| `mughni_duafa_dhahabi` | al-Mughnī fī al-Ḍuʿafāʾ | al-Dhahabī (748) | 7,851 | per-narrator |
| `siyar_dhahabi` | Siyar Aʿlām al-Nubalāʾ | al-Dhahabī (748) | 5,943 | per-narrator |
| `tabaqat_ibnsad` | al-Ṭabaqāt al-Kubrā | Ibn Saʿd (230) | 5,529 | per-narrator |
| `duafa_ibnjawzi` | al-Ḍuʿafāʾ wa-l-matrūkīn | Ibn al-Jawzī (597) | 4,013 | per-narrator |
| `marifat_thiqat_ijli` | Maʿrifat al-Thiqāt | al-ʿIjlī (261) | 2,369 | per-narrator |
| `duafa_uqayli` | al-Ḍuʿafāʾ al-Kabīr | al-ʿUqaylī (322) | 2,107 | per-narrator |
| `thiqat_ibnshahin` | Tārīkh asmāʾ al-Thiqāt | Ibn Shāhīn (385) | 1,651 | per-narrator |
| `majruhin_ibnhibban` | al-Majrūḥīn | Ibn Ḥibbān (354) | 1,278 | per-narrator |
| `jami_tahsil_alai` | Jāmiʿ al-Taḥṣīl (marāsīl) | al-ʿAlāʾī (761) | 1,116 | per-narrator |
| `duafa_nasai` | al-Ḍuʿafāʾ wa-l-matrūkīn | al-Nasāʾī (303) | 675 | per-narrator |
| `tabaqat_mudallisin_ibnhajar` | Ṭabaqāt al-Mudallisīn (tadlīs) | Ibn Ḥajar (852) | 151 | per-narrator |
| `ightibat_sibt_ibnajami` | al-Ightibāṭ (ikhtilāṭ) | Sibṭ Ibn al-ʿAjamī (841) | 123 | per-narrator |
| `fath_mughith_sakhawi` | Fatḥ al-Mughīth (muṣṭalaḥ) | al-Sakhāwī (902) | 1,164 | per-page |
| `ilal_ibnabihatim` | ʿIlal al-ḥadīth | Ibn Abī Ḥātim (327) | 927 | per-page |
| `tadrib_rawi_suyuti` | Tadrīb al-Rāwī (muṣṭalaḥ) | al-Suyūṭī (911) | 703 | per-page |
| `kifaya_khatib` | al-Kifāya (muṣṭalaḥ) | al-Khaṭīb (463) | 437 | per-page |
| `tarikh_ibnmain` | Maʿrifat al-Rijāl (Tārīkh Ibn Maʿīn) | Ibn Maʿīn (233) | 352 | per-page |
| `marasil_ibnabihatim` | al-Marāsīl | Ibn Abī Ḥātim (327) | 262 | per-page |
| `muqaddimat_ibnsalah` | Muqaddimat Ibn al-Ṣalāḥ (muṣṭalaḥ) | Ibn al-Ṣalāḥ (643) | 284 | per-section |
| `ilal_daraqutni` | al-ʿIlal al-Wārida | al-Dāraquṭnī (385) | 223 | per-section |

**Total:** 24 books · ~70,600 biographical narrator rows across the workbooks.
