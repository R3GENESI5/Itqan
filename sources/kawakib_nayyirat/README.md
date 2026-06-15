# al-Kawākib al-Nayyirāt fī maʿrifat man ikhtalaṭa min al-ruwāt al-thiqāt

**الكواكب النيرات في معرفة من اختلط من الرواة الثقات**

A classical *rijāl* (narrator-criticism) text cataloguing the **mukhtaliṭūn** —
otherwise-trustworthy ḥadīth narrators whose memory became confused (*ikhtilāṭ*)
late in life, so that their hearers must be sorted into those who heard them
*before* vs. *after* the deterioration. A core reference when a chain's defect is
age-related confusion rather than a narrator who is matrūk/fabricator.

- **Author:** Abū al-Barakāt Muḥammad b. Aḥmad b. Muḥammad al-Khaṭīb, Zayn al-Dīn
  Ibn al-Kayyāl (d. **939 AH** per OpenITI authority; the digitised Shamela
  header gives 929 AH — both appear in the literature).
- **Editor (taḥqīq):** ʿAbd al-Qayyūm ʿAbd Rabb al-Nabī
- **Publisher:** Dār al-Maʾmūn, Beirut — 1st ed., 1981 CE
- **Biographical entries (mukhtaliṭ narrators):** 121

## Provenance

Retrieved from the **OpenITI** corpus (Open Islamicate Texts Initiative), which
is itself sourced from al-Maktaba al-Shāmila book **#309**. Fetched from GitHub
raw (no scraping of the live, Cloudflare-gated Shamela site).

OpenITI author/book URI: `0939IbnKayyal.KawakibNayyirat`
Base: <https://raw.githubusercontent.com/OpenITI/0950AH/master/data/0939IbnKayyal/0939IbnKayyal.KawakibNayyirat/>

## Files

| File | Version / source | Notes |
|------|------------------|-------|
| `KawakibNayyirat.Shamela0000309.mARkdown` | **Primary** (`Shamela0000309-ara1`) | PRIMARY · CLEANED · MARKDOWN · PAGINATION. Canonical, structured OpenITI mARkdown. **Use this for ingestion.** |
| `al-Kawakib-al-Nayyirat_clean.txt` | derived from the primary | Human-readable: soft-wraps joined, OpenITI milestone (`msNN`) tokens stripped, entries as `###`, page refs as `【ج1 ص57】`. |
| `KawakibNayyirat.JK000500.txt` | alt ed. (`JK000500-ara1`, Dār al-ʿIlm) | for variant collation |
| `KawakibNayyirat.ShamAY0034617.txt` | alt ed. (`ShamAY0034617-ara1`, ط. العلم) | for variant collation |
| `KawakibNayyirat.Shia003352.txt` | alt ed. (`Shia003352-ara1`) | for variant collation |
| `clean.py` | script | regenerates `*_clean.txt` from the primary mARkdown |

## mARkdown markers (primary file)

- `#META# … #META#Header#End#` — bibliographic header block
- `### $ N - Name` — a biographical (narrator) entry
- `### | Title` — section heading
- `# …` paragraph start; `~~…` soft-wrap continuation of the previous line
- `PageVxxPyyy` — inline page marker (volume `xx`, page `yyy`)
- `msNN` — OpenITI every-~300-words milestone (not content)

## License

The underlying work is a classical/public-domain text. The digital edition is
distributed by OpenITI; reuse under the OpenITI corpus terms
(CC BY-SA / see <https://github.com/OpenITI>). Editorial apparatus of the 1981
Dār al-Maʾmūn print belongs to its editor/publisher.

## Regenerate

```bash
base="https://raw.githubusercontent.com/OpenITI/0950AH/master/data/0939IbnKayyal/0939IbnKayyal.KawakibNayyirat"
curl -L "$base/0939IbnKayyal.KawakibNayyirat.Shamela0000309-ara1.mARkdown" -o KawakibNayyirat.Shamela0000309.mARkdown
python3 clean.py
```
