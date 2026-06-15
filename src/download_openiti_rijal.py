"""
download_openiti_rijal.py
=========================
Downloads classical rijal (narrator criticism) texts from OpenITI GitHub.
These are the primary sources for Phase 14: structured narrator parsing.

Texts downloaded:
  1. Tahdhib al-Kamal (al-Mizzi, d.742)      — Six Books narrator encyclopedia
  2. Mizan al-I'tidal (al-Dhahabi, d.748)     — Critical narrator assessments
  3. Al-Jarh wa al-Ta'dil (Ibn Abi Hatim, d.327) — Reliability evaluations
  4. Al-Thiqat (Ibn Hibban, d.354)             — Reliable narrator list
  5. Al-Kamil fi Du'afa (Ibn 'Adi, d.365)      — Weak narrator catalog
  6. Tarikh Baghdad (al-Khatib, d.463)         — Baghdad scholar biographies
  7. Tahdhib al-Tahdhib (Ibn Hajar, d.852)     — Condensed encyclopedia
  8. Taqrib al-Tahdhib (Ibn Hajar, d.852)      — Grading manual
  9. Al-Kawakib al-Nayyirat (Ibn al-Kayyal, d.939) — Mukhtalitun (ikhtilat) reference

Usage:
    python src/download_openiti_rijal.py
"""

import os, sys, urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "src" / "rijal_raw"
OUT.mkdir(exist_ok=True)

BASE = "https://raw.githubusercontent.com/OpenITI"

TEXTS = [
    {
        "id": "tahdhib_kamal",
        "title": "Tahdhib al-Kamal (al-Mizzi)",
        "url": f"{BASE}/0750AH/master/data/0742Mizzi/0742Mizzi.TahdhibKamal/0742Mizzi.TahdhibKamal.Shamela0003722-ara1.mARkdown",
        "filename": "tahdhib_kamal.txt",
    },
    {
        "id": "mizan_itidal",
        "title": "Mizan al-I'tidal (al-Dhahabi)",
        "url": f"{BASE}/0750AH/master/data/0748Dhahabi/0748Dhahabi.MizanIctidal/0748Dhahabi.MizanIctidal.JK001293BK1-ara1.mARkdown",
        "filename": "mizan_itidal.txt",
    },
    {
        "id": "jarh_tadil",
        "title": "Al-Jarh wa al-Ta'dil (Ibn Abi Hatim)",
        "url": f"{BASE}/0350AH/master/data/0327IbnAbiHatimRazi/0327IbnAbiHatimRazi.JarhWaTacdil/0327IbnAbiHatimRazi.JarhWaTacdil.Shamela0002170-ara1.completed",
        "filename": "jarh_tadil.txt",
    },
    {
        "id": "thiqat",
        "title": "Al-Thiqat (Ibn Hibban)",
        "url": f"{BASE}/0375AH/master/data/0354IbnHibbanBusti/0354IbnHibbanBusti.Thiqat/0354IbnHibbanBusti.Thiqat.Shamela0005816-ara1.completed",
        "filename": "thiqat.txt",
    },
    {
        "id": "kamil_duafa",
        "title": "Al-Kamil fi Du'afa (Ibn 'Adi)",
        "url": f"{BASE}/0375AH/master/data/0365IbnCadiJurjani/0365IbnCadiJurjani.KamilFiDucafa/0365IbnCadiJurjani.KamilFiDucafa.Shamela0012579-ara1.mARkdown",
        "filename": "kamil_duafa.txt",
    },
    {
        "id": "tarikh_baghdad",
        "title": "Tarikh Baghdad (al-Khatib al-Baghdadi)",
        "url": f"{BASE}/0475AH/master/data/0463KhatibBaghdadi/0463KhatibBaghdadi.TarikhBaghdad/0463KhatibBaghdadi.TarikhBaghdad.Shamela0000736-ara2.mARkdown",
        "filename": "tarikh_baghdad.txt",
    },
    {
        "id": "tahdhib_tahdhib",
        "title": "Tahdhib al-Tahdhib (Ibn Hajar)",
        "url": f"{BASE}/0875AH/master/data/0852IbnHajarCasqalani/0852IbnHajarCasqalani.TahdhibTahdhib/0852IbnHajarCasqalani.TahdhibTahdhib.JK000134-ara1.mARkdown",
        "filename": "tahdhib_tahdhib.txt",
    },
    {
        "id": "taqrib_tahdhib",
        "title": "Taqrib al-Tahdhib (Ibn Hajar)",
        "url": f"{BASE}/0875AH/master/data/0852IbnHajarCasqalani/0852IbnHajarCasqalani.TaqribTahdhib/0852IbnHajarCasqalani.TaqribTahdhib.JK000121-ara1.completed",
        "filename": "taqrib_tahdhib.txt",
    },
    {
        "id": "kawakib_nayyirat",
        "title": "Al-Kawakib al-Nayyirat (Ibn al-Kayyal)",
        "url": f"{BASE}/0950AH/master/data/0939IbnKayyal/0939IbnKayyal.KawakibNayyirat/0939IbnKayyal.KawakibNayyirat.Shamela0000309-ara1.mARkdown",
        "filename": "kawakib_nayyirat.txt",
    },
    {
        "id": "tarikh_kabir",
        "title": "التاريخ الكبير",
        "url": "https://raw.githubusercontent.com/OpenITI/0275AH/master/data/0256Bukhari/0256Bukhari.TarikhKabir/0256Bukhari.TarikhKabir.Shamela0000956-ara1.completed",
        "filename": "tarikh_kabir.txt",
    },
    {
        "id": "majruhin",
        "title": "كتاب المجروحين",
        "url": "https://raw.githubusercontent.com/OpenITI/0375AH/master/data/0354IbnHibbanBusti/0354IbnHibbanBusti.Majruhin/0354IbnHibbanBusti.Majruhin.Shia003101Vols-ara1.completed",
        "filename": "majruhin.txt",
    },
    {
        "id": "duafa_uqayli",
        "title": "الضعفاء الكبير",
        "url": "https://raw.githubusercontent.com/OpenITI/0325AH/master/data/0322AbuJacfarCuqayli/0322AbuJacfarCuqayli.DucafaKabir/0322AbuJacfarCuqayli.DucafaKabir.Shamela0013041-ara1.completed",
        "filename": "duafa_uqayli.txt",
    },
    {
        "id": "marifat_thiqat",
        "title": "معرفة الثقات من رجال أهل العلم والحديث ومن الضعفاء وذكر مذاهبهم وأخبارهم",
        "url": "https://raw.githubusercontent.com/OpenITI/0275AH/master/data/0261AbuHasanCijli/0261AbuHasanCijli.MacrifatThiqat/0261AbuHasanCijli.MacrifatThiqat.JK000497-ara1.mARkdown",
        "filename": "marifat_thiqat.txt",
    },
    {
        "id": "lisan_mizan",
        "title": "لسان الميزان",
        "url": "https://raw.githubusercontent.com/OpenITI/0875AH/master/data/0852IbnHajarCasqalani/0852IbnHajarCasqalani.LisanMizan/0852IbnHajarCasqalani.LisanMizan.Shamela0036357-ara1.mARkdown",
        "filename": "lisan_mizan.txt",
    },
    {
        "id": "kashif",
        "title": "الكاشف في معرفة من له رواية في كتب الستة",
        "url": "https://raw.githubusercontent.com/OpenITI/0750AH/master/data/0748Dhahabi/0748Dhahabi.Kashif/0748Dhahabi.Kashif.Shia003276Vols-ara1.mARkdown",
        "filename": "kashif.txt",
    },
    {
        "id": "mughni_duafa",
        "title": "المغني في الضعفاء",
        "url": "https://raw.githubusercontent.com/OpenITI/0750AH/master/data/0748Dhahabi/0748Dhahabi.MughniFiDucafa/0748Dhahabi.MughniFiDucafa.JK001307-ara1.mARkdown",
        "filename": "mughni_duafa.txt",
    },
    {
        "id": "siyar_alam_nubala",
        "title": "سير أعلام النبلاء",
        "url": "https://raw.githubusercontent.com/OpenITI/0750AH/master/data/0748Dhahabi/0748Dhahabi.SiyarAclamNubala/0748Dhahabi.SiyarAclamNubala.Shamela0010906-ara1.mARkdown",
        "filename": "siyar_alam_nubala.txt",
    },
    {
        "id": "duafa_nasai",
        "title": "الضعفاء والمتروكين",
        "url": "https://raw.githubusercontent.com/OpenITI/0325AH/master/data/0303Nasai/0303Nasai.DucafaWaMatrukin/0303Nasai.DucafaWaMatrukin.JK000509-ara2.completed",
        "filename": "duafa_nasai.txt",
    },
    {
        "id": "duafa_ibnjawzi",
        "title": "الضعفاء والمتروكون",
        "url": "https://raw.githubusercontent.com/OpenITI/0600AH/master/data/0597IbnJawzi/0597IbnJawzi.DucafaWaMatrukin/0597IbnJawzi.DucafaWaMatrukin.Shamela0005830-ara1.completed",
        "filename": "duafa_ibnjawzi.txt",
    },
    {
        "id": "tarikh_ibnmain",
        "title": "معرفة الرجال عن يحيى بن معين وفيه عن علي بن المديني وأبي بكر بن أبي شيبة ومحمد بن عبد الله بن نمير وغيرهم/ رواية أحمد بن محمد بن القاسم بن محرز",
        "url": "https://raw.githubusercontent.com/OpenITI/0250AH/master/data/0233YahyaIbnMacin/0233YahyaIbnMacin.MacrifatRijal/0233YahyaIbnMacin.MacrifatRijal.Shamela0000101-ara1",
        "filename": "tarikh_ibnmain.txt",
    },
    {
        "id": "tabaqat_kubra",
        "title": "الطبقات الكبرى",
        "url": "https://raw.githubusercontent.com/OpenITI/0250AH/master/data/0230IbnSacd/0230IbnSacd.TabaqatKubra/0230IbnSacd.TabaqatKubra.ShamAY0035884-ara1.mARkdown",
        "filename": "tabaqat_kubra.txt",
    },
    {
        "id": "thiqat_ibnshahin",
        "title": "تاريخ أسماء الثقات",
        "url": "https://raw.githubusercontent.com/OpenITI/0400AH/master/data/0385IbnShahin/0385IbnShahin.TarikhAsmaThiqat/0385IbnShahin.TarikhAsmaThiqat.JK000511-ara1.completed",
        "filename": "thiqat_ibnshahin.txt",
    },
    {
        "id": "tabaqat_mudallisin",
        "title": "طبقات المدلسين",
        "url": "https://raw.githubusercontent.com/OpenITI/0875AH/master/data/0852IbnHajarCasqalani/0852IbnHajarCasqalani.TacrifAhlTaqdis/0852IbnHajarCasqalani.TacrifAhlTaqdis.Shia003340BK1-ara1.mARkdown",
        "filename": "tabaqat_mudallisin.txt",
    },
    {
        "id": "marasil",
        "title": "المراسيل",
        "url": "https://raw.githubusercontent.com/OpenITI/0350AH/master/data/0327IbnAbiHatimRazi/0327IbnAbiHatimRazi.Marasil/0327IbnAbiHatimRazi.Marasil.JK000743-ara1",
        "filename": "marasil.txt",
    },
    {
        "id": "jami_tahsil",
        "title": "جامع التحصيل في أحكام المراسيل",
        "url": "https://raw.githubusercontent.com/OpenITI/0775AH/master/data/0761IbnKaykaldiCalai/0761IbnKaykaldiCalai.JamicTahsil/0761IbnKaykaldiCalai.JamicTahsil.Shamela0025864-ara1.completed",
        "filename": "jami_tahsil.txt",
    },
    {
        "id": "ightibat",
        "title": "الاغتباط بمن رمي من الرواة بالاختلاط",
        "url": "https://raw.githubusercontent.com/OpenITI/0850AH/master/data/0841BurhanDinSibtIbnCajami/0841BurhanDinSibtIbnCajami.Ightibat/0841BurhanDinSibtIbnCajami.Ightibat.Shamela0000130-ara1.completed",
        "filename": "ightibat.txt",
    },
    {
        "id": "ilal_daraqutni",
        "title": "العلل الواردة في الأحاديث النبوية.",
        "url": "https://raw.githubusercontent.com/OpenITI/0400AH/master/data/0385Daraqutni/0385Daraqutni.CilalWarida/0385Daraqutni.CilalWarida.Shamela0009082-ara1.completed",
        "filename": "ilal_daraqutni.txt",
    },
    {
        "id": "ilal_ibnabihatim",
        "title": "علل الحديث",
        "url": "https://raw.githubusercontent.com/OpenITI/0350AH/master/data/0327IbnAbiHatimRazi/0327IbnAbiHatimRazi.CilalHadith/0327IbnAbiHatimRazi.CilalHadith.JK000682-ara1",
        "filename": "ilal_ibnabihatim.txt",
    },
    {
        "id": "muqaddimat_ibnsalah",
        "title": "علوم الحديث",
        "url": "https://raw.githubusercontent.com/OpenITI/0650AH/master/data/0643IbnSalahShahrazuri/0643IbnSalahShahrazuri.MuqaddimatCulumHadith/0643IbnSalahShahrazuri.MuqaddimatCulumHadith.JK000537-ara1.completed",
        "filename": "muqaddimat_ibnsalah.txt",
    },
    {
        "id": "tadrib_rawi",
        "title": "تدريب الراوي في شرح تقريب النواوي",
        "url": "https://raw.githubusercontent.com/OpenITI/0925AH/master/data/0911Suyuti/0911Suyuti.TadribRawi/0911Suyuti.TadribRawi.JK000138-ara1",
        "filename": "tadrib_rawi.txt",
    },
    {
        "id": "fath_mughith",
        "title": "فتح المغيث شرح ألفية الحديث",
        "url": "https://raw.githubusercontent.com/OpenITI/0925AH/master/data/0902Sakhawi/0902Sakhawi.FathMughith/0902Sakhawi.FathMughith.JK006675-ara1",
        "filename": "fath_mughith.txt",
    },
    {
        "id": "kifaya_riwaya",
        "title": "الكفاية في علم الرواية",
        "url": "https://raw.githubusercontent.com/OpenITI/0475AH/master/data/0463KhatibBaghdadi/0463KhatibBaghdadi.KifayaFiCilmRiwaya/0463KhatibBaghdadi.KifayaFiCilmRiwaya.JK000135-ara1",
        "filename": "kifaya_riwaya.txt",
    },
]


def download(text):
    dest = OUT / text["filename"]
    if dest.exists():
        size = dest.stat().st_size
        print(f"  [skip] {text['title']} — already downloaded ({size:,} bytes)")
        return True

    print(f"  [download] {text['title']}...")
    try:
        urllib.request.urlretrieve(text["url"], dest)
        size = dest.stat().st_size
        print(f"    -> {size:,} bytes saved to {dest.name}")
        return True
    except Exception as e:
        print(f"    [ERROR] {e}")
        return False


if __name__ == "__main__":
    print("Downloading OpenITI rijal texts...\n")
    ok, fail = 0, 0
    for t in TEXTS:
        if download(t):
            ok += 1
        else:
            fail += 1
    print(f"\nDone: {ok} downloaded, {fail} failed.")
