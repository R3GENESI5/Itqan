"""Vocabulary specific to the tafsir genre, which a general analyser misses.

Two closed sets dominate the words al-Alusi uses that Buckwalter's modern-MSA
dictionary has no entry for:

* the authorities he cites on almost every page (الزمخشري، سيبويه، قتادة …),
  which are proper names, and
* the technical vocabulary of tafsir and Arabic grammar (محذوف، مستأنفة،
  مصدرية …), which is used in a precise sense the everyday gloss would miss.

Both are finite and both are worth glossing properly, because they are exactly
the words a reader working through the Arabic will stop on.
"""

# Cited authorities → (gloss, root). Death dates are hijri, as al-Alusi gives them.
NAMES = {
    'سيبويه':    ('Sibawayh (d. 180), author of al-Kitab', None),
    'الخليل':    ('al-Khalil ibn Ahmad (d. 175), lexicographer', None),
    'الأخفش':    ('al-Akhfash (d. 215), grammarian', None),
    'الفراء':    ('al-Farra (d. 207), Kufan grammarian', None),
    'الكسائي':   ('al-Kisa’i (d. 189), reciter and grammarian', None),
    'المبرد':    ('al-Mubarrad (d. 285), grammarian', None),
    'الزجاج':    ('al-Zajjaj (d. 311), grammarian', None),
    'ابن جني':   ('Ibn Jinni (d. 392), grammarian', None),
    'الأنباري':  ('al-Anbari (d. 577), grammarian', None),
    'الزمخشري':  ('al-Zamakhshari (d. 538), author of al-Kashshaf', None),
    'الرازي':    ('Fakhr al-Din al-Razi (d. 606), author of Mafatih al-Ghayb', None),
    'البيضاوي':  ('al-Baydawi (d. 685), exegete', None),
    'النسفي':    ('al-Nasafi (d. 710), exegete', None),
    'أبو حيان':  ('Abu Hayyan (d. 745), author of al-Bahr al-Muhit', None),
    'الخفاجي':   ('al-Khafaji (d. 1069), glossator on al-Baydawi', None),
    'الطبرسي':   ('al-Tabrisi (d. 548), Shi’i exegete', None),
    'الواحدي':   ('al-Wahidi (d. 468), on the occasions of revelation', None),
    'الثعلبي':   ('al-Tha’labi (d. 427), exegete', None),
    'البغوي':    ('al-Baghawi (d. 516), exegete', None),
    'القرطبي':   ('al-Qurtubi (d. 671), exegete and jurist', None),
    'السيوطي':   ('al-Suyuti (d. 911), polymath and exegete', None),
    'الغزالي':   ('al-Ghazali (d. 505), theologian', None),
    'ابن عطية':  ('Ibn Atiyya (d. 542), exegete', None),
    'ابن كثير':  ('Ibn Kathir (d. 774), exegete and historian', None),
    'ابن عربي':  ('Ibn Arabi (d. 638), Sufi metaphysician', None),
    'ابن تيمية': ('Ibn Taymiyya (d. 728), jurist and theologian', None),
    # early authorities and transmitters
    'قتادة':     ('Qatada (d. 118), Successor and exegete', None),
    'مجاهد':     ('Mujahid (d. 104), Successor and exegete', None),
    'عكرمة':     ('Ikrima (d. 105), student of Ibn Abbas', None),
    'الضحاك':    ('al-Dahhak (d. 105), exegete', None),
    'السدي':     ('al-Suddi (d. 127), exegete', None),
    'الحسن':     ('al-Hasan al-Basri (d. 110), Successor', None),
    'الشعبي':    ('al-Sha’bi (d. 103), Successor', None),
    'عطاء':      ('Ata ibn Abi Rabah (d. 114), Successor', None),
    'جريج':      ('Ibn Jurayj (d. 150), transmitter', None),
    'الكلبي':    ('al-Kalbi (d. 146), exegete', None),
    'مقاتل':     ('Muqatil ibn Sulayman (d. 150), exegete', None),
    'الزهري':    ('al-Zuhri (d. 124), traditionist', None),
    'الربيع':    ('al-Rabi ibn Anas (d. 139), exegete', None),
    'زيد':       ('Zayd, transmitter', 'زيد'),
    # hadith collectors
    'البخاري':   ('al-Bukhari (d. 256), hadith collector', None),
    'مسلم':      ('Muslim (d. 261), hadith collector; a Muslim', 'سلم'),
    'الترمذي':   ('al-Tirmidhi (d. 279), hadith collector', None),
    'النسائي':   ('al-Nasa’i (d. 303), hadith collector', None),
    'البيهقي':   ('al-Bayhaqi (d. 458), hadith collector', None),
    'الطبراني':  ('al-Tabarani (d. 360), hadith collector', None),
    'الحاكم':    ('al-Hakim (d. 405), hadith collector', None),
    'مردويه':    ('Ibn Mardawayh (d. 410), hadith collector', None),
    'الدارقطني': ('al-Daraqutni (d. 385), hadith critic', None),
    'أحمد':      ('Ahmad ibn Hanbal (d. 241), jurist and traditionist', 'حمد'),
    'الطبري':    ('al-Tabari (d. 310), exegete and historian', None),
    # schools
    'الشافعي':   ('al-Shafi’i (d. 204), founder of the Shafi’i school', None),
    'أبو حنيفة': ('Abu Hanifa (d. 150), founder of the Hanafi school', None),
    'مالك':      ('Malik (d. 179), founder of the Maliki school; owner', 'ملك'),
    'المعتزلة':  ('the Mu’tazila, rationalist theologians', None),
    'الأشاعرة':  ('the Ash’aris, theological school', None),
    'الصوفية':   ('the Sufis', None),
}

# Technical vocabulary of tafsir, grammar and rhetoric → (gloss, root)
TERMS = {
    # ellipsis and syntax
    'محذوف':    ('elided, omitted (understood but unstated)', 'حذف'),
    'الحذف':    ('ellipsis, omission', 'حذف'),
    'مستأنفة':  ('resumptive (a new sentence, not linked to what precedes)', 'أنف'),
    'مستأنف':   ('resumptive', 'أنف'),
    'الاستئناف': ('resumption, starting a new clause', 'أنف'),
    'مصدرية':   ('masdar-forming (turns the clause into a verbal noun)', 'صدر'),
    'المصدرية': ('the masdar-forming particle', 'صدر'),
    'مصدر':     ('verbal noun, masdar', 'صدر'),
    'متعلق':    ('governed by, attached to', 'علق'),
    'خبر':      ('predicate; a report', 'خبر'),
    'مبتدأ':    ('subject of a nominal sentence', 'بدأ'),
    'حال':      ('circumstantial accusative (state)', 'حول'),
    'تمييز':    ('specifier (accusative of specification)', 'ميز'),
    'بدل':      ('appositive substitute', 'بدل'),
    'اشتمال':   ('inclusion (badal al-ishtimal)', 'شمل'),
    'عطف':      ('coordination, conjoining', 'عطف'),
    'معطوف':    ('coordinated, conjoined to', 'عطف'),
    'صفة':      ('adjective, attribute', 'وصف'),
    'موصوف':    ('the described, the qualified noun', 'وصف'),
    'ضمير':     ('pronoun', 'ضمر'),
    'مفعول':    ('object of a verb', 'فعل'),
    'فاعل':     ('agent, subject of a verb', 'فعل'),
    'إعراب':    ('case inflection, parsing', 'عرب'),
    'نصب':      ('accusative case', 'نصب'),
    'رفع':      ('nominative case', 'رفع'),
    'جر':       ('genitive case', 'جرر'),
    'جزم':      ('jussive mood', 'جزم'),
    'مسوق':     ('adduced, brought forward (for a purpose)', 'سوق'),
    'سياق':     ('context, the run of the passage', 'سوق'),
    'المشبه':   ('the likened (in a simile)', 'شبه'),
    'تشبيه':    ('simile, likening', 'شبه'),
    'استعارة':  ('metaphor', 'عور'),
    'مجاز':     ('figurative usage', 'جوز'),
    'حقيقة':    ('literal sense, reality', 'حقق'),
    'كناية':    ('metonymy, allusion', 'كني'),
    # reading and transmission
    'قرىء':     ('it was read (a variant reading)', 'قرأ'),
    'قرئ':      ('it was read (a variant reading)', 'قرأ'),
    'القراءة':  ('the reading, recitation', 'قرأ'),
    'قراءة':    ('a reading, recitation', 'قرأ'),
    'المصحف':   ('the written codex', 'صحف'),
    'أخرج':     ('he transmitted, reported', 'خرج'),
    'روي':      ('it was related', 'روي'),
    'مرفوع':    ('traced to the Prophet (of a report)', 'رفع'),
    'موقوف':    ('stopping at a Companion (of a report)', 'وقف'),
    'مرسل':     ('with a gap in the chain (of a report)', 'رسل'),
    'الإسناد':  ('the chain of transmission', 'سند'),
    # exegetical discourse
    'المتبادر': ('the sense that comes first to mind', 'بدر'),
    'الظاهر':   ('the apparent sense', 'ظهر'),
    'المراد':   ('what is meant, the intended sense', 'رود'),
    'القولين':  ('the two views', 'قول'),
    'الأقوال':  ('the views, opinions', 'قول'),
    'الوجه':    ('the aspect, the way of taking it', 'وجه'),
    'الأوجه':   ('the aspects, the possible readings', 'وجه'),
    'التأويل':  ('interpretation beyond the literal', 'أول'),
    'النظم':    ('the arrangement of the wording', 'نظم'),
    'الفائدة':  ('the point, the benefit of the wording', 'فيد'),
    'الجمهور':  ('the majority (of scholars)', 'جمهر'),
    'الأصح':    ('the sounder view', 'صحح'),
    'نسخ':      ('abrogation', 'نسخ'),
    'منسوخ':    ('abrogated', 'نسخ'),
    'ناسخ':     ('abrogating', 'نسخ'),
    'هاهنا':    ('here, in this place', None),
    'هاتيك':    ('those (f.)', None),
    'ثمة':      ('there; there is', None),
    'حينئذ':    ('at that point, then', 'حين'),
    'إذ ذاك':   ('at that time', None),
    'لاسيما':   ('especially', None),
    'فتأمل':    ('so reflect on this', 'أمل'),
    'انتهى':    ('end of quotation', 'نهي'),
    'اه':       ('end of quotation (abbrev.)', None),
    'جيء':      ('it was brought, was made to come', 'جيأ'),
    'المستتر':  ('the concealed (implicit) pronoun', 'ستر'),
    'مستتر':    ('concealed, implicit', 'ستر'),
    'نافية':    ('negating (of a particle)', 'نفي'),
    'النافية':  ('the negating particle', 'نفي'),
    'المفعولية': ('objecthood, being the object', 'فعل'),
    'الفعلين':  ('the two verbs', 'فعل'),
    'الاقتصار': ('restricting, confining to', 'قصر'),
    'الاهتداء': ('being guided', 'هدي'),
    'إيتاء':    ('the giving, bestowal', 'أتي'),
    'أوتوا':    ('they were given', 'أتي'),
    'الطاعات':  ('acts of obedience', 'طوع'),
    'خواص':     ('special properties; the elite', 'خصص'),
    'الصحاح':   ('the sound collections (of hadith)', 'صحح'),
    'التقدير':  ('the implied wording, the reconstruction', 'قدر'),
    'تقديره':   ('its implied wording is', 'قدر'),
    'الالتفات': ('shift of grammatical person (iltifat)', 'لفت'),
    'الإضافة':  ('genitive annexation (idafa)', 'ضيف'),
    'التنكير':  ('using the indefinite', 'نكر'),
    'التعريف':  ('using the definite article', 'عرف'),
    'الاستفهام': ('interrogation, the question form', 'فهم'),
    'التوكيد':  ('emphasis', 'وكد'),
    'المبالغة': ('intensification, hyperbole', 'بلغ'),
}


def build():
    out = {}
    out.update(NAMES)
    out.update(TERMS)
    return out


TABLE = build()

if __name__ == '__main__':
    print(len(TABLE), 'tafsir-specific entries')
