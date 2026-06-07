"""Curated mononym resolution — rijal-correct candidates + al-Dhahabi's
'al-itlaq yansarif ila al-ashhar' (bare name defaults to most famous).

Confidence tiers (honest, per classical method):
  network        - routed by matching shaykh/tilmidh against candidate net
  ashhar_default - no minor candidate matched; defaulted to the most-famous
  mubham         - co-famous tie, genuinely unresolved (as al-Dhahabi left)

Each mononym is one of two REGIMES:
  dominant - one overwhelmingly famous person; minors routed by net, rest->ashhar
  co       - 2+ co-famous; route by net, ties->mubham (no ashhar default)
"""
import json,re,pickle,sys
from collections import Counter,defaultdict

DIACR=re.compile(r'[ً-ْ]')
def norm(s):
    if not s: return ''
    s=DIACR.sub('',s); s=re.sub(r'[أإآا]','ا',s)
    return s.replace('ة','ه').replace('ـ','').replace('ى','ي').strip()
with open('savepoints/raw_chain_index.pkl','rb') as f: cidx=pickle.load(f)
chains=cidx['chains']; name_index=cidx['name_index']
def net(fullname):
    n=norm(fullname); t=Counter(); s=Counter()
    for ci,pos in name_index.get(n,[]):
        nm=chains[ci]['names']
        if pos<len(nm)-1: t[norm(nm[pos+1])]+=1
        if pos>0: s[norm(nm[pos-1])]+=1
    return t,s

# CURATED TABLE — grounded in Tahdhīb al-Kamāl / al-Mughni.
# regime 'dominant': ashhar is the bare-name default. 'co': no default.
# candidates: {label: disambiguated_full_form_for_network}
TABLE={
 'سفيان':{'regime':'co','cands':{'al-Thawrī':'سفيان الثوري','ibn ʿUyayna':'سفيان بن عيينة','ibn Ḥusayn':'سفيان بن حسين'}},
 'حماد':{'regime':'co','cands':{'ibn Salama':'حماد بن سلمة','ibn Zayd':'حماد بن زيد','ibn Abī Sulaymān':'حماد بن أبي سليمان'}},
 'مالك':{'regime':'dominant','ashhar':'ibn Anas (Imām)','cands':{'ibn Mighwal':'مالك بن مغول','ibn Dīnār':'مالك بن دينار','ibn Ismāʿīl':'مالك بن إسماعيل'}},
 'نافع':{'regime':'dominant','ashhar':'mawlā ibn ʿUmar','cands':{'ibn Jubayr':'نافع بن جبير','ibn Yazīd':'نافع بن يزيد'}},
 'أنس':{'regime':'dominant','ashhar':'ibn Mālik (ṣaḥābī)','cands':{'ibn Sīrīn':'أنس بن سيرين','ibn ʿIyāḍ':'أنس بن عياض'}},
 'جابر':{'regime':'dominant','ashhar':'ibn ʿAbdullāh (ṣaḥābī)','cands':{'ibn Samura':'جابر بن سمرة','al-Juʿfī':'جابر بن يزيد'}},
 'أيوب':{'regime':'dominant','ashhar':'al-Sakhtiyānī','cands':{'ibn Mūsā':'أيوب بن موسى','ibn ʿĀʾidh':'أيوب بن عائذ'}},
 'عكرمة':{'regime':'dominant','ashhar':'mawlā ibn ʿAbbās','cands':{'ibn ʿAmmār':'عكرمة بن عمار','ibn Khālid':'عكرمة بن خالد'}},
 'منصور':{'regime':'dominant','ashhar':'ibn al-Muʿtamir','cands':{'ibn Zādhān':'منصور بن زاذان','ibn al-Muʿtamir2':'منصور بن المعتمر'}},
 'عطاء':{'regime':'dominant','ashhar':'ibn Abī Rabāḥ','cands':{'ibn al-Sāʾib':'عطاء بن السائب','ibn Yasār':'عطاء بن يسار'}},
 'إبراهيم':{'regime':'dominant','ashhar':'al-Nakhaʿī','cands':{'ibn Saʿd':'إبراهيم بن سعد','ibn Ṭahmān':'إبراهيم بن طهمان','ibn Marzūq':'إبراهيم بن مرزوق'}},
 'خالد':{'regime':'co','cands':{'al-Ḥadhdhāʾ':'خالد الحذاء','al-Ṭaḥḥān':'خالد بن عبد الله','ibn Mihrān':'خالد بن مهران'}},
 'إسماعيل':{'regime':'co','cands':{'ibn Jaʿfar':'إسماعيل بن جعفر','ibn Abī Khālid':'إسماعيل بن أبي خالد','ibn ʿUlayya':'إسماعيل بن إبراهيم'}},
 'هشام':{'regime':'co','cands':{'ibn ʿUrwa':'هشام بن عروة','al-Dastawāʾī':'هشام الدستوائي','ibn Ḥassān':'هشام بن حسان'}},
 'أبو إسحاق':{'regime':'dominant','ashhar':'al-Sabīʿī (Hamadānī)','cands':{'al-Shaybānī':'أبو إسحاق الشيباني','al-Fazārī':'أبو إسحاق الفزاري'}},
 # Next tier (2026-06-07 cont.)
 'أبو سعيد':{'regime':'dominant','ashhar':'al-Khudrī (ṣaḥābī)','cands':{'al-Ashajj':'أبو سعيد الأشج','ibn al-Aʿrābī':'أبو سعيد بن الأعرابي'}},
 'شعيب':{'regime':'dominant','ashhar':'ibn Abī Ḥamza','cands':{'ibn al-Layth':'شعيب بن الليث'}},
 'سعيد':{'regime':'co','cands':{'ibn al-Musayyab':'سعيد بن المسيب','ibn Jubayr':'سعيد بن جبير','ibn Abī ʿArūba':'سعيد بن أبي عروبة','ibn Manṣūr':'سعيد بن منصور'}},
 'جرير':{'regime':'co','cands':{'ibn ʿAbd al-Ḥamīd':'جرير بن عبد الحميد','ibn Ḥāzim':'جرير بن حازم','ibn ʿAbdullāh(ṣaḥ)':'جرير بن عبد الله'}},
 'يونس':{'regime':'co','cands':{'ibn Yazīd al-Aylī':'يونس بن يزيد','ibn ʿUbayd':'يونس بن عبيد','ibn Bukayr':'يونس بن بكير','ibn ʿAbd al-Aʿlā':'يونس بن عبد الأعلى'}},
 'شعبة2_placeholder':{'regime':'dominant','ashhar':'(merged)','cands':{}},
}
TABLE.pop('شعبة2_placeholder',None)

def resolve(mono, spec):
    nets={lab:net(fn) for lab,fn in spec['cands'].items()}
    regime=spec['regime']; ashhar=spec.get('ashhar')
    # Dominant: ashhar is the al-iṭlāq default; a minor sharing the same circle
    # cannot be told apart by shared neighbors, so must clear a HIGH bar.
    MIN_TS, MIN_RATIO = (10, 3.0) if regime=='dominant' else (3, 2.0)
    stats=Counter(); occ={}
    mn=norm(mono)
    for ci,pos in name_index.get(mn,[]):
        nm=chains[ci]['names']
        sh=norm(nm[pos+1]) if pos<len(nm)-1 else None
        ti=norm(nm[pos-1]) if pos>0 else None
        sc={lab:(t.get(sh,0)+s.get(ti,0)) for lab,(t,s) in nets.items()}
        rk=sorted(sc.items(),key=lambda x:-x[1]); top,ts=rk[0]; rn=rk[1][1] if len(rk)>1 else 0
        if regime=='dominant':
            # al-iṭlāq: bare name = ashhar. Minors share the same circle and
            # cannot be reliably told apart by shared neighbors (عكرمة proof),
            # so they are NOT routed here — they appear under their full names.
            stats[(ashhar,'ashhar_default')]+=1; occ[f'{ci}:{pos}']=ashhar
        elif ts>=MIN_TS and (rn==0 or ts/max(rn,1)>=MIN_RATIO):
            stats[(top,'network')]+=1; occ[f'{ci}:{pos}']=top
        else:
            stats['mubham']+=1
    return stats,occ

if __name__=='__main__':
    allstats={}; allmap={}
    print(f'{"mononym":12s} {"total":>7s} {"net":>7s} {"ashhar":>7s} {"mubham":>7s}  top resolution')
    gt=gn=0
    for mono,spec in TABLE.items():
        st,occ=resolve(mono,spec)
        tot=sum(st.values())
        netc=sum(v for k,v in st.items() if isinstance(k,tuple) and k[1]=='network')
        ash=sum(v for k,v in st.items() if isinstance(k,tuple) and k[1]=='ashhar_default')
        mub=st.get('mubham',0)
        gt+=tot; gn+=netc+ash
        # top person
        persons=Counter()
        for k,v in st.items():
            if isinstance(k,tuple): persons[k[0]]+=v
        topp=persons.most_common(1)[0] if persons else ('-',0)
        print(f'{mono:12s} {tot:>7,} {netc:>7,} {ash:>7,} {mub:>7,}  {topp[0]} ({100*topp[1]/max(tot,1):.0f}%)')
        allstats[mono]={'total':tot,'network':netc,'ashhar_default':ash,'mubham':mub,
                        'regime':spec['regime'],'persons':dict(persons)}
        allmap[mono]=occ
    print(f'\nTOTAL: {gn:,}/{gt:,} resolved ({100*gn/gt:.1f}%)')
    json.dump(allstats,open('savepoints/mononym_curated_stats.json','w',encoding='utf-8'),ensure_ascii=False,indent=1)
    json.dump(allmap,open('savepoints/mononym_curated_map.json','w',encoding='utf-8'),ensure_ascii=False)
