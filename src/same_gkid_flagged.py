"""Re-examine the 358 same-gkid flagged. Split:
  - sparse: one profile has <3 chain neighbors -> disjoint was data-poverty,
    not different-person. Same gk_id authority => SAFE merge.
  - truly-disjoint: both well-attested, zero overlap => keep flagged (may be
    a wrong gk_id assignment or genuinely different people)."""
import json,re,pickle,sys
from collections import defaultdict
DIACR=re.compile(r'[ً-ْ]')
def norm(s):
    if not s: return ''
    s=DIACR.sub('',s); s=re.sub(r'[أإآا]','ا',s)
    return s.replace('ة','ه').replace('ـ','').replace('ى','ي').strip()
profs=json.load(open('savepoints/sanadset_final_20260416_213020.json',encoding='utf-8'))['profiles']
alive={pid:p for pid,p in profs.items() if isinstance(p,dict) and not p.get('_redirect_to')
       and not p.get('_kinship_placeholder') and not p.get('_abandoned')}
with open('savepoints/raw_chain_index.pkl','rb') as f: cidx=pickle.load(f)
chains=cidx['chains']; name_index=cidx['name_index']
def fp(name):
    n=norm(name); s=set()
    for ci,pos in name_index.get(n,[])[:400]:
        nm=chains[ci]['names']
        if pos>0: s.add(norm(nm[pos-1]))
        if pos<len(nm)-1: s.add(norm(nm[pos+1]))
    return s
gk2=defaultdict(list)
for pid,p in alive.items():
    if p.get('gk_rawy_id'): gk2[str(p['gk_rawy_id'])].append(pid)
clusters={g:v for g,v in gk2.items() if len(v)>1}
safe=[]; disjoint=[]
for g,pids in clusters.items():
    ranked=sorted(pids,key=lambda x:-(alive[x].get('frequency') or 0))
    canon=ranked[0]; cfp=fp(alive[canon].get('full_name') or canon)
    for d in ranked[1:]:
        dfp=fp(alive[d].get('full_name') or d)
        if len(cfp)<3 or len(dfp)<3:
            safe.append((d,canon,g,'sparse_same_gkid'))   # data-poverty; gk authority suffices
        elif len(cfp&dfp)>=1:
            safe.append((d,canon,g,'shared_neighbor'))
        else:
            disjoint.append((d,canon,g,len(cfp),len(dfp)))
print(f'same-gkid flagged -> SAFE (sparse/shared): {len(safe):,}')
print(f'same-gkid flagged -> truly disjoint (keep flagged): {len(disjoint):,}')
bycanon=defaultdict(list)
for d,c,g,r in safe: bycanon[c].append((d,g))
plans=[{'gkid':f'gkidflag_{ms[0][1]}','canonical':c,'canonical_name':alive[c].get('full_name') or c,
        'donors':[d for d,g in ms],'donor_names':[alive[d].get('full_name') or d for d,g in ms],
        'reasons':['same_gkid_sparse_safe']*len(ms)} for c,ms in bycanon.items()]
json.dump({'plans':plans},open('savepoints/gkid_flagged_clusters.json','w',encoding='utf-8'),ensure_ascii=False)
print(f'{len(plans)} clusters, {sum(len(p["donors"]) for p in plans)} donors')
print('\nSample truly-disjoint (kept flagged, may be wrong gk_id):')
for d,c,g,a,b in disjoint[:10]:
    print(f'  gk[{g}] [{(alive[d].get("full_name") or d)[:24]:24s}] ({b}n) vs [{(alive[c].get("full_name") or c)[:24]:24s}] ({a}n)')
