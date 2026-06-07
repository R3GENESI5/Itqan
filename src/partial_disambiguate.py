"""Disambiguate partial-ambiguous names (short name = prefix of MULTIPLE
longer alive names) by chain fingerprint dominance.

A short name X with longer candidates {A,B,C}: if X's chain fingerprint
shares >=4 neighbors with exactly ONE candidate AND that candidate has
>=3x the runner-up's overlap, X IS that person -> merge. Else leave
(genuinely ambiguous or a distinct person).

This is SAFE: dominant fingerprint overlap to one specific fuller name is
strong same-person evidence (unlike kunya-ism where circles overlap)."""
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
    for ci,pos in name_index.get(n,[])[:500]:
        nm=chains[ci]['names']
        if pos>0: s.add(norm(nm[pos-1]))
        if pos<len(nm)-1: s.add(norm(nm[pos+1]))
    return s
longer_by_head=defaultdict(list)
for pid,p in alive.items():
    t=norm(p.get('full_name') or pid).split()
    if len(t)>=3: longer_by_head[t[0]].append((pid,t))

merges=[]; ambiguous=0; distinct=0
for pid,p in alive.items():
    n=norm(p.get('full_name') or pid); st=n.split()
    if not st or len(st)>2: continue
    if st[0] in ('ابو','ابي','ابا','ام','ابن','بن'): continue
    cands=[(lp,lt) for lp,lt in longer_by_head.get(st[0],[]) if lt[:len(st)]==st and lp!=pid]
    if len(cands)<2: continue   # only the genuinely-ambiguous (multi-candidate)
    myfp=fp(p.get('full_name') or pid)
    if len(myfp)<4: continue
    scored=sorted(((lp,len(myfp&fp(alive[lp].get('full_name') or lp))) for lp,lt in cands),
                  key=lambda x:-x[1])
    top,ts=scored[0]; rn=scored[1][1] if len(scored)>1 else 0
    if ts>=4 and (rn==0 or ts/max(rn,1)>=3.0):
        merges.append((pid,top,ts,rn)); 
    else: ambiguous+=1

print(f'partial-ambiguous resolved by fingerprint dominance: {len(merges):,}')
print(f'still ambiguous (no dominant match): {ambiguous:,}')
bycanon=defaultdict(list)
for d,c,ts,rn in merges: bycanon[c].append((d,ts,rn))
plans=[{'gkid':'partialdisamb','canonical':c,'canonical_name':alive[c].get('full_name') or c,
        'donors':[d for d,ts,rn in ms],'donor_names':[alive[d].get('full_name') or d for d,ts,rn in ms],
        'reasons':[f'fp_dominance_{ts}vs{rn}' for d,ts,rn in ms]} for c,ms in bycanon.items()]
json.dump({'plans':plans},open('savepoints/partial_disamb_clusters.json','w',encoding='utf-8'),ensure_ascii=False)
print(f'{len(plans)} clusters, {sum(len(p["donors"]) for p in plans)} donors')
print('\n=== Sample (REVIEW) ===')
for d,c,ts,rn in merges[:20]:
    print(f'  fp={ts:>3}vs{rn:<3} [{(alive[d].get("full_name") or d)[:26]:26s}] -> [{(alive[c].get("full_name") or c)[:34]}]')
