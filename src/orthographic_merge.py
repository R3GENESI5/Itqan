"""Merge same-normalized-name alive duplicates (ة/ه, ى/ي, أ/ا spelling
variants of one person). Safety against common-name collision:
  - multi-token name (>=3 tokens) => strong identity, OR
  - fingerprint shared >=3 neighbors
  - never merge bare 1-token names (too collision-prone)
"""
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
def shared(a,b):
    fa,fb=fp(a),fp(b)
    if len(fa)<2 or len(fb)<2: return -1
    return len(fa&fb)
norm2pids=defaultdict(list)
for pid,p in alive.items(): norm2pids[norm(p.get('full_name') or pid)].append(pid)

merges=[]; flagged=[]
for n,pids in norm2pids.items():
    if len(pids)<2: continue
    ntok=len(n.split())
    if ntok<2: flagged.append((n,pids,'single_token_too_risky')); continue
    ranked=sorted(pids,key=lambda x:-(alive[x].get('frequency') or 0))
    canon=ranked[0]
    for d in ranked[1:]:
        sn=shared(alive[canon].get('full_name') or canon, alive[d].get('full_name') or d)
        # multi-token(>=3) safe by itself; 2-token needs fingerprint corroboration
        if ntok>=3 or sn>=3 or (sn==-1 and (alive[d].get('frequency') or 0)<=30):
            merges.append((d,canon,n,sn,ntok))
        else:
            flagged.append((n,[d,canon],f'2tok_sn{sn}'))

print(f'orthographic MERGE candidates: {len(merges):,}')
print(f'flagged (need review): {len(flagged):,}')
from collections import defaultdict as dd
bycanon=dd(list)
for d,c,n,sn,nt in merges: bycanon[c].append((d,n,sn))
plans=[{'gkid':'ortho','canonical':c,'canonical_name':alive[c].get('full_name') or c,
        'donors':[d for d,n,sn in ms],'donor_names':[alive[d].get('full_name') or d for d,n,sn in ms],
        'reasons':[f'ortho_sn{sn}' for d,n,sn in ms]} for c,ms in bycanon.items()]
json.dump({'plans':plans},open('savepoints/ortho_clusters.json','w',encoding='utf-8'),ensure_ascii=False)
print(f'{len(plans)} canonical clusters')
print('\n=== Sample ===')
for d,c,n,sn,nt in merges[:18]:
    print(f'  ntok={nt} sn={sn:>3} [{(alive[d].get("full_name") or d)[:30]:30s}] -> [{(alive[c].get("full_name") or c)[:30]}]')
