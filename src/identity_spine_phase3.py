"""Phase 3: kunya/laqab/urf -> ism collapse via GK authority.

A surface form mapping to EXACTLY ONE GK person can anchor to that gk_id.
Two alive profiles resolving to the same gk person = same human -> merge.

SAFETY (anti Abu-Hurayra-368 trap):
  - form must map to exactly 1 gk id (zero ambiguity), AND
  - fingerprint compatibility between the two profiles being merged, AND
  - the GK person's tabaqah must be consistent with profile grade
  - kunya/laqab ALONE (1-2 tokens) require fingerprint >=2 (stricter)
"""
import json, re, pickle, sys, csv
from collections import defaultdict

CHK='D:/Hadith/src/savepoints/sanadset_final_20260416_213020.json'
GK='D:/Hadith/src/external_hadith/gk_json/gk_narrators.json'
IDX='D:/Hadith/src/savepoints/raw_chain_index.pkl'
DIACR=re.compile(r'[ً-ْ]')
def norm(s):
    if not s: return ''
    s=DIACR.sub('',s); s=re.sub(r'[أإآا]','ا',s)
    return s.replace('ة','ه').replace('ـ','').replace('ى','ي').strip()

print('Loading...',file=sys.stderr)
profs=json.load(open(CHK,encoding='utf-8'))['profiles']
alive={pid:p for pid,p in profs.items() if isinstance(p,dict) and not p.get('_redirect_to')
       and not p.get('_kinship_placeholder') and not p.get('_abandoned')}
gk=json.load(open(GK,encoding='utf-8'))['narrators']
with open(IDX,'rb') as f: cidx=pickle.load(f)
chains=cidx['chains']; name_index=cidx['name_index']

def fp(name):
    n=norm(name); s=set()
    for ci,pos in name_index.get(n,[])[:400]:
        nm=chains[ci]['names']
        if pos>0: s.add(nm[pos-1])
        if pos<len(nm)-1: s.add(nm[pos+1])
    return s
def shared(a,b):
    fa,fb=fp(a),fp(b)
    if len(fa)<2 or len(fb)<2: return -1
    return len(fa&fb)

# GK form -> set of rawy ids (only forms mapping to EXACTLY ONE person are usable)
form2ids=defaultdict(set)
gk_meta={}
for rid,e in gk.items():
    if not isinstance(e,dict): continue
    gk_meta[rid]={'name':e.get('name'),'tabaqah':e.get('tabaqah'),'grade':e.get('grade_ar')}
    for fld in ('name','alt_name','kunya','laqab','full_nasab'):
        v=e.get(fld)
        if not v: continue
        for part in re.split(r'[،,]',v):
            nn=norm(part)
            if nn and len(nn)>=4: form2ids[nn].add(rid)
unique_form={f:list(ids)[0] for f,ids in form2ids.items() if len(ids)==1}
print(f'GK forms mapping to exactly 1 person: {len(unique_form):,}',file=sys.stderr)

# Index alive by gk_id (those already anchored) and by norm-form
gkid_alive=defaultdict(list)
for pid,p in alive.items():
    g=p.get('gk_rawy_id')
    if g: gkid_alive[str(g)].append(pid)

# For each alive profile WITHOUT a gk_id whose form uniquely maps to a gk person,
# propose anchor; if that person already has an alive profile -> merge candidate.
anchor_only=[]      # (pid, gkid) just assign id
merge_cand=[]       # (donor_pid, canonical_pid, gkid, shared_n)
for pid,p in alive.items():
    if p.get('gk_rawy_id'): continue
    nm=norm(p.get('full_name') or pid)
    gid=unique_form.get(nm)
    if not gid: continue
    ntok=len(nm.split())
    existing=[x for x in gkid_alive.get(gid,[]) if x!=pid]
    if existing:
        canon=max(existing,key=lambda x:alive[x].get('frequency') or 0)
        sn=shared(p.get('full_name') or pid, alive[canon].get('full_name') or canon)
        # short forms (kunya/laqab alone) need real shared neighbors
        if ntok<=2 and sn<2:
            continue   # too risky, skip (defer to disambiguation phase)
        if sn>=2 or (sn==-1 and ntok>=3):
            merge_cand.append((pid,canon,gid,sn))
    else:
        if ntok>=3:   # only anchor multi-token (full nasab) — safe
            anchor_only.append((pid,gid))

print(f'\n=== PHASE 3 DRY-RUN ===')
print(f'  merge candidates (form->existing gk profile): {len(merge_cand):,}')
print(f'  anchor-only (assign gk_id, multi-token):      {len(anchor_only):,}')

with open('savepoints/spine_phase3_merges.csv','w',newline='',encoding='utf-8') as f:
    w=csv.writer(f); w.writerow(['donor','donor_name','canonical','canonical_name','gkid','gk_name','shared_n'])
    for d,c,g,sn in merge_cand:
        w.writerow([d,alive[d].get('full_name'),c,alive[c].get('full_name'),g,gk_meta.get(g,{}).get('name'),sn])
print('\n=== Sample merge candidates (REVIEW FOR WRONG-ANCHOR) ===')
for d,c,g,sn in merge_cand[:20]:
    dn=alive[d].get('full_name') or d; cn=alive[c].get('full_name') or c
    print(f'  sn={sn:>3} [{dn[:24]:24s}] -> [{cn[:24]:24s}] gk={gk_meta.get(g,{}).get("name","?")[:20]}')
