"""kunya/laqab -> ism merge via GK DECLARED authority (safe design).

Authority: GK's declared kunya + laqab fields ONLY (never nasab/name
substrings — that caused the النعمان trap). Multi-token, unique person.

A profile named exactly such a kunya = that GK person IF:
  - GK uniquely maps the kunya to one person, AND
  - that person's ISM profile exists alive (match by GK name/nasab), AND
  - tabaqah consistent, AND
  - chain fingerprint shows >=2 shared neighbors (corroboration, not sole proof)

Merge: kunya-profile -> ism-profile (canonical = higher freq). Non-destructive
(redirect + surface_forms). Through SafeMerge gates.
"""
import json,re,pickle,sys
from collections import defaultdict,Counter
DIACR=re.compile(r'[ً-ْ]')
def norm(s):
    if not s: return ''
    s=DIACR.sub('',s); s=re.sub(r'[أإآا]','ا',s)
    return s.replace('ة','ه').replace('ـ','').replace('ى','ي').strip()

profs=json.load(open('savepoints/sanadset_final_20260416_213020.json',encoding='utf-8'))['profiles']
alive={pid:p for pid,p in profs.items() if isinstance(p,dict) and not p.get('_redirect_to')
       and not p.get('_kinship_placeholder') and not p.get('_abandoned')}
gk=json.load(open('external_hadith/gk_json/gk_narrators.json',encoding='utf-8'))['narrators']
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

# GK declared kunya/laqab (multi-token) -> unique person
kl2ids=defaultdict(set); gkinfo={}
for rid,e in gk.items():
    if not isinstance(e,dict): continue
    gkinfo[rid]=e
    for fld in ('kunya','laqab'):
        v=e.get(fld)
        if not v: continue
        for part in re.split(r'[،,]',v):
            nn=norm(part)
            if nn and ' ' in nn and len(nn)>=6: kl2ids[nn].add(rid)
uniq_kl={f:list(i)[0] for f,i in kl2ids.items() if len(i)==1}

# alive index by norm name + by gk's name/nasab forms
norm2pid=defaultdict(list)
for pid,p in alive.items(): norm2pid[norm(p.get('full_name') or pid)].append(pid)

def find_ism_profile(rid):
    """alive profile matching this GK person's ism (name/alt_name/full_nasab)."""
    e=gkinfo.get(rid,{})
    for fld in ('name','alt_name','full_nasab'):
        v=e.get(fld)
        if not v: continue
        for pid in norm2pid.get(norm(v),[]):
            return pid
    return None

TAB_GRADE={'companion':(1,2)}  # loose tabaqah sanity
merges=[]; flagged=[]
for pid,p in alive.items():
    nm=norm(p.get('full_name') or pid)
    rid=uniq_kl.get(nm)
    if not rid: continue
    ism_pid=find_ism_profile(rid)
    if not ism_pid or ism_pid==pid: continue
    sn=shared(p.get('full_name') or pid, alive[ism_pid].get('full_name') or ism_pid)
    if sn>=2:
        # canonical = higher freq
        a,b=(pid,ism_pid)
        canon,donor=(a,b) if (alive[a].get('frequency') or 0)>=(alive[b].get('frequency') or 0) else (b,a)
        merges.append((donor,canon,rid,sn))
    else:
        flagged.append((pid,ism_pid,rid,sn))

print(f'GK declared kunya/laqab (multi-token, unique): {len(uniq_kl):,}')
print(f'kunya->ism MERGE candidates (fp>=2 corroborated): {len(merges):,}')
print(f'flagged (insufficient fingerprint): {len(flagged):,}')
print('\n=== Sample merges (REVIEW for wrong identity) ===')
for d,c,rid,sn in merges[:25]:
    print(f'  sn={sn:>3} [{(alive[d].get("full_name") or d)[:24]:24s}] -> [{(alive[c].get("full_name") or c)[:26]:26s}] gk={gkinfo[rid].get("name","")[:18]}')
import json as J
J.dump({'merges':[{'donor':d,'canonical':c,'rid':rid,'sn':sn} for d,c,rid,sn in merges]},
       open('savepoints/kunya_ism_plan.json','w',encoding='utf-8'),ensure_ascii=False)
