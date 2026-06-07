"""Phase 3 SAFE: kunya/laqab -> ism collapse via GK DECLARED agnomen.

Authority = GK's explicit kunya/laqab field ONLY (never name/nasab substrings).
A profile named exactly K (multi-token kunya/laqab, unique GK person P) merges
into P's alive ism-profile IF:
  - P's ism-profile is alive (matched by gk_id OR exact name)
  - fingerprint does NOT contradict (disjoint well-attested => FLAG)
  - tabaqah consistent (no companion<->late-tabaqah collisions)
  - donor freq <= canonical freq (kunya is the shortcut, ism is fuller)

Authority asserts identity; fingerprint is corroboration (veto only).
Outputs plan; apply via spine_apply. DRY-RUN review mandatory.
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
def fp_contradicts(a,b):
    fa,fb=fp(a),fp(b)
    if len(fa)<3 or len(fb)<3: return False   # too sparse to contradict
    return len(fa&fb)==0                        # disjoint & both attested => contradict

# Authority: declared kunya/laqab ONLY, multi-token, unique person
form2ids=defaultdict(set); gkm={}
for rid,e in gk.items():
    if not isinstance(e,dict): continue
    gkm[rid]=e
    for fld in ('kunya','laqab'):
        v=e.get(fld)
        if not v: continue
        for part in re.split(r'[،,]',v):
            nn=norm(part)
            if nn and ' ' in nn and len(nn)>=5: form2ids[nn].add(rid)
uniq={f:list(i)[0] for f,i in form2ids.items() if len(i)==1}

# alive index by gk_id and by norm-name
gkid_alive=defaultdict(list); name_alive=defaultdict(list)
for pid,p in alive.items():
    g=p.get('gk_rawy_id')
    if g: gkid_alive[str(g)].append(pid)
    name_alive[norm(p.get('full_name') or pid)].append(pid)

TAB={'companion':(1,1),'very_reliable':(1,12),'reliable':(1,12),'mostly_reliable':(1,12),
     'acceptable':(1,12),'slightly_weak':(1,12),'weak':(1,12)}
def tab_ok(grade, tabaqah):
    if not tabaqah: return True
    lo,hi=TAB.get(grade or '',(1,12)); return lo<=tabaqah<=hi

merges=[]; flagged=[]
for pid,p in alive.items():
    nm=norm(p.get('full_name') or pid)
    gid=uniq.get(nm)
    if not gid: continue                         # not a unique declared kunya/laqab
    e=gkm[gid]; pname=norm(e.get('name') or '')
    # find canonical = P's alive ism-profile (by gk_id, else by exact name)
    cands=[x for x in gkid_alive.get(gid,[]) if x!=pid]
    if not cands and pname:
        cands=[x for x in name_alive.get(pname,[]) if x!=pid]
    if not cands:
        flagged.append((pid,'NO_ISM_PROFILE',gid,'')); continue
    canon=max(cands,key=lambda x:alive[x].get('frequency') or 0)
    # vetoes
    if (alive[pid].get('frequency') or 0) > (alive[canon].get('frequency') or 0):
        flagged.append((pid,canon,gid,'kunya_freq>ism_freq')); continue
    if not tab_ok(p.get('grade_en'), e.get('tabaqah')):
        flagged.append((pid,canon,gid,'tabaqah_mismatch')); continue
    if fp_contradicts(p.get('full_name') or pid, alive[canon].get('full_name') or canon):
        flagged.append((pid,canon,gid,'fingerprint_disjoint')); continue
    merges.append((pid,canon,gid,e.get('name')))

print(f'\n=== PHASE 3 SAFE DRY-RUN ===')
print(f'  merges: {len(merges):,}   flagged: {len(flagged):,}')
from collections import Counter
print(f'  flag reasons: {dict(Counter(f[3] or f[1] for f in flagged))}')

# cluster format for spine_apply
by_c=defaultdict(list)
for d,c,g,gn in merges: by_c[c].append((d,g,gn))
plans=[{'gkid':f'kunyaGK{ms[0][1]}','canonical':c,
        'canonical_name':alive[c].get('full_name') or c,
        'donors':[m[0] for m in ms],
        'donor_names':[alive[m[0]].get('full_name') or m[0] for m in ms],
        'reasons':['kunya_to_ism_declared_authority']*len(ms)} for c,ms in by_c.items()]
json.dump({'plans':plans},open('savepoints/spine_phase3_clusters.json','w',encoding='utf-8'),ensure_ascii=False)
with open('savepoints/spine_phase3_merges.csv','w',newline='',encoding='utf-8') as f:
    w=csv.writer(f); w.writerow(['kunya_pid','ism_canonical','gk_name'])
    for d,c,g,gn in merges: w.writerow([alive[d].get('full_name'),alive[c].get('full_name'),gn])

print(f'\n=== ALL {len(merges)} MERGES (review for wrong-anchor) ===')
for d,c,g,gn in merges:
    print(f'  [{(alive[d].get("full_name") or d)[:26]:26s}] -> [{(alive[c].get("full_name") or c)[:30]:30s}]')
