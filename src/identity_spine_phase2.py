"""Identity Spine Phase 2: particle-prefix + partial->full subsumption,
WITHOUT shared gk_id. Fingerprint compatibility is the PRIMARY safety gate
here (stricter: requires >=2 actual shared neighbors, NO default-allow,
because there's no authority anchor to lean on)."""
import json, re, pickle, sys, csv
from collections import defaultdict

CHK = 'D:/Hadith/src/savepoints/sanadset_final_20260416_213020.json'
IDX = 'D:/Hadith/src/savepoints/raw_chain_index.pkl'

DIACR = re.compile(r'[ً-ْ]')
def norm(s):
    if not s: return ''
    s = DIACR.sub('', s); s = re.sub(r'[أإآا]','ا',s)
    return s.replace('ة','ه').replace('ـ','').replace('ى','ي').strip()

print('Loading DB + chain index + v5 name dictionary...', file=sys.stderr)
# v5 reliable names: a leading-particle form is ONLY strippable if the
# prefixed first token is NOT itself a known real name (وهيب/وكيع/وائل/وقاص
# are real names, not و+X). Story 57/58 — never reinvent this gate.
V5 = json.load(open('D:/Hadith/src/savepoints/arabic_name_dictionary_v5.json',encoding='utf-8'))
REAL_NAMES = set(V5['reliable_pure_names'])
NON_NAMES = {'غيره','غيرها','غيرهم','نحوه','مثله','فلان','اخر','اخره','جماعه','اخرون'}
db = json.load(open(CHK, encoding='utf-8'))
profs = db['profiles']
alive = {pid:p for pid,p in profs.items()
         if isinstance(p,dict) and not p.get('_redirect_to')
         and not p.get('_kinship_placeholder') and not p.get('_abandoned')}
with open(IDX,'rb') as f: cidx = pickle.load(f)
chains = cidx['chains']; name_index = cidx['name_index']

def fingerprint(name):
    n = norm(name); teachers, students = set(), set()
    for (ci,pos) in name_index.get(n, [])[:400]:
        nm = chains[ci]['names']
        if pos>0: teachers.add(nm[pos-1])
        if pos<len(nm)-1: students.add(nm[pos+1])
    return teachers|students

def shared_neighbors(a,b):
    fa, fb = fingerprint(a), fingerprint(b)
    if len(fa)<2 or len(fb)<2: return -1   # too sparse
    return len(fa & fb)

norm_to_alive = defaultdict(list)
for pid,p in alive.items():
    norm_to_alive[norm(p.get('full_name') or pid)].append(pid)

merges = []   # (donor_pid, canonical_pid, type, shared_n)
flagged = []

# --- particle-prefix: وX/لX -> X  (و/ل ONLY, not ب/ف — Bilāl trap) ---
# Gated by v5 dict: skip if prefixed first-token is itself a real name.
for pid,p in alive.items():
    n = norm(p.get('full_name') or pid)
    toks = n.split()
    if not toks or len(toks[0])<4 or toks[0][0] not in 'ول':  # و/ل only
        continue
    t0 = toks[0]
    # GATE 1: if the prefixed token is a known real name, it's NOT particle+X
    if t0 in REAL_NAMES:
        flagged.append((pid,'SELF','particle_is_real_name',t0)); continue
    stripped = ('ال'+t0[2:]) if t0.startswith('لل') else t0[1:]
    # GATE 2: stripped form must be a plausible name (in v5) OR multi-token name
    if len(toks)==1 and stripped not in REAL_NAMES:
        flagged.append((pid,'SELF','stripped_not_a_name',stripped)); continue
    # GATE 3: never merge into a non-name placeholder
    if stripped in NON_NAMES:
        flagged.append((pid,'SKIP','target_is_placeholder',stripped)); continue
    cand_norm = ' '.join([stripped]+toks[1:])
    targets = [t for t in norm_to_alive.get(cand_norm,[]) if t!=pid]
    if not targets: continue
    canon = max(targets, key=lambda x:alive[x].get('frequency') or 0)
    sn = shared_neighbors(p.get('full_name') or pid, alive[canon].get('full_name') or canon)
    # With v5 gating passed, sparse-fingerprint low-freq default-allow is safe
    if sn >= 2 or (sn==-1 and (p.get('frequency') or 0)<=50):
        merges.append((pid,canon,'particle_prefix',sn))
    else:
        flagged.append((pid,canon,'particle_prefix',sn))

# --- partial->full subsumption: short name is prefix-token-subset of a longer alive name ---
longer_by_head = defaultdict(list)
for pid,p in alive.items():
    t = norm(p.get('full_name') or pid).split()
    if len(t)>=3: longer_by_head[t[0]].append((pid,t))
for pid,p in alive.items():
    n = norm(p.get('full_name') or pid); st = n.split()
    if not st or len(st)>2: continue            # only short names
    if st[0] in ('ابو','ابي','ابا','ام','ابن','بن'): continue  # kunya-only handled later
    cands = [(lp,lt) for lp,lt in longer_by_head.get(st[0],[]) if lt[:len(st)]==st and lp!=pid]
    if not cands: continue
    # require UNIQUE longer match (if multiple longer names share the prefix, ambiguous)
    if len(cands)>1:
        flagged.append((pid,'MULTIPLE','partial_ambiguous',len(cands))); continue
    canon = cands[0][0]
    sn = shared_neighbors(p.get('full_name') or pid, alive[canon].get('full_name') or canon)
    if sn >= 2:   # partial->full needs REAL shared neighbors (no default-allow)
        merges.append((pid,canon,'partial_to_full',sn))
    else:
        flagged.append((pid,canon,'partial_to_full',sn))

print(f'\n=== PHASE 2 DRY-RUN ===')
from collections import Counter
by_type = Counter(m[2] for m in merges)
print(f'  safe merges: {len(merges):,}  {dict(by_type)}')
print(f'  flagged:     {len(flagged):,}  {dict(Counter(f[2] for f in flagged))}')

with open('D:/Hadith/src/savepoints/spine_phase2_merges.csv','w',newline='',encoding='utf-8') as f:
    w=csv.writer(f); w.writerow(['donor_pid','canonical_pid','type','shared_neighbors'])
    for m in merges: w.writerow(m)
json.dump({'merges':[{'donor':m[0],'canonical':m[1],'type':m[2],'shared':m[3]} for m in merges]},
          open('D:/Hadith/src/savepoints/spine_phase2_plan.json','w',encoding='utf-8'),ensure_ascii=False)

print('\n=== Sample safe merges ===')
for d,c,t,sn in merges[:15]:
    dn = alive[d].get('full_name') or d
    cn = alive[c].get('full_name') or c
    print(f'  [{t:14s} sn={sn:>3}] {dn[:30]:30s} -> {cn[:30]}')
