"""IDENTITY SPINE ENGINE — collapse duplicate profiles of the same person
into one canonical node, preserving every surface form as an alias.

Safety: two profiles merge only if chain-fingerprint compatible (shared or
non-contradictory teacher/student neighbors). Mononyms with contradictory
fingerprints are FLAGGED, never auto-merged.

Phase 1 (this run): same-gk_rawy_id clusters — proven same-person anchor.

Output: dry-run merge plan + CSV. No DB writes here; apply via SafeMerge.
"""
import json, re, pickle, sys
from collections import defaultdict, Counter

CHK = 'D:/Hadith/src/savepoints/sanadset_final_20260416_213020.json'
IDX = 'D:/Hadith/src/savepoints/raw_chain_index.pkl'

DIACR = re.compile(r'[ً-ْ]')
def norm(s):
    if not s: return ''
    s = DIACR.sub('', s); s = re.sub(r'[أإآا]','ا',s)
    return s.replace('ة','ه').replace('ـ','').replace('ى','ي').strip()

print('Loading DB...', file=sys.stderr)
db = json.load(open(CHK, encoding='utf-8'))
profs = db['profiles']
alive = {pid:p for pid,p in profs.items()
         if isinstance(p,dict) and not p.get('_redirect_to')
         and not p.get('_kinship_placeholder') and not p.get('_abandoned')}

print('Loading chain index...', file=sys.stderr)
with open(IDX,'rb') as f: cidx = pickle.load(f)
chains = cidx['chains']; name_index = cidx['name_index']

def fingerprint(name):
    """Return (teachers_set, students_set) = immediate chain neighbors."""
    n = norm(name)
    teachers, students = set(), set()
    for (ci, pos) in name_index.get(n, [])[:400]:   # cap for speed
        names = chains[ci]['names']
        if pos > 0: teachers.add(names[pos-1])
        if pos < len(names)-1: students.add(names[pos+1])
    return teachers, students

def fp_compatible(a_name, b_name):
    """Same person if neighbor sets overlap, or one side is too small to judge."""
    at, ast = fingerprint(a_name)
    bt, bst = fingerprint(b_name)
    a_neigh = at | ast
    b_neigh = bt | bst
    if len(a_neigh) < 3 or len(b_neigh) < 3:
        return True, 'insufficient_chain_data_default_allow'   # too sparse to contradict
    overlap = a_neigh & b_neigh
    if overlap:
        return True, f'shared_{len(overlap)}_neighbors'
    # No overlap at all between two well-attested forms = suspicious
    return False, 'disjoint_neighbors_possible_different_person'

def name_compatible(a, b):
    """One name subsumes the other, or they share leading ism token."""
    na, nb = norm(a), norm(b)
    ta, tb = na.split(), nb.split()
    if not ta or not tb: return False
    short, lng = (ta, tb) if len(ta)<=len(tb) else (tb, ta)
    if lng[:len(short)] == short: return True       # prefix subsumption
    if ta[0] == tb[0]: return True                   # same ism head
    return False

# Build same-gkid clusters
gkid_to_pids = defaultdict(list)
for pid,p in alive.items():
    g = p.get('gk_rawy_id')
    if g: gkid_to_pids[str(g)].append(pid)
clusters = {g:pids for g,pids in gkid_to_pids.items() if len(pids)>1}
print(f'Same-gkid clusters: {len(clusters):,}', file=sys.stderr)

plans = []          # safe merges
flagged = []        # need review
for g, pids in clusters.items():
    # canonical = highest frequency
    ranked = sorted(pids, key=lambda x:-(alive[x].get('frequency') or 0))
    canon = ranked[0]
    canon_name = alive[canon].get('full_name') or canon
    donors_ok, donors_flag = [], []
    for d in ranked[1:]:
        d_name = alive[d].get('full_name') or d
        nc = name_compatible(canon_name, d_name)
        fc, fc_reason = fp_compatible(canon_name, d_name)
        if nc and fc:
            donors_ok.append((d, d_name, fc_reason))
        else:
            donors_flag.append((d, d_name, f'name_ok={nc} fp={fc_reason}'))
    if donors_ok:
        plans.append({'gkid':g, 'canonical':canon, 'canonical_name':canon_name,
                      'donors':[d[0] for d in donors_ok],
                      'donor_names':[d[1] for d in donors_ok],
                      'reasons':[d[2] for d in donors_ok]})
    if donors_flag:
        flagged.append({'gkid':g, 'canonical':canon, 'canonical_name':canon_name,
                        'flagged':[(d[0],d[1],d[2]) for d in donors_flag]})

n_merge_profiles = sum(len(p['donors']) for p in plans)
print(f'\n=== PHASE 1 DRY-RUN: same-gkid merges ===')
print(f'  safe merge clusters:   {len(plans):,}')
print(f'  donor profiles to redirect: {n_merge_profiles:,}')
print(f'  flagged clusters (review):  {len(flagged):,}')

import csv
with open('D:/Hadith/src/savepoints/spine_phase1_merges.csv','w',newline='',encoding='utf-8') as f:
    w = csv.writer(f); w.writerow(['gkid','canonical_pid','canonical_name','donor_pid','donor_name','reason'])
    for p in plans:
        for d,dn,r in zip(p['donors'],p['donor_names'],p['reasons']):
            w.writerow([p['gkid'],p['canonical'],p['canonical_name'],d,dn,r])
with open('D:/Hadith/src/savepoints/spine_phase1_flagged.csv','w',newline='',encoding='utf-8') as f:
    w = csv.writer(f); w.writerow(['gkid','canonical_pid','canonical_name','flagged_pid','flagged_name','reason'])
    for p in flagged:
        for d,dn,r in p['flagged']:
            w.writerow([p['gkid'],p['canonical'],p['canonical_name'],d,dn,r])

json.dump({'plans':plans,'flagged':flagged},
          open('D:/Hadith/src/savepoints/spine_phase1_plan.json','w',encoding='utf-8'),
          ensure_ascii=False)

print('\n=== Sample safe merges ===')
for p in plans[:12]:
    print(f"  gk[{p['gkid']}] canon={p['canonical_name'][:28]:28s} <- {p['donor_names']}  [{p['reasons'][0]}]")
print('\n=== Sample flagged (NOT merged) ===')
for p in flagged[:8]:
    for d,dn,r in p['flagged']:
        print(f"  gk[{p['gkid']}] canon={p['canonical_name'][:25]:25s} ?? {dn[:25]:25s} [{r}]")
