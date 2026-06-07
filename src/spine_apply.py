"""Apply Identity Spine merge plan through SafeMerge gates, in batches.

Each cluster -> 2 staged changes:
  1. canonical profile: union donor fields + append surface_forms[]
  2. donor profile: redirect_to canonical

Preserves: every field (union), every surface form (surface_forms list),
frequency (summed into canonical). Donor becomes redirect stub.

Usage: python spine_apply.py [--apply] [--batch N] [--clusters K]
"""
import argparse, json, sys
sys.path.insert(0,'D:/Hadith/src')
from gafsce_gates import SafeMerge

ap = argparse.ArgumentParser()
ap.add_argument('--apply', action='store_true')
ap.add_argument('--start', type=int, default=0)
ap.add_argument('--clusters', type=int, default=50)
args = ap.parse_args()

CHK = 'D:/Hadith/src/savepoints/sanadset_final_20260416_213020.json'
plan = json.load(open('D:/Hadith/src/savepoints/spine_phase1_plan.json',encoding='utf-8'))['plans']
batch = plan[args.start:args.start+args.clusters]
print(f'Processing clusters {args.start}..{args.start+len(batch)} of {len(plan)}', file=sys.stderr)

def union_list(a,b,keys=None):
    a=a or []; b=b or []; out=list(a); seen=set()
    if keys:
        for x in a:
            if isinstance(x,dict): seen.add(tuple(x.get(k) for k in keys))
    for x in b:
        if keys and isinstance(x,dict):
            k=tuple(x.get(kk) for kk in keys)
            if k in seen: continue
            seen.add(k)
        out.append(x)
    return out

LIST_FIELDS = [('dorar_results',('mohdith','grade')),('hawramani_urls',None),
               ('hawramani_source_books',None),('scholar_opinions',('scholar',)),
               ('teachers',('name',)),('students',('name',)),('top_books',('book',))]
FILL_FIELDS = ['hawramani_result','death_range','death_range_note','companion_evidence',
               'grade_source','book_sources','isnad_evidence','full_name_diacritics',
               'kunya','laqab','nisba','full_nasab','biography','audit_note','gk_rawy_id',
               'ism','alt_name','city','tabaqat_num']
SUM_FIELDS = ['frequency','book_count','teacher_count','student_count']

# Grade precedence: canonical inherits the BEST grade in the cluster so a
# high-grade donor (e.g. companion) is never lost to a lower-graded canonical.
GRADE_ORDER = {'companion':1,'very_reliable':2,'reliable':3,'mostly_reliable':4,
               'acceptable':5,'slightly_weak':6,'weak':7,'abandoned':8,
               'fabricator':9,'unknown':10}
def best_grade(*grades):
    best, best_o = '', 99
    for g in grades:
        if g and GRADE_ORDER.get(g, 50) < best_o:
            best, best_o = g, GRADE_ORDER.get(g, 50)
    return best

# expected freq delta = 0 (freq moves donor->canonical, conserved)
# expected alive delta = -(number of donors)
n_donors = sum(len(c['donors']) for c in batch)

with SafeMerge(script_name=f'spine_phase1_b{args.start}', db_path=CHK,
               expected_freq_delta=0, expected_alive_delta=-n_donors,
               apply_mode=args.apply, batch_cap=args.clusters*2+5) as sm:
    profs = sm.profs
    for c in batch:
        canon = c['canonical']
        if canon not in profs: continue
        cp = profs[canon]
        # snapshot canonical fields we'll touch
        old_canon = {f: cp.get(f) for f,_ in LIST_FIELDS}
        old_canon.update({f: cp.get(f) for f in SUM_FIELDS})
        old_canon['grade_en'] = cp.get('grade_en')
        new_canon = {}
        surface = list(cp.get('surface_forms') or [])
        # canonical's own form
        if not surface:
            surface.append({'form':cp.get('full_name') or canon,'type':'canonical',
                            'pid':canon,'freq':cp.get('frequency') or 0})
        cluster_grade = cp.get('grade_en')   # track best grade across cluster
        for d, dn in zip(c['donors'], c['donor_names']):
            if d not in profs: continue
            dp = profs[d]
            cluster_grade = best_grade(cluster_grade, dp.get('grade_en'))
            for f,keys in LIST_FIELDS:
                if f in dp: new_canon[f]=union_list(new_canon.get(f,cp.get(f)),dp[f],keys)
            for f in FILL_FIELDS:
                if dp.get(f) and not cp.get(f) and f not in new_canon:
                    new_canon[f]=dp[f]
            for f in SUM_FIELDS:
                new_canon[f]=(new_canon.get(f,cp.get(f,0) or 0))+(dp.get(f,0) or 0)
            surface.append({'form':dn,'type':'merged_duplicate','pid':d,
                            'freq':dp.get('frequency') or 0})
        # Promote canonical to best grade in cluster (never downgrade)
        if cluster_grade and cluster_grade != cp.get('grade_en'):
            new_canon['grade_en'] = cluster_grade
        new_canon['surface_forms']=surface
        sm.change(pid=canon, action='spine_merge_canonical',
                  old_values=old_canon, new_values=new_canon,
                  reason=f"identity_spine_phase1_gkid_{c['gkid']}")
        # redirect donors
        for d in c['donors']:
            if d not in profs: continue
            sm.change(pid=d, action=f'redirect_to:{canon}',
                      old_values={'frequency':profs[d].get('frequency'),
                                  'full_name':profs[d].get('full_name')},
                      new_values={'_redirect_to':canon},
                      reason=f"identity_spine_phase1_gkid_{c['gkid']}")
    sm.commit()
