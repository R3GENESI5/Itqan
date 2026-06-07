"""Attach mononym disambiguation summary to the 15 bare-mononym profiles.
Additive, non-destructive, through SafeMerge gates."""
import json,re,sys
sys.path.insert(0,'D:/Hadith/src')
from gafsce_gates import SafeMerge
DIACR=re.compile(r'[ً-ْ]')
def norm(s):
    if not s: return ''
    s=DIACR.sub('',s); s=re.sub(r'[أإآا]','ا',s)
    return s.replace('ة','ه').replace('ـ','').replace('ى','ي').strip()
CHK='D:/Hadith/src/savepoints/sanadset_final_20260416_213020.json'
stats=json.load(open('savepoints/mononym_curated_stats.json',encoding='utf-8'))
apply='--apply' in sys.argv
VALIDATED={'سفيان','حماد'}
# map bare-mononym string -> alive pid (by normalized full_name)
db=json.load(open(CHK,encoding='utf-8'))['profiles']
norm2pid={}
for pid,p in db.items():
    if isinstance(p,dict) and not p.get('_redirect_to'):
        norm2pid.setdefault(norm(p.get('full_name') or pid),pid)

with SafeMerge(script_name='attach_mononym_disambig',db_path=CHK,
               expected_freq_delta=0,expected_alive_delta=0,
               apply_mode=apply,batch_cap=40) as sm:
    for mono,st in stats.items():
        pid=norm2pid.get(norm(mono))
        if not pid or pid not in sm.profs:
            print(f'  skip {mono} (no alive profile)',file=sys.stderr); continue
        summary={'regime':st['regime'],'total_occurrences':st['total'],
                 'resolved':st['network']+st['ashhar_default'],'mubham':st['mubham'],
                 'persons':st['persons'],
                 'confidence':'network_validated_6of6' if mono in VALIDATED
                              else ('al_itlaq_default' if st['regime']=='dominant' else 'network_routed'),
                 'method':'tamyiz_al_mushtarak_mizzi_dhahabi','map':'mononym_curated_map.json'}
        sm.change(pid=pid,action='attach_mononym_disambiguation',
                  old_values={'mononym_disambiguation':sm.profs[pid].get('mononym_disambiguation')},
                  new_values={'mononym_disambiguation':summary},
                  reason='per_occurrence_tamyiz_resolution_attached')
    sm.commit()
