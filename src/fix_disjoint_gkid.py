"""The 204 same-gkid truly-disjoint pairs are DIFFERENT people sharing a
WRONG gk_id (web-confirmed: حذيفة الأزدي != حذيفة بن اليمان). Fix = remove
the wrong gk_id from the sparser/lower-freq profile of each pair, so they
are correctly distinct (NOT merged). Data correction through gates."""
import json,re,pickle,sys
from collections import defaultdict
sys.path.insert(0,'D:/Hadith/src')
from gafsce_gates import SafeMerge
DIACR=re.compile(r'[ً-ْ]')
def norm(s):
    if not s: return ''
    s=DIACR.sub('',s); s=re.sub(r'[أإآا]','ا',s)
    return s.replace('ة','ه').replace('ـ','').replace('ى','ي').strip()
CHK='D:/Hadith/src/savepoints/sanadset_final_20260416_213020.json'
apply='--apply' in sys.argv
profs=json.load(open(CHK,encoding='utf-8'))['profiles']
alive={pid:p for pid,p in profs.items() if isinstance(p,dict) and not p.get('_redirect_to') and not p.get('_kinship_placeholder') and not p.get('_abandoned')}
with open('savepoints/raw_chain_index.pkl','rb') as f: cidx=pickle.load(f)
chains=cidx['chains']; name_index=cidx['name_index']
def fp(name):
    n=norm(name); s=set()
    for ci,pos in name_index.get(n,[])[:400]:
        nm=chains[ci]['names']
        if pos>0: s.add(norm(nm[pos-1]))
        if pos<len(nm)-1: s.add(norm(nm[pos+1]))
    return s
g2p=defaultdict(list)
for pid,p in alive.items():
    if p.get('gk_rawy_id'): g2p[str(p['gk_rawy_id'])].append(pid)
# find disjoint pairs, mark sparser profile for gk_id removal
to_fix=[]
for g,pids in g2p.items():
    if len(pids)<2: continue
    ranked=sorted(pids,key=lambda x:-(alive[x].get('frequency') or 0))
    canon=ranked[0]; cfp=fp(alive[canon].get('full_name') or canon)
    for d in ranked[1:]:
        dfp=fp(alive[d].get('full_name') or d)
        if len(cfp)>=3 and len(dfp)>=3 and len(cfp&dfp)==0:
            to_fix.append(d)   # sparser profile keeps name, loses wrong gk_id
print(f'wrong-gk_id profiles to correct: {len(to_fix)}',file=sys.stderr)
with SafeMerge(script_name='fix_disjoint_gkid',db_path=CHK,expected_freq_delta=0,
               expected_alive_delta=0,apply_mode=apply,batch_cap=300) as sm:
    for pid in to_fix:
        if pid not in sm.profs: continue
        old=sm.profs[pid].get('gk_rawy_id')
        sm.change(pid=pid,action='remove_wrong_gkid',
                  old_values={'gk_rawy_id':old},
                  new_values={'gk_rawy_id':None,'_gkid_corrected':'disjoint_fingerprint_different_person'},
                  reason='same_gkid_disjoint_web_confirmed_different_person')
    sm.commit()
