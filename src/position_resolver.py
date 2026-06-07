"""Position-aware resolver for mega-mononyms (عبد الله, محمد, علي, عمر) with
56-92 candidates. Network routing (which implicitly encodes chain role:
a companion's students-network identifies terminal occurrences) + position
tiebreak (terminal->earlier tabaqah, compiler-end->later).

Auto-discovers top-N distinct candidates by freq. Routes only on strong
network evidence; rest mubham (honest — these are genuinely hard)."""
import json,re,pickle,sys
from collections import Counter,defaultdict
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

def net(fullname):
    n=norm(fullname); t=Counter(); s=Counter()
    for ci,pos in name_index.get(n,[]):
        nm=chains[ci]['names']
        if pos<len(nm)-1: t[norm(nm[pos+1])]+=1
        if pos>0: s[norm(nm[pos-1])]+=1
    return t,s

def discover(mono, topn=12, minfreq=200):
    """top distinct full-name candidates starting with mono."""
    mn=norm(mono); cands=[]
    for pid,p in alive.items():
        n=norm(p.get('full_name') or pid)
        if n!=mn and n.split()[:len(mn.split())]==mn.split() and len(n.split())>len(mn.split()):
            cands.append((p.get('full_name') or pid, p.get('frequency') or 0))
    return sorted(cands,key=lambda x:-x[1])[:topn]

def grade_of(fn,_cache={}):
    n=norm(fn)
    if n in _cache: return _cache[n]
    for pid,p in alive.items():
        if norm(p.get('full_name') or pid)==n:
            _cache[n]=p.get('grade_en'); return _cache[n]
    _cache[n]=None; return None

def resolve(mono, topn=12, companion_ashhar=None):
    """Position-aware. Chains ordered compiler(pos0)->source(pos last).
    Terminal (pos==len-1) => companion by al-iṭlāq; companion_ashhar is the
    default companion when the student-net doesn't resolve which one."""
    cands=discover(mono,topn)
    nets={fn:net(fn) for fn,fr in cands}
    comp={fn for fn,fr in cands if grade_of(fn)=='companion'}
    stats=Counter(); mn=norm(mono); tot=0
    for ci,pos in name_index.get(mn,[]):
        tot+=1
        nm=chains[ci]['names']
        terminal=(pos==len(nm)-1)
        sh=norm(nm[pos+1]) if pos<len(nm)-1 else None
        ti=norm(nm[pos-1]) if pos>0 else None
        pool=comp if (terminal and comp) else set(nets)
        sc={fn:(nets[fn][0].get(sh,0)+nets[fn][1].get(ti,0)) for fn in pool}
        rk=sorted(sc.items(),key=lambda x:-x[1]) if sc else [(None,0)]
        top,ts=rk[0]; rn=rk[1][1] if len(rk)>1 else 0
        if ts>=5 and (rn==0 or ts/max(rn,1)>=2.0):
            stats[top]+=1
        elif terminal and companion_ashhar:
            stats[companion_ashhar]+=1
        else: stats['mubham']+=1
    return stats,tot,cands

ASHHAR={'علي':'علي بن أبي طالب','عمر':'عمر بن الخطاب','عبد الله':None,'محمد':None}
for mono in ['عبد الله','محمد','علي','عمر']:
    st,tot,cands=resolve(mono,companion_ashhar=ASHHAR.get(mono))
    res=tot-st['mubham']
    print(f'=== {mono} (top {len(cands)} candidates) ===')
    print(f'  {tot:,} occurrences, {res:,} resolved ({100*res/max(tot,1):.1f}%), mubham {st["mubham"]:,}')
    for person,n in st.most_common(6):
        if person=='mubham': continue
        print(f'    {person[:34]:34s} {n:>6,} ({100*n/tot:.1f}%)')

def store():
    import json as J
    ASH={'علي':'علي بن أبي طالب','عمر':'عمر بن الخطاب','عبد الله':None,'محمد':None}
    allstats={}
    for mono in ['عبد الله','محمد','علي','عمر']:
        st,tot,cands=resolve(mono,companion_ashhar=ASH.get(mono))
        persons={k:v for k,v in st.items() if k!='mubham'}
        allstats[mono]={'total':tot,'resolved':tot-st['mubham'],'mubham':st['mubham'],
                        'regime':'position_aware','method':'tamyiz_position_network',
                        'persons':dict(sorted(persons.items(),key=lambda x:-x[1])[:10])}
    J.dump(allstats,open('savepoints/position_mononym_stats.json','w',encoding='utf-8'),ensure_ascii=False,indent=1)
    return allstats

if __name__=='__main__' and '--store' in sys.argv:
    s=store(); print('stored', len(s), 'position-aware mononyms')
