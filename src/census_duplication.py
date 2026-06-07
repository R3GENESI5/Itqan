"""READ-ONLY census of duplication landscape. No writes.
Quantifies each category in the taxonomy so the dedup engine is designed
against real numbers."""
import json, re, sys
from collections import defaultdict, Counter

CHK = 'D:/Hadith/src/savepoints/sanadset_final_20260416_213020.json'
GK  = 'D:/Hadith/src/external_hadith/gk_json/gk_narrators.json'

DIACR = re.compile(r'[ً-ْ]')
def norm(s):
    if not s: return ''
    s = DIACR.sub('', s); s = re.sub(r'[أإآا]','ا',s)
    return s.replace('ة','ه').replace('ـ','').replace('ى','ي').strip()

print('Loading DB...', file=sys.stderr)
db = json.load(open(CHK, encoding='utf-8'))
profs = db['profiles']

# alive set
alive = {pid:p for pid,p in profs.items()
         if isinstance(p,dict) and not p.get('_redirect_to')
         and not p.get('_kinship_placeholder') and not p.get('_abandoned')}
print(f'Total profiles: {len(profs):,}   Alive: {len(alive):,}', file=sys.stderr)

# Load GK authority — build kunya/laqab/name -> rawy_ids lookup
print('Loading GK authority...', file=sys.stderr)
gk = json.load(open(GK, encoding='utf-8'))['narrators']
gk_form_to_ids = defaultdict(set)   # normalized surface form -> set of gk rawy_ids
gk_id_count = 0
for rid, e in gk.items():
    if not isinstance(e, dict): continue
    gk_id_count += 1
    for fld in ('name','alt_name','kunya','laqab','full_nasab'):
        v = e.get(fld)
        if not v: continue
        # kunya/laqab may be comma-or-،-separated multi
        for part in re.split(r'[،,]', v):
            n = norm(part)
            if n and len(n) >= 3:
                gk_form_to_ids[n].add(rid)
print(f'GK entries: {gk_id_count:,}   distinct surface forms: {len(gk_form_to_ids):,}', file=sys.stderr)

# ---- CATEGORY COUNTS ----
report = {}

# Index alive by normalized full_name
norm_to_pids = defaultdict(list)
for pid,p in alive.items():
    norm_to_pids[norm(p.get('full_name') or pid)].append(pid)

# Cat 1: orthographic-only duplicate clusters (same norm, >1 alive pid)
ortho = {n:pids for n,pids in norm_to_pids.items() if len(pids)>1}
report['orthographic_dup_clusters'] = len(ortho)
report['orthographic_dup_profiles'] = sum(len(v) for v in ortho.values())

# Cat 2: particle-prefix forms that have a stripped-form alive twin
particle_dups = 0
for pid,p in alive.items():
    n = norm(p.get('full_name') or pid)
    toks = n.split()
    if toks and len(toks[0])>=4 and toks[0][0] in 'ولفب':
        stripped = ('ال'+toks[0][2:]) if toks[0].startswith('لل') else toks[0][1:]
        cand = ' '.join([stripped]+toks[1:])
        if cand in norm_to_pids:
            particle_dups += 1
report['particle_prefix_with_twin'] = particle_dups

# Cat 3: kunya-case variants (أبي/أبا/أبو X all alive)
kunya_case = 0
for pid,p in alive.items():
    n = norm(p.get('full_name') or pid)
    if n.startswith('ابي ') or n.startswith('ابا '):
        abu_form = 'ابو '+n[4:]
        if abu_form in norm_to_pids:
            kunya_case += 1
report['kunya_case_variant_with_twin'] = kunya_case

# Cat 4+5: profiles whose name is a kunya/laqab that GK maps to a known person
# (i.e. could collapse to an ism node)
kunya_resolvable = 0
laqab_resolvable = 0
mononym_ambiguous = 0
for pid,p in alive.items():
    n = norm(p.get('full_name') or pid)
    ids = gk_form_to_ids.get(n, set())
    toks = n.split()
    is_kunya = toks and toks[0] in ('ابو','ابي','ابا','ام')
    if len(ids) == 1:
        if is_kunya: kunya_resolvable += 1
        else: laqab_resolvable += 1
    elif len(ids) > 1:
        mononym_ambiguous += 1
report['kunya_resolvable_via_gk'] = kunya_resolvable
report['laqab_or_name_resolvable_via_gk'] = laqab_resolvable
report['ambiguous_form_multiple_gk_ids'] = mononym_ambiguous

# Cat 7: partial-name subsumption — alive short name that is a prefix-token-subset
# of a longer alive name (e.g. شعبة ⊂ شعبة بن الحجاج)
# Count short (<=2 token) alive names that appear as the leading tokens of a longer alive name
short_names = [(pid, norm(p.get('full_name') or pid)) for pid,p in alive.items()
               if len(norm(p.get('full_name') or pid).split()) <= 2]
longer_index = defaultdict(list)  # first-token -> list of (pid, norm, tokens)
for pid,p in alive.items():
    n = norm(p.get('full_name') or pid)
    t = n.split()
    if len(t) >= 3:
        longer_index[t[0]].append((pid, n, t))
subsumption = 0
for spid, sn in short_names:
    st = sn.split()
    if not st: continue
    for lpid, ln, lt in longer_index.get(st[0], []):
        if lt[:len(st)] == st:   # short is exact prefix of long
            subsumption += 1
            break
report['partial_name_subsumption_candidates'] = subsumption

# How many alive profiles already carry a gk_rawy_id?
with_gk = sum(1 for p in alive.values() if p.get('gk_rawy_id'))
report['alive_with_gk_id'] = with_gk
report['alive_without_gk_id'] = len(alive) - with_gk

# How many distinct gk_rawy_ids are used by >1 alive profile (= split person)?
gkid_to_pids = defaultdict(list)
for pid,p in alive.items():
    g = p.get('gk_rawy_id')
    if g: gkid_to_pids[str(g)].append(pid)
split_by_gkid = {g:pids for g,pids in gkid_to_pids.items() if len(pids)>1}
report['same_gkid_multiple_alive_profiles'] = len(split_by_gkid)
report['profiles_in_gkid_splits'] = sum(len(v) for v in split_by_gkid.values())

print('\n=== DUPLICATION CENSUS ===')
for k,v in report.items():
    print(f'  {k:42s} {v:>10,}')

# Show a few same-gkid split examples
print('\n=== Same GK-id, multiple alive profiles (top by count) ===')
for g,pids in sorted(split_by_gkid.items(), key=lambda x:-len(x[1]))[:10]:
    names = [alive[pid].get('full_name') or pid for pid in pids]
    gname = gk.get(g,{}).get('name','?')
    print(f'  gk[{g}] {gname[:30]:30s}: {len(pids)} copies -> {names[:4]}')

json.dump(report, open('D:/Hadith/src/savepoints/census_duplication.json','w',encoding='utf-8'),
          ensure_ascii=False, indent=1)
print('\nSaved census_duplication.json', file=sys.stderr)
