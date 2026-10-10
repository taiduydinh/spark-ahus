#!/usr/bin/env python3
"""Offline correctness regression for optional global-presence pruning (no Spark needed)."""
import random
import sys
from collections import defaultdict
from pathlib import Path
ROOT=Path(__file__).resolve().parent.parent
sys.path.insert(0,str(ROOT))
from spark_ahus import (parse_sequence_line, summarize_partition, pack_partition,
    build_global_presence_index, GlobalPresenceSWUBound, PureArrayLocalAHUS,
    CandidateTrie, TrieVerifier)

rng=random.Random(20261010)
for trial in range(30):
    n=rng.randint(6,16)
    lines=[]
    for j in range(n):
        groups=[]
        for k in range(rng.randint(1,4)):
            picks=sorted(rng.sample(range(1,6),rng.randint(1,3)))
            groups.append([(x,rng.randint(1,10)) for x in picks])
        su=sum(v for group in groups for x,v in group)
        lines.append(' '.join(' '.join(f'{x}[{v}]' for x,v in group)+' -1' for group in groups)+f' -2 SUtility:{su}')
    ratio=rng.choice([.15,.3,.45,.7])
    parsed=[]; swu=defaultdict(float)
    for line in lines:
        groups,su=parse_sequence_line(line,'utility',None)
        distinct=set(k for group in groups for k,_ in group)
        parsed.append((float(su),distinct))
        for item in distinct:swu[item]+=su
    threshold=sum(su for su,_ in parsed)*ratio
    promising=sorted(item for item,util in swu.items() if util>=threshold)
    dense={item:i for i,item in enumerate(promising)}
    if not promising:continue
    masks,weights=build_global_presence_index(((su,[dense[x] for x in present if x in dense])
                                                for su,present in parsed),len(promising))
    partitions=[lines[::3],lines[1::3],lines[2::3]]
    plain=set(); pruned=set()
    for j,part in enumerate(partitions):
        if not part:continue
        block=next(pack_partition(j,part,'utility',None,dense))
        local_threshold=block.total_utility*ratio
        x=PureArrayLocalAHUS(block,local_threshold,3)
        y=PureArrayLocalAHUS(block,local_threshold,3,
                             global_bound=GlobalPresenceSWUBound(masks,weights,threshold))
        plain.update(p for p,u in x.iter_patterns())
        pruned.update(p for p,u in y.iter_patterns())
    assert pruned<=plain,(trial,'pruning added candidates')
    global_block=next(pack_partition(0,lines,'utility',None,dense))
    def exact(candidates):
        pats=sorted(candidates)
        trie=CandidateTrie.build(pats)
        found=dict(TrieVerifier(global_block,trie).iter_local_utilities())
        return {p:found.get(i,0.0) for i,p in enumerate(pats) if found.get(i,0.0)+1e-8>=threshold}
    a=exact(plain);b=exact(pruned)
    assert a==b,(trial,a,b)
print('PASS: 30 randomized datasets; exact verified HUSPs identical before/after pruning')

p=ROOT/'prepared'/'paper_original'/'Tafeng.txt'
base=ROOT/'results'/'baselines'/'ahus'/'tafeng_length'/'L5_patterns.txt'
if p.is_file() and base.is_file():
    import re
    lines=p.read_text().splitlines()
    swu=defaultdict(float); rows=[]
    for line in lines:
        itemsets,su=parse_sequence_line(line,'utility',None)
        items=set(k for group in itemsets for k,_ in group)
        rows.append((su,items))
        for item in items:swu[item]+=su
    ratio=.06045; threshold=ratio*sum(su for su,_ in rows)
    prom=sorted(item for item,u in swu.items() if u>=threshold)
    dense={item:i for i,item in enumerate(prom)}
    masks,sus=build_global_presence_index(((su,[dense[k] for k in present if k in dense])
                                           for su,present in rows),len(prom))
    chunks=[[] for _ in range(12)]
    split=(p.stat().st_size+11)//12
    offset=0
    for line in lines:
        chunks[min(11,offset//split)].append(line)
        offset+=len(line.encode('utf-8'))+1
    found=set()
    for j,part in enumerate(chunks):
        block=next(pack_partition(j,part,'utility',None,dense))
        miner=PureArrayLocalAHUS(block,ratio*block.total_utility,5,
                                 global_bound=GlobalPresenceSWUBound(masks,sus,threshold))
        found.update(tuple(-1 if x==-1 else prom[x] for x in pat)
                     for pat,_ in miner.iter_patterns())
    required=set()
    for line in base.read_text().splitlines():
        groups=re.findall(r'\(([^()]*)\)',line.split(':')[0]);pattern=[]
        for group in groups:
            if pattern:pattern.append(-1)
            pattern.extend(int(v)-1 for v in group.split())
        required.add(tuple(pattern))
    missing=required-found
    assert not missing, f'Known TaFeng patterns missing: {missing}'
    print(f'PASS: TaFeng L5 contains all {len(required)} AHUS baseline HUSPs; '
          f'{len(found)} local candidates with approximate 12-way file splits')
else:
    print('INFO: TaFeng coverage test skipped; dataset or AHUS baseline unavailable')
