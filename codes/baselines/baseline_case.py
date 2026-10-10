#!/usr/bin/env python3
"""One single-core Python AHUS, USpan or HUS-Span run. No Spark APIs."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
import time
import resource
import re


def format_pattern(tokens,utility):
    groups=[[]]
    for t in tokens:
        t=int(t)
        if t==-1:
            if groups[-1]:groups.append([])
        else:
            groups[-1].append(t)
    groups=[g for g in groups if g]
    if not groups:raise ValueError("Empty baseline pattern")
    n=round(float(utility))
    if abs(float(utility)-n)>1e-5:raise ValueError(f"Noninteger scaled result: {utility}")
    return "<("+")(".join(" ".join(map(str,g)) for g in groups)+")>:"+str(int(n))


def normalize_output(algo,algorithm,out):
    tmp=out.with_name(out.name+".canonical.partial")
    try:
        with tmp.open("w") as w:
            if algorithm=="uspan":
                with out.open() as f:
                    for raw in f:
                        if not raw.strip():continue
                        before,sep,value=raw.strip().partition("#UTIL:")
                        if not sep:raise ValueError(f"USpan output missing #UTIL: {raw[:100]}")
                        tokens=[int(v) for v in before.strip().split()]
                        w.write(format_pattern(tokens,int(value.strip()))+"\n")
            else:
                patterns=algo.high_utility_patterns if algorithm=="ahus" else algo.high_utility_set
                for pattern,utility in patterns:
                    w.write(format_pattern(pattern,utility)+"\n")
        tmp.replace(out)
    finally:
        if tmp.exists():tmp.unlink()


def load_classes(repo):
    if repo:
        repo=Path(repo).resolve()
        if not (repo/'pypatternminer'/'ahus.py').is_file():
            raise RuntimeError(f'--repo must be the pypatternminer repository root; got {repo}')
        sys.path.insert(0,str(repo))
    from pypatternminer.ahus import AlgoAHUS
    from pypatternminer.uspan import AlgoUSpan
    from pypatternminer.husspan import HUSSpan
    return AlgoAHUS,AlgoUSpan,HUSSpan


def main():
    p=argparse.ArgumentParser();p.add_argument('--algorithm',choices=('ahus','uspan','husspan'),required=True)
    p.add_argument('--input',required=True);p.add_argument('--external',required=True)
    p.add_argument('--minutil',type=int,required=True);p.add_argument('--length',type=int,required=True)
    p.add_argument('--stats',required=True);p.add_argument('--output',required=True);p.add_argument('--repo')
    a=p.parse_args()
    AlgoAHUS,AlgoUSpan,HUSSpan=load_classes(a.repo)
    path=Path(a.input); stats=Path(a.stats);out=Path(a.output)
    stats.parent.mkdir(parents=True,exist_ok=True);out.parent.mkdir(parents=True,exist_ok=True)
    begin=time.perf_counter()
    if a.algorithm=='ahus':
        algo=AlgoAHUS();algo.set_max_pattern_length(a.length if a.length else 2**31-1)
        algo.load_data_with_internal_external(a.external,str(path),a.minutil,min_utility_is_ratio=False)
        algo.run_algorithm(str(out)); count=algo.pattern_count
    elif a.algorithm=='husspan':
        algo=HUSSpan();algo.set_max_pattern_length(a.length if a.length else 2**31-1)
        db=algo.load_data_with_internal_external(a.external,str(path),a.minutil)
        algo.run_algorithm(db,str(out)); count=algo.pattern_count
    else:
        algo=AlgoUSpan();algo.setMaxPatternLength(a.length if a.length else 1000000)
        algo.runAlgorithm(str(path),str(out),a.minutil);count=algo.patternCount
    elapsed=time.perf_counter()-begin
    normalize_output(algo,a.algorithm,out)
    rss=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    # Count actual output to catch mismatched counters/empty results.
    with out.open(encoding='utf-8') as f: filelines=sum(1 for _ in f)
    if count!=filelines:raise RuntimeError(f'pattern count {count} != output lines {filelines}')
    import pypatternminer.ahus,pypatternminer.uspan,pypatternminer.husspan
    import hashlib
    code_path=Path(sys.modules['pypatternminer.'+a.algorithm].__file__)
    code_sha=hashlib.sha256(code_path.read_bytes()).hexdigest()
    result={'code_sha256':code_sha,'algorithm':a.algorithm,'husp_count':int(count),'runtime_sec':elapsed,
        'max_rss_kb':rss,'min_utility_scaled':a.minutil,'max_pattern_length':a.length,
        'input':str(path),'patterns_file':str(out),'patterns_lines':filelines,
        'library_source':str(Path(a.repo).resolve()) if a.repo else 'imported pypatternminer',
        'python':sys.version.split()[0], 'cpu_affinity':sorted(os.sched_getaffinity(0))}
    tmp=stats.with_name(stats.name+'.partial')
    tmp.write_text(json.dumps(result,indent=2,sort_keys=True)+'\n');tmp.replace(stats)
    print(f'PASS {a.algorithm} HUSPs={count} time={elapsed:.3f}s maxRSS={rss/1024:.1f} MiB',flush=True)

if __name__=='__main__':main()
