#!/usr/bin/env python3
"""Resumable AHUS / USpan / HUS-Span baseline scheduler, isolated from Spark controller.

Uses the paper protocol from the existing project. One CPU per Python process, on
physical cores not allocated to the ongoing 3x12-core Spark controller.
"""
from __future__ import annotations
import argparse
import concurrent.futures as cf
import csv
from dataclasses import dataclass
from decimal import Decimal, ROUND_CEILING
from datetime import datetime, timezone
import fcntl
import json
import os
from pathlib import Path
import re
import resource
import shlex
import signal
import subprocess
import sys
import time
from types import SimpleNamespace

BASE=Path(__file__).resolve().parent
ROOT=BASE.parent
STATE=ROOT/'results'/'baselines'
LOG=ROOT/'logs'/'baselines'
PREP=ROOT/'prepared'/'baselines'
ALGORITHMS=('ahus','uspan','husspan')
sys.path.insert(0,str(ROOT))
from prepare_baseline_inputs import prepare, prefix, sha256
from run_all_spark_ahus import get_allowed_physical_cpus,exploratory_jobs

@dataclass(frozen=True)
class Case:
    key:str
    group:str
    case:str
    source:Path
    dataset:str
    kind:str
    ratio:str|None
    minutil:str|None
    length:int
    first_n:int|None


def now():return datetime.now(timezone.utc).isoformat(timespec='seconds')

def cases_from_protocol(suite,sign_length):
    p=json.loads((ROOT/'ahus_paper_protocol.json').read_text())
    result=[]
    for name,g in p['groups'].items():
        if name=='sign_pruning' and sign_length=='skip':continue
        d=p['datasets'][g['dataset']]
        path=ROOT/d['path']
        if not path.is_file():continue
        for c in g['cases']:
            leng=c.get('max_pattern_length')
            if name=='sign_pruning':leng=0 if sign_length=='unrestricted' else int(sign_length)
            if leng is None:raise ValueError(f'missing pattern length for {name}')
            result.append(Case(name+'/'+c['case'],name,c['case'],path,g['dataset'],'paper',
                               c.get('threshold_ratio'),str(c['min_utility']) if 'min_utility' in c else None,
                               int(leng),c.get('first_n_sequences')))
    if suite=='all':
        opts=SimpleNamespace(cores=12,partitions=12,driver_memory='12g',no_calibration=False)
        more,missing=exploratory_jobs(opts)
        for group,items in more.items():
            for j in items:
                result.append(Case(j.key,j.group,j.case,j.input_path,group.replace('exploratory_',''),
                                   'exploratory',None,str(j.min_util),3,None))
    return result


def current_spark_cores():
    # Read the active launcher, rather than assuming its CPU allocation never changed.
    for d in Path('/proc').iterdir():
        if not d.name.isdigit():continue
        try:cmd=(d/'cmdline').read_bytes().replace(b'\x00',b' ').decode('utf-8','replace')
        except OSError:continue
        if 'run_all_spark_ahus.py' in cmd and ' run ' in cmd and ('python ' in cmd or 'python -u' in cmd):
            tokens=cmd.split()
            def opt(name,default):
                if name in tokens:
                    i=tokens.index(name)
                    return int(tokens[i+1])
                return default
            return opt('--jobs',3)*opt('--cores',12),cmd
    return None,None


def free_physical(reserved,workers):
    physical, allowed=get_allowed_physical_cpus()
    # Existing Spark scheduler uses physical[slot*cores:(slot+1)*cores].
    live,launcher=current_spark_cores()
    if live is not None and reserved!=live:
        raise RuntimeError(f'ACTIVE Spark scheduler reserves {live} physical cores, but '
                           f'--spark-reserved={reserved}. Use --spark-reserved {live}; '
                           'never risk overlapping CPU affinity.')
    if reserved+workers+2>len(physical):
        raise RuntimeError(f'insufficient cores: reserved={reserved}, workers={workers}, '
                           f'physical={len(physical)}; leaving at least 2 for OS')
    return physical[reserved:reserved+workers], len(physical),len(allowed),live


def check_library(repo):
    cmd=[sys.executable,str(BASE/'baseline_case.py'),'--help']
    r=subprocess.run(cmd,capture_output=True,text=True)
    if r.returncode:raise RuntimeError(r.stderr)
    if repo:
        path=Path(repo)
        for name in ('ahus','uspan','husspan'):
            if not (path/'pypatternminer'/f'{name}.py').is_file():
                raise RuntimeError(f'Missing {path}/pypatternminer/{name}.py')
    import importlib
    if repo:sys.path.insert(0,str(Path(repo).resolve()))
    importlib.invalidate_caches()
    from pypatternminer.ahus import AlgoAHUS
    from pypatternminer.uspan import AlgoUSpan
    from pypatternminer.husspan import HUSSpan
    import pypatternminer.ahus,pypatternminer.uspan,pypatternminer.husspan
    return {x:str(getattr(sys.modules['pypatternminer.'+x],'__file__')) for x in ALGORITHMS}


def normalized(case, cache):
    key=str(case.source.resolve())
    if key not in cache:
        name=case.dataset
        out=PREP/(name+'.txt')
        print('PREPARE',name,case.source,flush=True)
        cache[key]=prepare(case.source,out)
    info=cache[key];normal=Path(info['target'])
    if case.first_n:
        path=prefix(normal,case.first_n)
        su=0; n=0
        with path.open() as f:
            for line in f:
                su+=int(line.rsplit('SUtility:',1)[1].strip());n+=1
        info=dict(info);info['total_utility_scaled']=su;info['sequences']=n
    return normal if not case.first_n else path, Path(info['external']),info


def threshold_scaled(case,info):
    sc=int(info['scale'])
    if case.ratio is not None:
        v=Decimal(case.ratio)*Decimal(info['total_utility_scaled'])
    else:v=Decimal(case.minutil)*Decimal(sc)
    # All common normalized utilities are integers; ceil ensures >= absolute threshold.
    return int(v.to_integral_value(rounding=ROUND_CEILING))


def result_paths(case,algorithm):
    base=STATE/algorithm/case.group
    return base/(case.case+'_stats.json'),base/(case.case+'_patterns.txt'),LOG/algorithm/case.group/(case.case+'.log'),STATE/'meta'/algorithm/case.group/(case.case+'.json')


def atomic_json(path,obj):
    path.parent.mkdir(parents=True,exist_ok=True)
    tmp=path.with_name(path.name+'.partial')
    tmp.write_text(json.dumps(obj,indent=2,sort_keys=True)+'\n')
    tmp.replace(path)


def valid_result(case,algo,meta):
    stats,_,_,_=result_paths(case,algo)
    if not stats.is_file() or not meta.is_file():return False
    try:
        m=json.loads(meta.read_text());s=json.loads(stats.read_text())
        return m.get('status')=='complete' and s['algorithm']==algo and s['husp_count']>=0 and \
               s['max_pattern_length']==case.length and s['min_utility_scaled']==m['min_utility_scaled'] and \
               s['library_source']==m['library_source'] and s['input']==m['input']
    except (KeyError,ValueError,TypeError):return False


def rss_kb(pid):
    try:
        with open(f'/proc/{pid}/status') as f:
            for l in f:
                if l.startswith('VmRSS:'):return int(l.split()[1])
    except OSError:pass
    return 0


def execute(case,algorithm,cpu,inp,external,meta_input,threshold,repo,timeout,budget_deadline):
    stats,out,log,meta_path=result_paths(case,algorithm)
    for x in (stats.parent,out.parent,log.parent,meta_path.parent):x.mkdir(parents=True,exist_ok=True)
    cmd=[sys.executable,str(BASE/'baseline_case.py'),'--algorithm',algorithm,
         '--input',str(inp),'--external',str(external),'--minutil',str(threshold),
         '--length',str(case.length),'--stats',str(stats),'--output',str(out)]
    if repo:cmd+=['--repo',str(Path(repo).resolve())]
    cmd=['taskset','-c',str(cpu),*cmd]
    started=time.monotonic();rc=None;peak=0
    print(f'START {algorithm}/{case.key} cpu={cpu} threshold_scaled={threshold} time={now()}',flush=True)
    with log.open('w') as logfile:
        logfile.write('EXEC: '+shlex.join(cmd)+'\n');logfile.flush()
        proc=subprocess.Popen(cmd,stdout=logfile,stderr=subprocess.STDOUT,cwd=ROOT,
                              start_new_session=True,env={**os.environ,'PYTHONDONTWRITEBYTECODE':'1'})
        while proc.poll() is None:
            peak=max(peak,rss_kb(proc.pid))
            if time.monotonic()-started>=timeout or time.monotonic()>=budget_deadline:
                try:os.killpg(proc.pid,signal.SIGTERM)
                except ProcessLookupError:pass
                try:proc.wait(timeout=8)
                except subprocess.TimeoutExpired:
                    try:os.killpg(proc.pid,signal.SIGKILL)
                    except ProcessLookupError:pass
                    proc.wait()
                break
            time.sleep(0.25)
        rc=proc.wait()
    elapsed=time.monotonic()-started
    status='complete' if rc==0 and stats.is_file() else 'timeout' if elapsed>=timeout-1 or time.monotonic()>=budget_deadline else 'failed'
    record={'status':status,'algorithm':algorithm,'group':case.group,'case':case.case,
            'input':str(inp),'source':str(case.source),'min_utility_scaled':threshold,
            'max_pattern_length':case.length,'library_source':str(Path(repo).resolve()) if repo else 'imported pypatternminer',
            'cpu':cpu,'exit_code':rc,'elapsed_wall_sec':round(elapsed,3),'observed_peak_rss_kb':peak,
            'stats_path':str(stats),'log':str(log),'finished':now(),
            'scale':meta_input['scale'],'item_id_offset':meta_input.get('item_id_offset',0),
            'duplicates_coalesced':meta_input.get('duplicates_coalesced',0),'source_sha256':meta_input['source_identity']['sha256']}
    atomic_json(meta_path,record)
    print(f'END {algorithm}/{case.key} {status} exit={rc} wall={elapsed:.1f}s',flush=True)
    return record


def save_summary(cases,algorithms):
    rows=[]
    for case in cases:
        for algo in algorithms:
            stats,out,log,meta=result_paths(case,algo)
            if not meta.exists():continue
            m=json.loads(meta.read_text());s={}
            if stats.is_file():
                try:s=json.loads(stats.read_text())
                except (ValueError,OSError):pass
            rows.append({'algorithm':algo,'group':case.group,'dataset':case.dataset,'case':case.case,
                 'status':m.get('status'),'max_length':case.length,
                 'paper_ratio':case.ratio,'unscaled_minutil':case.minutil,
                 'scaled_minutil':m.get('min_utility_scaled'), 'scale':m.get('scale'),
                 'item_id_offset':m.get('item_id_offset'), 'duplicates_coalesced':m.get('duplicates_coalesced'),
                 'husp_count':s.get('husp_count'),'runtime_sec':s.get('runtime_sec'),
                 'max_rss_kb':s.get('max_rss_kb'),'elapsed_wall_sec':m.get('elapsed_wall_sec'),
                 'cpu':m.get('cpu'),'log':str(log),'stats':str(stats),
                 'source_sha256':m.get('source_sha256')})
    path=STATE/'summary.csv';path.parent.mkdir(parents=True,exist_ok=True)
    if rows:
        tmp=path.with_name(path.name+'.partial')
        with tmp.open('w',newline='') as f:
            writer=csv.DictWriter(f,fieldnames=list(rows[0]));writer.writeheader();writer.writerows(rows)
        tmp.replace(path)
    return rows,path


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('action',choices=('plan','run','status','check','smoke'))
    p.add_argument('--suite',choices=('paper','all'),default='paper')
    p.add_argument('--jobs',type=int,default=6)
    p.add_argument('--spark-reserved',type=int,default=36)
    p.add_argument('--timeout',type=int,default=1200)
    p.add_argument('--budget-hours',type=float,default=12)
    p.add_argument('--algorithms',default='ahus,uspan,husspan')
    p.add_argument('--sign-length',default='skip',help='skip, unrestricted or explicit integer')
    p.add_argument('--repo',help='Path to original pypatternminer repository root; else imported installed package')
    p.add_argument('--retry-failed',action='store_true')
    args=p.parse_args()
    algos=tuple(x.strip().lower() for x in args.algorithms.split(','))
    if not algos or any(x not in ALGORITHMS for x in algos):p.error(f'algorithms must be subset of {ALGORITHMS}')
    if args.action=='status':
        summary=STATE/'summary.csv'
        if summary.is_file():
            with summary.open() as f:
                rows=list(csv.DictReader(f))
            from collections import Counter
            print(f'Baseline cases recorded: {len(rows)} statuses={dict(Counter(x["status"] for x in rows))}')
            print('summary:',summary)
            for x in rows[-12:]: print(x['algorithm'],x['group']+'/'+x['case'],x['status'],x.get('runtime_sec'))
        else: print('No baseline summary yet.')
        return 0
    cpus,phys,logical,live=free_physical(args.spark_reserved,args.jobs)
    if args.action in ('check','smoke'):
        sources=check_library(args.repo)
        print('Baseline sources:',json.dumps(sources,indent=2)); print('Reserved CPUs:',cpus,'Spark active:',live)
        if args.action=='check':return 0
        sample=PREP/'smoke_raw.txt'
        sample.parent.mkdir(parents=True,exist_ok=True)
        sample.write_text('1[2] -1 2[3] -1 -2 SUtility:5\n1[2] 2[1] -1 -2 SUtility:3\n2[2] -1 1[1] -1 -2 SUtility:3\n')
        m=prepare(sample,PREP/'smoke_int.txt')
        found={}
        for alg in algos:
            tmp=STATE/'smoke'/alg
            tmp.mkdir(parents=True,exist_ok=True)
            cmd=[sys.executable,str(BASE/'baseline_case.py'),'--algorithm',alg,'--input',m['target'],
                '--external',m['external'],'--minutil','3','--length','2',
                '--stats',str(tmp/'stats.json'),'--output',str(tmp/'patterns.txt')]
            if args.repo:cmd+=['--repo',args.repo]
            rr=subprocess.run(cmd,capture_output=True,text=True,timeout=90)
            print(f'SMOKE {alg} exit={rr.returncode} {rr.stdout.strip()}',flush=True)
            if rr.returncode:
                print(rr.stderr[-2000:]);return 3
            found[alg]=json.loads((tmp/'stats.json').read_text())['husp_count']
        print('SMOKE COUNTS',found)
        if len(set(found.values()))!=1:
            print('WARNING: pattern counts disagree; do NOT run overnight until investigated.')
            return 4
        return 0
    cases=cases_from_protocol(args.suite,args.sign_length)
    print(f'PLAN {len(cases)} cases x {len(algos)} algorithms = {len(cases)*len(algos)} jobs; '
          f'physical={phys}, logical={logical}, Spark reserved={args.spark_reserved}, '
          f'baseline CPUs={cpus}, active Spark reserved={live}.',flush=True)
    print('First groups:',', '.join(dict.fromkeys(j.group for j in cases)),flush=True)
    if args.action=='plan':return 0
    sources=check_library(args.repo)
    print('VERSIONS',json.dumps(sources),flush=True)
    STATE.mkdir(parents=True,exist_ok=True)
    lockfile=(STATE/'controller.lock').open('a+')
    try:fcntl.flock(lockfile,fcntl.LOCK_EX|fcntl.LOCK_NB)
    except BlockingIOError:
        print('Another baseline controller is running, refusing overlap.',file=sys.stderr);return 2
    deadline=time.monotonic()+args.budget_hours*3600
    cache={}; normalized_by_case={}
    for case in cases:
        if str(case.source.resolve()) not in cache:
            normalized(case,cache)
        normalized_by_case[case.key]=normalized(case,cache)
    # Independently schedule each algorithm/group as an ordered threshold chain.
    chains={}
    for case in cases:
        for algo in algos:chains.setdefault((algo,case.group),[]).append(case)
    indexes={key:0 for key in chains};blocked=set();futures={}; available=list(cpus)
    with cf.ThreadPoolExecutor(max_workers=len(cpus)) as pool:
        while True:
            while available and time.monotonic()<deadline:
                chosen=None
                for key,seq in chains.items():
                    if key in blocked or key in (k for k,cp in futures.values()):continue
                    pos=indexes[key]
                    while pos<len(seq):
                        c=seq[pos];_,_,_,meta=result_paths(c,key[0])
                        if valid_result(c,key[0],meta):
                            print('REUSE',key[0],c.key,flush=True);pos+=1;continue
                        if meta.is_file() and not args.retry_failed:
                            old=json.loads(meta.read_text())
                            if old.get('status') in ('timeout','failed'):
                                blocked.add(key);print('BLOCK',key,c.key,'previous failure',flush=True);break
                        break
                    indexes[key]=pos
                    if key in blocked or pos>=len(seq):continue
                    chosen=(key,seq[pos]);break
                if chosen is None:break
                key,case=chosen;cpu=available.pop(0)
                inp,ext,m=normalized_by_case[case.key]
                threshold=threshold_scaled(case,m)
                fut=pool.submit(execute,case,key[0],cpu,inp,ext,m,threshold,args.repo,args.timeout,deadline)
                futures[fut]=(key,cpu)
            if not futures:break
            completed,_=cf.wait(futures,return_when=cf.FIRST_COMPLETED)
            for fut in completed:
                key,cpu=futures.pop(fut);available.append(cpu);available.sort()
                try:r=fut.result()
                except Exception as e:
                    print('CRASH',key,str(e),flush=True);blocked.add(key);continue
                if r['status']=='complete':indexes[key]+=1
                else:blocked.add(key)
            _,summary=save_summary(cases,algos)
    _,summary=save_summary(cases,algos)
    print('FINISHED budget_expired=',time.monotonic()>=deadline,'summary=',summary,flush=True)
    return 0

if __name__=='__main__':
    try:raise SystemExit(main())
    except KeyboardInterrupt:
        print('Interrupted; saved results remain available.',file=sys.stderr)
        raise SystemExit(130)
