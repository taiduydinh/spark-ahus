#!/usr/bin/env python3
"""Resumable correctness- and performance-gated full Spark-AHUS benchmark suite.

All 56 configured (non-ambiguous) paper/pilot cases and 25 exploratory cases.
Always keeps new results separate from the original scheduler and baselines.
Runs one Spark application at a time for clean isolated measurements.
"""
from __future__ import annotations
import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone
from dataclasses import dataclass

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from overnight_optimized import compare_ahus, sha, atomic_json

PROTOCOL = ROOT / 'ahus_paper_protocol.json'
STATE = ROOT / 'results' / 'optimized_full_suite'
LOGS = ROOT / 'logs' / 'optimized_full_suite'
EXTRA = {
    'Leviathan':'prepared/direct_utility/Leviathan.txt',
    'Kosarak10K':'prepared/direct_utility/Kosarak10K.txt',
    'Yoochoose':'prepared/direct_utility/Yoochoose.txt',
    'FIFA':'prepared/direct_utility/FIFA.txt',
    'C8T1S5I8N5K':'prepared/direct_utility/C8T1S5I8N5K.txt',
}
PREFLIGHT = ['tafeng_length/L3', 'tafeng_length/L5', 'bms2_unrestricted/T3']
MAX_GLOBAL_SEQUENCES = 120000
OLD_TAFENG_L5_SECONDS = 329.25144779204857

@dataclass(frozen=True)
class Case:
    key: str
    input: Path
    threshold_type: str
    threshold: str
    length: int
    kind: str
    group: str
    case: str
    first_n: int = 0
    cores: int = 12
    partitions: int = 12


def now():
    return datetime.now(timezone.utc).isoformat(timespec='seconds')


def build_cases(protocol: dict, default_cores: int, default_partitions: int):
    cases=[]
    for group, g in protocol['groups'].items():
        # Paper SIGN pruning series has no published length cap. Keep the
        # separate explicitly documented pilot instead of inventing a cap.
        if group=='sign_pruning':
            continue
        src=ROOT/protocol['datasets'][g['dataset']]['path']
        for cfg in g['cases']:
            ttype='min-utility' if 'min_utility' in cfg else 'min-utility-ratio'
            val=str(cfg.get('min_utility', cfg.get('threshold_ratio')))
            length=int(cfg.get('max_pattern_length') or 0)
            ispilot=group=='sign_pilot_L3'
            cases.append(Case(group+'/'+cfg['case'],src,ttype,val,length,
                              'pilot' if ispilot else 'paper',group,cfg['case'],
                              int(cfg.get('first_n_sequences') or 0),
                              4 if ispilot else default_cores,
                              4 if ispilot else default_partitions))
    for name, rel in EXTRA.items():
        src=ROOT/rel
        group='exploratory_'+name
        # Reuse original recorded calibration, not a newly chosen threshold.
        meta=ROOT/'results'/'batch'/'meta'/f'{group}__T5.json'
        if meta.is_file():
            raw=json.loads(meta.read_text(encoding='utf-8'))
            calibration=(raw.get('calibration') or {})
            top=calibration.get('top_singleton_utility')
        else:
            top=None
        if top is None and src.is_file():
            # Recover the original calibration method when historical meta absent.
            from run_all_spark_ahus import singleton_max
            top=singleton_max(src)['top_singleton_utility']
        if top is None:
            continue
        for j,m in enumerate((5,3,2,1.5,1),1):
            threshold=str(max(1,math.ceil(float(top)*m)))
            cases.append(Case(group+f'/T{j}',src,'min-utility',threshold,3,
                              'exploratory',group,f'T{j}',0,default_cores,
                              default_partitions))
    return cases


def prepare_first_n(case: Case) -> Path:
    """Keep first N valid sequence records, as specified in protocol approximation."""
    if not case.first_n:
        return case.input
    out=ROOT/'prepared'/'optimized_full_suite'/f'{case.input.stem}_N{case.first_n}.txt'
    marker=out.with_suffix(out.suffix+'.identity.json')
    source_identity={'input':str(case.input.resolve()),
                     'size':case.input.stat().st_size,
                     'mtime_ns':case.input.stat().st_mtime_ns,
                     'first_n':case.first_n}
    if out.exists() and marker.exists():
        try:
            if json.loads(marker.read_text())==source_identity:
                return out
        except (ValueError,OSError):
            pass
    out.parent.mkdir(parents=True,exist_ok=True)
    temp=out.with_suffix(out.suffix+'.tmp')
    count=0
    with case.input.open('r',encoding='utf-8') as src, temp.open('w',encoding='utf-8') as dest:
        for line in src:
            if not line.strip() or line.lstrip().startswith(('#','%','@')):
                continue
            dest.write(line)
            count+=1
            if count==case.first_n:
                break
    if count!=case.first_n:
        temp.unlink(missing_ok=True)
        raise ValueError(f'Only {count} sequences for {case.key}; expected {case.first_n}')
    temp.replace(out)
    atomic_json(marker,source_identity)
    return out


def sequence_count_at_most(path: Path, cap: int, cache: dict) -> bool:
    """Conservative precheck (comments included): never overshoot memory cap."""
    key=(str(path),path.stat().st_size,path.stat().st_mtime_ns)
    if key not in cache:
        n=0
        with path.open('rb') as f:
            for _ in f:
                n+=1
                if n>cap:break
        cache[key]=n<=cap
    return cache[key]


def run_case(case: Case, args, deadline, source_sha, count_cache, *, require_pruning):
    safe=case.group+'__'+case.case
    meta=STATE/'meta'/(safe+'.json')
    stats=STATE/(safe+'_stats.json')
    output=STATE/(safe+'_patterns.txt')
    log=LOGS/(safe+'.log')
    if not case.input.is_file():
        print('MISSING dataset:',case.key,case.input,flush=True)
        return 'missing',None
    inp=prepare_first_n(case)
    use_pruning=sequence_count_at_most(inp,MAX_GLOBAL_SEQUENCES,count_cache)
    if require_pruning and not use_pruning:
        raise ValueError(f'Preflight {case.key} exceeds presence index guard')
    mode='global-presence' if use_pruning else 'plain-fallback-large-dataset'
    signature={'key':case.key,'source_sha256':source_sha,
               'dataset_sha256':sha(inp),'kind':case.kind,
               'threshold_type':case.threshold_type,'threshold':case.threshold,
               'max_pattern_length':case.length,'cores':case.cores,
               'partitions':case.partitions,'mode':mode}
    if meta.exists() and stats.exists() and output.exists():
        try:
            old=json.loads(meta.read_text())
            if all(old.get(k)==v for k,v in signature.items()):
                if old.get('status')=='complete':
                    match,msg=compare_ahus(case.key,output) if case.kind!='exploratory' else (None,'exploratory')
                    if match is False or (case.key in PREFLIGHT and match is not True):
                        print('RECHECK MISMATCH:',case.key,msg,flush=True)
                        return 'mismatch',None
                    print('REUSE',case.key,mode,msg,flush=True)
                    return 'complete',json.loads(stats.read_text())
                if old.get('status') in ('timeout','failed') and not args.retry_timeouts:
                    print('PREVIOUS',old['status'],case.key,'(pass --retry-timeouts to retry)',flush=True)
                    return old['status'],None
        except (OSError,ValueError):
            pass
    if time.monotonic()>=deadline:
        return 'budget',None
    cmd=['spark-submit','--master',f'local[{case.cores}]',
         '--driver-memory',('8g' if case.kind=='pilot' else args.driver_memory),
         str(ROOT/'spark_ahus.py'),'--input',str(inp),
         '--value-mode','utility',
         '--'+case.threshold_type,case.threshold,
         '--partitions',str(case.partitions),
         '--output',str(output),'--stats-json',str(stats)]
    if case.length:
        cmd.extend(['--max-pattern-length',str(case.length)])
    if use_pruning:
        cmd+=['--global-itemset-swu-pruning',
              '--global-bound-max-sequences',str(MAX_GLOBAL_SEQUENCES)]
    stats.parent.mkdir(parents=True,exist_ok=True)
    log.parent.mkdir(parents=True,exist_ok=True)
    output.unlink(missing_ok=True); stats.unlink(missing_ok=True)
    begun=time.monotonic()
    print('START',case.key,'mode='+mode,'cores='+str(case.cores),
          'partitions='+str(case.partitions),'timeout='+str(args.timeout),now(),flush=True)
    timed_out=False
    with log.open('w',encoding='utf-8') as f:
        f.write('COMMAND: '+' '.join(cmd)+'\n');f.flush()
        proc=subprocess.Popen(cmd,cwd=ROOT,stdout=f,stderr=subprocess.STDOUT,
                              env={**os.environ,'SPARK_LOCAL_IP':'127.0.0.1',
                                   'PYTHONDONTWRITEBYTECODE':'1'},start_new_session=True)
        try:
            proc.wait(timeout=max(1,min(args.timeout,deadline-time.monotonic())))
        except subprocess.TimeoutExpired:
            timed_out=True
            try:os.killpg(proc.pid,signal.SIGTERM)
            except ProcessLookupError:pass
            try:proc.wait(timeout=20)
            except subprocess.TimeoutExpired:
                try:os.killpg(proc.pid,signal.SIGKILL)
                except ProcessLookupError:pass
                proc.wait()
    wall=time.monotonic()-begun
    status='timeout' if timed_out else 'complete' if proc.returncode==0 and output.exists() and stats.exists() else 'failed'
    details=''
    result=None
    if status=='complete':
        try:
            result=json.loads(stats.read_text())
            if use_pruning and result.get('promising_items',0)>0 and result.get('global_itemset_swu_pruning') is not True:
                raise ValueError('Global pruning was not enabled in Spark statistics')
            if result.get('max_pattern_length') != (case.length if case.length else 2**31-1):
                raise ValueError('Maximum pattern length mismatch')
            if not math.isclose(float(result['min_utility']),
                                (float(case.threshold) if case.threshold_type=='min-utility' else
                                 float(case.threshold)*float(result['total_utility'])),
                                rel_tol=1e-10,abs_tol=0.05):
                raise ValueError('Effective minimum utility mismatch')
            if case.kind!='exploratory':
                matched,details=compare_ahus(case.key,output)
                if matched is False:
                    status='mismatch'
                elif case.key in PREFLIGHT and matched is not True:
                    status='failed'; details='AHUS patterns unavailable for correctness gate'
                elif matched is None:
                    # When no completed baseline is available, compare count to
                    # the original Spark statistics when they exist.
                    old_stats=ROOT/'results'/'paper_protocol'/case.group/(case.case+'_stats.json')
                    if old_stats.exists():
                        old=json.loads(old_stats.read_text())
                        if int(old.get('husp_count',-1))!=int(result.get('husp_count',-2)):
                            status='mismatch';details='Count disagrees with original Spark run'
        except (KeyError,ValueError,TypeError) as exc:
            status='failed';details=str(exc)
    atomic_json(meta,{**signature,'status':status,'comparison':details,
                      'wall_sec':round(wall,4),'exit_code':proc.returncode,
                      'timestamp':now(),'stats':str(stats),'patterns':str(output),
                      'log':str(log)})
    print('END',case.key,status,'wall='+f'{wall:.1f}s',details,flush=True)
    return status,result


def main():
    ap=argparse.ArgumentParser(description=__doc__)
    ap.add_argument('action',choices=['plan','preflight','overnight','status'])
    ap.add_argument('--budget-hours',type=float,default=16.0)
    ap.add_argument('--timeout',type=int,default=3600)
    ap.add_argument('--cores',type=int,default=12)
    ap.add_argument('--partitions',type=int,default=12)
    ap.add_argument('--driver-memory',default='12g')
    ap.add_argument('--retry-timeouts',action='store_true')
    args=ap.parse_args()
    if args.action=='status':
        for p in sorted((STATE/'meta').glob('*.json')):
            d=json.loads(p.read_text());print(d.get('key'),d.get('status'),d.get('wall_sec'),d.get('comparison',''))
        return 0
    if args.budget_hours<=0 or min(args.timeout,args.cores,args.partitions)<1:
        ap.error('Invalid budget, timeout, cores, or partitions')
    protocol=json.loads(PROTOCOL.read_text(encoding='utf-8'))
    all_cases=build_cases(protocol,args.cores,args.partitions)
    by_key={case.key:case for case in all_cases}
    print(f'CONFIGURED {len(all_cases)} cases (expect 81: 56 paper/pilot + 25 exploratory).',flush=True)
    print('Unspecified SIGN pruning length series excluded: no defensible published cap.',flush=True)
    print('Original benchmark results NOT overwritten. New output:',STATE,flush=True)
    print('Large datasets (>120000 lines) use original local-mining mode as a labeled fallback.',flush=True)
    if len(all_cases)!=81:
        print('ERROR: Expected 81 cases. Check prepared exploratory data/calibration metadata.',file=sys.stderr)
        return 2
    if args.action=='plan':
        for case in all_cases:
            print(case.key,case.threshold_type,case.threshold,'length=',case.length,
                  'input=',case.input,'first_n=',case.first_n)
        return 0
    if not all(by_key[key].input.exists() for key in PREFLIGHT):
        print('ERROR: Preflight datasets are missing',file=sys.stderr);return 2
    if '--global-itemset-swu-pruning' not in (ROOT/'spark_ahus.py').read_text():
        print('ERROR: Run optimizations/apply_global_swu_fix.py first',file=sys.stderr)
        return 2
    # Filesystem lock: never launch duplicate full-suite controllers.
    import fcntl
    STATE.mkdir(parents=True,exist_ok=True)
    handle=(STATE/'controller.lock').open('a+')
    try: fcntl.flock(handle,fcntl.LOCK_EX|fcntl.LOCK_NB)
    except BlockingIOError:
        print('ERROR: Optimized full-suite controller already running',file=sys.stderr);return 2
    deadline=time.monotonic()+args.budget_hours*3600
    source_sha=sha(ROOT/'spark_ahus.py')
    counts={}
    for key in PREFLIGHT:
        status,result=run_case(by_key[key],args,deadline,source_sha,counts,require_pruning=True)
        if status!='complete':
            print('STOP PRE-FLIGHT CORRECTNESS GATE',key,status,flush=True);return 3
    # Require a meaningful real Spark L5 speedup against the archived original.
    l5=json.loads((STATE/'tafeng_length__L5_stats.json').read_text())
    measured=float(l5['runtime_sec'])
    reference_path=ROOT/'results'/'paper_protocol'/'tafeng_length'/'L5_stats.json'
    reference=OLD_TAFENG_L5_SECONDS
    if reference_path.exists():
        reference=float(json.loads(reference_path.read_text())['runtime_sec'])
    ratio=reference/measured if measured>0 else float('inf')
    print(f'PERFORMANCE GATE TaFeng L5 previous={reference:.3f}s '
          f'new={measured:.3f}s speedup={ratio:.2f}x',flush=True)
    if ratio<1.2:
        print('STOP PERFORMANCE GATE: need >=1.20x improvement; '
              'full rerun was NOT started.',flush=True)
        return 4
    if args.action=='preflight':
        print('PREFLIGHT PASS: safe to run action=overnight',flush=True);return 0
    blocked=set()
    totals={'complete':0,'reused':0,'timeout':0,'failed':0,'mismatch':0,'missing':0,'blocked':0}
    for case in all_cases:
        if case.key in PREFLIGHT:continue
        if case.group in blocked:
            totals['blocked']+=1; continue
        if time.monotonic()>=deadline:
            print('BUDGET REACHED; resume later with the same command',flush=True);break
        status,result=run_case(case,args,deadline,source_sha,counts,require_pruning=False)
        totals[status]=totals.get(status,0)+1
        if status in ('timeout','failed','mismatch','missing'):
            # Original protocol scheduler blocks harder cases in each group.
            blocked.add(case.group)
            print('BLOCK remaining thresholds in group',case.group,flush=True)
        if status=='mismatch':
            print('STOP: correctness mismatch, inspect',LOGS,flush=True)
            return 5
    print('FINISHED',totals,'Preflight already passed.',flush=True)
    return 0

if __name__=='__main__':
    raise SystemExit(main())
