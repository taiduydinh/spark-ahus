#!/usr/bin/env python3
"""Parallel, resumable Spark-AHUS benchmark scheduler.

Published AHUS paper groups are taken unchanged from ahus_paper_protocol.json.
Additional current datasets use clearly labeled *exploratory* thresholds calibrated
from their own maximum exact one-item utility, NOT claimed to come from AHUS paper.
"""
from __future__ import annotations
import argparse
import concurrent.futures as cf
import csv
import fcntl
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import re
import shlex
import shutil
import subprocess
import sys
import threading
import time

ROOT = Path(__file__).resolve().parent
STATE = ROOT / 'results' / 'batch'
PROTOCOL = ROOT / 'ahus_paper_protocol.json'
EXPLORATORY = {
    'Leviathan': 'prepared/direct_utility/Leviathan.txt',
    'Kosarak10K': 'prepared/direct_utility/Kosarak10K.txt',
    'Yoochoose': 'prepared/direct_utility/Yoochoose.txt',
    'FIFA': 'prepared/direct_utility/FIFA.txt',
    'C8T1S5I8N5K': 'prepared/direct_utility/C8T1S5I8N5K.txt',
}
# Conservative but meaningful exploratory values; do not label these as paper thresholds.
# Multipliers of the largest exact singleton utility in the chosen dataset.
MULTIPLIERS = (5.0, 3.0, 2.0, 1.5, 1.0)

@dataclass(frozen=True)
class Job:
    key: str
    group: str
    case: str
    input_path: Path
    stats: Path
    log: Path
    cmd: tuple[str,...]
    kind: str
    expected_length: int|None
    min_util: int|None = None
    calibration: dict|None = None
    source_exists: bool = True


def time_now():
    return datetime.now(timezone.utc).isoformat(timespec='seconds')


def atomic_json(path, obj):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix+'.tmp')
    tmp.write_text(json.dumps(obj, indent=2, sort_keys=True)+'\n', encoding='utf-8')
    tmp.replace(path)


def get_allowed_physical_cpus():
    allowed = sorted(os.sched_getaffinity(0)) if hasattr(os,'sched_getaffinity') else list(range(os.cpu_count() or 1))
    by_core = {}
    for cpu in allowed:
        base=Path(f'/sys/devices/system/cpu/cpu{cpu}/topology')
        try:
            socket=(base/'physical_package_id').read_text().strip()
            core=(base/'core_id').read_text().strip()
            k=(socket,core)
        except (OSError,ValueError):
            k=('cpu',str(cpu))
        by_core.setdefault(k, []).append(cpu)
    return [v[0] for _,v in sorted(by_core.items())], allowed


def singleton_max(dataset: Path) -> dict:
    """Max-embedding utility of any one-item pattern, summed over sequences.
    Allows recurrent items: max per sequence, not total occurrence utility.
    Standard prepared direct_utility token grammar: item[value] -1 ... -2.
    """
    cache = STATE/'calibration'/f'{dataset.stem}.json'
    identity = {'size':dataset.stat().st_size, 'mtime_ns':dataset.stat().st_mtime_ns,
                'path':str(dataset.resolve())}
    if cache.exists():
        try:
            old=json.loads(cache.read_text())
            if old.get('identity')==identity:
                return old
        except (OSError,ValueError): pass
    from collections import defaultdict
    util_by_item=defaultdict(float)
    sequences=0
    with dataset.open(encoding='utf-8') as f:
        for line_number,raw in enumerate(f,1):
            line=raw.strip()
            if not line or line[0] in '#%@': continue
            per_seq={}
            has_data=False
            for token in line.split():
                if token in ('-1','-2') or token.startswith(('SUtility:','#UTIL:')):
                    continue
                item_str,sep,val=token.partition('[')
                if not sep or not val.endswith(']'):
                    raise ValueError(f'Bad prepared direct-utility token at {dataset}:{line_number}: {token!r}')
                item=int(item_str)
                utility=float(val[:-1]); has_data=True
                if utility<0 or not math.isfinite(utility):
                    raise ValueError(f'Non-positive-utility benchmark input at {dataset}:{line_number}')
                if utility>per_seq.get(item,float('-inf')):
                    per_seq[item]=utility
            if not has_data: continue
            sequences+=1
            for item,value in per_seq.items(): util_by_item[item]+=value
    if not util_by_item: raise ValueError(f'No items found in {dataset}')
    best_item,best_value=max(util_by_item.items(),key=lambda x:(x[1],-x[0]))
    result={'identity':identity,'sequences':sequences,'top_singleton_item':best_item,
            'top_singleton_utility':best_value,'computed_at':time_now(),
            'method':'per-sequence maximum occurrence utility, summed across sequences'}
    atomic_json(cache,result)
    return result


def protocol_jobs(p, args):
    from run_ahus_paper_protocol import make_command
    result={}
    missing=[]; ambiguous=[]
    fake=type('Opts', (), {'length':None,'unrestricted':False,'execute':False,
                           'cores':args.cores, 'partitions':args.partitions,
                           'driver_memory':args.driver_memory, 'write_patterns':False})()
    for gname,group in p['groups'].items():
        if gname=='sign_pruning':
            # Paper does not provide max length. Never silently substitute 3.
            ambiguous.append(gname)
            if args.sign_length == 'skip': continue
            fake.length=(int(args.sign_length) if args.sign_length!='unrestricted' else None)
            fake.unrestricted=(args.sign_length=='unrestricted')
        else:
            fake.length=None; fake.unrestricted=False
        dataset=group['dataset']; source=ROOT/p['datasets'][dataset]['path']
        if not source.is_file():
            missing.append((gname,dataset,str(source)))
            continue
        result[gname]=[]
        for c in group['cases']:
            command,stats,log,src,inp,length=make_command(fake,p,gname,group,c)
            # Historical SIGN T1--T4 pilots used local[4]/four partitions.
            # Keep T5 on the same settings for an interpretable pilot curve.
            requested_cores=4 if gname=='sign_pilot_L3' else args.cores
            requested_partitions=4 if gname=='sign_pilot_L3' else args.partitions
            requested_memory='8g' if gname=='sign_pilot_L3' else args.driver_memory
            cli=(sys.executable, str(ROOT/'run_ahus_paper_protocol.py'),
                 'run','--group',gname,'--case',c['case'],'--cores',str(requested_cores),
                 '--partitions',str(requested_partitions),'--driver-memory',requested_memory,
                 '--timeout',str(args.timeout),'--execute')
            if gname=='sign_pruning':
                cli+=('--unrestricted',) if args.sign_length=='unrestricted' else ('--length',args.sign_length)
            result[gname].append(Job(f'{gname}/{c["case"]}',gname,c['case'],inp,
                  stats,log,cli,'paper' if gname!='sign_pilot_L3' else 'pilot',length,
                  min_util=c.get('min_utility'),source_exists=True))
    return result,missing,ambiguous


def exploratory_jobs(args):
    res={}; missing=[]
    for name, relpath in EXPLORATORY.items():
        source=ROOT/relpath
        if not source.is_file():
            missing.append((f'exploratory_{name}',name,str(source)))
            continue
        if args.no_calibration: # plan without scanning is not possible for this type
            continue
        calibration=singleton_max(source)
        gname=f'exploratory_{name}'
        res[gname]=[]
        for j,multiplier in enumerate(MULTIPLIERS,1):
            min_util=max(1,math.ceil(calibration['top_singleton_utility']*multiplier))
            base=ROOT/'results'/'exploratory'/name
            stats=base/f'T{j}_stats.json'
            log=ROOT/'logs'/'exploratory'/name/f'T{j}.log'
            sparkcmd=('spark-submit','--master',f'local[{args.cores}]',
                      '--driver-memory',args.driver_memory,str(ROOT/'spark_ahus.py'),
                      '--input',str(source),'--value-mode','utility',
                      '--min-utility',str(min_util),'--max-pattern-length','3',
                      '--partitions',str(args.partitions),'--count-only',
                      '--stats-json',str(stats))
            res[gname].append(Job(f'{gname}/T{j}',gname,f'T{j}',source,
                                stats,log,sparkcmd,'exploratory',3,min_util,
                                calibration,True))
    return res,missing


def valid_results(job):
    if not job.stats.is_file():return False
    try:
        dat=json.loads(job.stats.read_text())
        if not all(k in dat for k in ('husp_count','runtime_sec','sequence_count','min_utility')):
            return False
        if job.expected_length not in (0,None):
            if int(dat.get('max_pattern_length',-1)) != job.expected_length:
                return False
        if job.min_util is not None:
            if not math.isclose(float(dat['min_utility']),float(job.min_util),abs_tol=0.01):
                return False
        return True
    except (OSError,ValueError,TypeError): return False


def execute(job,args, cpus, stop_at):
    start=time.monotonic()
    if args.cpu_affinity=='physical' and cpus:
        cmd=('taskset','-c',','.join(map(str,cpus)),*job.cmd)
    else:cmd=job.cmd
    job.log.parent.mkdir(parents=True,exist_ok=True)
    job.stats.parent.mkdir(parents=True,exist_ok=True)
    wrapper_log=STATE/'launcher_logs'/f'{job.group}__{job.case}.txt'
    wrapper_log.parent.mkdir(parents=True,exist_ok=True)
    try:
        with wrapper_log.open('w',encoding='utf-8') as out:
            if job.kind in ('paper','pilot'):
                result=subprocess.run(cmd,stdout=out,stderr=subprocess.STDOUT,cwd=ROOT,
                                      env={**os.environ,'SPARK_LOCAL_IP':'127.0.0.1'},check=False)
                rc=result.returncode
            else:
                with job.log.open('w',encoding='utf-8') as slog:
                    result=subprocess.run(('timeout','--signal=TERM','--kill-after=30s',str(args.timeout),*cmd),
                        stdout=slog,stderr=subprocess.STDOUT,cwd=ROOT,
                        env={**os.environ,'SPARK_LOCAL_IP':'127.0.0.1'},check=False)
                    rc=result.returncode
                out.write('Command: '+shlex.join(cmd)+'\n')
                out.write(f'Exit status: {rc}\n')
    except OSError as ex:
        rc=127
        wrapper_log.write_text(f'ERROR: {ex}\n')
    duration=time.monotonic()-start
    ok=rc==0 and valid_results(job)
    status='complete' if ok else ('timeout' if rc==124 else 'failed')
    meta={'key':job.key,'kind':job.kind,'status':status,'exit_code':rc,
          'duration_wall_sec':round(duration,3), 'finished_at':time_now(),
          'stats':str(job.stats.relative_to(ROOT)),'log':str(job.log.relative_to(ROOT)),
          'command':list(cmd),'physical_cpus':cpus if args.cpu_affinity=='physical' else None,
          'calibration':job.calibration, 'max_pattern_length':job.expected_length,
          'min_utility':job.min_util}
    atomic_json(STATE/'meta'/f'{job.group}__{job.case}.json',meta)
    return meta


def print_status():
    metas=sorted((STATE/'meta').glob('*.json'))
    if not metas:
        print('No completed controller jobs yet; existing SIGN pilot JSON may still be present.');return
    counts={};rows=[]
    for m in metas:
        try:
            z=json.loads(m.read_text());counts[z['status']]=counts.get(z['status'],0)+1
            dat={}
            stats=ROOT/z['stats']
            if stats.is_file():
                try:dat=json.loads(stats.read_text())
                except (OSError,ValueError):pass
            rows.append({'case':z['key'],'status':z['status'],
                         'husp_count':dat.get('husp_count',''),
                         'runtime_sec':dat.get('runtime_sec',''),
                         'exit_code':z['exit_code']})
        except (OSError,ValueError,KeyError):continue
    print('Experiment statuses:',counts)
    print(f'{"case":50} {"status":10} {"husp":>10} {"runtime (s)":>14} exit')
    for r in rows:
        print(f'{r["case"]:50} {r["status"]:10} {str(r["husp_count"]):>10} {str(r["runtime_sec"]):>14} {r["exit_code"]}')
    export=STATE/'summary.csv'
    with export.open('w',newline='',encoding='utf8') as f:
        w=csv.DictWriter(f,fieldnames=list(rows[0]) if rows else ['case','status','husp_count','runtime_sec','exit_code'])
        w.writeheader();w.writerows(rows)
    print('CSV:',export)


def main():
    ap=argparse.ArgumentParser(description=__doc__,formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    ap.add_argument('action',choices=['plan','run','status'])
    ap.add_argument('--suite',choices=['paper','exploratory','all'],default='all')
    ap.add_argument('--jobs',type=int,default=3,help='Concurrent independent Spark applications')
    ap.add_argument('--cores',type=int,default=12,help='Spark local task threads per application')
    ap.add_argument('--partitions',type=int,default=12)
    ap.add_argument('--driver-memory',default='12g')
    ap.add_argument('--timeout',type=int,default=3600,help='Maximum seconds per case')
    ap.add_argument('--budget-hours',type=float,default=36.,help='Stop starting new jobs after this time')
    ap.add_argument('--cpu-affinity',choices=['physical','none'],default='physical')
    ap.add_argument('--sign-length',choices=['skip','3','5','7','unrestricted'],default='skip',
                    help='Published SIGN pruning figure did not report a length: default skip')
    ap.add_argument('--no-calibration',action='store_true',help='Skip exploratory calibration (paper-only planning)')
    ap.add_argument('--retry-failed',action='store_true',help='Allow retry of a previously failed case')
    ap.add_argument('--group',action='append',default=[],help='Limit to group(s), can repeat')
    args=ap.parse_args()
    if args.action=='status':return print_status()
    if min(args.jobs,args.cores,args.partitions,args.timeout)<=0 or args.budget_hours<=0:
        ap.error('jobs, cores, partitions, timeout and budget-hours must be positive')
    physical,allowed=get_allowed_physical_cpus()
    if args.cpu_affinity=='physical':
        if not shutil.which('taskset'):
            ap.error('taskset is unavailable in this Docker container; set --cpu-affinity none')
        if args.jobs*args.cores>max(1,len(physical)-2):
            ap.error(f'{args.jobs} jobs × {args.cores} physical cores = {args.jobs*args.cores} '
                     f'> available physical cores with 2 reserved ({len(physical)-2}). '
                     'Reduce --jobs / --cores or use --cpu-affinity none.')
    else:
        if args.jobs*args.cores>len(allowed):
            ap.error('Total Spark cores exceeds Docker allowed logical CPUs')
    protocol=json.loads(PROTOCOL.read_text())
    groups={}; missing=[]
    if args.suite in ('paper','all'):
        paper,miss,amb=protocol_jobs(protocol,args)
        groups.update(paper);missing+=miss
    if args.suite in ('exploratory','all'):
        extra,miss=exploratory_jobs(args)
        groups.update(extra);missing+=miss
    if args.group:
        requested=set(args.group)
        unknown=requested.difference(groups)
        if unknown:ap.error(f'Requested groups unavailable: {sorted(unknown)}')
        groups={key:value for key,value in groups.items() if key in requested}
    total=sum(map(len,groups.values()))
    print(f'Prepared {total} runnable cases in {len(groups)} groups; {len(missing)} missing dataset group(s).',flush=True)
    print(f'Docker CPUs allowed={len(allowed)}, physical cores accessible={len(physical)}; '
          f'jobs={args.jobs}, local[{args.cores}], partitions={args.partitions}, '
          f'affinity={args.cpu_affinity}.',flush=True)
    for g,d,f in sorted(set(missing)):
        print(f'NOT AVAILABLE: {g} ({d}): {f}',flush=True)
    if args.sign_length=='skip' and args.suite in ('paper','all'):
        print('SIGN published pruning 5 cases deferred: paper does not specify max length; '
              'use --sign-length 3 or unrestricted as a declared NEW choice.',flush=True)
    if args.suite in ('exploratory','all'):
        print('Exploratory thresholds: 5×, 3×, 2×, 1.5×, 1× largest exact singleton utility; '
              'NOT original AHUS thresholds.',flush=True)
    for group,items in groups.items():
        print('  '+group+': '+', '.join(f'{item.case}' + (f'({item.min_util})' if item.min_util else '') for item in items),flush=True)
    if args.action=='plan':
        print('PLAN ONLY. Use action=run to execute; existing stats will be reused.',flush=True)
        return
    STATE.mkdir(parents=True,exist_ok=True)
    # Linux advisory lock prevents accidentally starting a second controller.
    lock_handle=(STATE/'controller.lock').open('a+')
    try:
        fcntl.flock(lock_handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        ap.error('Another Spark-AHUS batch controller is already running. Do not start two simultaneously.')
    deadline=time.monotonic()+args.budget_hours*3600
    # Independent chains: one case at a time per group, block harder cases after timeout/failure.
    group_indices={g:0 for g in groups}
    blocked=set(); running={}; next_group=0
    group_names=list(groups)
    pinned={slot:physical[slot*args.cores:(slot+1)*args.cores]
            for slot in range(args.jobs)} if args.cpu_affinity=='physical' else {}
    slots=list(range(args.jobs))
    totals={'complete':0,'reused':0,'timeout':0,'failed':0,'blocked':0,'expired':0}
    with cf.ThreadPoolExecutor(max_workers=args.jobs) as pool:
        while True:
            while slots and time.monotonic()<deadline:
                selected=None
                for offset in range(len(group_names)):
                    idx=(next_group+offset)%len(group_names)
                    g=group_names[idx]
                    if g in blocked or g in (j.group for j,_ in running.values()):continue
                    seq=groups[g]; k=group_indices[g]
                    # Reuse valid existing cases before finding a runnable one.
                    while k<len(seq) and valid_results(seq[k]):
                        print(f'REUSE {seq[k].key} — valid existing stats',flush=True)
                        m=STATE/'meta'/f'{seq[k].group}__{seq[k].case}.json'
                        if not m.is_file():
                            atomic_json(m,{'key':seq[k].key,'kind':seq[k].kind,
                                'status':'complete','origin':'preexisting',
                                'exit_code':0,'finished_at':time_now(),
                                'stats':str(seq[k].stats.relative_to(ROOT)),
                                'log':str(seq[k].log.relative_to(ROOT)),
                                'duration_wall_sec':None})
                        totals['reused']+=1; k+=1
                    group_indices[g]=k
                    if k==len(seq):continue
                    j=seq[k]
                    old=STATE/'meta'/f'{j.group}__{j.case}.json'
                    if old.is_file() and not args.retry_failed:
                        try:om=json.loads(old.read_text())
                        except (OSError,ValueError):om={}
                        if om.get('status') in ('failed','timeout'):
                            print(f'BLOCK {g}: previous {om.get("status")} at {j.case} '
                                  '(use --retry-failed to retry)',flush=True)
                            blocked.add(g);totals['blocked']+=len(seq)-k
                            continue
                    selected=(j,idx); break
                if selected is None:break
                job,idx=selected
                slot=slots.pop(0);cpus=pinned.get(slot,[])
                print(f'START {job.key} | slot={slot} | cores={4 if job.group=="sign_pilot_L3" else args.cores} '
                      f'| affinity={cpus or "none"} | at={time_now()}',flush=True)
                fut=pool.submit(execute,job,args,cpus,deadline)
                running[fut]=(job,slot)
                next_group=(idx+1)%len(group_names)
            if not running:break
            done,_=cf.wait(running,return_when=cf.FIRST_COMPLETED)
            for fut in done:
                job,slot=running.pop(fut);slots.append(slot);slots.sort()
                try:record=fut.result()
                except Exception as ex:
                    record={'status':'failed','exit_code':-1,'duration_wall_sec':0}
                    print(f'EXCEPTION {job.key}: {ex}',flush=True)
                status=record['status'];totals[status]+=1
                print(f'END {job.key}: {status}, exit={record["exit_code"]} '
                      f'wall={record["duration_wall_sec"]}s, {time_now()}',flush=True)
                if status=='complete': group_indices[job.group]+=1
                else:
                    blocked.add(job.group)
                    totals['blocked']+=len(groups[job.group])-group_indices[job.group]-1
                    print(f'STOP group {job.group}: failed case; avoiding harder cases.',flush=True)
            atomic_json(STATE/'last_run.json',{'updated_at':time_now(),'totals':totals,
                'active':[x.key for x,_ in running.values()],
                'budget_expired':time.monotonic()>=deadline})
    print('FINISHED:',totals,flush=True)
    if time.monotonic()>=deadline:
        print('Time budget reached; safe to resume later with the same command.',flush=True)
    print_status()
    return 0

if __name__=='__main__':
    try:raise SystemExit(main())
    except KeyboardInterrupt:
        print('\nInterrupted. Completed results are preserved; resume with same command.',file=sys.stderr)
        raise SystemExit(130)
