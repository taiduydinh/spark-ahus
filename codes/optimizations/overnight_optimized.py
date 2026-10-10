#!/usr/bin/env python3
"""Resumable, correctness-gated Spark-AHUS global-SWU benchmarking.

Intentionally separate outputs from the original Spark-AHUS batch.
First verifies exact outputs against AHUS baseline; stops on any discrepancy.
Sequential runs avoid self-contention and make new runtime measurements cleaner.
"""
from __future__ import annotations
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone
from decimal import Decimal

ROOT=Path(__file__).resolve().parent.parent
STATE=ROOT/'results'/'optimized_global_swu'
LOGS=ROOT/'logs'/'optimized_global_swu'

PREFLIGHT=[
    'tafeng_length/L3',
    'tafeng_length/L5',
    'bms2_unrestricted/T3',
]
NIGHT=[
    'tafeng_length/L7',
    'bms2_unrestricted/T5',
    'foodmart_unrestricted/T1',
    'bms1_fixed_L5/T1',
    'bms1_length/L5',
    'online_retail_fixed_L5/T1',
    'online_retail_length/L3',
]

def sha(path):
    obj=hashlib.sha256()
    with Path(path).open('rb') as f:
        for chunk in iter(lambda:f.read(4*1024*1024),b''):
            obj.update(chunk)
    return obj.hexdigest()

def atomic_json(path, obj):
    path.parent.mkdir(parents=True,exist_ok=True)
    tmp=path.with_name(path.name+'.temporary')
    tmp.write_text(json.dumps(obj,indent=2,sort_keys=True)+'\n',encoding='utf-8')
    tmp.replace(path)

def parse_patterns(path, offset=0, scale=Decimal(1)):
    out={}
    for raw in Path(path).read_text(encoding='utf-8').splitlines():
        if not raw.strip():continue
        before, sep, value = raw.rpartition(':')
        if not sep:raise ValueError('Malformed pattern line: '+raw[:100])
        groups=re.findall(r'\(([^()]*)\)',before)
        if not groups:raise ValueError('Malformed pattern: '+raw[:100])
        key=tuple(tuple(int(x)-offset for x in grp.split()) for grp in groups)
        if key in out:raise ValueError('Duplicate pattern in '+str(path))
        out[key]=Decimal(value.strip())/scale
    return out

def compare_ahus(key, spark_output):
    group,casename=key.split('/')
    meta=ROOT/'results'/'baselines'/'meta'/'ahus'/group/(casename+'.json')
    baseline=ROOT/'results'/'baselines'/'ahus'/group/(casename+'_patterns.txt')
    if not meta.is_file() or not baseline.is_file():
        return None,'No completed AHUS pattern file to compare'
    d=json.loads(meta.read_text())
    if d.get('status')!='complete':
        return None,'AHUS baseline has status '+str(d.get('status'))
    offset=int(d.get('item_id_offset') or 0)
    scale=Decimal(str(d.get('scale') or 1))
    a=parse_patterns(baseline,offset,scale)
    s=parse_patterns(spark_output)
    missing=set(a)-set(s)
    extra=set(s)-set(a)
    utility_errors=[(p,a[p],s[p]) for p in set(a)&set(s)
                    if abs(a[p]-s[p])>Decimal('0.0001')]
    if missing or extra or utility_errors:
        message=(f'MISMATCH count {len(s)}/{len(a)} missing={len(missing)} '
                 f'extra={len(extra)} utility_mismatches={len(utility_errors)} '
                 f'sample_missing={list(missing)[:2]} sample_extra={list(extra)[:2]} '
                 f'sample_utility_errors={utility_errors[:2]}')
        return False,message
    return True,f'Exact pattern match: {len(s)} patterns; utilities agree within 0.0001'

def source_case(protocol,key):
    group,name=key.split('/')
    g=protocol['groups'][group]
    c=next(c for c in g['cases'] if c['case']==name)
    inp=ROOT/protocol['datasets'][g['dataset']]['path']
    if not inp.is_file():raise FileNotFoundError('Missing original dataset: '+str(inp))
    if c.get('first_n_sequences'):
        raise ValueError('Truncated-dataset cases not supported in overnight runner: '+key)
    return inp,c

def run_one(key, protocol, args, deadline, source_sha):
    group,name=key.split('/')
    inp,case=source_case(protocol,key)
    safe=group+'__'+name
    path=STATE/safe
    log=LOGS/(safe+'.log')
    stats=path.with_name(safe+'_stats.json')
    out=path.with_name(safe+'_patterns.txt')
    meta=STATE/'meta'/(safe+'.json')
    log.parent.mkdir(parents=True,exist_ok=True)
    sig={'algorithm_sha256':source_sha,'dataset_sha256':sha(inp),
         'case_config':case,'max_pattern_length':case.get('max_pattern_length'),
         'partitions':args.partitions,'cores':args.cores}
    if meta.is_file() and stats.is_file() and out.is_file():
        d=json.loads(meta.read_text())
        if d.get('status')=='complete' and all(d.get(k)==v for k,v in sig.items()):
            passed,message=compare_ahus(key,out)
            if passed is not False and (key not in PREFLIGHT or passed is True):
                print('REUSE',key,message,flush=True)
                return True
    if time.monotonic()>=deadline:
        print('BUDGET EXPIRED before',key,flush=True)
        return None
    cmd=[shutil.which('spark-submit') or 'spark-submit',
         '--master',f'local[{args.cores}]',
         '--driver-memory',args.driver_memory,
         str(ROOT/'spark_ahus.py'),
         '--input',str(inp),
         '--value-mode','utility',
         '--partitions',str(args.partitions),
         '--global-itemset-swu-pruning',
         '--output',str(out),
         '--stats-json',str(stats)]
    if 'threshold_ratio' in case:
        cmd+=['--min-utility-ratio',str(case['threshold_ratio'])]
    elif 'min_utility' in case:
        cmd+=['--min-utility',str(case['min_utility'])]
    else:
        raise ValueError('No utility threshold in '+key)
    length=case.get('max_pattern_length')
    if length:
        cmd+=['--max-pattern-length',str(length)]
    # Never accept stale outputs after a rerun failure.
    stats.unlink(missing_ok=True)
    out.unlink(missing_ok=True)
    begun=time.monotonic()
    print('START',key,'timeout',args.timeout,'seconds',datetime.now(timezone.utc).isoformat(),flush=True)
    with log.open('w') as f:
        f.write('COMMAND: '+' '.join(cmd)+'\n')
        f.flush()
        proc=subprocess.Popen(cmd,stdout=f,stderr=subprocess.STDOUT,cwd=ROOT,
                              env={**os.environ,'SPARK_LOCAL_IP':'127.0.0.1',
                                   'PYTHONDONTWRITEBYTECODE':'1'},start_new_session=True)
        timed_out=False
        try:
            proc.wait(timeout=max(1,min(args.timeout,deadline-time.monotonic())))
        except subprocess.TimeoutExpired:
            timed_out=True
            try:os.killpg(proc.pid,signal.SIGTERM)
            except ProcessLookupError:pass
            try:proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                try:os.killpg(proc.pid,signal.SIGKILL)
                except ProcessLookupError:pass
                proc.wait()
        rc=proc.returncode
    wall=time.monotonic()-begun
    status='timeout' if timed_out else 'complete' if rc==0 and stats.exists() and out.exists() else 'failed'
    compared=None;details=''
    if status=='complete':
        result=json.loads(stats.read_text())
        if result.get('global_itemset_swu_pruning') is not True:
            status='failed';details='Optimized flag not reflected in stats'
        else:
            compared,details=compare_ahus(key,out)
            if compared is False:
                status='mismatch'
            elif key in PREFLIGHT and compared is not True:
                status='failed';details='Missing completed AHUS baseline for correctness gate'
    record={**sig,'key':key,'status':status,'compare_ahus':compared,
            'compare_details':details,'exit_code':rc,'wall_sec':wall,'log':str(log),
            'stats_path':str(stats),'patterns_path':str(out),
            'completed_at':datetime.now(timezone.utc).isoformat()}
    atomic_json(meta,record)
    print('END',key,status,'wall',round(wall,1),'s',details,flush=True)
    return status=='complete'

def main():
    ap=argparse.ArgumentParser(description=__doc__)
    ap.add_argument('action',choices=['plan','smoke','overnight'])
    ap.add_argument('--cores',type=int,default=12)
    ap.add_argument('--partitions',type=int,default=12)
    ap.add_argument('--driver-memory',default='12g')
    ap.add_argument('--timeout',type=int,default=3600)
    ap.add_argument('--budget-hours',type=float,default=10.)
    a=ap.parse_args()
    if a.cores<1 or a.partitions<1 or a.timeout<1 or a.budget_hours<=0:
        ap.error('Invalid resource/budget settings')
    checks=PREFLIGHT if a.action=='smoke' else PREFLIGHT+NIGHT
    print('Selected cases:',checks,flush=True)
    print('Original results untouched. Optimized results:',STATE,flush=True)
    if a.action=='plan':return 0
    source=ROOT/'spark_ahus.py'
    if '--global-itemset-swu-pruning' not in source.read_text():
        print('ERROR: First run apply_global_swu_fix.py',file=sys.stderr)
        return 2
    source_sha=sha(source)
    protocol=json.loads((ROOT/'ahus_paper_protocol.json').read_text())
    for key in checks:
        source_case(protocol,key)
    deadline=time.monotonic()+3600*a.budget_hours
    for key in checks:
        status=run_one(key,protocol,a,deadline,source_sha)
        if status is None:
            print('STOP budget reached; all existing results saved',flush=True)
            break
        if not status and key in PREFLIGHT:
            print('STOP correctness preflight failed; NOT running remaining cases',flush=True)
            return 3
    print('FINISHED optimized runner; inspect',STATE/'meta',flush=True)
    return 0

if __name__=='__main__':
    raise SystemExit(main())
