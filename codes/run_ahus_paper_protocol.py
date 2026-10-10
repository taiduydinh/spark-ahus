#!/usr/bin/env python3
"""Safe, one-case-at-a-time Spark-AHUS launcher using original AHUS paper settings.

Never executes an entire experimental suite automatically. Default action is list;
run requires an explicitly selected group/case and --execute.
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import shlex
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
CONFIG = ROOT / 'ahus_paper_protocol.json'


def load():
    return json.loads(CONFIG.read_text(encoding='utf-8'))


def format_length(n):
    if n is None:
        return 'UNSPECIFIED (must override)'
    return 'unrestricted' if n == 0 else str(n)


def get_case(protocol, group, case):
    if group not in protocol['groups']:
        raise ValueError(f"Unknown group {group}. Run `list` to see available groups.")
    gr = protocol['groups'][group]
    for item in gr['cases']:
        if item['case'] == case:
            return gr, item
    raise ValueError(f"Unknown case {case} in {group}. Run `list` to see cases.")


def list_cases(protocol):
    print('Original AHUS paper protocol — Spark-AHUS execution plan')
    print('Paper-specific thresholds are ratios unless group=sign_pilot_L3.')
    for gname, group in protocol['groups'].items():
        ds=group['dataset']
        db=protocol['datasets'][ds]
        available = 'present' if (ROOT / db['path']).exists() else 'MISSING'
        ls=', '.join(c['case'] + ' (' + ('minUtil='+str(c['min_utility']) if 'min_utility' in c else 'delta='+c['threshold_ratio'])
                     +', L='+format_length(c['max_pattern_length']) +')' for c in group['cases'])
        print(f'\n{gname}: {ds}, input={available}, source={db["has_matching_dataset"]}')
        print('  ' + ls)
        if group.get('notes'):
            print('  NOTE:', group['notes'])


def make_prefix(src: Path, dst: Path, n: int):
    if dst.is_file() and dst.stat().st_size > 0:
        return
    dst.parent.mkdir(parents=True,exist_ok=True)
    tmp=dst.with_suffix(dst.suffix+'.partial')
    count=0
    try:
        with src.open('r', encoding='utf-8') as fin, tmp.open('w', encoding='utf-8') as fout:
            for line in fin:
                if not line.strip() or line.lstrip().startswith(('#','%','@')):
                    continue
                fout.write(line if line.endswith('\n') else line+'\n')
                count+=1
                if count>=n:
                    break
        if count!=n:
            raise ValueError(f"Requested {n} sequences, found {count} in {src}")
        tmp.replace(dst)
    finally:
        if tmp.exists():
            tmp.unlink()


def make_command(args, protocol, group_name, g, c):
    dataset = g['dataset']
    db=protocol['datasets'][dataset]
    src=ROOT/db['path']
    if not src.is_file():
        raise FileNotFoundError(f"Missing dataset for {dataset}: {src}. Do not substitute a different dataset silently.")
    length = c.get('max_pattern_length')
    if args.length is not None and args.unrestricted:
        raise ValueError('Use at most one of --length and --unrestricted')
    if args.length is not None:
        if args.length < 1:
            raise ValueError('--length must be >=1')
        length = args.length
    if args.unrestricted:
        length = 0
    if length is None:
        raise ValueError(f"Original paper does not report SIGN max length for {group_name}; supply --length N or --unrestricted explicitly.")

    if c.get('first_n_sequences'):
        n=c['first_n_sequences']
        dataset_file=ROOT/f'prepared/paper_protocol/scalability/KOSARAK_first_{n}.txt'
        if args.execute:
            make_prefix(src, dataset_file, n)
        input_path=dataset_file
    else:
        input_path=src

    case_name=c['case']
    if group_name=='sign_pilot_L3':
        base=ROOT/f"results/SIGN/sign_{c['min_utility']}"
        stats=Path(str(base)+'_stats.json')
        log=ROOT/f"logs/SIGN_{c['min_utility']}.log"
    else:
        base=ROOT/'results/paper_protocol'/group_name/case_name
        stats=ROOT/'results/paper_protocol'/group_name/(case_name+'_stats.json')
        log=ROOT/'logs/paper_protocol'/group_name/(case_name+'.log')

    cmd=['spark-submit','--master',f'local[{args.cores}]', '--driver-memory',args.driver_memory,
         str(ROOT/'spark_ahus.py'),'--input',str(input_path),
         '--value-mode','utility', '--partitions',str(args.partitions),
         '--stats-json',str(stats)]
    if 'min_utility' in c:
        cmd+=['--min-utility',str(c['min_utility'])]
    else:
        cmd+=['--min-utility-ratio',c['threshold_ratio']]
    if length!=0:
        cmd+=['--max-pattern-length',str(length)]
    if args.write_patterns:
        cmd+=['--output',str(base)+'.txt']
    else:
        cmd+=['--count-only']
    return cmd,stats,log,src,input_path,length


def run_case(args,protocol):
    group, c = get_case(protocol,args.group,args.case)
    cmd,stats,log,src,input_file,length=make_command(args,protocol,args.group,group,c)
    print('Experiment:',args.group,args.case,flush=True)
    print('Data source:',src,flush=True)
    print('Dataset integrity note:',protocol['benchmark_caveats'][group['dataset']],flush=True)
    print('Pattern length:',format_length(length),flush=True)
    print('Command:',shlex.join(cmd),flush=True)
    if not args.execute:
        print('DRY RUN: nothing was executed; pass --execute to launch one case.')
        return 0
    if stats.exists() and not args.force:
        raise FileExistsError(f"Existing stats file {stats}; refusing to overwrite. Use --force only if intentional.")
    stats.parent.mkdir(parents=True,exist_ok=True)
    log.parent.mkdir(parents=True,exist_ok=True)
    env=os.environ.copy()
    env.setdefault('SPARK_LOCAL_IP','127.0.0.1')
    start=time.perf_counter()
    # GNU timeout enforces termination even with an unresponsive Spark subprocess.
    command=['timeout','--signal=TERM','--kill-after=30s',str(args.timeout)]+cmd
    with log.open('w',encoding='utf-8') as fout:
        try:
            result=subprocess.run(command,cwd=str(ROOT),env=env,stdout=fout,
                                  stderr=subprocess.STDOUT,check=False)
            code=result.returncode
        except FileNotFoundError as ex:
            fout.write(str(ex)+'\n'); code=127
    duration=time.perf_counter()-start
    meta={'group':args.group,'case':args.case,'dataset':group['dataset'],
          'source':str(src),'input':str(input_file),'length':length,
          'threshold_ratio':c.get('threshold_ratio'), 'min_utility':c.get('min_utility'),
          'cores':args.cores,'partitions':args.partitions,'timeout_sec':args.timeout,
          'exit_code':code,'wall_sec':round(duration,3),'command':cmd}
    meta_file=log.with_suffix('.meta.json')
    meta_file.write_text(json.dumps(meta,indent=2)+'\n',encoding='utf8')
    print('Finished: exit code',code, 'wall time',round(duration,2),'s')
    print('Log:',log)
    print('Run metadata:',meta_file)
    print('Spark stats:',stats if stats.is_file() else 'not produced')
    if code == 124:
        print('Timed out. Do not run a more difficult threshold automatically.')
    return code


def main():
    p=argparse.ArgumentParser(description=__doc__)
    sub=p.add_subparsers(dest='action',required=True)
    sub.add_parser('list',help='Display exact paper settings and file status')
    r=sub.add_parser('run',help='Run exactly one named case; dry-run by default')
    r.add_argument('--group',required=True)
    r.add_argument('--case',required=True)
    r.add_argument('--cores',type=int,default=4)
    r.add_argument('--partitions',type=int,default=4)
    r.add_argument('--driver-memory',default='8g')
    r.add_argument('--timeout',type=int,default=600)
    r.add_argument('--length',type=int)
    r.add_argument('--unrestricted',action='store_true')
    r.add_argument('--write-patterns',action='store_true')
    r.add_argument('--execute',action='store_true')
    r.add_argument('--force',action='store_true')
    a=p.parse_args()
    pr=load()
    if a.action=='list':
        list_cases(pr);return 0
    if a.cores<1 or a.partitions<1 or a.timeout<1:
        p.error('cores, partitions and timeout must be positive')
    try:
        return run_case(a,pr)
    except (FileNotFoundError,FileExistsError,ValueError) as e:
        print('ERROR:',e,file=sys.stderr);return 2

if __name__=='__main__':
    raise SystemExit(main())
