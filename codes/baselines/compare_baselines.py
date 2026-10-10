#!/usr/bin/env python3
"""Compare normalized complete pattern-and-utility outputs, not merely counts.

The normalizer emits a canonical <(1 2)(3)>:utility_scaled representation.
Unix sort enables hashing without holding all patterns in RAM.
"""
import argparse
import csv
import hashlib
import json
import os
from pathlib import Path
import subprocess

ROOT=Path(__file__).resolve().parent.parent
STATE=ROOT/'results'/'baselines'

def sorted_digest(p):
    proc=subprocess.Popen(['sort','-S','64M',str(p)],stdout=subprocess.PIPE,
         stderr=subprocess.PIPE,env={**os.environ,'LC_ALL':'C'})
    h=hashlib.sha256();count=0
    for chunk in iter(lambda:proc.stdout.read(1024*1024),b''):
        h.update(chunk);count+=chunk.count(b'\n')
    stderr=proc.stderr.read().decode('utf-8','replace')
    rc=proc.wait()
    if rc:raise RuntimeError(f'sort failed: {p}: {stderr[-300:]}')
    return count,h.hexdigest()


def main():
    p=argparse.ArgumentParser();p.add_argument('--group');p.add_argument('--case');args=p.parse_args()
    files={}
    for a in ('ahus','uspan','husspan'):
        for meta in (STATE/'meta'/a).glob('*/*.json') if (STATE/'meta'/a).exists() else []:
            m=json.loads(meta.read_text())
            if m.get('status')!='complete':continue
            if args.group and m['group']!=args.group:continue
            if args.case and m['case']!=args.case:continue
            key=(m['group'],m['case']);pattern=STATE/a/m['group']/(m['case']+'_patterns.txt')
            if pattern.is_file():files.setdefault(key,{})[a]=pattern
    n_ok=0;n_diff=0;n_missing=0
    for key,f in files.items():
        if len(f)!=3:n_missing+=1;continue
        hashes={a:sorted_digest(path) for a,path in f.items()}
        success=len(set(hashes.values()))==1
        n_ok+=success;n_diff+=not success
        print('MATCH' if success else 'DIFFER', '/'.join(key),hashes)
    print(f'SUMMARY match={n_ok}, differ={n_diff}, incomplete={n_missing}')
    if n_diff:return 1
    return 0
if __name__=='__main__':raise SystemExit(main())
