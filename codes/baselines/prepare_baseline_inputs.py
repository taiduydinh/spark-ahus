#!/usr/bin/env python3
"""Convert Spark direct-utility HUSPM files to a common exact integer-utility format.

USpan's Python reconstruction requires integers; AHUS and HUS-Span are given the
same normalized input and an external profit of one per item. Never edits source.
"""
from __future__ import annotations
import argparse
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import re

ROOT=Path(__file__).resolve().parent


def sha256(path):
    h=hashlib.sha256()
    with path.open('rb') as f:
        for b in iter(lambda:f.read(1024*1024),b''):h.update(b)
    return h.hexdigest()


def parse_line(raw,line_num):
    toks=raw.strip().split()
    if not toks or toks[0].startswith(('#','%','@')):return None
    sets=[]; cur=[]; declared=None; ended=False
    for t in toks:
        if t=='-1':
            if cur:sets.append(cur);cur=[]
        elif t=='-2':
            ended=True
        elif t.startswith('SUtility:'):
            declared=Decimal(t.split(':',1)[1])
        elif '[' in t and t.endswith(']') and not ended:
            item_s,val=t.split('[',1)
            item=int(item_s);u=Decimal(val[:-1]);
            if item < 0 or not u.is_finite() or u < 0: raise ValueError(f'bad item/utility at line {line_num}: {t}')
            cur.append((item,u))
        else:raise ValueError(f'unexpected token on line {line_num}: {t!r}')
    if cur: sets.append(cur)
    if not ended or declared is None:raise ValueError(f'missing -2 or SUtility on line {line_num}')
    if not sets:
        if declared==0:return None  # e.g. the C8 trailing empty sequence
        raise ValueError(f'empty but nonzero sequence on line {line_num}')
    su=sum((u for itset in sets for _,u in itset),Decimal(0))
    if su!=declared:raise ValueError(f'SUtility mismatch line {line_num}: {declared} vs {su}')
    # Itemsets are mathematical sets. Preserve the sum of occurrence utilities,
    # coalescing repeated IDs within one itemset (38 repeats in archived Foodmart).
    merged=[];duplicate_count=0
    for itset in sets:
        items={}
        for i,v in itset:
            if i in items:duplicate_count+=1
            items[i]=items.get(i,Decimal(0))+v
        merged.append(list(items.items()))
    return merged,su,duplicate_count


def decimal_places(v):
    return max(0,-v.as_tuple().exponent)


def prepare(source,target,force=False,max_decimals=6):
    source=Path(source).resolve();target=Path(target).resolve()
    manifest=target.with_suffix('.json');ex=target.with_name(target.stem+'_external.txt')
    identity={'source':str(source),'bytes':source.stat().st_size,'sha256':sha256(source)}
    if not force and all(z.is_file() for z in (target,manifest,ex)):
        old=json.loads(manifest.read_text())
        if old.get('source_identity')==identity and old.get('target_sha256')==sha256(target):return old
    # first pass discovers exact decimal precision without buffering sequences
    places=0;count=0;items=set(); total=Decimal(0); merged_count=0
    with source.open(encoding='utf-8') as f:
        for n,line in enumerate(f,1):
            data=parse_line(line,n)
            if data is None:continue
            sets,su,dups=data; count+=1;total+=su;merged_count+=dups
            places=max(places,decimal_places(su))
            for itset in sets:
                for item,v in itset:
                    places=max(places,decimal_places(v));items.add(item)
            if places>max_decimals:
                raise ValueError(f'utility decimals exceed {max_decimals} digits; inspect source before benchmarking')
    if not count:raise ValueError('no usable sequences')
    scale=10**places
    id_offset=1 if 0 in items else 0  # AHUS uses zero as its itemset separator
    target.parent.mkdir(parents=True,exist_ok=True)
    tmp=target.with_name(target.name+'.partial')
    try:
        with source.open(encoding='utf-8') as fin,tmp.open('w',encoding='utf-8') as fout:
            for n,line in enumerate(fin,1):
                data=parse_line(line,n)
                if data is None:continue
                sets,su,dups=data
                parts=[]
                for itset in sets:
                    for item,v in sorted(itset):
                        iv=v*scale
                        if iv!=iv.to_integral_value():raise AssertionError(f'noninteger after scaling at {n}')
                        parts.append(f'{item+id_offset}[{int(iv)}]')
                    parts.append('-1')
                parts +=['-2',f'SUtility:{int(su*scale)}']
                fout.write(' '.join(parts)+'\n')
        tmp.replace(target)
    finally:
        if tmp.exists():tmp.unlink()
    tmp=ex.with_name(ex.name+'.partial')
    try:
        with tmp.open('w') as f:
            for item in sorted(items):f.write(f'{item+id_offset}:1\n')
        tmp.replace(ex)
    finally:
        if tmp.exists():tmp.unlink()
    m={'source_identity':identity,'target':str(target),'target_sha256':sha256(target),
        'external':str(ex),'external_sha256':sha256(ex),'scale':scale,
        'sequences':count,'items':len(items),'item_id_offset':id_offset,'duplicates_coalesced':merged_count,'total_utility_scaled':int(total*scale),
        'total_utility_original':str(total),'integerization':'exact decimal scaling, same input for all three baselines',
        'normalization_note':'Repeated item IDs within one itemset are coalesced by summing occurrence utilities; Spark must be rerun on normalized input for a fully controlled comparison.'}
    tmp=manifest.with_name(manifest.name+'.partial')
    tmp.write_text(json.dumps(m,indent=2,sort_keys=True)+'\n');tmp.replace(manifest)
    return m


def prefix(full_path,n):
    full_path=Path(full_path);out=full_path.with_name(full_path.stem+f'_N{n}.txt')
    if out.is_file() and sum(1 for _ in out.open())==n:return out
    tmp=out.with_name(out.name+'.partial');found=0
    try:
        with full_path.open() as fin,tmp.open('w') as fout:
            for line in fin:
                if not line.strip():continue
                if found==n:break
                fout.write(line);found+=1
        if found!=n:raise ValueError(f'expected {n} lines, only {found}')
        tmp.replace(out)
    finally:
        if tmp.exists():tmp.unlink()
    return out

if __name__=='__main__':
    ap=argparse.ArgumentParser();ap.add_argument('--input',required=True);ap.add_argument('--output',required=True)
    ap.add_argument('--force',action='store_true');args=ap.parse_args()
    print(json.dumps(prepare(args.input,args.output,args.force),indent=2))
