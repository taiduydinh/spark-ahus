#!/usr/bin/env python3
"""Build an isolated, runnable legacy-layout workspace from this ZIP.

The evaluated code remains unchanged. Files are COPIED, not linked, to protect
archived datasets and results. Full KOSARAK inputs excluded from the ZIP must
be independently obtained before attempting all 81 runs.
"""
from pathlib import Path
import argparse, shutil
ROOT=Path(__file__).resolve().parent.parent
p=argparse.ArgumentParser(description=__doc__)
p.add_argument('--destination',type=Path,default=ROOT/'.local_work'/'work')
p.add_argument('--force',action='store_true',help='Recreate destination (deletes its contents)')
args=p.parse_args()
d=args.destination.expanduser().resolve()
if d.exists():
    if not args.force:
        raise SystemExit(f'Already exists: {d}. Use --force only if disposable.')
    if d==ROOT or d==Path('/') or ROOT in d.parents and len(d.parts)<=len(ROOT.parts)+1:
        raise SystemExit('Refusing to delete a top-level or repository directory')
    shutil.rmtree(d)
d.mkdir(parents=True)
shutil.copytree(ROOT/'codes',d,dirs_exist_ok=True)
shutil.copytree(ROOT/'dataset/prepared',d/'prepared')
# Baseline comparisons and calibration metadata; keep new optimized outputs isolated.
for n in ('baselines','paper_protocol','batch','verification'):
    s=ROOT/'results'/n
    if s.exists():shutil.copytree(s,d/'results'/n,dirs_exist_ok=True)
(d/'logs').mkdir(exist_ok=True)
print('Isolated workspace:',d)
print('Original 2026 experimental outputs remain in repo/results/.')
print('Try: cd',d)
print('     python test_spark_ahus_core.py')
print('     python optimizations/verify_optimized_core.py')
print('     python optimizations/run_full_optimized.py plan')
print('     spark-submit --master "local[4]" --driver-memory 4g spark_ahus.py --help')
print('NOTE: Spark requires Java 17 / PySpark 3.5.3; larger benchmarks require server resources.')
