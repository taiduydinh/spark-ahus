#!/usr/bin/env python3
"""Check saved Spark-AHUS repository integrity without PySpark."""
from pathlib import Path
import hashlib,csv,json,collections,sys
root=Path(__file__).resolve().parent.parent
sha=lambda p: hashlib.sha256(p.read_bytes()).hexdigest()
manifest=root/'MANIFEST_SHA256.txt'
errors=[];num=0
for line in manifest.read_text().splitlines():
    digest,rel=line.split('  ',1);path=root/rel
    num+=1
    if not path.is_file() or sha(path)!=digest:errors.append(str(rel))
print('Original manifest entries checked:',num,'mismatches:',len(errors))
if errors: print('Mismatches:',errors[:10]);sys.exit(1)
with (root/'results/experiment_coverage_81_cases.csv').open(newline='') as f:
    data=list(csv.DictReader(f))
print('Case coverage:',dict(collections.Counter(x['status'] for x in data)))
with (root/'results/exact_ahus_comparisons.csv').open(newline='') as f:
    exact=list(csv.DictReader(f))
print('Exact AHUS comparisons:',len(exact),'passed:',sum(x['outcome']=='PASS' for x in exact))
assert len(data)==81
assert len(exact)==27 and all(x['outcome']=='PASS' for x in exact)
expected='8d222ac56b68a3d5d3f4254a2fc0a867a1636fbeeafaa30b89465d1d5b61ddbe'
assert sha(root/'codes/spark_ahus.py')==expected
print('PASS: static archival audit (does not rerun the Spark experiments)')
