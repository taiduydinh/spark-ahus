#!/usr/bin/env python3
"""Verify exact patch scope against the originals and check available imports."""
from pathlib import Path
import hashlib
import sys
root=Path(__file__).resolve().parent
p=root/'pypatternminer'
o=root/'original_sources'
for f in ('ahus.py','uspan.py','husspan.py','ahusp_p.py'):
  assert (o/f).exists() and (p/f).exists(), f
  original=(o/f).read_text()
  expected=original
  if f=='ahus.py':
    old='asu + self.one_item_peu.get(item, 0.0) >= self.min_utility'
    assert expected.count(old)==2
    expected=expected.replace(old,'asu + self.map_item_to_swu.get(item, 0.0) >= self.min_utility')
  if f=='husspan.py':
    old='                nb_itemsets -= 1\n'
    assert expected.count(old)==1
    expected=expected.replace(old,'                # Count was initialized to 1 and increased for each internal itemset boundary.\n                # The final -1 is excluded by tokens[:-3], so decrementing loses the last itemset.\n')
  assert (p/f).read_text()==expected, f'Unexpected changes in {f}'
  print(f'{f}: PASS original={hashlib.sha256((o/f).read_bytes()).hexdigest()[:12]} active={hashlib.sha256((p/f).read_bytes()).hexdigest()[:12]}')
sys.path.insert(0,str(root))
from pypatternminer.ahus import AlgoAHUS
from pypatternminer.uspan import AlgoUSpan
from pypatternminer.husspan import HUSSpan
from pypatternminer.ahusp_p import AlgoAHUSP
print('PASS: algorithm imports and exact patch scope')
