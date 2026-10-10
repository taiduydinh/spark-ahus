#!/usr/bin/env python3
"""Exercise the fixed USpan buffers beyond the old 2,000-entry limit.

Run from /work: python vendor/pypatternminer/verify_uspan_buffer_fix.py
This smoke test does NOT replace validation against real datasets.
"""
from pathlib import Path
import hashlib
import sys
import tempfile

ROOT = Path(__file__).resolve().parent.parent / 'vendor' / 'pypatternminer'
sys.path.insert(0, str(ROOT))
from pypatternminer.uspan import AlgoUSpan


def run_case(text: str, threshold: int, expected_patterns: int, tag: str) -> None:
    with tempfile.TemporaryDirectory(prefix='uspan_long_') as tmp:
        inp = Path(tmp) / 'input.txt'
        out = Path(tmp) / 'patterns.txt'
        inp.write_text(text)
        algo = AlgoUSpan()
        algo.setMaxPatternLength(1)
        algo.runAlgorithm(str(inp), str(out), threshold)
        actual_lines = sum(1 for line in out.read_text().splitlines() if line.strip())
        assert algo.patternCount == expected_patterns == actual_lines, (tag, algo.patternCount, actual_lines)
        print(f'PASS {tag}: {actual_lines} HUSPs, input tokens >2000')

# 2,051 occurrences in 2,051 successive itemsets; both aligned and item arrays must grow.
n = 2051
run_case(('1[1] -1 ' * n) + f'-2 SUtility:{n}\n', 1, 1, 'many itemsets')
# 2,051 different items within a single itemset; all three arrays must grow.
run_case(' '.join(f'{i}[1]' for i in range(1, n+1)) + f' -1 -2 SUtility:{n}\n', 1, n, 'wide itemset')


code = ROOT / 'pypatternminer' / 'uspan.py'
print('PASS USpan buffer expansion; SHA256=' + hashlib.sha256(code.read_bytes()).hexdigest())
