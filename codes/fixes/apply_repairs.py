#!/usr/bin/env python3
"""Targeted and reversible server-side repair for USpan and Spark-AHUS.

Run: python fixes/apply_repairs.py  (from /work)
Does not touch data, output files, scheduler, or other algorithms.
"""
from __future__ import annotations
import argparse
from pathlib import Path
import os
import shutil
import tempfile
from datetime import datetime, timezone

OLD_BOUNDARY = '''                        if current_itemset_has_promising_item:
                            item_buffer[item_buffer_length] = -1
                            utility_buffer[item_buffer_length] = -1
'''
NEW_BOUNDARY = '''                        if current_itemset_has_promising_item:
                            # Grow both aligned buffers together for long sequences.
                            if item_buffer_length >= len(item_buffer):
                                increase = max(1, len(item_buffer))
                                item_buffer.extend([0] * increase)
                                utility_buffer.extend([0] * increase)
                            item_buffer[item_buffer_length] = -1
                            utility_buffer[item_buffer_length] = -1
'''
OLD_ITEM = '''                            current_itemset_has_promising_item = True
                            item_buffer[item_buffer_length] = item
                            utility_buffer[item_buffer_length] = item_utility
                            item_buffer_length += 1
                            items_sequence_buffer[items_length] = item
'''
NEW_ITEM = '''                            current_itemset_has_promising_item = True
                            if item_buffer_length >= len(item_buffer):
                                increase = max(1, len(item_buffer))
                                item_buffer.extend([0] * increase)
                                utility_buffer.extend([0] * increase)
                            item_buffer[item_buffer_length] = item
                            utility_buffer[item_buffer_length] = item_utility
                            item_buffer_length += 1
                            if items_length >= len(items_sequence_buffer):
                                items_sequence_buffer.extend([0] * max(1, len(items_sequence_buffer)))
                            items_sequence_buffer[items_length] = item
'''


def patch_uspan(s: str):
    if NEW_BOUNDARY in s and NEW_ITEM in s:
        return s, False
    if s.count(OLD_BOUNDARY) != 1 or s.count(OLD_ITEM) != 1:
        raise ValueError('USpan does not match the expected uploaded source; no change made.')
    return s.replace(OLD_BOUNDARY, NEW_BOUNDARY).replace(OLD_ITEM, NEW_ITEM), True


def patch_spark(s: str):
    a = s.index('        if not promising_original:')
    b = s.index('        dense_map_bc = sc.broadcast(', a)
    block = s[a:b]
    addition = '                "max_pattern_length": int(args.max_pattern_length),\n'
    if addition in block:
        return s, False
    needle = '                "promising_items": 0,\n'
    if block.count(needle) != 1:
        raise ValueError('Spark early-return result differs from the expected source; no change made.')
    block = block.replace(needle, needle + addition)
    return s[:a] + block + s[b:], True


def replace_safely(path: Path, replacement: str):
    timestamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
    backup = path.with_name(path.name + '.before_fixes_' + timestamp)
    while backup.exists():
        backup = backup.with_name(backup.name + '_1')
    shutil.copy2(path, backup)
    fd, name = tempfile.mkstemp(prefix=path.name + '.', suffix='.tmp', dir=path.parent)
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as f:
            f.write(replacement)
        os.chmod(name, path.stat().st_mode & 0o777)
        os.replace(name, path)
    finally:
        if os.path.exists(name):
            os.unlink(name)
    print(f'PATCHED {path}\nBACKUP  {backup}')


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--root', type=Path, default=Path('/work'))
    a = ap.parse_args()
    root = a.root.resolve()
    targets = [
        (root/'vendor/pypatternminer/pypatternminer/uspan.py', patch_uspan),
        (root/'spark_ahus.py', patch_spark),
    ]
    planned = []
    for path, patch in targets:
        if not path.is_file():
            raise FileNotFoundError(f'Missing {path}')
        old = path.read_text(encoding='utf-8')
        new, changed = patch(old)
        compile(new, str(path), 'exec')
        planned.append((path, new, changed))
    # Validate both files before writing either one.
    for path, new, changed in planned:
        if changed:
            replace_safely(path, new)
        else:
            print(f'ALREADY PATCHED {path}')
    print('PASS two targeted repairs; original files backed up; no dataset or results changed')


if __name__ == '__main__':
    main()
