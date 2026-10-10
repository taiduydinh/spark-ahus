#!/usr/bin/env python3
"""Install exact-safe optional global item-presence SWU pruning with backup.

Safe by default: refuses to overwrite a source that does not match the audited
snapshot. Existing result and dataset files are never touched.
"""
from pathlib import Path
from datetime import datetime, timezone
import hashlib
import shutil
import sys

ROOT = Path(__file__).resolve().parent.parent
SOURCE = ROOT / 'spark_ahus.py'
REPLACEMENT = Path(__file__).with_name('spark_ahus_optimized.py')
# These fingerprints are computed from the user's uploaded audit archive.
EXPECTED_SHA = '3a3821bc2ef204908e351dbd94d25d4401cf015c62c2d44a59cf4714be9decbd'
NEW_SHA = '8d222ac56b68a3d5d3f4254a2fc0a867a1636fbeeafaa30b89465d1d5b61ddbe'

def sha(p):
    return hashlib.sha256(p.read_bytes()).hexdigest()

def main():
    if not SOURCE.is_file() or not REPLACEMENT.is_file():
        print('ERROR: Missing source or replacement. Extract the ZIP in /work.', file=sys.stderr)
        return 2
    actual=sha(SOURCE)
    if actual == NEW_SHA:
        print('Already installed:', SOURCE)
        return 0
    if actual != EXPECTED_SHA:
        print('REFUSED TO OVERWRITE a different version of spark_ahus.py.',file=sys.stderr)
        print('Current SHA256:',actual,file=sys.stderr)
        print('Expected SHA256:',EXPECTED_SHA,file=sys.stderr)
        print('Keep your source unchanged and send the SHA to ChatGPT for a compatible patch.',file=sys.stderr)
        return 3
    if sha(REPLACEMENT) != NEW_SHA:
        print('ERROR: Replacement checksum mismatch.',file=sys.stderr)
        return 4
    when=datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
    backup=SOURCE.with_name('spark_ahus.py.before_global_swu_'+when)
    shutil.copy2(SOURCE,backup)
    shutil.copy2(REPLACEMENT,SOURCE)
    print('INSTALLED:',SOURCE)
    print('BACKUP:',backup)
    print('SOURCE SHA256:',sha(SOURCE))
    print('This changes only spark_ahus.py. All old results remain untouched.')
    return 0

if __name__=='__main__':
    raise SystemExit(main())
