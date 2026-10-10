HISTORICAL INSTRUCTIONS — server-specific, not portable.

# AHUS, USpan, HUS-Span: Overnight Python baseline suite

This package adds only `baselines/` to your existing `/work` directory. **Do not alter the running Spark controller**. The reference experiments use the same `ahus_paper_protocol.json`, source inputs and case-specific thresholds. Original AHUS manuscript: AHUS, AHUS-P and HUS-Span (not HHUSP). USpan is an additional baseline from related work. These baselines are **Python reconstructions from pypatternminer**, not the original C# implementations reported in the paper.

## One-time installation

1. Windows CMD (use the path where you downloaded the ZIP):

   `scp "<Windows-user-dir>\Downloads\Spark_AHUS_baselines_overnight_20261008.zip" <username>@<server-address>:<Ubuntu-project-path>/`

2. Ubuntu HOST:

   ```bash
   cd <Ubuntu-project-path>
   unzip -o Spark_AHUS_baselines_overnight_20261008.zip
   docker exec -it spark_ahus_env bash
   ```

3. INSIDE Docker:

   ```bash
   conda activate spark-ahus
   cd /work
   python -m pip install 'pypatternminer==0.0.3'
   python baselines/run_all_baselines.py check --jobs 6 --spark-reserved 36
   python baselines/run_all_baselines.py smoke --jobs 6 --spark-reserved 36
   python baselines/run_all_baselines.py plan --suite paper --jobs 6 --spark-reserved 36
   ```

   If PyPI cannot be accessed, use a local checkout of your `taiduydinh/pypatternminer` repo and add `--repo /path/to/pypatternminer` to `check`, `smoke`, and `run` (the path must contain `pypatternminer/ahus.py`, `uspan.py`, `husspan.py`). The repo itself is private. Do **not** place it inside the Spark results folders. The code locations and SHA256 hashes are recorded so you can report exact implementation versions.

**Important:** Do not start overnight runs if `smoke` fails. The smoke test compares *counts* from the three algorithms on a tiny normalized synthetic input; it does not replace full pattern/utility verification. The optional `compare_baselines.py` tool compares complete canonical output pattern sets with utilities after the batch.

## Recommended priority: published-paper parameter groups

The original-paper threshold sweep matters most for the deadline. Start it now; run the additional exploratory datasets later if time permits. USpan and HUS-Span are serial Python implementations: six concurrent single-core jobs is more useful than allocating many threads to a single serial program. The currently running Spark controller uses 36 **physical** cores (3 x 12) from the first physical-core CPU-affinity representatives; this suite uses 6 *other* representatives, leaving 6 further physical cores free from the original reserve.

```bash
cd /work
conda activate spark-ahus
mkdir -p logs/baselines results/baselines
nohup python -u baselines/run_all_baselines.py run \
  --suite paper --jobs 6 --spark-reserved 36 \
  --timeout 1200 --budget-hours 12 \
  > logs/baselines/controller.log 2>&1 < /dev/null &
echo "Baseline controller PID: $!"
```

**Never start a second baseline controller** while the first is active; the package uses its own advisory lock. It never acquires or modifies the Spark controller lock.

The paper-aligned suite includes 56 cases (the 61 defined protocol cases excluding five ambiguous SIGN pruning settings), repeated for three algorithms = **168 baseline jobs**. If later desired, use `--suite all` for 81 cases (243 baseline jobs) including the 25 exploratory cases. Case timeout defaults to 20 min and global scheduling budget to 12 hours; these are guards, not completion-time guarantees. A failed or timed-out case blocks only later thresholds within the same algorithm/group. On a restart, completed results are reused.

To monitor:

```bash
python baselines/run_all_baselines.py status
 tail -n 50 logs/baselines/controller.log
```

To compare exact canonical outputs when all three algorithms have completed a case:

```bash
python baselines/compare_baselines.py --group bms2_unrestricted --case T1
```

Results:

- `results/baselines/summary.csv` — baseline runtime, RSS and HUSP counts.
- `results/baselines/<algorithm>/<group>/<case>_stats.json` — detailed stats.
- `results/baselines/<algorithm>/<group>/<case>_patterns.txt` — **canonical pattern + scaled utility**.
- `results/baselines/meta/<algorithm>/<group>/<case>.json` — source hash, affinity, status, timeouts.
- `logs/baselines/<algorithm>/<group>/<case>.log` — stdout/stderr; `logs/baselines/controller.log` — scheduler.
- `prepared/baselines/<dataset>.*` — generated normalized datasets and source-file SHA256 manifests.

## Scientific integrity and caveats

1. The original-paper source inputs under `prepared/` are **never modified**. USpan requires integer utilities, and the Python HUS-Span parser also reads integer quantities. The converter scales each dataset's **exact decimal** direct occurrence utilities to integers by a power of ten. The three algorithms receive the same normalized data and the same scaled minUtil: `ceil(delta * total_utility_scaled)`. For absolute thresholds, the threshold is scaled by the same factor. With positive utilities and exact integer scaling, the threshold decisions are preserved. The external utility table contains `1` for each item, since source values are already occurrence utilities.
2. **Archived Foodmart contains 38 duplicate item occurrences within itemsets.** These are *coalesced* (their utilities summed) for conventional itemset semantics; the manifest records the count. **Archived TaFeng contains item ID 0**; all item IDs are shifted by +1 for the Python AHUS separator convention. This is a bijective relabeling. For exact apples-to-apples runtime comparisons with Spark-AHUS, rerun Spark on the **same normalized baseline files** using `--value-mode utility`, `--min-utility` equal to the scaled value, and the same maximum length. Do not directly equate old Spark result counts with these normalized-input baseline outputs without verification.
3. The manuscript's SIGN pruning plot does not explicitly specify the maximum length, so the five `sign_pruning` cases are excluded by default. The new SIGN length-3 pilot cases are labeled as such. If desired, choose `--sign-length 3` or `--sign-length unrestricted`, explicitly marking this as a new assumption.
4. **Speedup/efficiency:** With simultaneous Spark and baseline jobs, recorded runtimes are *throughput/pilot results*. They are not defensible uncontended speedup figures. Repeat final runtime trials with only one contender on the host, same normalized input and threshold, and present serial Python vs distributed Spark appropriately.
5. The original paper's C# implementation and current Python source have different language runtimes and potentially different optimizations. No claim is made that numbers reproduce historical C# runtime.
6. Even if all three baselines report the same HUSP count, compare **full pattern sets and their exact utilities** with `compare_baselines.py` before manuscript inclusion.
