HISTORICAL INSTRUCTIONS — server-specific, not portable.

# Spark-AHUS global-presence pruning + gated 81-case rerun (2026-10-10)

## Motivation and scientific status

The original TaFeng L5 run enumerated 604,528 distinct local candidates, but global verification retained only 20 HUSPs. Almost all local work was concentrated in a single Spark partition. This cannot be cured by simply adding executors.

This package adds **optional, global sequence-presence SWU pruning**. For each globally promising item it indexes the sequences containing that item (ignoring order). The intersection over all distinct items in a candidate prefix identifies a superset of the pattern's possible supporting sequences. Summing the **full** sequence utilities of those sequences yields a safe global upper bound. If that upper bound is below the global threshold, neither the candidate nor its descendants can qualify, and its local projection can be skipped. This is an algorithmic change, not merely a CPU/memory setting change.

The change has been tested offline with 30 deterministic randomized positive-utility databases: the exact verified HUSPs and utilities before/after pruning agree. An offline 12-way TaFeng L5 split retained all 20 known AHUS patterns with **475** candidate patterns, versus 604,528 in the earlier *real Spark* run. These two counts come from different execution configurations and **do not establish measured speedup**. Only the server-side end-to-end preflight will establish whether the optimized Spark method is actually faster.

**The original benchmarks, AHUS/USpan/HUS-Span baselines, all thresholds and source datasets remain unchanged.** The installer verifies the SHA-256 of the audited source before replacing it, creates a full backup, and refuses unknown source versions.

## Installation

In Windows PowerShell, from the downloaded ZIP:

```powershell
scp "$env:USERPROFILE\Downloads\Spark_AHUS_optimized_full_suite_20261010.zip" <username>@<server-address>:<Ubuntu-project-path>/
```

On the Ubuntu server (outside Docker):

```bash
cd <Ubuntu-project-path>
unzip -o Spark_AHUS_optimized_full_suite_20261010.zip
docker exec -it spark_ahus_env bash
```

Inside Docker:

```bash
conda activate spark-ahus
cd /work
python optimizations/apply_global_swu_fix.py
python optimizations/verify_optimized_core.py
python optimizations/run_full_optimized.py plan
```

The plan should show **81 configured cases**. If the source fingerprint is not accepted, **do not force the patch**. Upload the current `/work/spark_ahus.py` or its SHA for review.

## One-command correctness/performance-gated overnight suite

```bash
mkdir -p logs/optimized_full_suite
nohup python -u optimizations/run_full_optimized.py overnight \
  --cores 12 --partitions 12 --driver-memory 12g \
  --timeout 3600 --budget-hours 16 \
  > logs/optimized_full_suite/controller.log 2>&1 < /dev/null &
echo "Controller PID: $!"
```

Before the remaining cases, the runner performs 3 **exact-output correctness gates**: TaFeng L3, TaFeng L5, BMS2 T3. It parses both pattern identities and utilities and accounts for the baseline's ID offset and utility scaling. Any mismatch stops the suite. If correctness passes, it also requires the new TaFeng L5 Spark runtime to improve by at least **1.20×** relative to the old archived Spark L5 runtime. If not, it stops, leaving the old implementation comparison intact. **Do not describe the optimized algorithm as faster until this real Spark performance gate passes.**

When successful, it attempts all remaining configured 81 cases (56 original-paper/pilot, 25 exploratory). The five `sign_pruning` cases with ambiguous published length caps remain excluded, as they were in the original 81-case schedule; SIGN L3 pilots are included, identified as pilot measurements. It retains case-specific SIGN pilot 4 threads/4 partitions and uses the given 12/12 configuration for all other cases. Thresholds come from the original protocol and stored exploratory calibration. The first-N Kosarak scalability subsets are materialized deterministically.

The global presence index is enabled automatically for inputs of at most 120,000 lines, when <=5000 globally promising items. For larger datasets, such as full Kosarak and Yoochoose, the runner switches to the original local-mining logic using the **same patched source**, explicitly recording `plain-fallback-large-dataset`; those cases cannot be claimed to benefit from the new pruning. If the globally promising-item guard triggers, the case fails rather than being silently misreported as an optimized result. The index adds preprocessing work and memory. The algorithm validates declared SUtility against actual occurrence utilities before trusting them as bounds.

An already-timed-out case blocks increasingly hard cases *within the same group*, consistent with the original experiment scheduler. Therefore 81 configured does **not** mean every case will finish successfully in 16 hours. Stop/resume is safe: the runner preserves completed cases and skips the same-source/same-configuration outputs automatically. To retry previous timeouts intentionally, add `--retry-timeouts`. Do not launch two copies of this controller.

## Checking progress

```bash
ps -ef | grep '[r]un_full_optimized.py overnight'
grep -E '^(CONFIGURED|START|END|REUSE|PERFORMANCE GATE|STOP|BLOCK|FINISHED)' \
  logs/optimized_full_suite/controller.log | tail -50
python optimizations/run_full_optimized.py status
```

New outputs: `/work/results/optimized_full_suite/`, logs: `/work/logs/optimized_full_suite/`. Neither the original `/work/results/batch/` nor `/work/results/paper_protocol/` is overwritten. The original Spark benchmarks must be retained as a separately labeled ablation. When presenting efficiency claims, disclose 12 Spark local CPU threads vs one pinned CPU for each Python baseline, and don't claim historical C# reproducibility.

## Rollback

`apply_global_swu_fix.py` prints the full backup name, e.g. `spark_ahus.py.before_global_swu_YYYYMMDD_HHMMSS`. Restore it using `cp` if needed. Existing output files remain untouched.

## Important limitations

1. Off-line unit tests and candidate reductions are **not** real Spark performance results. The server-side gate must run before the full rerun is justified.
2. The bitmap/SWU bound only holds when sequence utility bounds are valid and all utilities are nonnegative; the implementation checks for declared underestimation.
3. Some datasets lack byte-identical historical published files. Re-running with unchanged *current prepared* files does not fix those provenance differences.
4. Timing from concurrent old experiments and isolated new runs is not a final controlled speedup study; collect repeated isolated timings for the final IEEE paper.
