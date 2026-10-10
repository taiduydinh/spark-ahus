HISTORICAL INSTRUCTIONS — server-specific, not portable.

# Targeted repairs for USpan and Spark-AHUS

Fixes two independent errors discovered in overnight logs:

1. `vendor/pypatternminer/pypatternminer/uspan.py`: the original 2,000-entry item/utility/sequence buffers can overflow on long sequences. Extend these buffers geometrically (2x) only when needed, without changing the mining algorithm.
2. `spark_ahus.py`: when `promising_original` is empty, the successful early-return stats omit `max_pattern_length`, while `run_all_spark_ahus.py` requires this field. Add the missing metadata key. This does not change mining behavior.

## Installation

1. Extract this ZIP at the root of your Ubuntu project `<Ubuntu-project-path>`. Inside Docker the code appears at `/work/fixes/`.
2. Confirm the old controllers are not running; the fixes should not be made while jobs are active.
3. Inside Docker, with `(spark-ahus)` active:

```bash
cd /work
python fixes/apply_repairs.py
python fixes/verify_uspan_buffer_fix.py
python baselines/run_all_baselines.py check --repo /work/vendor/pypatternminer --jobs 6 --spark-reserved 36
python baselines/run_all_baselines.py smoke --repo /work/vendor/pypatternminer --jobs 6 --spark-reserved 36
```

The patch automatically backs up both originals in the same directories with `.before_fixes_<UTC timestamp>` suffixes and is idempotent. If it sees unexpected source code, it refuses to patch. Existing results are not touched. SHA256 fingerprints in all *new* baseline JSON results identify the changed USpan version. The old `vendor/pypatternminer/verify_source_package.py` predates this change and would need its original-source comparison updated; use `fixes/verify_uspan_buffer_fix.py` to check the buffer fix.

## Yoochoose diagnosis

Check whether `/work/results/exploratory/Yoochoose/T1_stats.json` exists and shows `promising_items: 0` and no `max_pattern_length` field. If yes, the early-return metadata mismatch explains the exit-0 failure. The patch does not retrofit old JSON. Either rerun that one Spark case or carefully add the missing metadata to its old result, keeping its provenance explicit; rerunning is preferable. Do not restart *all* previously timed-out Spark groups without a deliberate decision.

## Resumption

The baseline runner has `--retry-failed` to permit retries of previously failed cases; without it, the scheduler blocks the later thresholds of those groups. `--retry-failed` also retries old **timeouts**, which could consume time. Use `--algorithms uspan` to limit the resumption to USpan, and budget/time-out consciously. Existing complete stats will be reused. Full runtime comparisons should be rerun uncontended, on identical normalized inputs, and exact pattern sets should be compared.
