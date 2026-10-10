# Spark-AHUS

**Spark-AHUS: An Adaptive Pure-Array Parallel Framework for High-Utility Sequential Pattern Mining**

Research code and preserved experiment records for the associated IEEE manuscript.
The main proposed method is the **final evaluated Spark-AHUS** implementation; older
measurements are retained only for configuration-level optimization comparisons.

## Repository layout

| Folder | Content |
| --- | --- |
| `dataset/prepared/` | Prepared experimental inputs and dataset provenance (some full-size inputs omitted) |
| `codes/spark_ahus.py` | **Evaluated Spark-AHUS algorithm** |
| `codes/optimizations/` | Optimized experiment runner and verification scripts |
| `codes/baselines/` and `codes/vendor/` | Reference baseline runners and vendored baseline snapshots (license review required) |
| `results/optimized_full_suite/` | Final optimized Spark-AHUS pattern files, per-case statistics and metadata |
| `results/baselines/` | AHUS, USpan and HUS-Span baseline measurements |
| `results/batch/`, `results/paper_protocol/` | Earlier measurements used for configuration-level optimization analysis |
| `results/experiment_coverage_81_cases.csv` | One row per configured experiment, including blocked cases |
| `results/exact_ahus_comparisons.csv` | Independently checked normalized pattern/utility equality on completed paper/pilot cases |
| `tools/` | Portable workspace creation and archive-audit utilities |
| `historical_notes/` | Old, server-specific working notes (not reproduction instructions) |

## Recorded experiment coverage

The final evaluation defined **81 cases**: **56 paper/pilot** and **25 exploratory**.
It produced **52 complete**, **7 timeout**, **22 blocked**, **0 failed** and **0
mismatched** run statuses. Of the 52 successful cases, **27 paper/pilot cases
were exact-pattern/utility compared to AHUS**; the remaining 25 exploratory
cases have no recorded independent AHUS comparison. Blocked cases were *not
run*, so they are neither completions nor measured timeouts.

Selected measured outcomes of the optimized Spark-AHUS algorithm:

| Case | Optimized runtime | HUSPs | Candidates |
| --- | ---: | ---: | ---: |
| TaFeng L5 | 6.955 s | 20 | 475 |
| TaFeng L7 | 12.050 s | 20 | 915 |
| BMS-WebView-2 T5 | 157.035 s | 1,110 | 81,566 |

The earlier TaFeng L5 implementation needed 329.251 s and generated 604,528
candidates. These **historical configuration-level comparisons** are *not*
feature-by-feature controlled ablation studies.

### Experimental environment and comparability

The reported evaluation used **Spark 3.5.3 in single-server `local[12]` mode**
(Java 17, Python 3.11). The SIGN pilot used four cores. Python AHUS, USpan and
HUS-Span baselines ran with separate CPU allocations; their timings are not
controlled equal-core speedups. A multi-node cluster was **not** tested.

## Reproducing the environment

1. Install Python 3.11, Java 17 and PySpark 3.5.3 (other dependencies in
   `requirements.txt`). `spark-submit` must be on `PATH`.
2. Extract this repository ZIP in a writable location.
3. Prepare an **isolated local workspace** using `python tools/setup_workspace.py`.
   The script copies source, prepared datasets and reference baseline records
   to `.local_work/work/`. It never changes the archived experimental files.
4. `cd .local_work/work` and run the offline test and a dry-run plan:

   ```bash
   python test_spark_ahus_core.py
   python optimizations/verify_optimized_core.py
   python optimizations/run_full_optimized.py plan
   ```

5. To run an individual complete, small case (depending on available RAM/cores):

   ```bash
   spark-submit --master 'local[4]' --driver-memory 4g spark_ahus.py \
     --input prepared/paper_original/Tafeng.txt \
     --value-mode utility --min-utility-ratio 0.06045 \
     --max-pattern-length 3 --partitions 4 \
     --global-itemset-swu-pruning --global-bound-max-sequences 120000 \
     --output results/manual_tafeng_L3_patterns.txt \
     --stats-json results/manual_tafeng_L3_stats.json
   ```

   This **new** `local[4]` run is not the paper's `local[12]` measurement.
   The original controller, `python optimizations/run_full_optimized.py overnight`,
   may run for many hours and requires the omitted full KOSARAK input.

## Archive integrity

The evaluated source `codes/spark_ahus.py` has SHA-256:

`8d222ac56b68a3d5d3f4254a2fc0a867a1636fbeeafaa30b89465d1d5b61ddbe`

`codes/optimizations/spark_ahus_optimized.py` has the same hash.
Run `python tools/audit_repository.py` for the source and static archive audit.
Archived outputs and input fingerprints remain in `results/` metadata.

## Citation

Spark-AHUS is being prepared for submission to the [4th International Conference on Sustainable Computing and Data Communication Systems (ICSCDS 2026)](https://icscds.com/2026/), scheduled for 15–17 December 2026. The citation below is **provisional** and will be updated if the paper is accepted and published.

```bibtex
@unpublished{dinh2026sparkahus,
  author = {Dinh, Tai},
  title = {Spark-AHUS: An Adaptive Pure-Array Parallel Framework for High-Utility Sequential Pattern Mining},
  year = {2026},
  note = {Manuscript prepared for submission to the 4th International Conference on Sustainable Computing and Data Communication Systems (ICSCDS 2026)},
  url = {https://github.com/taiduydinh/spark-ahus}
}
```
