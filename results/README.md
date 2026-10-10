# Experimental results

- `optimized_full_suite/`: final evaluated Spark-AHUS (`codes/spark_ahus.py`)
- `baselines/`: single-CPU Python AHUS, USpan and HUS-Span implementations
- `batch/`, `paper_protocol/`, `exploratory/`: earlier experiment records;
  use only for clearly labeled optimization comparisons, not as competing
  proposed algorithms or multi-node speedup evidence
- `verification/`: independent selected-case pattern/utility checks
- `logs/`: selected controller logs from the actual server run
- `experiment_coverage_81_cases.csv`: all configured cases and statuses
- `exact_ahus_comparisons.csv`: normalized pattern equality and utility
  checks on the 27 completed paper/pilot cases (tolerance 0.0001)
- `dataset_hash_audit.json`: matching input hashes where files are available

**Do not interpret a blocked case as a measured timeout.**

Runtime values inside `*_stats.json` refer to Spark algorithm timing, while
controller log wall times include process startup and validation overhead.
For example TaFeng L5 `runtime_sec` was ~6.955s, whereas the controller
wall time was ~11.6s. These are different metrics; do not mix them.

The 25 exploratory complete cases have no recorded AHUS pattern comparison,
although successful Spark result files and their pattern counts are preserved.
