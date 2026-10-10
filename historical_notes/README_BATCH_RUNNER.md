HISTORICAL INSTRUCTIONS — server-specific, not portable.

# Spark-AHUS: parallel and resumable experiments (deadline-ready)

This **adds an experiment scheduler**. It does not change your mining algorithm, Docker image, prepared benchmark bytes, original AHUS protocol, or existing SIGN results. It does not install anything.

## Three different kinds of experiment

1. **Published-parameter groups** (`--suite paper`) use threshold ratios and pattern lengths extracted from `ahus(1).tex`. The group `sign_pruning` is **not executed by default**, because the paper does **not** specify its maximum pattern length. The `sign_pilot_L3` group keeps your new length-three SIGN runs (35k, 30k, 25k, 20k, 15k), without calling this a manuscript reproduction. The current dataset files match only the *names* of SIGN, Bible, and KOSARAK, and use variant data/utilities: comparisons are at the level of parameter **protocol**, not exact historical reproducibility. Five original datasets (BMS1, BMS2, Foodmart, OnlineRetail, TaFeng) are absent and will be **reported and skipped** until prepared.
2. **New exploratory datasets** (`--suite exploratory`) use all five other currently prepared datasets: Leviathan, Kosarak10K, Yoochoose, FIFA, C8T1S5I8N5K. **These five are not present in the AHUS paper.** The scheduler reads each direct-utility dataset, calculates the *exact singleton utility* for every item (maximum occurrence utility per sequence, summed over sequences), and generates five **new** conservative absolute thresholds (5.0, 3.0, 2.0, 1.5, 1.0 times the largest singleton utility). These are a documented preliminary study, **NOT** the AHUS paper's thresholds or a claim about optimal thresholds.
3. `--suite all` runs both categories, preserving these labels and using the same safe scheduling logic. It schedules all runnable, unambiguous cases, rather than silently substituting missing original datasets. The original paper protocol contains **61** defined cases in **14** groups, but many require unavailable original inputs; only runnable groups are included.

### CPU allocation and Spark runtime

The existing server has 48 physical cores / 96 logical threads; the **default** uses three separate Spark applications × 12 physical cores each = **36 physical cores**, with 12 Spark task slots and 12 data partitions per application **except SIGN pilot T5**, which retains 4 task slots and 4 partitions to match the already completed SIGN T1--T4 runs. `taskset` pins each application, including its Spark JVM and workers, to a *disjoint* group of physical cores. The remaining physical cores are reserved for the OS, other server jobs and overhead. **The scheduler checks the actual Docker CPU affinity at runtime** and refuses the allocation if too few physical cores are available. Note that 96 hardware threads are **not** equivalent to 96 independent physical cores. This is a **throughput setup**, not a measurement of parallel speedup. For a scaling figure you must measure 1/2/4/8/16/32/48 core settings separately under controlled conditions, with matching thresholds/input and without competing benchmark jobs.

The default **per-case timeout is one hour**; the overall **start-new-jobs budget is 36 hours**. A failed or timed-out case stops any harder cases in its own dataset/experiment group but does not stop the other groups. Completed runs are retained. Resuming with the same command reuses valid JSON stats and does not overwrite them. Overwritten old stats are never intended. `--retry-failed` retries a failed case after you've inspected its log; valid existing results are still reused.

## Install and launch

**Windows CMD** (change Downloads path if needed):

```cmd
scp "<Windows-user-dir>\Downloads\Spark_AHUS_parallel_batch_20261008.zip" <username>@<server-address>:<Ubuntu-project-path>/
ssh <username>@<server-address>
```

**Linux server host** (do not touch other Docker containers):

```bash
cd <Ubuntu-project-path>
unzip -o Spark_AHUS_parallel_batch_20261008.zip
docker exec -it spark_ahus_env bash
```

**Inside `spark_ahus_env`**:

```bash
cd /work
conda activate spark-ahus
export SPARK_LOCAL_IP=127.0.0.1
python test_protocol.py
python run_all_spark_ahus.py plan --suite all --jobs 3 --cores 12 --partitions 12
```

**Start in the background (SSH logout-safe if the Docker container remains running):**

```bash
mkdir -p logs results/batch
nohup python -u run_all_spark_ahus.py run \
  --suite all --jobs 3 --cores 12 --partitions 12 \
  --driver-memory 12g --timeout 3600 --budget-hours 36 \
  > logs/spark_ahus_batch_controller.log 2>&1 < /dev/null &
echo "Controller PID: $!"
```

**Follow progress from another Docker terminal**:

```bash
cd /work
conda activate spark-ahus
tail -f logs/spark_ahus_batch_controller.log
# Stop tail with Ctrl-C; it does not stop the running batch.
python run_all_spark_ahus.py status
cat results/batch/summary.csv
```

**Resume (after shutdown/interruption):** use the identical `nohup python -u ...` command. *Never run two controllers at once; a lock also prevents accidental duplicate execution.* The scheduler uses existing JSON to skip completed cases. If the CPU check reports fewer physical cores available to the container, inspect `nproc`, `taskset -pc $$`, and `docker inspect` CPU constraints, or reduce `--jobs` / `--cores`; don't force CPU oversubscription.

**Prefer the strongest paper matches only?** Change `--suite all` to `--suite paper`. **Do not automatically run SIGN original pruning as length-3 or unrestricted:** if you deliberately decide one, add `--sign-length 3` or `--sign-length unrestricted`, and explicitly label it an interpretation rather than manuscript-specified.

**Possible faster scheduling**: Four jobs × 8 physical cores (32 cores total): `--jobs 4 --cores 8 --partitions 8`. This may yield better experiment throughput on small datasets, but test resource contention. Existing SIGN runs used 4 cores / 4 partitions and thus are *pilot data*, not directly comparable to 12-core runs.

## Files produced

- `results/SIGN/sign_<threshold>_stats.json`: reuses existing signed SIGN pilot results.
- `results/paper_protocol/<group>/<case>_stats.json`: original manuscript's experimental parameter families.
- `results/exploratory/<dataset>/T#_stats.json`: new-data exploratory cases.
- `results/batch/calibration/<dataset>.json`: singleton threshold calibration details for non-paper datasets.
- `logs/...`: complete Spark job logs.
- `results/batch/meta/`: JSON status and exact CPU selection per case.
- `results/batch/summary.csv`: case summary, can be imported into Excel, R, or Pandas.

The current Spark-AHUS `driver_process_tree_peak_mb` is **not a validated whole-application or cluster-wide peak memory metric**, and `--count-only` does not save full pattern sets for correctness comparisons. Before final manuscript figures, run at least selected identical-dataset serial baselines and save complete patterns and utilities; perform independent runs for scaling without concurrent job competition. Experiment CPU counts, dataset checksums, utility variants, and every timeout/exception must be reported honestly.

## Cautions

- This controller orchestrates `spark-submit` on **one server in local mode**. It is not a multi-node Spark cluster.
- No speedup is proven by running multiple cases at once. To measure actual parallel speedup, isolate one benchmark per trial and vary allocated cores.
- For original-paper datasets still absent, the script will **skip**, not synthesize data.
- This parallel scheduler has been tested with a synthetic stub of Spark for successful execution, skip/resume and result summaries, **not yet run in the user's Docker environment**. Run the `plan` first; review that the file paths exist and the expected number of physical CPUs is accessible.
