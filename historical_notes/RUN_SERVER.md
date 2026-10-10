HISTORICAL INSTRUCTIONS — server-specific, not portable.

# Spark-AHUS server run guide

## 1. Environment

Recommended first environment for the current research prototype:

```bash
conda create -n spark-ahus python=3.11.5 -y
conda activate spark-ahus
pip install -r requirements.txt
```

Check Java and Spark:

```bash
java -version
python -c "import pyspark; print(pyspark.__version__)"
spark-submit --version
```

PySpark 3.5.3 normally works with Java 8/11/17. For a new server setup, Java 17 is a good choice.

To avoid hidden BLAS oversubscription:

```bash
export OMP_NUM_THREADS=1
export OPENBLAS_NUM_THREADS=1
export MKL_NUM_THREADS=1
export NUMEXPR_NUM_THREADS=1
```

## 2. Core correctness test (no Spark required)

```bash
python test_spark_ahus_core.py
```

Expected:

```text
PASS: pure-array local miner matches brute force.
PASS: partition-adaptive candidate union + trie verification is exact.
Exact HUSPs at minUtil=6.0: 11
```

## 3. Spark smoke test

The included demo file stores direct occurrence utilities in `item[utility]`.

```bash
spark-submit \
  --master local[4] \
  --driver-memory 4g \
  spark_ahus.py \
  --input demo_husp_utility.txt \
  --value-mode utility \
  --min-utility 6 \
  --max-pattern-length 3 \
  --partitions 4 \
  --output demo_spark_ahus_output.txt \
  --stats-json demo_spark_ahus_stats.json
```

The exact HUSP count should be 11.

## 4. Run with the same input convention as pypatternminer AHUS

`pypatternminer/ahus.py` treats the number inside `item[value]` as a quantity and multiplies it by an external utility from a second file. Use:

```bash
spark-submit \
  --master local[8] \
  --driver-memory 16g \
  spark_ahus.py \
  --input /path/to/demo_husp.txt \
  --external-utility /path/to/demo_husp_externalutility.txt \
  --value-mode quantity \
  --min-utility 10 \
  --partitions 16 \
  --output output_spark_ahus.txt \
  --stats-json stats_spark_ahus.json
```

For paper-scale experiments, `--count-only` avoids storing/writing potentially huge output sets:

```bash
spark-submit \
  --master local[8] \
  --driver-memory 16g \
  spark_ahus.py \
  --input /path/to/data.txt \
  --external-utility /path/to/external_utility.txt \
  --value-mode quantity \
  --min-utility 1000 \
  --partitions 16 \
  --count-only \
  --stats-json stats.json
```

## 5. Recommended first scaling test on the PA-HUIM server

The server previously used for PA-HUIM has 48 physical cores / 96 logical CPUs and 1 TiB RAM. Do not begin with all 48 physical cores. First validate correctness and the speedup curve:

```text
workers:     1, 2, 4, 8, 16, 24, 32, 48
partitions:  2, 4, 8, 16, 32, 48, 64, 96
```

That is approximately two database partitions per physical worker at the larger settings.

Example for 16 workers:

```bash
/usr/bin/time -v spark-submit \
  --master local[16] \
  --driver-memory 64g \
  --conf spark.python.worker.reuse=true \
  --conf spark.speculation=false \
  --conf spark.locality.wait=0s \
  spark_ahus.py \
  --input /path/to/data.txt \
  --external-utility /path/to/external_utility.txt \
  --value-mode quantity \
  --min-utility 1000 \
  --partitions 32 \
  --shuffle-partitions 32 \
  --verification-chunk-size 50000 \
  --count-only \
  --stats-json results/data_mu1000_w16.json \
  > results/data_mu1000_w16.log 2>&1
```

`/usr/bin/time -v` is important because Spark has JVM and Python-worker memory. The JSON field `driver_process_tree_peak_mb` is useful telemetry, but it should not be treated as the sole publication-quality Spark memory measure.

## 6. Parameters that matter most

### `--partitions`

Controls database partitioning and therefore the adaptive local thresholds.

Too few partitions:
- large local projected databases;
- poor load balance;
- high worker memory.

Too many partitions:
- more local candidate duplication;
- more Spark scheduling/shuffle overhead.

Start with about `2 x workers`.

### `--verification-chunk-size`

Distinct candidates are exact-verified in broadcast tries. Larger values reduce the number of verification passes but use more driver/executor memory.

Good starting values:

```text
10,000   conservative
50,000   default
100,000  larger-memory server
```

### `--max-driver-candidates`

Safety guard before distinct candidates are collected for chunked verification. The default is 2,000,000. Increase only after checking driver memory.

### `--count-only`

Use this for runtime/memory experiments unless the actual pattern file is needed.

## 7. Correctness validation against serial AHUS

For every dataset/threshold used in the paper:

1. Run serial `pypatternminer/ahus.py`.
2. Run Spark-AHUS with the same utility interpretation and threshold.
3. Compare:
   - HUSP count;
   - complete `(pattern, utility)` set for small/medium cases.
4. Only after exact agreement should large count-only experiments be admitted into the final benchmark.

## 8. Suggested experiment groups

For the IEEE paper, keep three experiment families separate:

1. **Threshold experiment**: several min-utility settings per dataset.
2. **Strong scaling**: fixed full dataset and threshold, workers 1→48.
3. **Data scalability**: fixed worker count, 20/40/60/80/100% of the sequence database.

Record at least:
- end-to-end runtime;
- preprocessing/SWU time;
- array-packing time;
- local mining time;
- global verification time;
- distinct local candidate count;
- final HUSP count;
- peak memory from `/usr/bin/time -v`;
- Spark spill/shuffle metrics when available.

Do not claim a memory advantage from Spark-AHUS until the measured peak-memory definition is made identical across the compared algorithms.
