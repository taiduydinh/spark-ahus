# Spark-AHUS protocol matched to original AHUS manuscript

**Scope:** Experimental parameters extracted from the user's `ahus(1).tex`, not a claim of reproducing the published timing measurements. This package adds configuration and a safe single-case launcher; it does **not** modify `spark_ahus.py` or the eight prepared datasets.

## Important distinctions

The published study used C# AHUS, C# AHUS-P and HUS-Span, tested on a four-core Intel Xeon W3520 (2.67 GHz), 8 GB RAM, Windows 10. Spark-AHUS is a new PySpark implementation running on a different server. Matching *thresholds and pattern lengths* does **not** make the runtimes directly comparable or reproduce the same implementation.

The original paper's eight datasets are SIGN, BIBLE, BMS-WebView-1, BMS-WebView-2, KOSARAK, Foodmart, TaFeng and OnlineRetail. The current Spark preparation contains only three overlapping *names*: SIGN, BIBLE and KOSARAK. Their underlying sequence counts and/or utility generation are not necessarily identical to the paper's. In particular, original SIGN reports 800 sequences/310 items, while current SIGN is 730/267. The paper's OnlineRetail differs from the currently downloaded Online Retail II variant. **Never substitute a file solely because of a similar name.**

The paper generates internal quantities in [1,10] and external utility in [1,10] for raw data, using a log-normal external-utility distribution. The available public quantitative benchmark release uses different generation parameters. As a result, paper ratios can be reused as protocol settings for a new experiment, but exact numerical equivalence requires original processed input files and their utility tables.

## All paper-defined experimental families captured

1. **Candidate pruning / memory on SIGN**: five original ratios; **the SIGN figure does not explicitly state a maximum length**. `sign_pruning` therefore requires an intentional `--length N` or `--unrestricted` override.
2. **Full-length**: BMS-WebView-2 and Foodmart, five thresholds each, no maximum length.
3. **Fixed maximum length**: BMS-WebView-1 length 5, Bible length 3, OnlineRetail length 5, TaFeng length 3, KOSARAK length 3. Ratios are copied from the paper figures.
4. **Incremental maximum lengths**: 3, 5 and 7 for BMS-WebView-1 (delta=0.01816), Bible (0.0351), OnlineRetail (0.0833), TaFeng (0.06045).
5. **KOSARAK scalability**: 100K, 150K, 200K, 300K and 600K sequences, delta=0.0045, maximum length 3; and full 990K at five separate ratios. The new launcher's first-N prefix construction is a **reproducible new selection rule**, since the paper did not specify how the subsamples were drawn.
6. **SIGN length-3 Spark pilots**: 35,000; 30,000; 25,000; 20,000; 15,000. This separate group preserves your completed new measurements. The original paper does **not** explicitly specify length 3 for SIGN, so these are not claimed as exact manuscript replicas.

Dataset and threshold cases are in `ahus_paper_protocol.json`, with 14 experiment groups / 61 cases. The paper also tests serial AHUS, AHUS-P and HUS-Span; those baselines must be implemented/run separately on identical input bytes, since this launcher runs Spark-AHUS only.

## Install into existing /work Docker bind mount

On Windows, upload `AHUS_paper_protocol_setup.zip` using `scp` to the Linux server project directory, then on the Linux host:

```bash
cd /mnt/hdd1/work/taidinh/spark_ahus
unzip -o AHUS_paper_protocol_setup.zip

docker exec -it spark_ahus_env bash
```

Inside Docker:

```bash
cd /work
conda activate spark-ahus
export SPARK_LOCAL_IP=127.0.0.1
python run_ahus_paper_protocol.py list
```

The launcher **never executes by default**. To preview the next SIGN pilot:

```bash
python run_ahus_paper_protocol.py run --group sign_pilot_L3 --case T5
```

To launch it (10-minute timeout):

```bash
python run_ahus_paper_protocol.py run --group sign_pilot_L3 --case T5 --timeout 600 --execute
```

It writes `results/SIGN/sign_15000_stats.json`, `logs/SIGN_15000.log`, and a small `logs/SIGN_15000.meta.json` containing exit status and exact invocation. It preserves your existing T1–T4 results; it will refuse to overwrite existing JSON unless you pass `--force`.

To preview a Bible experiment with **paper-specified length 3**:

```bash
python run_ahus_paper_protocol.py run --group bible_fixed_L3 --case T1
```

To launch that case:

```bash
python run_ahus_paper_protocol.py run --group bible_fixed_L3 --case T1 --timeout 1200 --execute
```

To preview a SIGN original ratio without making up its pattern length (this **must refuse**):

```bash
python run_ahus_paper_protocol.py run --group sign_pruning --case T1
```

Select length explicitly before running:

```bash
python run_ahus_paper_protocol.py run --group sign_pruning --case T1 --unrestricted
# or: --length 3, if deliberately doing a length-limited study
```

For a complete-pattern correctness check, use `--write-patterns` on one run; the default `--count-only` avoids writing potentially very large result files.

## Measurement and comparison rules

- Keep the utility ratios as **fractions**, not percentages. The program's `--min-utility-ratio` takes `0.0351085`, not `3.51085`.
- Keep original threshold and length family names in tables/plots; never pool unrestricted counts and length-3 counts as if equivalent.
- Record exact data SHA-256 and source version, full Spark job runtime, local/global phase timings, candidate counts, number of HUSPs and success/timeout status.
- `driver_process_tree_peak_mb` is not a reliable cluster-wide memory metric and must not be compared numerically to the C# paper's peak RAM figures. Collect a consistent full JVM/Python worker peak measurement before memory claims.
- Compare Spark-AHUS with serial AHUS / HUSP-SP on **identical inputs, length, utility threshold**, separately from historical AHUS figures.
- Avoid automatically launching all 61 cases. Use high thresholds and small pilots first, and honor timeout status `124` before any harder setting.

## Provenance within original manuscript

- Environment and datasets: manuscript `Experimental Results / Datasets characteristics` (~lines 690–715).
- SIGN candidate pruning: `Evaluation of Pruning Strategies` (~lines 805–845).
- Full-length: `Execution Time with Full Length of Patterns` (~lines 994–1133).
- Fixed limits: `Execution Time with Maximum Length Thresholds` (~lines 1135–1310).
- Incremental lengths: `Execution Time with Incremental Length of Patterns` (~lines 1312–1462).
- KOSARAK scaling: `Scalability Evaluations on Large-scale Datasets` (~lines 1464–1555).
- Memory: `Memory Usage` (~lines 1557–1820).
