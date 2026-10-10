# Spark-AHUS benchmark dataset package

This package downloads, verifies, and prepares the **eight datasets selected for the Spark-AHUS IEEE experiments**.

It does **not** redistribute the datasets itself. Original files are downloaded from their public upstream repositories/releases and retained unchanged under `raw/`.

## Selected eight datasets

| Dataset | Experimental purpose | Upstream source used here |
|---|---|---|
| SIGN | long/dense small benchmark; pruning/correctness | DSI-Lab1/HUSPM |
| Leviathan | medium text-sequence benchmark | DSI-Lab1/HUSPM |
| Kosarak10K | established smaller clickstream benchmark | DSI-Lab1/HUSPM |
| Bible | larger text-sequence benchmark | DSI-Lab1/HUSPM |
| Yoochoose | large real session data; multi-item events | DSI-Lab1/HUSPM |
| FIFA | medium/large clickstream benchmark | huspm-datasets v1.2.1 |
| C8T1S5I8N5K | synthetic multi-item-event benchmark; stresses I-extension | huspm-datasets v1.2.1 |
| KOSARAK-full | 990,002-sequence large benchmark; main Spark scaling target | huspm-datasets v1.2.1 |

The five DSI-Lab1/HUSPM files are pinned to commit:

```text
8b1f5f04c941fc2f7f4a0c14ae143d6bc67ffa4a
```

The other three are pinned to:

```text
tran-minh-thai/huspm-datasets
release v1.2.1-measured-quantity
```

The release archive SHA-256 is:

```text
f397dfaaa7f96d5504eab14dd0c5cd5cedee682448ef09189cfb6c353baba88c
```

## Why two sources?

The HUSP-SP repository provides several benchmark files directly in the `item[utility] ... SUtility:...` representation used by HUSP-SP.

The `huspm-datasets` release provides quantitative sequences as:

```text
item[quantity] ... -1 ... -2
```

plus:

```text
item:external_utility
```

for each item. For FIFA, C8T1S5I8N5K, and full KOSARAK, this package creates a direct-utility working copy using:

```text
occurrence utility = quantity × external utility
```

No sequence order, itemset boundary, or item identifier is changed.

This gives all eight prepared datasets a **single common input representation**, allowing Spark-AHUS and serial HUSPM baselines to mine exactly the same utility sequences.

## Server use

Extract this package and run:

```bash
chmod +x fetch_prepare_datasets.sh
./fetch_prepare_datasets.sh
```

The resulting layout is:

```text
spark_ahus_datasets/
├── raw/
│   ├── husp_sp_commit/
│   └── huspm_datasets_v1.2.1/
├── prepared/
│   ├── direct_utility/
│   │   ├── SIGN.txt
│   │   ├── Leviathan.txt
│   │   ├── Kosarak10K.txt
│   │   ├── Bible.txt
│   │   ├── Yoochoose.txt
│   │   ├── FIFA.txt
│   │   ├── C8T1S5I8N5K.txt
│   │   └── KOSARAK_full.txt
│   ├── dataset_stats.csv
│   ├── dataset_config.json
│   ├── provenance.json
│   └── SHA256SUMS.txt
├── fetch_prepare_datasets.sh
├── prepare_huspm_datasets.py
└── make_scalability_splits.py
```

All files under `prepared/direct_utility/` should be run with:

```bash
--value-mode utility
```

For example:

```bash
spark-submit \
  --master local[8] \
  --driver-memory 16g \
  /path/to/spark_ahus.py \
  --input prepared/direct_utility/SIGN.txt \
  --value-mode utility \
  --min-utility 1000 \
  --partitions 16 \
  --count-only \
  --stats-json results/SIGN_test.json
```

The `1000` above is only an example. **Do not use it as a paper threshold.** We will calibrate the T10–T1 thresholds after the dataset statistics and initial pilot runs are available.

## Scalability splits

For the main large-data scalability experiment, create deterministic first-N fractions of KOSARAK-full:

```bash
python make_scalability_splits.py \
  --input prepared/direct_utility/KOSARAK_full.txt \
  --name KOSARAK_full \
  --output-dir prepared/scalability/KOSARAK_full
```

This creates 20%, 40%, 60%, 80%, and 100% files and a checksum manifest.

Yoochoose can be prepared in the same way:

```bash
python make_scalability_splits.py \
  --input prepared/direct_utility/Yoochoose.txt \
  --name Yoochoose \
  --output-dir prepared/scalability/Yoochoose
```

## Integrity checks

`prepare_huspm_datasets.py` performs two source-level integrity checks:

1. the five HUSP-SP downloads are checked against the Git blob SHA-1 values at the pinned commit;
2. the six source files used for FIFA, C8T1S5I8N5K, and KOSARAK-full are checked against the official SHA-256 values in the pinned `v1.2.1-measured-quantity` release manifest.

It then computes SHA-256 checksums for all eight prepared working files.

## Licensing/provenance note

The `huspm-datasets` repository states that its generated utilities, parameters, manifests, and collection arrangement are CC BY 4.0, while rights in the underlying SPMF-derived sequence structures remain with their original holders.

The DSI-Lab1/HUSPM repository provides the HUSP-SP benchmark files publicly but its README does not state a dataset-specific redistribution licence. This package therefore **downloads them from upstream instead of bundling or redistributing the files**.

For the paper/repository, cite the upstream benchmark source(s) and record the exact revision/checksums written by this preparation package.
