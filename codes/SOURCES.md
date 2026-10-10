# Dataset provenance for Spark-AHUS

## DSI-Lab1/HUSPM

Repository:

https://github.com/DSI-Lab1/HUSPM

Pinned commit:

```text
8b1f5f04c941fc2f7f4a0c14ae143d6bc67ffa4a
```

Used files:

```text
datasets/SIGN.txt
datasets/Leviathan.txt
datasets/Kosarak10k.txt
datasets/BIBLE.txt
datasets/Yoochoose.txt
```

These are already direct occurrence-utility sequence databases.

The repository describes HUSP-SP and states that additional benchmark datasets can be obtained from SPMF. Its README does not state a separate redistribution licence for these data, so the Spark-AHUS preparation package downloads them from upstream rather than embedding them.

## tran-minh-thai/huspm-datasets

Repository:

https://github.com/tran-minh-thai/huspm-datasets

Pinned release:

```text
v1.2.1-measured-quantity
```

Used files:

```text
FIFA_seq.txt
FIFA_eui.txt
C8T1S5I8N5K_seq.txt
C8T1S5I8N5K_eui.txt
KOSARAK_seq.txt
KOSARAK_eui.txt
```

The release documentation describes these as quantitative sequence databases derived from public SPMF sequence structures, with generated internal quantities and external profits. Its repository states CC BY 4.0 for the generated utilities, parameter choices, manifests, and collection arrangement, while rights in the underlying sequence structures remain with the original holders.

For Spark-AHUS, the quantity/eui pairs are converted deterministically to the same direct occurrence-utility representation used by the HUSP-SP files:

```text
u(item occurrence) = quantity × external utility(item)
```

The exact source and prepared checksums are recorded automatically in `prepared/provenance.json` and `prepared/SHA256SUMS.txt`.
