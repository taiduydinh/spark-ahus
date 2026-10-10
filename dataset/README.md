# Benchmark datasets and provenance

This review archive contains prepared, not necessarily raw, sequence databases.
Files were preserved byte-for-byte from the DeepStation export. The paired
`prepared/baselines/` inputs are **normalized** for Python baseline semantics
and can shift IDs and scale utility values (notably TaFeng, ID offset +1 and
utility scale x100). Do not directly compare baseline pattern text without
reversing those transformations.

`prepared/provenance.json`, `prepared/paper_original/*PROVENANCE*.json`,
`prepared/SHA256SUMS.txt`, and the two TSV inventories document the source
and retained/omitted files. The original paper-protocol JSON includes several
older `has_matching_dataset: missing` strings; these are stale labels, not
proof that staged BMS/Foodmart/OnlineRetail/TaFeng files are absent.

Three large KOSARAK files were omitted by the original staging limit and their
SHA-256 values were retained in `excluded_large_files.tsv`:
- `prepared/direct_utility/KOSARAK_full.txt`
- `prepared/baselines/KOSARAK.txt` (same bytes as direct-utility full file)
- `prepared/baselines/KOSARAK_N600000.txt`

The full KOSARAK dataset cannot be reproduced from a shorter first-N prefix;
retrieve the documented original source/release and check byte hashes.

## Third-party redistribution

See source details in `codes/SOURCES.md`, `prepared/provenance.json`, and
`prepared/paper_original/AHUS_ARCHIVED_DATA_PROVENANCE.json`.
- `DSI-Lab1/HUSPM` contains sequence datasets without a clear dataset
  redistribution license in the exported evidence.
- `tran-minh-thai/huspm-datasets` identifies CC BY 4.0 terms for generated
  utility attributes; original SPMF sequence structures retain their own rights.
- Other archived collections (BMS1/2, Foodmart, OnlineRetail, TaFeng) need a
  separate provenance and redistribution review.

**Dataset redistribution is not cleared by this README.** The archive is for
private review until publication permissions have been checked.
