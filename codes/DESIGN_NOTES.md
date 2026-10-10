# Spark-AHUS design notes

## Working title

**Spark-AHUS: An Adaptive Pure-Array Parallel Framework for High-Utility Sequential Pattern Mining**

## Core architecture

Spark-AHUS does not parallelize recursive AHUS nodes with Python threads. Instead it distributes the sequence database, performs exact candidate generation independently within compact partition-local pure arrays, and globally verifies the union of locally promising patterns.

The design has six phases:

1. global SWU preprocessing;
2. global unpromising-item removal and dense item remapping;
3. partition-local pure-array construction;
4. partition-adaptive local AHUS mining;
5. global candidate deduplication;
6. chunked exact global verification with prefix-sharing candidate tries.

## Pure-array partition structure

For each Spark database partition, all active sequences are concatenated into typed arrays:

- `seq_offsets`: sequence boundaries;
- `items`: dense `int32` item identifiers;
- `utilities`: `float64` occurrence utilities;
- `remaining`: suffix utility strictly after each occurrence;
- `itemsets`: itemset identifiers;
- `pos_sid`: sequence id for every flat position;
- `root_order`, `root_items`, `root_offsets`: compact root occurrence index.

A recursive prefix projection stores only:

- flat ending positions;
- prefix utility at each ending position.

The underlying sequence data are never copied into a projected database.

## Partition-adaptive threshold theorem

Let the database be partitioned into disjoint subsets

\[
D = D_1 \cup D_2 \cup \cdots \cup D_m,
\]

and let `TU(D)` denote the total sequence utility. Define the global relative threshold

\[
\theta = \delta / TU(D),
\]

where \(\delta\) is the absolute global minimum utility.

For each partition use

\[
\delta_j = \theta\,TU(D_j).
\]

For any sequential pattern \(P\),

\[
U(P,D)=\sum_j U(P,D_j).
\]

Therefore, if \(U(P,D)\ge \delta\), at least one partition must satisfy

\[
U(P,D_j)\ge \delta_j.
\]

Otherwise summing \(U(P,D_j)<\theta TU(D_j)\) over all partitions would imply
\(U(P,D)<\theta TU(D)=\delta\), a contradiction.

Thus the union of partition-local HUSPs contains every global HUSP. Local mining is only candidate generation; exact global utility is recomputed afterward.

## Bound-first projection materialization

For each active prefix, Spark-AHUS first computes a safe extension upper bound without constructing each child's projection.

For a reachable extension ending at position \(q\), the local bound contribution is

\[
pu(P) + u(q) + ru(q),
\]

where `pu` is prefix utility and `ru` is remaining utility after `q`.

Within each sequence the maximum contribution for each extension item is retained; these maxima are summed across sequences. A child projection is materialized only if the resulting bound reaches the partition-local threshold.

This deliberately exchanges a limited second scan of the parent projection for much lower temporary projection memory.

## Exact global verification

Local HUSPs are deduplicated globally. Candidates are verified in bounded-size chunks.

Each chunk is converted into a prefix-sharing trie. Intermediate trie nodes are created even when the corresponding prefix is not itself a candidate. Each executor traverses the candidate trie over its local pure-array block using the same I- and S-extension projection semantics as AHUS.

The executor emits only `(candidate_id, local_exact_utility)` pairs. Spark sums these local utilities across partitions, and a candidate is output iff its global exact utility reaches the original global minimum utility.

## Why this is different from the current `ahusp_p.py`

The current `pypatternminer/ahusp_p.py` uses:
- Python `threading.Thread`;
- one shared task `Queue`;
- locks for global output/candidate counters;
- nested dictionary/list projections;
- recursive nodes as shared-memory scheduling units.

Spark-AHUS instead uses:
- process-level PySpark parallelism;
- coarse database partitions rather than one scheduler task per recursive node;
- compact NumPy arrays;
- no shared mutable mining state across workers;
- adaptive local thresholds with a completeness guarantee;
- bound-first child projection materialization;
- trie-based exact global verification.

## Current scope

The implementation is intentionally restricted to positive-utility HUSPM, matching the standard setting of the AHUS implementation currently in `pypatternminer`.

Before paper experiments, the implementation should be validated on:
- the AHUS demonstration database;
- several public HUSPM benchmarks;
- multiple thresholds;
- complete output sets for cases where output materialization is tractable.

The included `test_spark_ahus_core.py` also performs a brute-force exact test and a two-partition adaptive-threshold verification test without requiring Spark.
