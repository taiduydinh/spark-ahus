#!/usr/bin/env python3
"""Core correctness checks for Spark-AHUS without requiring PySpark."""

from __future__ import annotations

import itertools
import pickle
from typing import Dict, Iterable, List, Sequence, Tuple

from spark_ahus import (
    CandidateTrie,
    Pattern,
    PureArrayLocalAHUS,
    TrieVerifier,
    pack_partition,
)


def parse_direct_sequences(lines: Sequence[str]):
    sequences = []
    for line in lines:
        toks = line.split()
        itemsets = []
        cur = {}
        for tok in toks:
            if tok == "-1":
                if cur:
                    itemsets.append(cur)
                    cur = {}
            elif tok == "-2" or tok.startswith("SUtility:"):
                break
            elif not tok.startswith("-"):
                l = tok.index("[")
                r = tok.index("]")
                cur[int(tok[:l])] = float(tok[l + 1:r])
        if cur:
            itemsets.append(cur)
        sequences.append(itemsets)
    return sequences


def pattern_to_itemsets(pattern: Pattern):
    out = [[]]
    for token in pattern:
        if token == -1:
            out.append([])
        else:
            out[-1].append(int(token))
    return [tuple(x) for x in out if x]


def sequence_pattern_utility(sequence, pattern: Pattern) -> float:
    psets = pattern_to_itemsets(pattern)

    def rec(pidx: int, start_iset: int, acc: float) -> float:
        if pidx == len(psets):
            return acc
        pset = psets[pidx]
        best = 0.0
        for sidx in range(start_iset, len(sequence)):
            iset = sequence[sidx]
            if all(item in iset for item in pset):
                util = sum(iset[item] for item in pset)
                best = max(best, rec(pidx + 1, sidx + 1, acc + util))
        return best

    return rec(0, 0, 0.0)


def global_pattern_utility(sequences, pattern: Pattern) -> float:
    return sum(sequence_pattern_utility(seq, pattern) for seq in sequences)


def enumerate_patterns(items: Sequence[int], max_items: int) -> List[Pattern]:
    seen = set()
    stack = [(tuple([i]), 1) for i in items]
    while stack:
        pattern, count = stack.pop()
        if pattern in seen:
            continue
        seen.add(pattern)
        if count >= max_items:
            continue

        last = next(x for x in reversed(pattern) if x != -1)
        for item in items:
            if item > last:
                stack.append((pattern + (item,), count + 1))
            stack.append((pattern + (-1, item), count + 1))
    return sorted(seen)


def make_block(pid: int, lines: Iterable[str]):
    dense = {1: 0, 2: 1, 3: 2}
    blocks = list(
        pack_partition(
            pid,
            iter(lines),
            value_mode="utility",
            external_utility=None,
            dense_map=dense,
        )
    )
    assert len(blocks) == 1
    return blocks[0]


def dense_to_original(pattern: Pattern) -> Pattern:
    mapping = [1, 2, 3]
    return tuple(-1 if x == -1 else mapping[x] for x in pattern)


def main():
    lines = [
        "1[2] 2[3] -1 3[4] -1 -2 SUtility:9",
        "1[1] -1 2[5] 3[2] -1 -2 SUtility:8",
        "2[2] -1 1[4] 3[1] -1 -2 SUtility:7",
    ]
    sequences = parse_direct_sequences(lines)
    global_min = 6.0
    total_utility = 24.0
    ratio = global_min / total_utility
    max_len = 3

    # Brute-force exact answer.
    all_patterns = enumerate_patterns([1, 2, 3], max_len)
    brute = {
        p: global_pattern_utility(sequences, p)
        for p in all_patterns
        if global_pattern_utility(sequences, p) >= global_min
    }

    # One-partition local miner should equal the exact answer at the global threshold.
    block = make_block(0, lines)
    miner = PureArrayLocalAHUS(block, global_min, max_len)
    mined_dense = dict(miner.iter_patterns())
    mined = {dense_to_original(p): u for p, u in mined_dense.items()}

    assert set(mined) == set(brute), (
        "One-partition miner pattern mismatch\n"
        f"missing={set(brute)-set(mined)}\nextra={set(mined)-set(brute)}"
    )
    for p in brute:
        assert abs(mined[p] - brute[p]) < 1e-9, (p, mined[p], brute[p])

    # Partition-adaptive candidate generation: split DB, mine locally at
    # ratio * partition utility, union candidates, exact-verify globally.
    block1 = make_block(0, lines[:2])  # TU=17
    block2 = make_block(1, lines[2:])  # TU=7

    candidates = set()
    for b in (block1, block2):
        local_min = ratio * b.total_utility
        m = PureArrayLocalAHUS(b, local_min, max_len)
        candidates.update(p for p, _ in m.iter_patterns())

    candidate_list = sorted(candidates)
    trie = CandidateTrie.build(candidate_list)

    # Regression: Spark workers cannot unpickle a custom class defined in
    # spark-submit's __main__. Broadcast a wire-format trie containing only
    # built-in lists, tuples, dicts and integers instead.
    wire_trie = pickle.loads(pickle.dumps(trie.to_wire()))
    assert isinstance(wire_trie, list)
    for terminal_id, i_children, s_children in wire_trie:
        assert isinstance(terminal_id, int)
        assert isinstance(i_children, dict)
        assert isinstance(s_children, dict)
        assert all(isinstance(k, int) and isinstance(v, int)
                   for k, v in i_children.items())
        assert all(isinstance(k, int) and isinstance(v, int)
                   for k, v in s_children.items())

    sums: Dict[int, float] = {}
    for b in (block1, block2):
        in_process = dict(TrieVerifier(b, trie).iter_local_utilities())
        broadcast = dict(TrieVerifier(b, wire_trie).iter_local_utilities())
        assert in_process == broadcast, (
            "Broadcasted wire trie verification differs from the original trie",
            in_process,
            broadcast,
        )
        for cid, util in broadcast.items():
            sums[cid] = sums.get(cid, 0.0) + util

    verified = {}
    for cid, p in enumerate(candidate_list):
        util = sums.get(cid, 0.0)
        if util >= global_min:
            verified[dense_to_original(p)] = util

    assert set(verified) == set(brute), (
        "Adaptive partitioned pipeline mismatch\n"
        f"missing={set(brute)-set(verified)}\nextra={set(verified)-set(brute)}"
    )
    for p in brute:
        assert abs(verified[p] - brute[p]) < 1e-9, (
            p, verified[p], brute[p]
        )

    print("PASS: pure-array local miner matches brute force.")
    print("PASS: pickle-safe broadcast trie exactly matches in-process verification.")
    print("PASS: partition-adaptive candidate union + trie verification is exact.")
    print(f"Exact HUSPs at minUtil={global_min}: {len(brute)}")


if __name__ == "__main__":
    main()
