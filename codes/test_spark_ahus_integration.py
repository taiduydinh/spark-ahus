#!/usr/bin/env python3
"""Full PySpark regression for Spark-AHUS's distributed trie verification.

Run inside the configured Docker environment:
    spark-submit --master 'local[4]' --driver-memory 4g test_spark_ahus_integration.py

Uses a tiny known dataset and an independent exhaustive utility oracle; tests
end-to-end preprocessing, local mining, candidate broadcast, exact verification
and file output. Does not alter any downloaded benchmark datasets.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from spark_ahus import (
    build_arg_parser,
    pattern_to_string,
    run_spark,
    validate_args,
)
from test_spark_ahus_core import (
    enumerate_patterns,
    global_pattern_utility,
    parse_direct_sequences,
)


LINES = [
    "1[2] 2[3] -1 3[4] -1 -2 SUtility:9",
    "1[1] -1 2[5] 3[2] -1 -2 SUtility:8",
    "2[2] -1 1[4] 3[1] -1 -2 SUtility:7",
]


def main() -> None:
    sequences = parse_direct_sequences(LINES)
    minutil = 6.0
    expected = {
        pattern_to_string(pattern, utility)
        for pattern in enumerate_patterns([1, 2, 3], max_items=3)
        if (utility := global_pattern_utility(sequences, pattern)) >= minutil
    }
    assert len(expected) == 11, f"Unexpected oracle count: {len(expected)}"

    with tempfile.TemporaryDirectory(prefix="spark_ahus_integration_") as tmp:
        temp = Path(tmp)
        data = temp / "tiny.txt"
        output = temp / "out.txt"
        stats = temp / "stats.json"
        data.write_text("\n".join(LINES) + "\n", encoding="utf-8")

        args = build_arg_parser().parse_args([
            "--input", str(data),
            "--value-mode", "utility",
            "--min-utility", str(minutil),
            "--max-pattern-length", "3",
            "--partitions", "4",
            "--verification-chunk-size", "2",
            "--output", str(output),
            "--stats-json", str(stats),
            "--spark-log-level", "ERROR",
        ])
        validate_args(args)
        result = run_spark(args)
        actual = set(output.read_text(encoding="utf-8").splitlines())

        assert result["husp_count"] == len(expected), (
            result["husp_count"], len(expected)
        )
        assert actual == expected, (
            f"Missing: {sorted(expected - actual)}\n"
            f"Extra: {sorted(actual - expected)}"
        )
        assert stats.is_file(), "Statistics JSON was not written"

    print("PASS: real PySpark candidate broadcast, 4 partitions, 2 candidates/chunk")
    print("PASS: all 11 HUSPs and their exact utilities match brute force")


if __name__ == "__main__":
    main()
