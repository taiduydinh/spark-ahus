#!/usr/bin/env python3
"""Create deterministic first-N sequence fractions for Spark-AHUS scalability tests."""

from __future__ import annotations

import argparse
import csv
import hashlib
import math
from pathlib import Path


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def valid_line(s: str) -> bool:
    t = s.strip()
    return bool(t) and t[0] not in "#%@"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", type=Path, required=True)
    ap.add_argument("--name", required=True)
    ap.add_argument("--output-dir", type=Path, required=True)
    ap.add_argument(
        "--fractions", nargs="+", type=float,
        default=[0.2, 0.4, 0.6, 0.8, 1.0]
    )
    args = ap.parse_args()

    fractions = sorted(set(args.fractions))
    if not fractions or fractions[0] <= 0 or fractions[-1] > 1:
        raise ValueError("Fractions must lie in (0, 1].")

    lines = []
    with args.input.open("r", encoding="utf-8") as f:
        for raw in f:
            if valid_line(raw):
                lines.append(raw if raw.endswith("\n") else raw + "\n")

    n = len(lines)
    if n == 0:
        raise ValueError("No sequences found.")

    outdir = args.output_dir
    outdir.mkdir(parents=True, exist_ok=True)
    rows = []

    for frac in fractions:
        count = n if frac == 1.0 else max(1, int(math.floor(n * frac)))
        pct = int(round(frac * 100))
        path = outdir / f"{args.name}_{pct:03d}.txt"
        with path.open("w", encoding="utf-8", newline="\n") as w:
            w.writelines(lines[:count])
        rows.append({
            "dataset": args.name,
            "fraction": f"{frac:.6f}",
            "percent": pct,
            "sequences": count,
            "file": path.name,
            "sha256": sha256(path),
        })
        print(f"{pct:>3}%  {count:>9,} sequences  {path}")

    with (outdir / "split_manifest.csv").open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=rows[0].keys())
        w.writeheader()
        w.writerows(rows)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
