#!/usr/bin/env python3
"""Prepare the exact Spark-AHUS eight-dataset benchmark suite.

The downloader keeps original source files under raw/.  This script verifies
the pinned source bytes, creates one common direct-occurrence-utility format
under prepared/direct_utility/, computes descriptive statistics, and records
SHA-256 checksums and machine-readable provenance.

Selected suite
--------------
HUSP-SP repository, direct occurrence utility:
  SIGN, Leviathan, Kosarak10K, Bible, Yoochoose

huspm-datasets v1.2.1-measured-quantity, quantity + external utility:
  FIFA, C8T1S5I8N5K, KOSARAK-full

The three quantity datasets are converted by:
  occurrence_utility = quantity * external_utility(item)
No sequence structure, item identity, or ordering is changed.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import shutil
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Sequence, Tuple


HUSP_COMMIT = "8b1f5f04c941fc2f7f4a0c14ae143d6bc67ffa4a"
QUANT_TAG = "v1.2.1-measured-quantity"

# Git blob SHA-1 values at the pinned DSI-Lab1/HUSPM commit.
HUSP_GIT_BLOBS = {
    "SIGN.txt": "b4363850375a536cc315d2fc962a73ad9f7f13d1",
    "Leviathan.txt": "6d68b8c37b7e2cff440a8e97aa656718cb7a5b37",
    "Kosarak10k.txt": "a3d93fbebb9da683038692149fe274ad423e5c42",
    "BIBLE.txt": "6010217433079d063477c744170df275d453e0ab",
    "Yoochoose.txt": "9e77e42e787960788636c00b22856293dfa452bd",
}

# Official SHA-256 values from the v1.2.1 release MANIFEST.sha256.
QUANT_SHA256 = {
    "FIFA_eui.txt": "a1d1548817fd1c3002712bb23734a92388914aa0cdb769faf517ecf6d102f5a0",
    "FIFA_seq.txt": "39abf6507d035466017a3726d1a93b3fcc2ae2783e1a1046634614c9d2dad13a",
    "C8T1S5I8N5K_eui.txt": "81c960709e7663d44bf2e0c79afecd76c6409b045415a0f82443b2ee8f46b5b6",
    "C8T1S5I8N5K_seq.txt": "37f8dd5ab5c00d191c8bb54eccab34367d8811c1306ac63d9ce9fe302f7063d9",
    "KOSARAK_eui.txt": "176c4036de190047cedc669af4a4450231c77f3fdc4004c7077f56f0cd8a0413",
    "KOSARAK_seq.txt": "5694d65a18c347120891d0549a5ab8599dc17826d970b3c95ef00096ef83019e",
}

DIRECT_SOURCES = [
    ("SIGN", "SIGN.txt"),
    ("Leviathan", "Leviathan.txt"),
    ("Kosarak10K", "Kosarak10k.txt"),
    ("Bible", "BIBLE.txt"),
    ("Yoochoose", "Yoochoose.txt"),
]

QUANTITY_SOURCES = [
    ("FIFA", "FIFA_seq.txt", "FIFA_eui.txt"),
    ("C8T1S5I8N5K", "C8T1S5I8N5K_seq.txt", "C8T1S5I8N5K_eui.txt"),
    ("KOSARAK-full", "KOSARAK_seq.txt", "KOSARAK_eui.txt"),
]

OUTPUT_FILENAMES = {
    "SIGN": "SIGN.txt",
    "Leviathan": "Leviathan.txt",
    "Kosarak10K": "Kosarak10K.txt",
    "Bible": "Bible.txt",
    "Yoochoose": "Yoochoose.txt",
    "FIFA": "FIFA.txt",
    "C8T1S5I8N5K": "C8T1S5I8N5K.txt",
    "KOSARAK-full": "KOSARAK_full.txt",
}


@dataclass
class Stats:
    dataset: str
    source: str
    representation: str
    sequences: int
    distinct_items: int
    positions: int
    itemsets: int
    avg_length: float
    avg_itemsets_per_sequence: float
    items_per_event: float
    multi_item_events: int
    multi_item_event_pct: float
    max_sequence_length: int
    max_itemset_size: int
    total_utility: float
    prepared_file: str
    prepared_sha256: str


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def git_blob_sha1(path: Path) -> str:
    data = path.read_bytes()
    h = hashlib.sha1()
    h.update(f"blob {len(data)}\0".encode("ascii"))
    h.update(data)
    return h.hexdigest()


def find_unique(root: Path, filename: str) -> Path:
    matches = [p for p in root.rglob(filename) if p.is_file()]
    if len(matches) != 1:
        raise FileNotFoundError(
            f"Expected exactly one {filename!r} under {root}, found {len(matches)}: "
            + ", ".join(str(x) for x in matches[:10])
        )
    return matches[0]


def fmt_num(x: float) -> str:
    if abs(x - round(x)) < 1e-9:
        return str(int(round(x)))
    return f"{x:.12f}".rstrip("0").rstrip(".")


def parse_item_token(token: str) -> Tuple[int, float]:
    l = token.find("[")
    r = token.rfind("]")
    if l <= 0 or r <= l:
        raise ValueError(f"Invalid token {token!r}")
    return int(token[:l]), float(token[l + 1:r])


def parse_declared_su(tokens: Sequence[str]) -> Optional[float]:
    for token in reversed(tokens):
        if token.startswith("SUtility:"):
            return float(token.split(":", 1)[1])
    return None


def read_eui(path: Path) -> Dict[int, float]:
    ans: Dict[int, float] = {}
    with path.open("r", encoding="utf-8") as f:
        for raw in f:
            s = raw.strip()
            if not s or s[0] in "#%@":
                continue
            item, val = s.split(":", 1)
            ans[int(item.strip())] = float(val.strip())
    if not ans:
        raise ValueError(f"No external utilities found in {path}")
    return ans


def line_to_itemsets_direct(line: str) -> Tuple[List[List[Tuple[int, float]]], Optional[float]]:
    tokens = line.split()
    declared = parse_declared_su(tokens)
    itemsets: List[List[Tuple[int, float]]] = []
    cur: List[Tuple[int, float]] = []
    for token in tokens:
        if token == "-1":
            if cur:
                itemsets.append(cur)
                cur = []
        elif token == "-2":
            break
        elif token.startswith("SUtility:"):
            break
        elif token and not token.startswith("-"):
            cur.append(parse_item_token(token))
    if cur:
        itemsets.append(cur)
    return itemsets, declared


def stats_direct(path: Path, dataset: str, source: str, representation: str) -> Stats:
    sequences = 0
    items = set()
    positions = 0
    itemsets_n = 0
    multi_item_events = 0
    max_seq_len = 0
    max_itemset_size = 0
    total_utility = 0.0

    with path.open("r", encoding="utf-8", errors="strict") as f:
        for lineno, raw in enumerate(f, 1):
            s = raw.strip()
            if not s or s[0] in "#%@":
                continue
            itemsets, declared = line_to_itemsets_direct(s)
            if not itemsets:
                continue

            calc_su = 0.0
            seq_len = 0
            for iset in itemsets:
                itemsets_n += 1
                if len(iset) > 1:
                    multi_item_events += 1
                max_itemset_size = max(max_itemset_size, len(iset))
                for item, util in iset:
                    if util < 0:
                        raise ValueError(
                            f"{path}:{lineno}: negative utility {util} for item {item}"
                        )
                    items.add(item)
                    positions += 1
                    seq_len += 1
                    calc_su += util

            if declared is not None and not math.isclose(
                calc_su, declared, rel_tol=1e-10, abs_tol=1e-8
            ):
                raise ValueError(
                    f"{path}:{lineno}: SUtility mismatch: declared={declared}, "
                    f"calculated={calc_su}"
                )

            sequences += 1
            max_seq_len = max(max_seq_len, seq_len)
            total_utility += calc_su

    if sequences == 0:
        raise ValueError(f"No valid sequences in {path}")

    return Stats(
        dataset=dataset,
        source=source,
        representation=representation,
        sequences=sequences,
        distinct_items=len(items),
        positions=positions,
        itemsets=itemsets_n,
        avg_length=positions / sequences,
        avg_itemsets_per_sequence=itemsets_n / sequences,
        items_per_event=(positions / itemsets_n if itemsets_n else 0.0),
        multi_item_events=multi_item_events,
        multi_item_event_pct=(100.0 * multi_item_events / itemsets_n if itemsets_n else 0.0),
        max_sequence_length=max_seq_len,
        max_itemset_size=max_itemset_size,
        total_utility=total_utility,
        prepared_file=path.name,
        prepared_sha256=sha256_file(path),
    )


def convert_quantity_to_direct(seq_path: Path, eui_path: Path, out_path: Path) -> None:
    eui = read_eui(eui_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with seq_path.open("r", encoding="utf-8") as src, out_path.open(
        "w", encoding="utf-8", newline="\n"
    ) as dst:
        for lineno, raw in enumerate(src, 1):
            s = raw.strip()
            if not s:
                continue
            if s[0] in "#%@":
                continue

            tokens = s.split()
            out_tokens: List[str] = []
            calc_su = 0.0
            seen_end = False

            for token in tokens:
                if token == "-1":
                    out_tokens.append("-1")
                elif token == "-2":
                    out_tokens.append("-2")
                    seen_end = True
                    break
                elif token.startswith("SUtility:"):
                    break
                elif token and not token.startswith("-"):
                    item, qty = parse_item_token(token)
                    if item not in eui:
                        raise KeyError(
                            f"{seq_path}:{lineno}: no external utility for item {item}"
                        )
                    utility = qty * eui[item]
                    if utility < 0:
                        raise ValueError(
                            f"{seq_path}:{lineno}: negative utility for item {item}"
                        )
                    calc_su += utility
                    out_tokens.append(f"{item}[{fmt_num(utility)}]")

            if not seen_end:
                out_tokens.append("-2")
            out_tokens.append(f"SUtility:{fmt_num(calc_su)}")
            dst.write(" ".join(out_tokens) + "\n")


def write_stats(path: Path, rows: Sequence[Stats]) -> None:
    fields = list(asdict(rows[0]).keys())
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for row in rows:
            d = asdict(row)
            for key in ("avg_length", "avg_itemsets_per_sequence", "items_per_event",
                        "multi_item_event_pct", "total_utility"):
                d[key] = f"{float(d[key]):.6f}"
            w.writerow(d)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw-root", type=Path, required=True)
    ap.add_argument("--output-root", type=Path, required=True)
    args = ap.parse_args()

    raw = args.raw_root.resolve()
    out = args.output_root.resolve()
    direct_dir = out / "direct_utility"
    direct_dir.mkdir(parents=True, exist_ok=True)

    husp_raw = raw / "husp_sp_commit"
    quant_extract = raw / "huspm_datasets_v1.2.1" / "extracted"

    provenance: List[dict] = []
    stats_rows: List[Stats] = []

    print("Verifying pinned HUSP-SP files...")
    for dataset, filename in DIRECT_SOURCES:
        src = husp_raw / filename
        if not src.is_file():
            raise FileNotFoundError(src)
        actual_blob = git_blob_sha1(src)
        expected_blob = HUSP_GIT_BLOBS[filename]
        if actual_blob != expected_blob:
            raise ValueError(
                f"Git blob mismatch for {src}: {actual_blob} != {expected_blob}"
            )

        dst = direct_dir / OUTPUT_FILENAMES[dataset]
        shutil.copy2(src, dst)
        row = stats_direct(
            dst,
            dataset,
            source=f"DSI-Lab1/HUSPM@{HUSP_COMMIT}",
            representation="direct occurrence utility (copied byte-for-byte)",
        )
        stats_rows.append(row)
        provenance.append({
            "dataset": dataset,
            "source_repository": "https://github.com/DSI-Lab1/HUSPM",
            "source_revision": HUSP_COMMIT,
            "source_file": f"datasets/{filename}",
            "source_integrity": {"git_blob_sha1": expected_blob},
            "prepared_file": f"direct_utility/{dst.name}",
            "transformation": "none; byte-for-byte copy",
            "spark_ahus_value_mode": "utility",
        })
        print(f"  OK {dataset:14s} {row.sequences:>8,} sequences")

    print("Locating and verifying v1.2.1 quantitative files...")
    for dataset, seq_name, eui_name in QUANTITY_SOURCES:
        seq = find_unique(quant_extract, seq_name)
        eui = find_unique(quant_extract, eui_name)
        for p in (seq, eui):
            expected = QUANT_SHA256[p.name]
            actual = sha256_file(p)
            if actual != expected:
                raise ValueError(
                    f"SHA-256 mismatch for {p}: {actual} != {expected}"
                )

        dst = direct_dir / OUTPUT_FILENAMES[dataset]
        convert_quantity_to_direct(seq, eui, dst)
        row = stats_direct(
            dst,
            dataset,
            source=f"tran-minh-thai/huspm-datasets release {QUANT_TAG}",
            representation="converted quantity × external utility -> direct occurrence utility",
        )
        stats_rows.append(row)
        provenance.append({
            "dataset": dataset,
            "source_repository": "https://github.com/tran-minh-thai/huspm-datasets",
            "source_revision": QUANT_TAG,
            "source_files": [seq_name, eui_name],
            "source_integrity": {
                seq_name: QUANT_SHA256[seq_name],
                eui_name: QUANT_SHA256[eui_name],
            },
            "prepared_file": f"direct_utility/{dst.name}",
            "transformation": "occurrence utility = quantity * external utility; sequence/itemset order unchanged",
            "spark_ahus_value_mode": "utility",
        })
        print(f"  OK {dataset:14s} {row.sequences:>8,} sequences")

    # Preserve the chosen paper order.
    desired = [x[0] for x in DIRECT_SOURCES] + [x[0] for x in QUANTITY_SOURCES]
    stats_rows.sort(key=lambda x: desired.index(x.dataset))
    provenance.sort(key=lambda x: desired.index(x["dataset"]))

    write_stats(out / "dataset_stats.csv", stats_rows)

    config = {
        "suite": "Spark-AHUS IEEE eight-dataset benchmark",
        "all_prepared_files_use": "--value-mode utility",
        "datasets": [
            {
                "name": r.dataset,
                "path": f"direct_utility/{r.prepared_file}",
                "sequences": r.sequences,
                "total_utility": r.total_utility,
                "sha256": r.prepared_sha256,
            }
            for r in stats_rows
        ],
    }
    (out / "dataset_config.json").write_text(
        json.dumps(config, indent=2) + "\n", encoding="utf-8"
    )
    (out / "provenance.json").write_text(
        json.dumps(provenance, indent=2) + "\n", encoding="utf-8"
    )

    sums = []
    for p in sorted(direct_dir.glob("*.txt")):
        sums.append(f"{sha256_file(p)}  direct_utility/{p.name}")
    (out / "SHA256SUMS.txt").write_text("\n".join(sums) + "\n", encoding="utf-8")

    print("\nPrepared suite:")
    for r in stats_rows:
        print(
            f"  {r.dataset:14s} N={r.sequences:>8,} "
            f"items={r.distinct_items:>6,} avgLen={r.avg_length:>8.2f} "
            f"items/event={r.items_per_event:>6.2f} TU={fmt_num(r.total_utility)}"
        )
    print(f"\nWrote {out / 'dataset_stats.csv'}")
    print(f"Wrote {out / 'SHA256SUMS.txt'}")
    print(f"Wrote {out / 'dataset_config.json'}")
    print(f"Wrote {out / 'provenance.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
