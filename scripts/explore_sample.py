"""One-off exploration of raw_data/sample_data_enriched/sample_enriched_500.csv.

Prints, for each column: coverage, dtype-ish signal, distinct count, top-10
distinct values with counts, length stats for strings, min/max for numbers,
and a handful of example values. Not production code.
"""
from __future__ import annotations

import csv
import re
import statistics
from collections import Counter
from pathlib import Path
from typing import Any


CSV_PATH = (
    Path(__file__).resolve().parents[1]
    / "raw_data"
    / "sample_data_enriched"
    / "sample_enriched_500.csv"
)

NUMERIC_LIKE = re.compile(r"^-?\d+(\.\d+)?$")


def is_null(v: str) -> bool:
    return v == "" or v.lower() in {"null", "none", "nan"}


def classify(values: list[str]) -> str:
    non_null = [v for v in values if not is_null(v)]
    if not non_null:
        return "all-null"
    if all(NUMERIC_LIKE.match(v) for v in non_null):
        return "numeric"
    if all(v in {"0", "1"} for v in non_null):
        return "binary"
    return "string"


def analyze_column(name: str, values: list[str]) -> None:
    total = len(values)
    nulls = sum(1 for v in values if is_null(v))
    non_null = [v for v in values if not is_null(v)]
    coverage = (total - nulls) / total * 100

    kind = classify(values)
    distinct = len(set(non_null))
    counter = Counter(non_null)

    print(f"\n{'='*78}")
    print(f"COLUMN: {name}")
    print(f"  coverage: {total - nulls}/{total} = {coverage:.1f}%   "
          f"distinct_non_null: {distinct}   kind: {kind}")

    if kind in ("numeric", "binary"):
        nums = [float(v) for v in non_null]
        if nums:
            print(f"  min={min(nums)}  max={max(nums)}  "
                  f"median={statistics.median(nums)}  "
                  f"mean={statistics.fmean(nums):.2f}")
    if kind == "string":
        lens = [len(v) for v in non_null]
        if lens:
            print(f"  len_min={min(lens)}  len_max={max(lens)}  "
                  f"len_median={statistics.median(lens)}")

    shown = 12 if distinct <= 30 else 10
    print(f"  top {min(shown, distinct)} values:")
    for val, count in counter.most_common(shown):
        v_show = val if len(val) <= 80 else val[:77] + "..."
        print(f"     {count:>4}  {v_show!r}")

    if distinct > shown:
        tail = distinct - shown
        print(f"     ...and {tail} more distinct values")

    if kind == "string" and distinct > 30:
        sample = [
            v for v in non_null[:20] if v not in {x for x, _ in counter.most_common(5)}
        ][:5]
        if sample:
            print(f"  sample (non-top): {sample}")


def main() -> None:
    with CSV_PATH.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        rows = list(reader)

    print(f"Loaded {len(rows)} rows, {len(reader.fieldnames or [])} columns")
    for col in reader.fieldnames or []:
        values = [row[col] for row in rows]
        analyze_column(col, values)


if __name__ == "__main__":
    main()
