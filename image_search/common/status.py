"""Status reporting for the image_search pipeline.

Two modes:

1. Live CLI printer  — one line per pipeline step (for the user to watch).
2. JSONL log writer — append-only file under data/pilot_results/ that other
   tooling can tail or post-process.
"""
from __future__ import annotations

import json
import sys
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator


def _now() -> float:
    return time.time()


def _println(msg: str) -> None:
    print(msg, file=sys.stdout, flush=True)


@contextmanager
def step(name: str, *, total: int | None = None, log_path: Path | None = None) -> Iterator[dict[str, Any]]:
    """Wrap a pipeline step. Yields a mutable dict the caller can update with
    `count`, `extra`, etc. On exit, prints a one-liner and optionally appends
    a JSONL record to log_path.
    """
    start = _now()
    state: dict[str, Any] = {"step": name, "total": total, "count": 0, "warnings": 0, "extra": {}}

    header = f"[STEP] {name} starting"
    if total is not None:
        header += f" (n={total})"
    _println(header)

    try:
        yield state
    finally:
        state["duration_s"] = round(_now() - start, 3)
        footer = f"[STEP] {name} done in {state['duration_s']}s count={state['count']}"
        if state["warnings"]:
            footer += f" warnings={state['warnings']}"
        _println(footer)

        if log_path is not None:
            log_path.parent.mkdir(parents=True, exist_ok=True)
            with log_path.open("a") as fh:
                fh.write(json.dumps({
                    "step": state["step"],
                    "started_at": start,
                    "duration_s": state["duration_s"],
                    "count": state["count"],
                    "warnings": state["warnings"],
                    "total": state["total"],
                    "extra": state["extra"],
                }) + "\n")
