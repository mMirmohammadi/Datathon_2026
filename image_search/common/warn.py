"""[WARN] emitter per project CLAUDE.md §5.

Every fallback path in this codebase logs a [WARN] line. Format is stable so
downstream tooling can grep/count warnings:

    [WARN] <context>: key1=value1 key2=value2 ...
"""
from __future__ import annotations

import sys
from typing import Any


def warn(context: str, /, **kv: Any) -> None:
    body = " ".join(f"{k}={v!r}" for k, v in kv.items())
    line = f"[WARN] {context}"
    if body:
        line += f": {body}"
    print(line, file=sys.stderr, flush=True)
