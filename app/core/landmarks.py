"""Load + look up Swiss landmarks from the teammate-shipped gazetteer.

Each landmark has a stable ``key`` (e.g. ``eth_zentrum``) plus a few
multilingual aliases (``"ETH"``, ``"ETH Zürich"``, ``"ETHZ"``). The migration
script populates one ``dist_landmark_<key>_m`` column on
``listings_ranking_signals`` per landmark, so the runtime lookup only needs to
turn a free-text alias or exact key into that column name.

Resolution is slug + accent-fold insensitive so the LLM can emit any of the
aliases the user typed.
"""
from __future__ import annotations

import json
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from app.core.normalize import slug


DEFAULT_LANDMARKS_PATH = (
    Path(__file__).resolve().parents[2] / "data" / "ranking" / "landmarks.json"
)


@dataclass(frozen=True, slots=True)
class Landmark:
    key: str
    kind: str
    lat: float
    lon: float
    aliases: tuple[str, ...]


_LOCK = threading.Lock()
_STATE: dict[str, object] = {"by_slug": None, "by_key": None, "path": None}


def _slug_keys_for(lm: Landmark) -> Iterable[str]:
    """Every alias plus the raw key, slugged."""
    seen: set[str] = set()
    for name in (lm.key, *lm.aliases):
        s = slug(name)
        if s and s not in seen:
            seen.add(s)
            yield s


def load(path: Path = DEFAULT_LANDMARKS_PATH) -> None:
    """Read the JSON gazetteer and populate the module state. Idempotent."""
    with _LOCK:
        if _STATE["by_slug"] is not None and _STATE["path"] == path:
            return
        if not path.exists():
            print(
                f"[WARN] landmarks.load: expected={path}, got=missing, "
                f"fallback=empty gazetteer (near_landmark queries no-op)",
                flush=True,
            )
            _STATE["by_slug"] = {}
            _STATE["by_key"] = {}
            _STATE["path"] = path
            return
        raw = json.loads(path.read_text(encoding="utf-8"))
        by_slug: dict[str, Landmark] = {}
        by_key: dict[str, Landmark] = {}
        for entry in raw:
            lm = Landmark(
                key=entry["key"],
                kind=entry.get("kind", "unknown"),
                lat=float(entry["lat"]),
                lon=float(entry["lon"]),
                aliases=tuple(entry.get("aliases") or ()),
            )
            by_key[lm.key] = lm
            for s in _slug_keys_for(lm):
                # Last writer wins on duplicates; landmarks.json is hand-curated
                # and conflicts would be a teammate bug.
                by_slug[s] = lm
        _STATE["by_slug"] = by_slug
        _STATE["by_key"] = by_key
        _STATE["path"] = path


def resolve(name: str) -> Landmark | None:
    """Turn a free-text alias or the exact key into a :class:`Landmark`.

    Slug + accent-fold insensitive so ``"ETH"`` / ``"ETH Zürich"`` / ``"ETHZ"``
    all resolve to ``eth_zentrum``. Returns ``None`` when the name is not
    known; callers log a ``[WARN]`` and skip that landmark entry.
    """
    if not name or not isinstance(name, str):
        return None
    if _STATE["by_slug"] is None:
        load()  # default path; no-op if already loaded at a different path
    by_key = _STATE["by_key"] or {}
    if name in by_key:  # type: ignore[operator]
        return by_key[name]  # type: ignore[index]
    s = slug(name)
    if not s:
        return None
    by_slug = _STATE["by_slug"] or {}
    return by_slug.get(s)  # type: ignore[union-attr]


def column_for(key: str) -> str:
    """The ``listings_ranking_signals`` column that stores distance in metres
    to this landmark. Column is populated by the migration.
    """
    return f"dist_landmark_{key}_m"


def all_landmarks() -> list[Landmark]:
    load()
    return list((_STATE["by_key"] or {}).values())  # type: ignore[union-attr]
