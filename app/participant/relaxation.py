"""Relaxation ladder: progressively relax a HardFilters set when the SQL gate
returns zero candidates.

Each rung is a (HardFilters, description) pair. Callers iterate in order,
applying each relaxation cumulatively until count ≥ min_hits or the ladder is
exhausted. The descriptions are appended to `meta.relaxations` so users see
what the system changed.

Rungs (skipped if the precondition doesn't apply to the current filter):
  1. price window ±10%   (requires min_price or max_price set)
  2. drop city           (requires city set; canton is kept)
  3. drop canton         (requires canton set)
  4. expand radius ×1.5  (requires lat/lng + radius_km set)
  5. drop required features (requires features set)

Every rung is pure — returns a new HardFilters instance; the input is never
mutated.
"""
from __future__ import annotations

from collections.abc import Iterator

from app.models.schemas import HardFilters


def relax(hf: HardFilters) -> Iterator[tuple[HardFilters, str]]:
    """Yield progressively relaxed (filter, description) pairs.

    Relaxations are cumulative: each yielded filter contains all previous
    relaxations. Callers iterate until they get enough hits or the ladder ends.
    """
    current = hf.model_copy(deep=True)

    # Rung 1: price window ±10%
    if current.min_price is not None or current.max_price is not None:
        before = f"price=({current.min_price},{current.max_price})"
        new = current.model_copy(deep=True)
        if new.min_price is not None:
            new.min_price = max(0, int(new.min_price * 0.9))
        if new.max_price is not None:
            new.max_price = int(new.max_price * 1.1)
        desc = f"Expanded price ±10% ({before} → ({new.min_price},{new.max_price}))"
        yield new, desc
        current = new

    # Rung 2: drop city (keep canton)
    if current.city:
        before = list(current.city)
        new = current.model_copy(deep=True)
        new.city = None
        desc = f"Dropped city={before} (kept canton={new.canton!r})"
        yield new, desc
        current = new

    # Rung 3: drop canton
    if current.canton:
        before = current.canton
        new = current.model_copy(deep=True)
        new.canton = None
        desc = f"Dropped canton={before!r}"
        yield new, desc
        current = new

    # Rung 4: expand radius
    if (
        current.radius_km is not None
        and current.latitude is not None
        and current.longitude is not None
    ):
        before = current.radius_km
        new = current.model_copy(deep=True)
        new.radius_km = round(float(new.radius_km) * 1.5, 3)
        desc = f"Expanded radius {before:.2f}km → {new.radius_km:.2f}km"
        yield new, desc
        current = new

    # Rung 5: drop required features
    if current.features:
        before = list(current.features)
        new = current.model_copy(deep=True)
        new.features = None
        desc = f"Dropped required_features={before}"
        yield new, desc
        current = new
