"""Pass 1a — offline reverse-geocode lat/lng → city + canton.

Fills `city_filled` + `canton_filled` for every row where:
  * city_source='UNKNOWN-pending' after pass 0, AND
  * latitude + longitude are populated.

Uses reverse_geocoder (offline KDTree of GeoNames cities). Network-free,
deterministic, ~2s on 22k coords.

Pass 1b (Nominatim for postal/street) is a separate script.

Usage:
    python -m enrichment.scripts.pass1_geocode --db /data/listings.db
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from enrichment.common.cantons import admin1_to_canton_code
from enrichment.common.db import connect
from enrichment.common.provenance import UNKNOWN_VALUE, write_field
from enrichment.common.sources import (
    DROPPED_BAD_DATA,
    REV_GEO_OFFLINE,
    UNKNOWN_PENDING,
)

# CH bounding box.
# Sources: REPORT.md §5 L76 observed lat range 45.83-47.79 / lng 6.04-9.87,
# expanded slightly to tolerate OSM precision on border municipalities.
CH_LAT_MIN, CH_LAT_MAX = 45.8, 47.9
CH_LNG_MIN, CH_LNG_MAX = 5.9, 10.5

CITY_CONFIDENCE = 0.90    # offline pkg name is best-effort (nearest city in KDTree)
CANTON_CONFIDENCE = 0.95  # admin1 is more reliable — canton boundaries are coarse


def _is_null_island(lat: float, lng: float) -> bool:
    return lat == 0.0 and lng == 0.0


def _is_in_ch_bbox(lat: float, lng: float) -> bool:
    return CH_LAT_MIN <= lat <= CH_LAT_MAX and CH_LNG_MIN <= lng <= CH_LNG_MAX


def _collect_pending_rows(conn) -> list[tuple[str, float, float]]:
    rows = conn.execute(
        """
        SELECT le.listing_id, l.latitude, l.longitude
        FROM listings_enriched le
        JOIN listings l USING(listing_id)
        WHERE le.city_source = ?
          AND l.latitude IS NOT NULL
          AND l.longitude IS NOT NULL;
        """,
        (UNKNOWN_PENDING,),
    ).fetchall()
    return [(r["listing_id"], r["latitude"], r["longitude"]) for r in rows]


def _drop_both(conn, listing_id: str, reason: str) -> None:
    """Mark city + canton as DROPPED_bad_data so they stop counting as pending."""
    for field in ("city", "canton"):
        write_field(
            conn,
            listing_id=listing_id,
            field=field,
            filled=UNKNOWN_VALUE,
            source=DROPPED_BAD_DATA,
            confidence=0.0,
            raw=reason,
        )


def run(db_path: Path) -> dict[str, int]:
    import reverse_geocoder as rg  # lazy: loading the KDTree takes ~2s

    conn = connect(db_path)
    try:
        pending = _collect_pending_rows(conn)
        stats = {
            "pending_in": len(pending),
            "filled_rev_geo_offline": 0,
            "dropped_null_island": 0,
            "dropped_oob_ch": 0,
            "unmapped_admin1": 0,
        }

        if not pending:
            return stats

        coords = [(lat, lng) for _, lat, lng in pending]
        results = rg.search(coords, mode=2)

        unmapped_admin1_samples: list[str] = []

        for (listing_id, lat, lng), r in zip(pending, results, strict=True):
            if _is_null_island(lat, lng):
                _drop_both(conn, listing_id, "null_island")
                stats["dropped_null_island"] += 1
                continue

            cc = r.get("cc", "")
            if cc != "CH" or not _is_in_ch_bbox(lat, lng):
                _drop_both(conn, listing_id, f"oob_ch(cc={cc!r})")
                stats["dropped_oob_ch"] += 1
                continue

            admin1 = r.get("admin1", "")
            city_name = (r.get("name") or "").strip()
            canton_code = admin1_to_canton_code(admin1)

            if canton_code is None:
                # CLAUDE.md §5: announce the fallback with context.
                if len(unmapped_admin1_samples) < 5:
                    unmapped_admin1_samples.append(admin1)
                print(
                    f"[WARN] pass1_geocode: expected=mapped_canton "
                    f"got={admin1!r} lat={lat} lng={lng} "
                    f"listing_id={listing_id} fallback=keep_pending",
                    flush=True,
                )
                stats["unmapped_admin1"] += 1
                continue

            if city_name:
                write_field(
                    conn,
                    listing_id=listing_id,
                    field="city",
                    filled=city_name,
                    source=REV_GEO_OFFLINE,
                    confidence=CITY_CONFIDENCE,
                    raw=None,
                )
            write_field(
                conn,
                listing_id=listing_id,
                field="canton",
                filled=canton_code,
                source=REV_GEO_OFFLINE,
                confidence=CANTON_CONFIDENCE,
                raw=admin1,
            )
            stats["filled_rev_geo_offline"] += 1

        conn.commit()
        if unmapped_admin1_samples:
            print(
                f"[WARN] pass1_geocode: unmapped admin1 samples "
                f"(count={stats['unmapped_admin1']}): "
                f"{unmapped_admin1_samples}",
                flush=True,
            )
        return stats
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, required=True)
    args = parser.parse_args()
    if not args.db.exists():
        print(f"[ERROR] DB not found at {args.db}", file=sys.stderr)
        return 2
    stats = run(args.db)
    print("Pass 1a complete:")
    for k, v in stats.items():
        print(f"  {k}: {v}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
