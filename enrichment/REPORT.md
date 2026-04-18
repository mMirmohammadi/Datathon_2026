# Enrichment Audit — Datathon 2026

Generated: 2026-04-18T19:17:53.048410+00:00
Total rows in listings_enriched: 25546

## 1 Summary

Every listing in the corpus has a non-null entry in every covered column (either a real value or the explicit `UNKNOWN` sentinel). No value was fabricated: if a field could not be recovered by pass 0 (original data), pass 1a (offline reverse-geocoder), pass 1b (Nominatim), or pass 2 (multilingual regex), it was sentinel-filled by pass 3 with `source=UNKNOWN, confidence=0.0`.

- Total nulls before enrichment: **549,456**
- Total rows sentinel-filled after: **450,850**
- Net recovered (real values added): **98,606**
- Dropped-as-bad rows: **2590** (see §6)
- Structured-vs-geocoded canton disagreements: **300** (see §5)

## 2 Before / After Null Counts

| field | nulls before | nulls (UNKNOWN) after | recovered |
|---|---:|---:|---:|
| `city` | 11,105 | 0 | 11,105 |
| `canton` | 14,879 | 3,177 | 11,702 |
| `postal_code` | 11,126 | 13 | 11,113 |
| `street` | 13,600 | 611 | 12,989 |
| `price` | 824 | 775 | 49 |
| `rooms` | 3,697 | 3,697 | 0 |
| `area` | 5,142 | 4,127 | 1,015 |
| `available_from` | 17,063 | 13,280 | 3,783 |
| `latitude` | 1,637 | 1,637 | 0 |
| `longitude` | 1,637 | 1,637 | 0 |
| `distance_public_transport` | 24,783 | 24,783 | 0 |
| `distance_shop` | 24,877 | 24,877 | 0 |
| `distance_kindergarten` | 25,119 | 25,119 | 0 |
| `distance_school_1` | 25,015 | 25,015 | 0 |
| `distance_school_2` | 25,145 | 25,145 | 0 |
| `feature_balcony` | 11,105 | 3,554 | 7,551 |
| `feature_elevator` | 11,105 | 8,110 | 2,995 |
| `feature_parking` | 11,105 | 4,889 | 6,216 |
| `feature_garage` | 11,105 | 6,659 | 4,446 |
| `feature_fireplace` | 11,209 | 10,507 | 702 |
| `feature_child_friendly` | 23,771 | 19,556 | 4,215 |
| `feature_pets_allowed` | 11,105 | 10,373 | 732 |
| `feature_temporary` | 25,546 | 23,163 | 2,383 |
| `feature_new_build` | 11,194 | 9,302 | 1,892 |
| `feature_wheelchair_accessible` | 11,902 | 11,459 | 443 |
| `feature_private_laundry` | 11,902 | 7,929 | 3,973 |
| `feature_minergie_certified` | 11,902 | 11,460 | 442 |
| `offer_type` | 1,042 | 1,042 | 0 |
| `object_category` | 12,064 | 12,064 | 0 |
| `object_type` | 24,749 | 24,749 | 0 |
| `original_url` | 11,105 | 11,105 | 0 |
| `floor` | 21,153 | 14,435 | 6,718 |
| `year_built` | 23,000 | 21,868 | 1,132 |
| `status` | 11,105 | 11,105 | 0 |
| `agency_name` | 25,546 | 25,010 | 536 |
| `agency_phone` | 25,546 | 23,610 | 1,936 |
| `agency_email` | 25,546 | 25,008 | 538 |

## 3 Source Distribution

| field | original | rev_geo_offline | rev_geo_nominatim | text_regex_* | default | cross_ref | DROPPED | UNKNOWN |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `city` | 14,441 | 11,024 | 0 | 0 | 0 | 0 | 81 | 0 |
| `canton` | 10,667 | 11,024 | 597 | 0 | 0 | 0 | 81 | 3,177 |
| `postal_code` | 14,420 | 0 | 11,113 | 0 | 0 | 0 | 0 | 13 |
| `street` | 11,946 | 0 | 12,989 | 0 | 0 | 0 | 0 | 611 |
| `price` | 22,262 | 0 | 0 | 0 | 0 | 0 | 2,509 | 775 |
| `rooms` | 20,890 | 0 | 0 | 0 | 0 | 0 | 959 | 3,697 |
| `area` | 20,404 | 0 | 0 | 0 | 0 | 0 | 0 | 4,127 |
| `available_from` | 8,483 | 0 | 0 | 0 | 0 | 0 | 0 | 13,280 |
| `latitude` | 23,909 | 0 | 0 | 0 | 0 | 0 | 0 | 1,637 |
| `longitude` | 23,909 | 0 | 0 | 0 | 0 | 0 | 0 | 1,637 |
| `distance_public_transport` | 763 | 0 | 0 | 0 | 0 | 0 | 0 | 24,783 |
| `distance_shop` | 669 | 0 | 0 | 0 | 0 | 0 | 0 | 24,877 |
| `distance_kindergarten` | 427 | 0 | 0 | 0 | 0 | 0 | 0 | 25,119 |
| `distance_school_1` | 531 | 0 | 0 | 0 | 0 | 0 | 0 | 25,015 |
| `distance_school_2` | 401 | 0 | 0 | 0 | 0 | 0 | 0 | 25,145 |
| `feature_balcony` | 14,441 | 0 | 0 | 0 | 0 | 0 | 0 | 3,554 |
| `feature_elevator` | 14,441 | 0 | 0 | 0 | 0 | 0 | 0 | 8,110 |
| `feature_parking` | 14,441 | 0 | 0 | 0 | 0 | 0 | 0 | 4,889 |
| `feature_garage` | 14,441 | 0 | 0 | 0 | 0 | 0 | 0 | 6,659 |
| `feature_fireplace` | 14,337 | 0 | 0 | 0 | 0 | 0 | 0 | 10,507 |
| `feature_child_friendly` | 1,775 | 0 | 0 | 0 | 0 | 0 | 0 | 19,556 |
| `feature_pets_allowed` | 14,441 | 0 | 0 | 0 | 0 | 0 | 0 | 10,373 |
| `feature_temporary` | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 23,163 |
| `feature_new_build` | 14,352 | 0 | 0 | 0 | 0 | 0 | 0 | 9,302 |
| `feature_wheelchair_accessible` | 13,644 | 0 | 0 | 0 | 0 | 0 | 0 | 11,459 |
| `feature_private_laundry` | 13,644 | 0 | 0 | 0 | 0 | 0 | 0 | 7,929 |
| `feature_minergie_certified` | 13,644 | 0 | 0 | 0 | 0 | 0 | 0 | 11,460 |
| `offer_type` | 24,504 | 0 | 0 | 0 | 0 | 0 | 0 | 1,042 |
| `object_category` | 13,482 | 0 | 0 | 0 | 0 | 0 | 0 | 12,064 |
| `object_type` | 797 | 0 | 0 | 0 | 0 | 0 | 0 | 24,749 |
| `original_url` | 14,441 | 0 | 0 | 0 | 0 | 0 | 0 | 11,105 |
| `floor` | 4,393 | 0 | 0 | 0 | 0 | 0 | 0 | 14,435 |
| `year_built` | 2,546 | 0 | 0 | 0 | 0 | 0 | 0 | 21,868 |
| `status` | 14,441 | 0 | 0 | 0 | 0 | 0 | 0 | 11,105 |
| `agency_name` | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 25,010 |
| `agency_phone` | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 23,610 |
| `agency_email` | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 25,008 |

## 4 Confidence Histogram

Per-field 10-bin histogram; each column is the row count in that bin.

| field | 0.0–0.1 | 0.1–0.2 | 0.2–0.3 | 0.3–0.4 | 0.4–0.5 | 0.5–0.6 | 0.6–0.7 | 0.7–0.8 | 0.8–0.9 | 0.9–1.0 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `city` | 81 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 25,465 |
| `canton` | 3,258 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 22,288 |
| `postal_code` | 13 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 11,113 | 14,420 |
| `street` | 611 | 0 | 0 | 0 | 0 | 0 | 0 | 12,989 | 0 | 11,946 |
| `price` | 3,284 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 22,262 |
| `rooms` | 4,656 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 20,890 |
| `area` | 4,127 | 0 | 0 | 1 | 0 | 0 | 2 | 23 | 989 | 20,404 |
| `available_from` | 13,280 | 0 | 0 | 0 | 0 | 10 | 30 | 73 | 3,670 | 8,483 |
| `latitude` | 1,637 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 23,909 |
| `longitude` | 1,637 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 23,909 |
| `distance_public_transport` | 24,783 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 763 |
| `distance_shop` | 24,877 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 669 |
| `distance_kindergarten` | 25,119 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 427 |
| `distance_school_1` | 25,015 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 531 |
| `distance_school_2` | 25,145 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 401 |
| `feature_balcony` | 3,554 | 0 | 0 | 0 | 230 | 1 | 8 | 46 | 7,266 | 14,441 |
| `feature_elevator` | 8,110 | 0 | 0 | 0 | 0 | 664 | 1 | 2 | 2,328 | 14,441 |
| `feature_parking` | 4,889 | 0 | 0 | 0 | 266 | 7 | 34 | 5,909 | 0 | 14,441 |
| `feature_garage` | 6,659 | 0 | 0 | 0 | 307 | 15 | 61 | 445 | 3,618 | 14,441 |
| `feature_fireplace` | 10,507 | 0 | 0 | 0 | 0 | 14 | 1 | 3 | 684 | 14,337 |
| `feature_child_friendly` | 19,556 | 0 | 0 | 0 | 54 | 69 | 4,092 | 0 | 0 | 1,775 |
| `feature_pets_allowed` | 10,373 | 0 | 0 | 0 | 244 | 0 | 3 | 485 | 0 | 14,441 |
| `feature_temporary` | 23,163 | 0 | 0 | 0 | 103 | 4 | 42 | 103 | 2,131 | 0 |
| `feature_new_build` | 9,302 | 0 | 0 | 0 | 0 | 272 | 53 | 164 | 1,403 | 14,352 |
| `feature_wheelchair_accessible` | 11,459 | 0 | 0 | 0 | 0 | 68 | 10 | 15 | 350 | 13,644 |
| `feature_private_laundry` | 7,929 | 0 | 0 | 0 | 175 | 2 | 21 | 3,775 | 0 | 13,644 |
| `feature_minergie_certified` | 11,460 | 0 | 0 | 0 | 0 | 5 | 0 | 0 | 3 | 14,078 |
| `offer_type` | 1,042 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 24,504 |
| `object_category` | 12,064 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 13,482 |
| `object_type` | 24,749 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 797 |
| `original_url` | 11,105 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 14,441 |
| `floor` | 14,435 | 0 | 0 | 0 | 13 | 18 | 38 | 134 | 6,515 | 4,393 |
| `year_built` | 21,868 | 0 | 0 | 0 | 4 | 1 | 13 | 22 | 33 | 3,605 |
| `status` | 11,105 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 14,441 |
| `agency_name` | 25,010 | 0 | 0 | 0 | 0 | 1 | 535 | 0 | 0 | 0 |
| `agency_phone` | 23,610 | 0 | 1 | 4 | 8 | 11 | 5 | 3 | 1,904 | 0 |
| `agency_email` | 25,008 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 2 | 536 |

## 5 Cross-Pass Disagreements

300 rows have a structured `canton` that disagrees with what reverse_geocoder would have returned for their lat/lng. Full JSON at `enrichment/data/disagreements.json`. Top 10 below.

| listing_id | raw canton | geocoded canton | city |
|---|:---:|:---:|---|
| `695fb9669ba220265d3629b6` | SG | AR | Herisau |
| `6967c6d0bb38f258c15feb70` | BE | FR | Flamatt |
| `697caa67786716632b070626` | BE | FR | Flamatt |
| `698dc1db26a6b0f79a5ee3bf` | BE | FR | Flamatt |
| `6994691ba7b4cbb191d48fb2` | BS | BL | Birsfelden |
| `69946939a7b4cbb191d4900d` | BS | BL | Birsfelden |
| `69946963a7b4cbb191d4908d` | BS | BL | Binningen |
| `69a6373d7ac8e6046a32010d` | BE | FR | Flamatt |
| `69a85bf4c623e610a704963d` | BS | BL | Munchenstein |
| `69a85bf9c623e610a704964f` | SG | AI | Balgach |

## 6 Known-Bad Rows

2590 listings had at least one field marked `DROPPED_bad_data` (price < 200, price > 50k, or rooms = 0). Full JSON at `enrichment/data/dropped_rows.json`. Top 10 below.

| listing_id | dropped field(s) | reason(s) |
|---|---|---|
| `10` | price, rooms | price=rooms_zero_non_residential:original_was=1070; rooms=rooms_zero_non_residential:original_was=0.0 |
| `10000` | price | price=price_below_200_chf:original_was=120 |
| `1001` | price | price=price_below_200_chf:original_was=130 |
| `1002` | price, rooms | price=rooms_zero_non_residential:original_was=3300; rooms=rooms_zero_non_residential:original_was=0.0 |
| `10037` | price | price=price_below_200_chf:original_was=55 |
| `10044` | price | price=price_below_200_chf:original_was=75 |
| `10050` | price | price=price_below_200_chf:original_was=70 |
| `10055` | price | price=price_below_200_chf:original_was=110 |
| `10058` | price | price=price_below_200_chf:original_was=150 |
| `1006` | price, rooms | price=rooms_zero_non_residential:original_was=1610; rooms=rooms_zero_non_residential:original_was=0.0 |

## 7 Re-validation vs analysis/REPORT.md

- Total row count: analysis=22819, enriched=25546 ✗
- SRED city fill via rev_geo_offline: 11,024 (expected ≈ 11,105 from REPORT §4 L64; gap 81 — OOB drops + new structured rows since REPORT was written)

## 8 Commands

```bash
# Full pipeline
docker compose exec api uv run python -m enrichment.scripts.enrich_all --db /data/listings.db --skip-1b
docker compose exec api uv run python -m enrichment.scripts.generate_report --db /data/listings.db

# Tests
docker compose exec api uv run pytest enrichment/tests/ -v
```
