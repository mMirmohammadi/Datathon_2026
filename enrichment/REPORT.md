# Enrichment Audit — Datathon 2026

Generated: 2026-04-18T13:54:40.663659+00:00
Total rows in listings_enriched: 25546

## 1 Summary

Every listing in the corpus has a non-null entry in every covered column (either a real value or the explicit `UNKNOWN` sentinel). No value was fabricated: if a field could not be recovered by pass 0 (original data), pass 1a (offline reverse-geocoder), pass 1b (Nominatim), or pass 2 (multilingual regex), it was sentinel-filled by pass 3 with `source=UNKNOWN, confidence=0.0`.

- Total nulls before enrichment: **549,456**
- Total rows sentinel-filled after: **491,993**
- Net recovered (real values added): **57,463**
- Dropped-as-bad rows: **2590** (see §6)
- Structured-vs-geocoded canton disagreements: **0** (see §5)

## 2 Before / After Null Counts

| field | nulls before | nulls (UNKNOWN) after | recovered |
|---|---:|---:|---:|
| `city` | 11,105 | 0 | 11,105 |
| `canton` | 14,879 | 3,774 | 11,105 |
| `postal_code` | 11,126 | 11,126 | 0 |
| `street` | 13,600 | 13,600 | 0 |
| `price` | 824 | 775 | 49 |
| `rooms` | 3,697 | 3,697 | 0 |
| `area` | 5,142 | 4,073 | 1,069 |
| `available_from` | 17,063 | 14,259 | 2,804 |
| `latitude` | 1,637 | 1,637 | 0 |
| `longitude` | 1,637 | 1,637 | 0 |
| `distance_public_transport` | 24,783 | 24,783 | 0 |
| `distance_shop` | 24,877 | 24,877 | 0 |
| `distance_kindergarten` | 25,119 | 25,119 | 0 |
| `distance_school_1` | 25,015 | 25,015 | 0 |
| `distance_school_2` | 25,145 | 25,145 | 0 |
| `feature_balcony` | 11,105 | 2,552 | 8,553 |
| `feature_elevator` | 11,105 | 7,944 | 3,161 |
| `feature_parking` | 11,105 | 8,653 | 2,452 |
| `feature_garage` | 11,105 | 9,263 | 1,842 |
| `feature_fireplace` | 11,209 | 10,582 | 627 |
| `feature_child_friendly` | 23,771 | 23,182 | 589 |
| `feature_pets_allowed` | 11,105 | 10,828 | 277 |
| `feature_temporary` | 25,546 | 25,172 | 374 |
| `feature_new_build` | 11,194 | 10,015 | 1,179 |
| `feature_wheelchair_accessible` | 11,902 | 11,741 | 161 |
| `feature_private_laundry` | 11,902 | 10,097 | 1,805 |
| `feature_minergie_certified` | 11,902 | 11,526 | 376 |
| `offer_type` | 1,042 | 1,042 | 0 |
| `object_category` | 12,064 | 12,064 | 0 |
| `object_type` | 24,749 | 24,749 | 0 |
| `original_url` | 11,105 | 11,105 | 0 |
| `floor` | 21,153 | 14,459 | 6,694 |
| `year_built` | 23,000 | 22,787 | 213 |
| `status` | 11,105 | 11,105 | 0 |
| `agency_name` | 25,546 | 24,993 | 553 |
| `agency_phone` | 25,546 | 23,673 | 1,873 |
| `agency_email` | 25,546 | 24,944 | 602 |

## 3 Source Distribution

| field | original | rev_geo_offline | rev_geo_nominatim | text_regex_* | default | cross_ref | DROPPED | UNKNOWN |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `city` | 14,441 | 11,024 | 0 | 0 | 0 | 0 | 81 | 0 |
| `canton` | 10,667 | 11,024 | 0 | 0 | 0 | 0 | 81 | 3,774 |
| `postal_code` | 14,420 | 0 | 0 | 0 | 0 | 0 | 0 | 11,126 |
| `street` | 11,946 | 0 | 0 | 0 | 0 | 0 | 0 | 13,600 |
| `price` | 22,262 | 0 | 0 | 0 | 0 | 0 | 2,509 | 775 |
| `rooms` | 20,890 | 0 | 0 | 0 | 0 | 0 | 959 | 3,697 |
| `area` | 20,404 | 0 | 0 | 1,069 | 0 | 0 | 0 | 4,073 |
| `available_from` | 8,483 | 0 | 0 | 2,804 | 0 | 0 | 0 | 14,259 |
| `latitude` | 23,909 | 0 | 0 | 0 | 0 | 0 | 0 | 1,637 |
| `longitude` | 23,909 | 0 | 0 | 0 | 0 | 0 | 0 | 1,637 |
| `distance_public_transport` | 763 | 0 | 0 | 0 | 0 | 0 | 0 | 24,783 |
| `distance_shop` | 669 | 0 | 0 | 0 | 0 | 0 | 0 | 24,877 |
| `distance_kindergarten` | 427 | 0 | 0 | 0 | 0 | 0 | 0 | 25,119 |
| `distance_school_1` | 531 | 0 | 0 | 0 | 0 | 0 | 0 | 25,015 |
| `distance_school_2` | 401 | 0 | 0 | 0 | 0 | 0 | 0 | 25,145 |
| `feature_balcony` | 14,441 | 0 | 0 | 8,553 | 0 | 0 | 0 | 2,552 |
| `feature_elevator` | 14,441 | 0 | 0 | 3,161 | 0 | 0 | 0 | 7,944 |
| `feature_parking` | 14,441 | 0 | 0 | 2,452 | 0 | 0 | 0 | 8,653 |
| `feature_garage` | 14,441 | 0 | 0 | 1,842 | 0 | 0 | 0 | 9,263 |
| `feature_fireplace` | 14,337 | 0 | 0 | 627 | 0 | 0 | 0 | 10,582 |
| `feature_child_friendly` | 1,775 | 0 | 0 | 589 | 0 | 0 | 0 | 23,182 |
| `feature_pets_allowed` | 14,441 | 0 | 0 | 277 | 0 | 0 | 0 | 10,828 |
| `feature_temporary` | 0 | 0 | 0 | 374 | 0 | 0 | 0 | 25,172 |
| `feature_new_build` | 14,352 | 0 | 0 | 1,179 | 0 | 0 | 0 | 10,015 |
| `feature_wheelchair_accessible` | 13,644 | 0 | 0 | 161 | 0 | 0 | 0 | 11,741 |
| `feature_private_laundry` | 13,644 | 0 | 0 | 1,805 | 0 | 0 | 0 | 10,097 |
| `feature_minergie_certified` | 13,644 | 0 | 0 | 376 | 0 | 0 | 0 | 11,526 |
| `offer_type` | 24,504 | 0 | 0 | 0 | 0 | 0 | 0 | 1,042 |
| `object_category` | 13,482 | 0 | 0 | 0 | 0 | 0 | 0 | 12,064 |
| `object_type` | 797 | 0 | 0 | 0 | 0 | 0 | 0 | 24,749 |
| `original_url` | 14,441 | 0 | 0 | 0 | 0 | 0 | 0 | 11,105 |
| `floor` | 4,393 | 0 | 0 | 6,694 | 0 | 0 | 0 | 14,459 |
| `year_built` | 2,546 | 0 | 0 | 213 | 0 | 0 | 0 | 22,787 |
| `status` | 14,441 | 0 | 0 | 0 | 0 | 0 | 0 | 11,105 |
| `agency_name` | 0 | 0 | 0 | 553 | 0 | 0 | 0 | 24,993 |
| `agency_phone` | 0 | 0 | 0 | 1,873 | 0 | 0 | 0 | 23,673 |
| `agency_email` | 0 | 0 | 0 | 602 | 0 | 0 | 0 | 24,944 |

## 4 Confidence Histogram

Per-field 10-bin histogram; each column is the row count in that bin.

| field | 0.0–0.1 | 0.1–0.2 | 0.2–0.3 | 0.3–0.4 | 0.4–0.5 | 0.5–0.6 | 0.6–0.7 | 0.7–0.8 | 0.8–0.9 | 0.9–1.0 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `city` | 81 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 25,465 |
| `canton` | 3,855 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 21,691 |
| `postal_code` | 11,126 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 14,420 |
| `street` | 13,600 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 11,946 |
| `price` | 3,284 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 22,262 |
| `rooms` | 4,656 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 20,890 |
| `area` | 4,073 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 1,069 | 20,404 |
| `available_from` | 14,259 | 0 | 0 | 0 | 66 | 0 | 0 | 0 | 2,374 | 8,847 |
| `latitude` | 1,637 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 23,909 |
| `longitude` | 1,637 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 23,909 |
| `distance_public_transport` | 24,783 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 763 |
| `distance_shop` | 24,877 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 669 |
| `distance_kindergarten` | 25,119 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 427 |
| `distance_school_1` | 25,015 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 531 |
| `distance_school_2` | 25,145 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 401 |
| `feature_balcony` | 2,552 | 0 | 0 | 0 | 213 | 0 | 0 | 0 | 8,340 | 14,441 |
| `feature_elevator` | 7,944 | 0 | 0 | 0 | 0 | 704 | 0 | 0 | 2,457 | 14,441 |
| `feature_parking` | 8,653 | 0 | 0 | 0 | 166 | 0 | 0 | 2,286 | 0 | 14,441 |
| `feature_garage` | 9,263 | 0 | 0 | 0 | 33 | 0 | 0 | 0 | 1,809 | 14,441 |
| `feature_fireplace` | 10,582 | 0 | 0 | 0 | 0 | 3 | 0 | 0 | 624 | 14,337 |
| `feature_child_friendly` | 23,182 | 0 | 0 | 0 | 19 | 0 | 570 | 0 | 0 | 1,775 |
| `feature_pets_allowed` | 10,828 | 0 | 0 | 0 | 34 | 0 | 0 | 243 | 0 | 14,441 |
| `feature_temporary` | 25,172 | 0 | 0 | 0 | 34 | 0 | 0 | 0 | 340 | 0 |
| `feature_new_build` | 10,015 | 0 | 0 | 0 | 0 | 560 | 0 | 0 | 619 | 14,352 |
| `feature_wheelchair_accessible` | 11,741 | 0 | 0 | 0 | 0 | 3 | 0 | 0 | 158 | 13,644 |
| `feature_private_laundry` | 10,097 | 0 | 0 | 0 | 7 | 0 | 0 | 1,798 | 0 | 13,644 |
| `feature_minergie_certified` | 11,526 | 0 | 0 | 0 | 0 | 1 | 0 | 0 | 0 | 14,019 |
| `offer_type` | 1,042 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 24,504 |
| `object_category` | 12,064 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 13,482 |
| `object_type` | 24,749 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 797 |
| `original_url` | 11,105 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 14,441 |
| `floor` | 14,459 | 0 | 0 | 0 | 18 | 201 | 0 | 0 | 3,410 | 7,458 |
| `year_built` | 22,787 | 0 | 0 | 0 | 0 | 7 | 0 | 0 | 0 | 2,752 |
| `status` | 11,105 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 14,441 |
| `agency_name` | 24,993 | 0 | 0 | 0 | 0 | 0 | 553 | 0 | 0 | 0 |
| `agency_phone` | 23,673 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 1,873 | 0 |
| `agency_email` | 24,944 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 602 |

## 5 Cross-Pass Disagreements

_No structured-vs-geocoded canton disagreements._

## 6 Known-Bad Rows

2590 listings had at least one field marked `DROPPED_bad_data` (price < 200, price > 50k, or rooms = 0). Full JSON at `enrichment/data/dropped_rows.json`. Top 10 below.

| listing_id | dropped field(s) | reason(s) |
|---|---|---|
| `10` | price, rooms | price=rooms_zero_non_residential; rooms=rooms_zero_non_residential |
| `10000` | price | price=price_below_200_chf |
| `1001` | price | price=price_below_200_chf |
| `1002` | price, rooms | price=rooms_zero_non_residential; rooms=rooms_zero_non_residential |
| `10037` | price | price=price_below_200_chf |
| `10044` | price | price=price_below_200_chf |
| `10050` | price | price=price_below_200_chf |
| `10055` | price | price=price_below_200_chf |
| `10058` | price | price=price_below_200_chf |
| `1006` | price, rooms | price=rooms_zero_non_residential; rooms=rooms_zero_non_residential |

## 7 Re-validation vs analysis/REPORT.md

_Could not load `analysis/data/stats.json` for cross-validation._

## 8 Commands

```bash
# Full pipeline
docker compose exec api uv run python -m enrichment.scripts.enrich_all --db /data/listings.db --skip-1b
docker compose exec api uv run python -m enrichment.scripts.generate_report --db /data/listings.db

# Tests
docker compose exec api uv run pytest enrichment/tests/ -v
```
