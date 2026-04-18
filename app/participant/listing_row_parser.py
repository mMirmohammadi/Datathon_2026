from __future__ import annotations

import json
from datetime import date
from typing import Any


def _clean_text(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = value.strip()
    if not cleaned or cleaned.upper() == "NULL":
        return None
    return cleaned


def _parse_json_object(value: str | None) -> dict[str, Any]:
    cleaned = _clean_text(value)
    if not cleaned:
        return {}
    try:
        parsed = json.loads(cleaned)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _parse_float(value: str | None) -> float | None:
    cleaned = _clean_text(value)
    if cleaned is None:
        return None
    normalized = cleaned.replace("'", "").replace(",", ".")
    try:
        return float(normalized)
    except ValueError:
        return None


def _parse_int(value: str | None) -> int | None:
    number = _parse_float(value)
    if number is None:
        return None
    return int(round(number))


def _parse_bool(value: str | None) -> bool | None:
    cleaned = _clean_text(value)
    if cleaned is None:
        return None

    normalized = cleaned.lower()
    if normalized in {"true", "1", "yes", "y"}:
        return True
    if normalized in {"false", "0", "no", "n"}:
        return False

    return True if cleaned else None


def _is_truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value > 0
    if isinstance(value, str):
        normalized = value.strip().lower()
        return normalized in {"true", "1", "yes", "y", "ja"} or normalized.isdigit() and int(normalized) > 0
    return False


def _merge_optional_bools(*values: bool | None) -> bool | None:
    saw_false = False
    for value in values:
        if value is True:
            return True
        if value is False:
            saw_false = True
    return False if saw_false else None


def _feature_list_flag(
    feature_keys: set[str],
    *,
    list_present: bool,
    keys: tuple[str, ...],
) -> bool | None:
    if any(key in feature_keys for key in keys):
        return True
    return False if list_present else None


def _main_data_flag(
    main_data_values: dict[str, Any],
    *,
    list_present: bool,
    keys: tuple[str, ...],
) -> bool | None:
    if not list_present:
        return None

    for key in keys:
        if key not in main_data_values:
            continue
        return _is_truthy(main_data_values.get(key))

    return False


def _parse_date(value: str | None) -> str | None:
    cleaned = _clean_text(value)
    if cleaned is None:
        return None

    for separator in ("-", "."):
        parts = cleaned.split(separator)
        if len(parts) != 3:
            continue
        try:
            if separator == "-":
                return date.fromisoformat(cleaned).isoformat()
            day, month, year = (int(part) for part in parts)
            return date(year, month, day).isoformat()
        except ValueError:
            continue

    return None


def _derive_price(row: dict[str, str]) -> int | None:
    for key in ("rent_gross", "price"):
        parsed = _parse_int(row.get(key))
        if parsed is not None and parsed > 0:
            return parsed

    rent_net = _parse_int(row.get("rent_net"))
    rent_extra = _parse_int(row.get("rent_extra")) or 0
    if rent_net is not None and rent_net > 0:
        return rent_net + rent_extra

    return None


def _derive_features(
    row: dict[str, str],
    orig_data: dict[str, Any],
) -> tuple[dict[str, bool | None], list[str]]:
    features_source = orig_data.get("Features")
    feature_list_present = isinstance(features_source, list)
    feature_keys = {
        item.get("Key")
        for item in (features_source or [])
        if isinstance(item, dict) and _is_truthy(item.get("Value", True))
    }
    main_data_source = orig_data.get("MainData")
    main_data_present = isinstance(main_data_source, list)
    main_data_values = {
        item.get("Key"): item.get("Value")
        for item in (main_data_source or [])
        if isinstance(item, dict) and item.get("Key")
    }

    feature_values = {
        "balcony": _merge_optional_bools(
            _parse_bool(row.get("prop_balcony")),
            _feature_list_flag(
                feature_keys,
                list_present=feature_list_present,
                keys=("HasBalconies", "HasTerraces"),
            ),
            _main_data_flag(
                main_data_values,
                list_present=main_data_present,
                keys=("NumBalconies", "NumTerraces"),
            ),
        ),
        "elevator": _merge_optional_bools(
            _parse_bool(row.get("prop_elevator")),
            _feature_list_flag(
                feature_keys,
                list_present=feature_list_present,
                keys=("HasLift",),
            ),
        ),
        "parking": _merge_optional_bools(
            _parse_bool(row.get("prop_parking")),
            _feature_list_flag(
                feature_keys,
                list_present=feature_list_present,
                keys=("HasParkingOutdoor", "HasParkingIndoor"),
            ),
        ),
        "garage": _merge_optional_bools(
            _parse_bool(row.get("prop_garage")),
            _feature_list_flag(
                feature_keys,
                list_present=feature_list_present,
                keys=("HasParkingIndoor",),
            ),
        ),
        "fireplace": _merge_optional_bools(
            _parse_bool(row.get("prop_fireplace")),
            _feature_list_flag(
                feature_keys,
                list_present=feature_list_present,
                keys=("HasFireplace",),
            ),
        ),
        "child_friendly": _parse_bool(row.get("prop_child_friendly")),
        "pets_allowed": _merge_optional_bools(
            _parse_bool(row.get("animal_allowed")),
            _main_data_flag(
                main_data_values,
                list_present=main_data_present,
                keys=("PetsAllowed",),
            ),
        ),
        "temporary": _parse_bool(row.get("maybe_temporary")),
        "new_build": _merge_optional_bools(
            _parse_bool(row.get("is_new_building")),
            _main_data_flag(
                main_data_values,
                list_present=main_data_present,
                keys=("IsNewBuilding",),
            ),
        ),
        "wheelchair_accessible": _main_data_flag(
            main_data_values,
            list_present=main_data_present,
            keys=("IsWheelchairAccessible",),
        ),
        "private_laundry": _feature_list_flag(
            feature_keys,
            list_present=feature_list_present,
            keys=("HasWashingmachine", "HasDryer"),
        ),
        "minergie_certified": _main_data_flag(
            main_data_values,
            list_present=main_data_present,
            keys=("IsMinergieCertified",),
        ),
    }
    enabled_features = [
        feature_name
        for feature_name, value in feature_values.items()
        if value is True
    ]
    return feature_values, enabled_features


def prepare_listing_row(row: dict[str, str]) -> tuple[Any, ...]:
    location = _parse_json_object(row.get("location_address"))
    city = _clean_text(row.get("object_city")) or _clean_text(location.get("City"))
    postal_code = _clean_text(row.get("object_zip")) or _clean_text(location.get("PostalCode"))
    canton = _clean_text(row.get("object_state")) or _clean_text(location.get("canton"))
    canton = canton.upper() if canton else None
    title = _clean_text(row.get("title")) or "Untitled listing"
    description = _clean_text(row.get("object_description")) or _clean_text(row.get("remarks"))
    offer_type = _clean_text(row.get("offer_type"))
    offer_type = offer_type.upper() if offer_type else None
    orig_data = _parse_json_object(row.get("orig_data"))
    feature_values, enabled_features = _derive_features(row, orig_data)
    images = _parse_json_object(row.get("images"))
    location_address = _parse_json_object(row.get("location_address"))
    street = _clean_text(row.get("object_street"))
    if street is None:
        street_name = _clean_text(location.get("Street"))
        street_number = _clean_text(location.get("StreetNumber"))
        if street_name and street_number:
            street = f"{street_name} {street_number}"
        else:
            street = street_name

    return (
        str(row.get("id", "")).strip(),
        _clean_text(row.get("platform_id")),
        _clean_text(row.get("scrape_source")),
        title,
        description,
        street,
        city,
        postal_code,
        canton,
        _derive_price(row),
        _parse_float(row.get("number_of_rooms")),
        _parse_float(row.get("area")),
        _parse_date(row.get("available_from")),
        _parse_float(row.get("geo_lat")),
        _parse_float(row.get("geo_lng")),
        _parse_int(row.get("distance_public_transport")),
        _parse_int(row.get("distance_shop")),
        _parse_int(row.get("distance_kindergarten")),
        _parse_int(row.get("distance_school_1")),
        _parse_int(row.get("distance_school_2")),
        1 if feature_values["balcony"] is True else 0 if feature_values["balcony"] is False else None,
        1 if feature_values["elevator"] is True else 0 if feature_values["elevator"] is False else None,
        1 if feature_values["parking"] is True else 0 if feature_values["parking"] is False else None,
        1 if feature_values["garage"] is True else 0 if feature_values["garage"] is False else None,
        1 if feature_values["fireplace"] is True else 0 if feature_values["fireplace"] is False else None,
        1 if feature_values["child_friendly"] is True else 0 if feature_values["child_friendly"] is False else None,
        1 if feature_values["pets_allowed"] is True else 0 if feature_values["pets_allowed"] is False else None,
        1 if feature_values["temporary"] is True else 0 if feature_values["temporary"] is False else None,
        1 if feature_values["new_build"] is True else 0 if feature_values["new_build"] is False else None,
        1 if feature_values["wheelchair_accessible"] is True else 0 if feature_values["wheelchair_accessible"] is False else None,
        1 if feature_values["private_laundry"] is True else 0 if feature_values["private_laundry"] is False else None,
        1 if feature_values["minergie_certified"] is True else 0 if feature_values["minergie_certified"] is False else None,
        json.dumps(enabled_features, ensure_ascii=True),
        offer_type,
        _clean_text(row.get("object_category")),
        _clean_text(row.get("object_type")),
        _clean_text(row.get("platform_url")),
        json.dumps(images, ensure_ascii=True),
        json.dumps(location_address, ensure_ascii=True),
        json.dumps(orig_data, ensure_ascii=True),
        json.dumps(row, ensure_ascii=True),
    )


def _prepare_listing_row(row: dict[str, str]) -> tuple[Any, ...]:
    return prepare_listing_row(row)
