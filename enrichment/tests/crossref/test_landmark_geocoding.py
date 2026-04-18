"""Crossref test — reverse-geocode 26 hand-labeled Swiss landmarks and assert
the resulting canton code matches the geographic ground truth.

This is the user-emphasized accuracy gate for pass 1a. Labels live at
`enrichment/tests/crossref/fixtures/landmark_truths.yaml`, collected from
geographic ground truth (Wikipedia canton membership), NOT from rg's own
output — so a regression in rg's admin1 labels or in our canton map will
fail this test.

Gate: 26/26 match. Single miss fails the suite.
"""
from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from enrichment.common.cantons import admin1_to_canton_code

FIXTURE = Path(__file__).with_suffix("").parent / "fixtures" / "landmark_truths.yaml"


def _load_landmarks() -> list[dict]:
    with FIXTURE.open() as f:
        data = yaml.safe_load(f)
    assert isinstance(data, list) and data, "landmark_truths.yaml must be a non-empty list"
    return data


LANDMARKS = _load_landmarks()


def test_fixture_covers_all_26_cantons():
    present = {lm["canton"] for lm in LANDMARKS}
    assert len(present) == 26, f"fixture missing cantons: {sorted({'ZH','BE','LU','UR','SZ','OW','NW','GL','ZG','FR','SO','BS','BL','SH','AR','AI','SG','GR','AG','TG','TI','VD','VS','NE','GE','JU'} - present)}"


@pytest.mark.parametrize(
    "landmark",
    LANDMARKS,
    ids=[lm["name"] for lm in LANDMARKS],
)
def test_reverse_geocode_returns_expected_canton(landmark: dict):
    import reverse_geocoder as rg

    result = rg.search([(landmark["lat"], landmark["lng"])], mode=2)[0]
    admin1 = result.get("admin1", "")
    cc = result.get("cc", "")

    assert cc == "CH", (
        f"{landmark['name']} at ({landmark['lat']}, {landmark['lng']}) "
        f"resolved to cc={cc!r} (expected CH). "
        f"Check coord accuracy or rg data drift."
    )

    actual_canton = admin1_to_canton_code(admin1)
    assert actual_canton == landmark["canton"], (
        f"{landmark['name']}: rg admin1={admin1!r} -> canton={actual_canton!r}, "
        f"expected {landmark['canton']!r}. "
        f"Either the admin1 string changed in rg (update common/cantons.py), "
        f"or the fixture coord is wrong."
    )


def test_all_26_landmarks_batched_together():
    """Same as the parametrized test but batched — mirrors how pass 1 actually calls rg."""
    import reverse_geocoder as rg

    coords = [(lm["lat"], lm["lng"]) for lm in LANDMARKS]
    results = rg.search(coords, mode=2)

    mismatches = []
    for lm, r in zip(LANDMARKS, results, strict=True):
        got = admin1_to_canton_code(r.get("admin1", ""))
        if got != lm["canton"]:
            mismatches.append((lm["name"], lm["canton"], got, r.get("admin1", "")))
    assert not mismatches, f"{len(mismatches)}/{len(LANDMARKS)} landmark mismatches: {mismatches}"
