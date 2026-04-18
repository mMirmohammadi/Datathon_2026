"""Unit tests for ranking/runtime/ojp_client.py — pure logic, no network."""
from __future__ import annotations

import time

import pytest

from ranking.runtime.ojp_client import (
    _TokenBucket,
    _build_ojp_xml,
    _iso_duration_to_minutes,
    _parse_trip_response,
    _xml_escape,
)


# ---------- duration parsing ----------------------------------------------


@pytest.mark.parametrize("iso, expected", [
    ("PT37M",    37),
    ("PT1H",     60),
    ("PT1H30M",  90),
    ("PT1H30M29S", 90),         # 29 s rounds down
    ("PT1H30M30S", 91),         # 30 s rounds up
    ("PT0M",     0),
    ("PT2H15M45S", 135 + 1),    # round up
])
def test_iso_duration_valid(iso, expected):
    assert _iso_duration_to_minutes(iso) == expected


@pytest.mark.parametrize("iso", ["", "37M", "P1D", "not-a-duration", "PT"])
def test_iso_duration_malformed(iso):
    # PT alone would be a full-zero match; reject it to be safe
    result = _iso_duration_to_minutes(iso)
    assert result in (None, 0)


# ---------- xml escape -----------------------------------------------------


def test_xml_escape_special_chars():
    assert _xml_escape("A&B") == "A&amp;B"
    assert _xml_escape("<tag>") == "&lt;tag&gt;"
    assert _xml_escape('he said "hi"') == "he said &quot;hi&quot;"
    assert _xml_escape("it's") == "it&apos;s"


# ---------- xml builder ----------------------------------------------------


def test_build_ojp_xml_contains_coords_and_dest():
    xml = _build_ojp_xml(
        lat=47.37, lng=8.54,
        dest_name="Zürich HB",
        dest_place_ref="8503000",
        departure_time_iso="2026-04-18T08:00:00",
    )
    assert "47.37" in xml
    assert "8.54" in xml
    assert "Zürich HB" in xml
    assert "8503000" in xml
    assert "<OJPTripRequest>" in xml
    assert "NumberOfResults>1<" in xml
    assert "<siri:Longitude>8.54" in xml
    assert "<siri:Latitude>47.37" in xml


def test_build_ojp_xml_no_place_ref_omits_element():
    xml = _build_ojp_xml(
        lat=47.37, lng=8.54,
        dest_name="ETH Hönggerberg",
        dest_place_ref=None,
        departure_time_iso="2026-04-18T08:00:00",
    )
    assert "StopPlaceRef" not in xml
    assert "ETH Hönggerberg" in xml


def test_build_ojp_xml_dest_name_is_escaped():
    xml = _build_ojp_xml(
        lat=47.37, lng=8.54,
        dest_name='Bahnhof "Hölder & Söhne"',
        dest_place_ref=None,
        departure_time_iso="2026-04-18T08:00:00",
    )
    assert "&quot;" in xml
    assert "&amp;" in xml


# ---------- response parser ------------------------------------------------


_SAMPLE_OK_RESPONSE = """<?xml version="1.0"?>
<OJP xmlns="http://www.vdv.de/ojp" xmlns:siri="http://www.siri.org.uk/siri" version="2.0">
  <OJPResponse>
    <siri:ServiceDelivery>
      <OJPTripDelivery>
        <TripResult>
          <Trip>
            <TripId>T1</TripId>
            <Duration>PT37M</Duration>
            <StartTime>2026-04-18T08:05:00Z</StartTime>
            <EndTime>2026-04-18T08:42:00Z</EndTime>
            <Transfers>1</Transfers>
          </Trip>
        </TripResult>
      </OJPTripDelivery>
    </siri:ServiceDelivery>
  </OJPResponse>
</OJP>
""".encode("utf-8")


def test_parse_trip_response_happy():
    dur, xf, dep, arr = _parse_trip_response(_SAMPLE_OK_RESPONSE)
    assert dur == 37
    assert xf == 1
    assert dep == "2026-04-18T08:05:00Z"
    assert arr == "2026-04-18T08:42:00Z"


def test_parse_trip_response_malformed_returns_nones():
    dur, xf, dep, arr = _parse_trip_response(b"<not-xml-at-all>")
    assert dur is None and xf is None and dep is None and arr is None


def test_parse_trip_response_missing_duration_is_loud():
    xml = b"""<OJP xmlns="http://www.vdv.de/ojp"><TripResult><Trip><TripId>X</TripId></Trip></TripResult></OJP>"""
    dur, xf, dep, arr = _parse_trip_response(xml)
    assert dur is None
    # Transfers/dep/arr also None when absent — no fabrication
    assert xf is None and dep is None and arr is None


# ---------- rate limiter ---------------------------------------------------


def test_token_bucket_allows_initial_burst():
    b = _TokenBucket(tokens_per_minute=60)   # 1/sec
    t0 = time.monotonic()
    for _ in range(5):
        b.acquire()
    # Initial 5 should all fit in the bucket capacity; no real wait
    assert time.monotonic() - t0 < 0.1


def test_token_bucket_throttles_after_burst():
    """After exhausting the capacity, subsequent acquires wait ≈ refill period."""
    b = _TokenBucket(tokens_per_minute=60)   # 1 token/sec refill
    # Exhaust the bucket
    for _ in range(60):
        b.acquire()
    t0 = time.monotonic()
    b.acquire()        # forces a refill wait
    elapsed = time.monotonic() - t0
    assert 0.5 <= elapsed <= 1.5, f"expected ~1s wait, got {elapsed}s"


def test_token_bucket_invalid_rate_still_works():
    b = _TokenBucket(tokens_per_minute=0)    # degenerate, should clamp to 1
    t0 = time.monotonic()
    b.acquire()
    # 1 token initially → no wait
    assert time.monotonic() - t0 < 0.1
