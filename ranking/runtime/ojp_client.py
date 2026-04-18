"""T4.1 — Swiss OJP 2.0 client for on-demand door-to-door travel time.

Called at query-time on the top-20 listings when the user's query mentions a
landmark (e.g. "30 min to ETH Hönggerberg"). Replaces the wrong haversine ×
rail-speed proxy with an actual OJP trip computation.

Policy (opentransportdata.swiss):
  * Free tier: 50 req/min and 20k/day per API key.
  * Paid tier: 500 CHF/mo → 2,500 req/min, 1M/day.
  * We enforce our own client-side rate limit so a 20-concurrent burst can't
    blow through the quota in milliseconds and earn us a 429.

Design:
  * `OjpClient(api_key, rate_per_minute=50)` — instantiate once per process.
  * `travel_minutes(listing_lat, listing_lng, dest_stop_place_ref, dep_time)` —
    returns int | None. None = any failure path (loudly logged).
  * `batch_travel_minutes(listings, dest_stop_place_ref)` — runs `CONCURRENCY`
    requests in parallel subject to the rate limit.
  * Resilience: one retry on 429 / 5xx with exp backoff; otherwise skip-and-
    log. NEVER fabricates a number.

Usage at rerank time:
    client = OjpClient.from_env()
    # user query: "under 30 min to ETH Hönggerberg"
    dest = resolve_landmark("ETH Hönggerberg")   # from data/ranking/landmarks.json
    travel_min = client.travel_minutes(lat=47.37, lng=8.54, dest_name="Zürich, ETH Hönggerberg")
"""
from __future__ import annotations

import os
import re
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx
from dotenv import load_dotenv

load_dotenv()

OJP_BASE_URL = os.getenv("OJP_BASE_URL", "https://api.opentransportdata.swiss/ojp20")
REQUESTOR_REF = os.getenv("OJP_REQUESTOR_REF", "datathon2026-robin-ranker")
DEFAULT_RATE_PER_MIN = int(os.getenv("OJP_RATE_PER_MIN", "50"))
DEFAULT_TIMEOUT_S = float(os.getenv("OJP_TIMEOUT_S", "15"))

_OJP_XMLNS = "http://www.vdv.de/ojp"
_SIRI_XMLNS = "http://www.siri.org.uk/siri"


@dataclass(slots=True, frozen=True)
class TravelResult:
    listing_id: str
    dest_query: str
    travel_min: int | None
    transfers: int | None
    departure: str | None
    arrival: str | None
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "listing_id": self.listing_id,
            "dest_query":  self.dest_query,
            "travel_min":  self.travel_min,
            "transfers":   self.transfers,
            "departure":   self.departure,
            "arrival":     self.arrival,
            "error":       self.error,
        }


class _TokenBucket:
    """Simple token-bucket rate limiter, thread-safe."""

    def __init__(self, tokens_per_minute: int):
        self._capacity = max(1, tokens_per_minute)
        self._tokens = float(self._capacity)
        self._refill_per_sec = self._capacity / 60.0
        self._last_refill = time.monotonic()
        self._lock = threading.Lock()

    def acquire(self) -> None:
        """Block until one token is available. Never raises."""
        while True:
            with self._lock:
                now = time.monotonic()
                elapsed = now - self._last_refill
                self._tokens = min(self._capacity, self._tokens + elapsed * self._refill_per_sec)
                self._last_refill = now
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
                deficit = 1.0 - self._tokens
                wait_s = deficit / self._refill_per_sec
            time.sleep(max(0.01, wait_s))


def _build_ojp_xml(
    *,
    lat: float,
    lng: float,
    dest_name: str,
    dest_place_ref: str | None,
    departure_time_iso: str,
) -> str:
    """Build an OJPTripRequest 2.0 XML body matching the OTD cookbook schema.

    OJP 2.0 changes vs older versions:
      * `<Name><Text>…</Text></Name>` (not `<LocationName>`)
      * `<siri:StopPointRef>` (not `<siri:StopPlaceRef>`)
      * `<DepArrTime>` belongs inside `<Origin>`

    Origin is a GeoPosition (the listing's lat/lng). Destination is either a
    named StopPointRef (SLOID or Didok — OJP accepts both) or a free-text
    name that OJP resolves internally.
    """
    dest_block = (
        f"<siri:StopPointRef>{dest_place_ref}</siri:StopPointRef>"
        if dest_place_ref else ""
    )
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<OJP xmlns="{_OJP_XMLNS}" xmlns:siri="{_SIRI_XMLNS}" version="2.0">
  <OJPRequest>
    <siri:ServiceRequest>
      <siri:RequestTimestamp>{now_iso}</siri:RequestTimestamp>
      <siri:RequestorRef>{REQUESTOR_REF}</siri:RequestorRef>
      <OJPTripRequest>
        <siri:RequestTimestamp>{now_iso}</siri:RequestTimestamp>
        <Origin>
          <PlaceRef>
            <GeoPosition>
              <siri:Longitude>{lng}</siri:Longitude>
              <siri:Latitude>{lat}</siri:Latitude>
            </GeoPosition>
            <Name><Text>listing</Text></Name>
          </PlaceRef>
          <DepArrTime>{departure_time_iso}</DepArrTime>
        </Origin>
        <Destination>
          <PlaceRef>
            {dest_block}
            <Name><Text>{_xml_escape(dest_name)}</Text></Name>
          </PlaceRef>
        </Destination>
        <Params>
          <NumberOfResults>1</NumberOfResults>
          <IncludeTrackSections>false</IncludeTrackSections>
          <IncludeLegProjection>false</IncludeLegProjection>
          <IncludeIntermediateStops>false</IncludeIntermediateStops>
        </Params>
      </OJPTripRequest>
    </siri:ServiceRequest>
  </OJPRequest>
</OJP>""".strip()


def _xml_escape(s: str) -> str:
    return (s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
              .replace('"', "&quot;").replace("'", "&apos;"))


_DURATION_RE = re.compile(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?")


def _iso_duration_to_minutes(iso: str) -> int | None:
    m = _DURATION_RE.fullmatch(iso.strip())
    if m is None:
        return None
    h, mn, s = (int(x) if x else 0 for x in m.groups())
    return h * 60 + mn + (1 if s >= 30 else 0)


def _parse_trip_response(xml: bytes) -> tuple[int | None, int | None, str | None, str | None]:
    """Return (duration_min, transfers, departure_iso, arrival_iso) or Nones."""
    # OJP responses are large XML. For our single-result trip we just need
    # the <OJPTripDelivery><TripResult><Trip><Duration/></Trip>... subtree.
    # A full XML parse via ElementTree is fine at ~50 kB per response.
    import xml.etree.ElementTree as ET

    try:
        root = ET.fromstring(xml)
    except ET.ParseError:
        return None, None, None, None

    ns = {"ojp": _OJP_XMLNS, "siri": _SIRI_XMLNS}
    # Duration: <Trip><Duration>PT37M</Duration>...
    dur = root.find(".//ojp:TripResult/ojp:Trip/ojp:Duration", ns)
    duration_min = _iso_duration_to_minutes(dur.text) if dur is not None and dur.text else None

    transfers_el = root.find(".//ojp:TripResult/ojp:Trip/ojp:Transfers", ns)
    transfers = int(transfers_el.text) if transfers_el is not None and transfers_el.text else None

    start = root.find(".//ojp:TripResult/ojp:Trip/ojp:StartTime", ns)
    end   = root.find(".//ojp:TripResult/ojp:Trip/ojp:EndTime", ns)
    departure = start.text if start is not None else None
    arrival   = end.text if end is not None else None
    return duration_min, transfers, departure, arrival


class OjpClient:
    """Thread-safe OJP 2.0 client with a process-wide rate limiter."""

    def __init__(
        self,
        api_key: str,
        *,
        base_url: str = OJP_BASE_URL,
        rate_per_minute: int = DEFAULT_RATE_PER_MIN,
        timeout_s: float = DEFAULT_TIMEOUT_S,
    ):
        if not api_key:
            raise RuntimeError(
                "OjpClient: api_key required. Set OJP_API_KEY in .env after "
                "registering at https://api-manager.opentransportdata.swiss/"
            )
        self._api_key = api_key
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout_s
        self._bucket = _TokenBucket(rate_per_minute)
        self._client = httpx.Client(
            timeout=timeout_s,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type":  "application/xml",
                "Accept":         "application/xml",
            },
        )

    @classmethod
    def from_env(cls) -> "OjpClient":
        key = os.getenv("OJP_API_KEY", "").strip()
        return cls(api_key=key)

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "OjpClient":
        return self

    def __exit__(self, *a) -> None:
        self.close()

    def travel_minutes(
        self,
        *,
        listing_id: str,
        lat: float,
        lng: float,
        dest_name: str,
        dest_place_ref: str | None = None,
        departure_time: datetime | None = None,
    ) -> TravelResult:
        """Return door-to-door travel time in minutes. None on any failure (loud)."""
        dep = (departure_time or (datetime.now() + timedelta(minutes=5))).replace(microsecond=0)
        dep_iso = dep.isoformat()
        xml = _build_ojp_xml(
            lat=lat, lng=lng,
            dest_name=dest_name, dest_place_ref=dest_place_ref,
            departure_time_iso=dep_iso,
        )

        for attempt in range(2):
            self._bucket.acquire()
            try:
                resp = self._client.post(self._base_url, content=xml.encode("utf-8"))
            except httpx.HTTPError as exc:
                print(
                    f"[WARN] ojp_client.travel_minutes: expected=response, "
                    f"got={type(exc).__name__}: {exc!s} (attempt {attempt + 1}/2), "
                    f"listing_id={listing_id} dest={dest_name!r}",
                    flush=True,
                )
                time.sleep(2 * (attempt + 1))
                continue
            if resp.status_code == 200:
                dur_min, transfers, depart, arrive = _parse_trip_response(resp.content)
                if dur_min is None:
                    print(
                        f"[WARN] ojp_client.travel_minutes: expected=duration in response, "
                        f"got=no Duration element, fallback=None "
                        f"listing_id={listing_id} dest={dest_name!r}",
                        flush=True,
                    )
                    return TravelResult(listing_id, dest_name, None, None, None, None,
                                        error="no Duration element in OJP response")
                return TravelResult(listing_id, dest_name, dur_min, transfers, depart, arrive)
            if resp.status_code in {429, 500, 502, 503, 504}:
                print(
                    f"[WARN] ojp_client.travel_minutes: expected=200, "
                    f"got={resp.status_code} (attempt {attempt + 1}/2), "
                    f"listing_id={listing_id} dest={dest_name!r}, fallback=retry-in-{2*(attempt+1)}s",
                    flush=True,
                )
                time.sleep(2 * (attempt + 1))
                continue
            # 4xx (non-429) — no retry
            print(
                f"[WARN] ojp_client.travel_minutes: expected=200, got={resp.status_code}, "
                f"body={resp.text[:200]!r}, fallback=None "
                f"listing_id={listing_id} dest={dest_name!r}",
                flush=True,
            )
            return TravelResult(listing_id, dest_name, None, None, None, None,
                                error=f"HTTP {resp.status_code}")
        return TravelResult(listing_id, dest_name, None, None, None, None,
                            error="exhausted retries")

    def batch_travel_minutes(
        self,
        *,
        items: list[dict[str, Any]],     # each: {listing_id, lat, lng}
        dest_name: str,
        dest_place_ref: str | None = None,
        concurrency: int = 8,
        departure_time: datetime | None = None,
    ) -> list[TravelResult]:
        """Parallel fetch for the top-N listings. Blocks on our own rate limit."""
        import concurrent.futures as cf

        results: list[TravelResult] = []
        with cf.ThreadPoolExecutor(max_workers=max(1, concurrency)) as ex:
            futs = [
                ex.submit(
                    self.travel_minutes,
                    listing_id=i["listing_id"],
                    lat=float(i["lat"]),
                    lng=float(i["lng"]),
                    dest_name=dest_name,
                    dest_place_ref=dest_place_ref,
                    departure_time=departure_time,
                )
                for i in items
            ]
            for f in cf.as_completed(futs):
                results.append(f.result())
        return results
