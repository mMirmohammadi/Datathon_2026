// Demo frontend for the Datathon 2026 listings harness.
// Calls POST /listings and renders the extracted query plan + ranking breakdown.
// No silent fallbacks: every missing/unexpected field is shown as such.

const els = {
  form: document.getElementById("search-form"),
  query: document.getElementById("query"),
  limit: document.getElementById("limit"),
  status: document.getElementById("status"),
  metaPanel: document.getElementById("meta-panel"),
  rawQuery: document.getElementById("raw-query"),
  hardView: document.getElementById("hard-filters-view"),
  softView: document.getElementById("soft-prefs-view"),
  pipelineView: document.getElementById("pipeline-view"),
  rawJson: document.getElementById("raw-json"),
  listings: document.getElementById("listings"),
  resultStatus: document.getElementById("result-status"),
  examples: document.querySelectorAll(".chip"),
  memIndicator: document.getElementById("mem-indicator"),
  authAnon: document.getElementById("auth-anon"),
  authUser: document.getElementById("auth-user"),
  authUsername: document.getElementById("auth-username"),
  authModal: document.getElementById("auth-modal"),
  authModalTitle: document.getElementById("auth-modal-title"),
  authModalForm: document.getElementById("auth-modal-form"),
  authTabs: document.getElementById("auth-tabs"),
  authError: document.getElementById("auth-error"),
  authSubmit: document.getElementById("auth-submit"),
  logoutBtn: document.getElementById("logout-btn"),
  personalizeToggle: document.getElementById("personalize-toggle"),
  favoritesBtn: document.getElementById("favorites-btn"),
  favoritesCount: document.getElementById("favorites-count"),
  favoritesModal: document.getElementById("favorites-modal"),
  favoritesList: document.getElementById("favorites-list"),
  clearHistoryBtn: document.getElementById("clear-history-btn"),
  deleteAccountBtn: document.getElementById("delete-account-btn"),
  detailModal: document.getElementById("listing-detail-modal"),
  detailBody: document.getElementById("listing-detail-body"),
  detailTitle: document.getElementById("listing-detail-title"),
  similarModal: document.getElementById("similar-modal"),
  similarBody: document.getElementById("similar-modal-body"),
  similarTitle: document.getElementById("similar-modal-title"),
  tasteBtn: document.getElementById("taste-btn"),
  tasteModal: document.getElementById("taste-modal"),
  tasteBody: document.getElementById("taste-body"),
  searchImageInput: document.getElementById("search-image-input"),
  searchImageChipClear: document.getElementById("search-image-chip-clear"),
};

// ---------- auth + interaction client state ---------------------------------
// Single source of truth for the UI: who we are + which listing_ids we've
// liked / bookmarked. Re-hydrated on page load via /auth/me + /me/likes +
// /me/favorites.
//
// Semantics split:
//   * like / unlike   → preference signal; feeds memory; drives ranking.
//   * bookmark / unbookmark → pure UX; populates the "Saved listings" drawer;
//                             does NOT change the ranker.
const authState = {
  user: null,                     // {id, username, email, ...} or null
  csrfToken: null,                // double-submit token (also in cookie)
  likedIds: new Set(),            // listing_ids with an outstanding "like"
  bookmarkedIds: new Set(),       // listing_ids with an outstanding "bookmark"
  dismissedIds: new Set(),        // sticky per-session demotions
};

// Tri-state for the DINOv2 "look-alike homes" feature:
//   null  = unknown (no probe yet, render the button optimistically)
//   true  = confirmed available (at least one 2xx seen)
//   false = confirmed off on this server (any 503 seen) — hide every button
// We discover this lazily from the first /similar response instead of a
// dedicated health endpoint, so there's no extra startup round-trip.
let similarFeatureAvailable = null;

function hideAllSimilarButtons() {
  document.querySelectorAll(".find-similar-btn").forEach((btn) => {
    btn.remove();
  });
}

const FEATURE_KEYS_HARD = new Set([
  "balcony",
  "elevator",
  "parking",
  "garage",
  "fireplace",
  "child_friendly",
  "pets_allowed",
  "temporary",
  "new_build",
  "wheelchair_accessible",
  "private_laundry",
  "minergie_certified",
]);

// ---------- utilities --------------------------------------------------------

function chf(n) {
  if (n == null) return "—";
  return new Intl.NumberFormat("de-CH", {
    style: "currency",
    currency: "CHF",
    maximumFractionDigits: 0,
  }).format(n);
}

function esc(s) {
  if (s == null) return "";
  return String(s).replace(/[&<>"']/g, (c) => ({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    '"': "&quot;",
    "'": "&#39;",
  }[c]));
}

// ---------- address + Google-Maps helpers (production-safety-critical) ------
//
// Every apartment in Switzerland has *some* subset of {street, house_number,
// postal_code, city, canton, latitude, longitude}. Real coverage in our DB:
// street ≈ 47 %, house_number ≈ 40 %, postal_code ≈ 56 %, latitude/longitude
// ≈ 94 %. These helpers produce a display string and a deep-link URL that
// degrade gracefully against any of those fields being null/empty.
//
// Security: user-sourced strings (street, city, etc.) flow through two
// escape layers before hitting the DOM — `esc()` for HTML context, and
// `encodeURIComponent()` for URL context. Never splice raw values into an
// href attribute. Always use rel="noopener noreferrer" + target="_blank"
// when opening an external link (reverse-tabnabbing mitigation).

// Finite-coord sanity: reject NaN/Infinity, the null island (0,0), and
// anything outside the Switzerland bounding box with a small margin. Outside
// that, the coordinate is almost certainly wrong and we should prefer the
// address string so the user doesn't land on open ocean.
function _hasValidCoords(L) {
  if (!L) return false;
  const lat = L.latitude;
  const lng = L.longitude;
  if (typeof lat !== "number" || typeof lng !== "number") return false;
  if (!Number.isFinite(lat) || !Number.isFinite(lng)) return false;
  if (lat === 0 && lng === 0) return false;
  if (lat < 45 || lat > 48) return false;
  if (lng < 5 || lng > 11) return false;
  return true;
}

// Build the display address string. Empty when nothing useful — callers
// should check truthiness before rendering a row.
//
// Two data-shape quirks we defend against:
//   1. In every row of our DB where both ``street`` and ``house_number`` are
//      present, the street field ALREADY ends with the number
//      (e.g. street="Herisauerstrasse 15", house_number="15"). Blindly
//      concatenating duplicates the number. Detect + skip the dupe.
//   2. A lone ``house_number`` without a street name is meaningless to a
//      reader ("5, Bern" is worse than just "Bern"), so only include the
//      number when a street is present.
function formatAddress(L) {
  if (!L) return "";
  const street = L.street == null ? "" : String(L.street).trim();
  const number = L.house_number == null ? "" : String(L.house_number).trim();
  let line1 = "";
  if (street) {
    // Use word-boundary check: matches "Strasse 15" but not "15 Strasse 15".
    const alreadySuffixed =
      number && new RegExp(`(^|\\s)${number.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}$`).test(street);
    line1 = alreadySuffixed ? street : [street, number].filter(Boolean).join(" ");
  }
  const line2 = [L.postal_code, L.city]
    .map((s) => (s == null ? "" : String(s).trim()))
    .filter(Boolean)
    .join(" ");
  const canton = L.canton ? String(L.canton).trim() : "";
  return [line1, line2, canton].filter(Boolean).join(", ");
}

// Google Maps deep link, or null when nothing is linkable. Coordinates
// preferred (93.6 % coverage, exact, multilingual-proof). Address fallback
// includes "Switzerland" so the geocoder doesn't resolve to e.g. a
// Bahnhofstrasse in Germany.
function googleMapsUrl(L) {
  if (!L) return null;
  if (_hasValidCoords(L)) {
    const lat = L.latitude.toFixed(6);
    const lng = L.longitude.toFixed(6);
    return `https://www.google.com/maps/search/?api=1&query=${lat},${lng}`;
  }
  const parts = [L.street, L.house_number, L.postal_code, L.city, L.canton, "Switzerland"]
    .map((s) => (s == null ? "" : String(s).trim()))
    .filter(Boolean);
  // "Switzerland" alone is not a listing; refuse a URL that would just drop
  // the user on the country border.
  if (parts.length <= 1) return null;
  return `https://www.google.com/maps/search/?api=1&query=${encodeURIComponent(parts.join(" "))}`;
}

// Render the address block (text + optional Maps pill). Empty string when
// we have neither a formatted address nor a URL.
function renderAddressBlock(L, { cls = "listing-address" } = {}) {
  const addr = formatAddress(L);
  const url = googleMapsUrl(L);
  if (!addr && !url) return "";
  const addrPart = addr
    ? `<span class="addr-text" title="${esc(addr)}">📫 ${esc(addr)}</span>`
    : "";
  const mapsPart = url
    ? `<a class="maps-link" href="${esc(url)}" target="_blank" rel="noopener noreferrer" title="Open in Google Maps">📍 Google Maps</a>`
    : "";
  return `<div class="${esc(cls)}">${addrPart}${mapsPart}</div>`;
}

// ---------- Nearby landmarks + Google Maps directions ---------------------
//
// Every listing carries a ``nearby_landmarks`` list (top-K closest of 45
// curated Swiss landmarks). Each chip is a deep-link into Google Maps
// Directions from the listing's coords → landmark coords, so the user sees
// not just "ETH Zentrum is 700 m away" but "14 min by transit, here's the
// route".
//
// Security: origin + destination coords go through `.toFixed(6)` (float →
// string, no URL metachars possible) before being spliced into the query
// string. ``travelmode`` is hardcoded to `transit`; no user-controlled
// enum. Labels are `esc()`-escaped before hitting the DOM.

// Build a Google Maps directions URL: origin → destination, transit mode.
// Returns null when either endpoint is not a valid coord pair; caller
// suppresses the chip in that case (not silent; the missing chip IS the
// signal that the deep-link is unavailable for this listing).
function googleMapsDirectionsUrl(origin, destination, { mode = "transit" } = {}) {
  const valid = (p) =>
    p &&
    typeof p.latitude === "number" &&
    typeof p.longitude === "number" &&
    Number.isFinite(p.latitude) &&
    Number.isFinite(p.longitude) &&
    !(p.latitude === 0 && p.longitude === 0);
  // Destination shape can be {lat, lng} OR {latitude, longitude}; normalise.
  const destOk =
    destination &&
    (typeof destination.latitude === "number" ||
      typeof destination.lat === "number");
  if (!valid(origin) || !destOk) return null;
  const destLat = destination.latitude ?? destination.lat;
  const destLng = destination.longitude ?? destination.lng;
  if (!Number.isFinite(destLat) || !Number.isFinite(destLng)) return null;
  const o = `${origin.latitude.toFixed(6)},${origin.longitude.toFixed(6)}`;
  const d = `${destLat.toFixed(6)},${destLng.toFixed(6)}`;
  const travelmode = mode === "walking" || mode === "driving" || mode === "bicycling" || mode === "transit"
    ? mode
    : "transit";
  return `https://www.google.com/maps/dir/?api=1&origin=${o}&destination=${d}&travelmode=${travelmode}`;
}

// Format one landmark's distance + optional transit minutes as a short
// secondary line. Degrades gracefully: "700 m" or "700 m · 14 min" or "" if
// we have nothing to say (caller then renders just the name + icon).
function _formatLandmarkMetric(lm) {
  const parts = [];
  if (typeof lm.distance_m === "number" && Number.isFinite(lm.distance_m)) {
    parts.push(lm.distance_m >= 1000
      ? `${(lm.distance_m / 1000).toFixed(1)} km`
      : `${Math.round(lm.distance_m)} m`);
  }
  if (typeof lm.transit_min === "number" && Number.isFinite(lm.transit_min)) {
    parts.push(`${lm.transit_min} min`);
  }
  return parts.join(" · ");
}

// Kind → pictograph for the chip prefix. Categories come from
// `data/ranking/landmarks.json` (university / transit / lake / neighborhood
// / oldtown / cultural / employer). Unknown kinds fall back to a neutral pin.
const _LANDMARK_ICONS = {
  university: "🎓",
  transit: "🚉",
  lake: "💧",
  neighborhood: "🏘",
  oldtown: "🏛",
  cultural: "🎭",
  employer: "🏢",
};

function _landmarkIcon(kind) {
  return _LANDMARK_ICONS[kind] || "📍";
}

// Render the nearby-landmarks chip row for one listing. ``variant`` controls
// layout (card vs compact); ``limit`` caps how many chips show on small
// surfaces. Empty string when the listing has no coords OR the landmark
// list is empty — never a stub, because a stub implies "we tried" which
// would be misleading.
function renderNearbyLandmarks(L, { variant = "card", limit = 5 } = {}) {
  if (!L) return "";
  const list = Array.isArray(L.nearby_landmarks) ? L.nearby_landmarks : [];
  if (list.length === 0) return "";
  if (
    typeof L.latitude !== "number" ||
    typeof L.longitude !== "number" ||
    !Number.isFinite(L.latitude) ||
    !Number.isFinite(L.longitude)
  ) {
    // No origin coords ⇒ can't build a directions link. Silently skip: the
    // chip without a link would just be trivia the user can't act on.
    return "";
  }
  const shown = list.slice(0, Math.max(1, Math.min(list.length, limit | 0)));
  const origin = { latitude: L.latitude, longitude: L.longitude };
  const chips = shown
    .map((lm) => {
      const url = googleMapsDirectionsUrl(origin, lm);
      if (!url) return "";
      const icon = _landmarkIcon(lm.kind);
      const name = lm.name || lm.key || "landmark";
      const metric = _formatLandmarkMetric(lm);
      const title = metric
        ? `Directions from this home to ${name} (${metric})`
        : `Directions from this home to ${name}`;
      return `<a class="landmark-chip" href="${esc(url)}" target="_blank" rel="noopener noreferrer" title="${esc(title)}">
        <span class="lm-ico">${icon}</span>
        <span class="lm-name">${esc(name)}</span>
        ${metric ? `<span class="lm-metric muted">${esc(metric)}</span>` : ""}
      </a>`;
    })
    .join("");
  if (!chips.trim()) return "";
  const label = variant === "compact" ? "" : `<div class="lm-label muted small">Nearby</div>`;
  return `<div class="nearby-landmarks variant-${esc(variant)}">${label}<div class="lm-chip-row">${chips}</div></div>`;
}

// Decide whether a result batch should render with rank-score badges (#1 TOP
// 0.123), or without (random / anon-default feed).
//
// True when any of:
//   * backend flagged it as an unpersonalized default feed, OR
//   * every listing's score is 0 / null (no channel fired)
//
// False as soon as one listing has a non-zero score — that means at least
// one ranking channel (BM25 / visual / semantic / soft / memory) fired and
// the ordering is meaningful.
//
// Extracted into a named helper so we can unit-test it independently of the
// DOM-heavy renderListings pipeline.
function isUnscoredBatch(listings, meta) {
  if (meta && meta.default_feed && !meta.personalized) return true;
  if (!Array.isArray(listings) || listings.length === 0) return true;
  return listings.every(
    (r) => !r || r.score == null || r.score === 0,
  );
}

// ---------- Map overlay (Leaflet + MarkerCluster) --------------------------
// A single module that owns the #map DOM node. Four layers on the same map:
//
//     tiles              CartoDB Positron (subtle, low-chrome basemap)
//     hullLayer          soft-fill polygon around the current result set;
//                        the "beautifully selected area" the user asked for
//     landmarkLayer      45 curated Swiss landmarks (HBs, universities, lakes,
//                        employer HQs). Always on; distinct amber pin style;
//                        hover reveals the name. Lets the user keep geo-
//                        context while panning away from the result cluster.
//     clusterLayer       per-listing circle markers, clustered with a custom
//                        divIcon that carries a size-ramp and gradient fill.
//                        Hover shows a mini-card tooltip (price, rooms, city);
//                        click scrolls + pulses the matching listing card.
//
// Two bespoke Leaflet controls:
//     recenterControl    always-visible button, top-right under the zoom
//                        widget, that flies back to the active result bbox.
//     areaFilterPill     HTML element overlaid on the map (already in the DOM);
//                        appears after a cluster click, resets card filter.
//
// State discipline:
//   * MAP_STATE.bounds is the ONLY source of truth for "where to fly on
//     recenter"; it is set when a result set lands and is kept across tab
//     switches, so the user can hit "map" then "recenter" at any time.
//   * Every async fetch increments MAP_STATE.fetchToken; stale responses
//     discard themselves (typical during quick multi-query typing).
//   * clearAreaFilter is idempotent; safe to call whether a filter was set.
//   * All fallbacks emit [WARN] (per CLAUDE.md §5).
const MAP_STATE = {
  map: null,
  clusterLayer: null,
  landmarkLayer: null,
  hullLayer: null,
  recenterControl: null,
  markersByListingId: new Map(),
  // Markers that belong to the ranker's top-N. Subset of markersByListingId;
  // used to style them distinctly and power the "only top" visibility toggle.
  topListingIds: new Set(),
  // Reverse-lookup for landmark markers so right-click semantics can find
  // the key associated with a given DOM element.
  landmarkMarkersByKey: new Map(),
  resultBounds: null,
  activeAreaFilter: null,
  fetchToken: 0,
  // Search results that arrived before the map was ever rendered (because
  // the user stayed in List view). Replayed on the first open of the Map
  // tab so no search ever lands "invisibly".
  pendingPoints: null,
  pendingTopIds: null,
  shownOnce: false,
  // Client-side visibility toggle: when true, faded (non-top) markers are
  // hidden from the cluster layer. Pure view state; no backend refetch.
  onlyTopVisible: false,
  // Full widget state. Hydrated from meta.query_plan on every response so
  // the panel visually tracks whatever the LLM extracted; any user change
  // flows back via debounced re-submit with hard_filters_override.
  widgetFilters: {
    min_price: null,
    max_price: null,
    min_rooms: null,
    max_rooms: null,
    min_area: null,             // living_area_sqm lower bound
    max_area: null,             // living_area_sqm upper bound
    bathroom_shared: null,      // tri-state: null | true | false
    kitchen_shared: null,
    has_cellar: null,
    near_landmark: [],          // list of alias strings; empty means "clear"
  },
  // "Did the user edit the landmark list since the last LLM hydration?"
  // When true, resubmit sends near_landmark even if it's empty (so removals
  // take effect); when false, resubmit defers to the LLM's extraction.
  landmarkListDirty: false,
  // Preserved across widget-driven re-submits so the backend rebuilds the
  // full RRF pipeline from the original natural-language text.
  lastQueryText: "",
  lastAttachedImageKept: false, // stub; image-query re-submit is out of scope
  // One debounce timer across all widget inputs so rapid slider drags
  // coalesce into a single backend call.
  widgetDebounceTimer: null,
  widgetSubmitToken: 0,
  // Set by the widget toggle handlers when the user clicks a toggle; the
  // same value is mirrored into button aria-pressed so CSS can theme it.
  // 'on' -> true  (require), 'off' -> false (exclude), 'any' -> null (reset)
};

const _WIDGET_DEBOUNCE_MS = 320;
const _PRICE_RANGE_MAX = 10000;  // CHF cap for the range-slider UI
const _AREA_RANGE_MAX = 400;     // m² cap for the size-slider UI (covers 99%)

// Swiss outer bbox (lat/lng). Slightly padded so the country doesn't hit the
// viewport edges when nothing else is loaded yet.
const SWISS_BOUNDS = L_BOUNDS(() => [[45.82, 5.96], [47.81, 10.49]]);

// Lazy factory: returns an actual L.latLngBounds only when Leaflet is loaded.
// Before that it returns whatever shape the caller gave. Keeps the module
// importable even if the CDN is still in flight.
function L_BOUNDS(factory) {
  return factory();
}

// ---- Init + layer setup --------------------------------------------------

function _initMapOnce() {
  if (MAP_STATE.map) return;
  const mapEl = document.getElementById("map");
  if (!mapEl || typeof L === "undefined") {
    console.warn(
      "[WARN] map_init_skipped: expected=window.L + #map, got=" +
        (typeof L === "undefined" ? "no Leaflet" : "no #map") +
        ", fallback=results render without map overlay",
    );
    return;
  }
  // Defer until the container has real dimensions. Leaflet caches its
  // measured container size at L.map() time and won't backfill cleanly.
  // If #map is hidden (Map tab not yet opened), wait for the first flip.
  if (mapEl.offsetHeight === 0 || mapEl.offsetWidth === 0) {
    // Per CLAUDE.md §5: announce the fallback path. One-time per session so
    // we don't spam the console when DOMContentLoaded fires before any tab
    // has been opened (the expected boot sequence).
    if (!MAP_STATE._deferredInitLogged) {
      MAP_STATE._deferredInitLogged = true;
      console.info(
        "[INFO] map_init_deferred: expected=#map visible, got=offsetSize 0, " +
          "fallback=init on first Map-tab click",
      );
    }
    return;
  }
  MAP_STATE.map = L.map(mapEl, {
    preferCanvas: true,
    zoomControl: false,             // custom position below
    attributionControl: true,
    fadeAnimation: true,
    zoomAnimation: true,
    markerZoomAnimation: true,
    worldCopyJump: false,
  });
  L.control.zoom({ position: "topright" }).addTo(MAP_STATE.map);
  L.tileLayer(
    "https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png",
    {
      maxZoom: 19,
      subdomains: "abcd",
      attribution:
        '© <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> · ' +
        '© <a href="https://carto.com/attributions">CARTO</a>',
    },
  ).addTo(MAP_STATE.map);
  MAP_STATE.map.fitBounds(SWISS_BOUNDS);

  // Layer 1: area hull (below markers)
  MAP_STATE.hullLayer = L.layerGroup().addTo(MAP_STATE.map);

  // Layer 2: landmarks (below cluster markers; lightweight regular FeatureGroup
  // because we want zoom-independent visibility — landmarks shouldn't cluster
  // with listing markers).
  MAP_STATE.landmarkLayer = L.layerGroup().addTo(MAP_STATE.map);

  // Layer 3: listing markers (top)
  MAP_STATE.clusterLayer = L.markerClusterGroup({
    showCoverageOnHover: false,
    spiderfyOnMaxZoom: true,
    disableClusteringAtZoom: 17,
    maxClusterRadius: 55,
    chunkedLoading: true,
    animate: true,
    animateAddingMarkers: false,    // skip the fly-in; looks busy on 500+
    iconCreateFunction: _buildClusterIcon,
  });
  MAP_STATE.clusterLayer.on("clusterclick", _onClusterClick);
  MAP_STATE.map.addLayer(MAP_STATE.clusterLayer);

  // Recenter control (custom L.Control)
  MAP_STATE.recenterControl = _buildRecenterControl().addTo(MAP_STATE.map);

  // Pill click -> reset card filter.
  const pill = document.getElementById("map-area-pill");
  if (pill) pill.addEventListener("click", clearAreaFilter);

  // Kick off the always-on landmarks layer. One request, kept in browser.
  _fetchAndRenderLandmarks();
}

// Leaflet measures DOM size at init; if #map was hidden (tab closed) during
// init, its internal dimensions are 0 and tiles don't render until we force
// a resize after the user first opens the tab. Idempotent.
function mapNudgeSize() {
  if (!MAP_STATE.map) return;
  if (!MAP_STATE.shownOnce) {
    MAP_STATE.shownOnce = true;
  }
  // setTimeout because the tab flip applies `hidden=false` synchronously and
  // Leaflet reads getBoundingClientRect; give the browser a frame to paint.
  setTimeout(() => {
    if (MAP_STATE.map) MAP_STATE.map.invalidateSize({ animate: false });
  }, 50);
}

// ---- Custom cluster icon (size ramp + gradient) ---------------------------

function _buildClusterIcon(cluster) {
  const n = cluster.getChildCount();
  // 4 steps instead of 3 so the ramp is smooth yet still compact — the user
  // asked for "not that much size difference", so the outer radius goes
  // 32 -> 38 -> 44 -> 50 px across the range. Still obvious which is larger
  // without any dwarfing.
  let bucket = "xs";
  let px = 32;
  if (n >= 200)       { bucket = "xl"; px = 50; }
  else if (n >= 50)   { bucket = "lg"; px = 44; }
  else if (n >= 10)   { bucket = "md"; px = 38; }
  return L.divIcon({
    html: `<div class="cluster-inner"><span>${_clusterLabel(n)}</span></div>`,
    className: `cluster-${bucket}`,
    iconSize: L.point(px, px),
    iconAnchor: L.point(px / 2, px / 2),
  });
}

function _clusterLabel(n) {
  if (n < 1000) return String(n);
  if (n < 10000) return `${(n / 1000).toFixed(1).replace(/\.0$/, "")}k`;
  return `${Math.round(n / 1000)}k`;
}

// ---- Listing-marker rendering --------------------------------------------

function renderMapPoints(points, topListingIds) {
  _initMapOnce();
  const topSet = topListingIds instanceof Set ? topListingIds : new Set(topListingIds || []);
  if (!MAP_STATE.map) {
    // Map container isn't visible yet — buffer both the points and the
    // top-id set; the tab-flip code path will replay them together.
    MAP_STATE.pendingPoints = points;
    MAP_STATE.pendingTopIds = topSet;
    MAP_STATE.topListingIds = topSet;
    _updateTabCounts(points.length);
    _updateTopToggleVisibility(topSet.size);
    return;
  }
  MAP_STATE.pendingPoints = null;
  MAP_STATE.pendingTopIds = null;
  MAP_STATE.clusterLayer.clearLayers();
  MAP_STATE.hullLayer.clearLayers();
  MAP_STATE.markersByListingId.clear();
  MAP_STATE.activeAreaFilter = null;
  MAP_STATE.topListingIds = topSet;
  _updateAreaPill(0);
  _updateMapCounter(points.length);
  _updateTabCounts(points.length);
  _updateTopToggleVisibility(topSet.size);
  if (!points.length) {
    MAP_STATE.resultBounds = null;
    MAP_STATE.map.flyToBounds(SWISS_BOUNDS, { duration: 0.7 });
    _setRecenterEnabled(false);
    return;
  }

  // Split top vs non-top so we can render them differently: tops as
  // DivIcon markers with halo animations + hover bloom, non-tops as
  // cheap canvas circleMarkers (soft amber, low opacity, still clickable).
  const topMarkers = [];
  const normalMarkers = [];
  for (const p of points) {
    const id = String(p.listing_id);
    const isTop = topSet.has(id);
    const m = isTop
      ? _buildTopMarker(p)
      : _buildNormalMarker(p);
    m._listingId = id;
    m._mapPoint = p;
    m._isTop = isTop;
    m.bindTooltip(_tooltipHtmlForListing(p, isTop), {
      className: "listing-tooltip",
      direction: "top",
      offset: isTop ? [0, -14] : [0, -8],
      opacity: 1,
    });
    m.on("click", () => _onMarkerClick(m._listingId));
    if (!isTop) {
      // Canvas circleMarker: hover animation is JS-driven.
      m.on("mouseover", _onNormalMarkerHover);
      m.on("mouseout", _onNormalMarkerUnhover);
    }
    MAP_STATE.markersByListingId.set(id, m);
    (isTop ? topMarkers : normalMarkers).push(m);
  }
  // Only add the non-top layer when the "only top" toggle is off.
  if (!MAP_STATE.onlyTopVisible) {
    MAP_STATE.clusterLayer.addLayers(normalMarkers);
  }
  MAP_STATE.clusterLayer.addLayers(topMarkers);

  _renderResultHull(points);

  const bounds = L.latLngBounds(points.map((p) => [p.lat, p.lng]));
  MAP_STATE.resultBounds = bounds;
  MAP_STATE.map.flyToBounds(bounds, {
    padding: [40, 40],
    maxZoom: 14,
    duration: 0.85,
  });
  _setRecenterEnabled(true);
}

// ---- Marker factories ----------------------------------------------------

function _buildTopMarker(p) {
  // DivIcon-based: the amber halo animation + gradient fill are CSS-driven,
  // so hover/transform behaviour lives alongside the rest of the design
  // system rather than in canvas rendering code.
  const icon = L.divIcon({
    className: "top-marker",
    html: `
      <div class="top-marker-halo" aria-hidden="true"></div>
      <div class="top-marker-core" aria-hidden="true"></div>
    `,
    iconSize: L.point(26, 26),
    iconAnchor: L.point(13, 13),
  });
  return L.marker([p.lat, p.lng], { icon, riseOnHover: true });
}

function _buildNormalMarker(p) {
  // Canvas circleMarker — fast even at 10k+ markers. Faded amber so the
  // top markers visually dominate. Still fully interactive.
  return L.circleMarker([p.lat, p.lng], {
    radius: 6,
    color: "#f59e0b",
    weight: 1,
    opacity: 0.45,
    fillColor: "#fde68a",
    fillOpacity: 0.35,
    className: "listing-dot listing-dot-normal",
  });
}

function _onNormalMarkerHover(ev) {
  ev.target.setStyle({
    radius: 9,
    weight: 1.5,
    opacity: 0.85,
    fillColor: "#fbbf24",
    fillOpacity: 0.85,
  });
}
function _onNormalMarkerUnhover(ev) {
  ev.target.setStyle({
    radius: 6,
    weight: 1,
    opacity: 0.45,
    fillColor: "#fde68a",
    fillOpacity: 0.35,
  });
}

// ---- "Only top" visibility toggle (client-side, no refetch) --------------

function _updateTopToggleVisibility(topCount) {
  const btn = document.getElementById("map-toptoggle");
  if (!btn) return;
  btn.hidden = topCount === 0;
}

function toggleOnlyTop() {
  const btn = document.getElementById("map-toptoggle");
  if (!btn) return;
  MAP_STATE.onlyTopVisible = !MAP_STATE.onlyTopVisible;
  btn.setAttribute("aria-pressed", String(MAP_STATE.onlyTopVisible));
  const label = document.getElementById("map-toptoggle-label");
  if (label) {
    label.textContent = MAP_STATE.onlyTopVisible
      ? "Showing top matches"
      : "Only top matches";
  }
  // Rebuild the cluster layer from scratch — quick and keeps cluster counts
  // accurate in both directions.
  if (!MAP_STATE.clusterLayer) return;
  MAP_STATE.clusterLayer.clearLayers();
  const all = Array.from(MAP_STATE.markersByListingId.values());
  const toAdd = MAP_STATE.onlyTopVisible
    ? all.filter((m) => m._isTop)
    : all;
  MAP_STATE.clusterLayer.addLayers(toAdd);
}

function _tooltipHtmlForListing(p, isTop) {
  // Build a tiny card in HTML. Every field user-provided, so escape.
  const line1Parts = [];
  if (p.price_chf != null) line1Parts.push(`<b>${chf(p.price_chf)}</b>`);
  if (p.rooms != null) line1Parts.push(`${esc(p.rooms)} rm`);
  if (p.living_area_sqm != null) line1Parts.push(`${esc(p.living_area_sqm)} m²`);
  const line2Parts = [];
  if (p.city) line2Parts.push(esc(p.city));
  if (p.object_category) line2Parts.push(esc(p.object_category));
  const badge = isTop
    ? `<span class="tt-top-badge">★ Top match</span>`
    : "";
  return `
    <div class="listing-tooltip-inner">
      ${badge}
      <div class="tt-line1">${line1Parts.join(" · ") || "Listing"}</div>
      ${line2Parts.length ? `<div class="tt-line2">${line2Parts.join(" · ")}</div>` : ""}
      <div class="tt-hint">click to open this listing</div>
    </div>`;
}

// ---- Area hull (convex hull of result points) -----------------------------

function _renderResultHull(points) {
  if (!points || points.length < 3) return;
  const hull = _convexHull(points.map((p) => [p.lng, p.lat]));
  if (hull.length < 3) return;
  // Leaflet polygons use [lat, lng] — hull returns [lng, lat].
  const latlngs = hull.map(([x, y]) => [y, x]);
  const poly = L.polygon(latlngs, {
    color: "#b45309",
    weight: 1,
    opacity: 0.45,
    fillColor: "#fbbf24",
    fillOpacity: 0.08,
    interactive: false,        // the hull is decorative; don't swallow clicks
    className: "result-hull",
  });
  MAP_STATE.hullLayer.addLayer(poly);
}

// Andrew's monotone chain — standard convex hull on [[x,y], ...]. Small,
// in-place, no deps. Skips tolerance/jittering; good enough at 1-10k points.
function _convexHull(pts) {
  if (pts.length < 3) return pts.slice();
  const a = pts.slice().sort((p, q) => (p[0] - q[0]) || (p[1] - q[1]));
  const cross = (O, A, B) => (A[0] - O[0]) * (B[1] - O[1]) - (A[1] - O[1]) * (B[0] - O[0]);
  const lower = [];
  for (const p of a) {
    while (lower.length >= 2 && cross(lower[lower.length - 2], lower[lower.length - 1], p) <= 0) {
      lower.pop();
    }
    lower.push(p);
  }
  const upper = [];
  for (let i = a.length - 1; i >= 0; i--) {
    const p = a[i];
    while (upper.length >= 2 && cross(upper[upper.length - 2], upper[upper.length - 1], p) <= 0) {
      upper.pop();
    }
    upper.push(p);
  }
  upper.pop();
  lower.pop();
  return lower.concat(upper);
}

// ---- Landmarks layer (persistent, non-clustered) --------------------------

// Category-specific inline SVGs (lucide-style; stroke=currentColor so the
// category palette lives in CSS). 14px viewBox so the pin class can tint
// them without re-encoding paths. Added categories beyond the seven
// present in data/ranking/landmarks.json fall back to `.landmark-other`.
const LANDMARK_ICONS = Object.freeze({
  university:
    // Graduation cap
    '<svg viewBox="0 0 24 24"><path d="M22 10v6M2 10l10-5 10 5-10 5z"/>' +
    '<path d="M6 12v5c3 3 9 3 12 0v-5"/></svg>',
  transit:
    // Train
    '<svg viewBox="0 0 24 24"><rect x="4" y="3" width="16" height="14" rx="2"/>' +
    '<path d="M4 11h16"/><circle cx="9" cy="15" r="1.2"/><circle cx="15" cy="15" r="1.2"/>' +
    '<path d="M8 21l-1-3M16 21l1-3"/></svg>',
  lake:
    // Waves
    '<svg viewBox="0 0 24 24"><path d="M2 9c2.5-2 4.5-2 7 0s4.5 2 7 0 4.5-2 6 0"/>' +
    '<path d="M2 14c2.5-2 4.5-2 7 0s4.5 2 7 0 4.5-2 6 0"/>' +
    '<path d="M2 19c2.5-2 4.5-2 7 0s4.5 2 7 0 4.5-2 6 0"/></svg>',
  employer:
    // Briefcase
    '<svg viewBox="0 0 24 24"><rect x="3" y="7" width="18" height="13" rx="2"/>' +
    '<path d="M8 7V5a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"/>' +
    '<path d="M3 13h18"/></svg>',
  oldtown:
    // Tower / castle
    '<svg viewBox="0 0 24 24"><path d="M4 20V9l4-2 4 2 4-2 4 2v11"/>' +
    '<path d="M4 20h16"/><path d="M10 20v-5h4v5"/>' +
    '<path d="M4 9V6M8 9V6M16 9V6M20 9V6"/></svg>',
  cultural:
    // Palette
    '<svg viewBox="0 0 24 24"><path d="M12 3a9 9 0 0 0 0 18c1.5 0 2-1 2-2s-.5-1.5 0-2 2.5 0 3.5-1a5 5 0 0 0 1.5-3.5c0-4.5-3-9.5-7-9.5z"/>' +
    '<circle cx="8" cy="10" r="1.2"/><circle cx="11" cy="7" r="1.2"/>' +
    '<circle cx="15" cy="8" r="1.2"/></svg>',
  neighborhood:
    // Two small houses
    '<svg viewBox="0 0 24 24"><path d="M3 21V11l5-4 5 4v10"/><path d="M3 21h18"/>' +
    '<path d="M13 21V13l4-3 4 3v8"/></svg>',
});

function _landmarkIconHtml(category) {
  return LANDMARK_ICONS[category] || LANDMARK_ICONS.neighborhood;
}

async function _fetchAndRenderLandmarks() {
  try {
    const r = await fetch("/landmarks", { credentials: "same-origin" });
    if (!r.ok) {
      console.warn(
        `[WARN] landmarks_fetch_failed: expected=200 from /landmarks, ` +
          `got=${r.status}, fallback=map renders without landmark overlay`,
      );
      return;
    }
    const landmarks = await r.json();
    if (!Array.isArray(landmarks) || !landmarks.length) return;
    MAP_STATE.landmarkMarkersByKey.clear();
    for (const lm of landmarks) {
      const category = lm.category || "other";
      const icon = L.divIcon({
        className: `landmark-pin landmark-${category}`,
        html: _landmarkIconHtml(category),
        iconSize: L.point(26, 26),
        iconAnchor: L.point(13, 13),
      });
      const m = L.marker([lm.lat, lm.lng], {
        icon,
        keyboard: false,
        riseOnHover: true,
        interactive: true,
      });
      m._landmark = lm;
      m.bindTooltip(
        `<span class="landmark-tooltip-inner">
           <b>${esc(lm.name)}</b>
           <span class="muted">· ${esc(category)}</span>
         </span>`,
        {
          className: "landmark-tooltip",
          direction: "top",
          offset: [0, -12],
          opacity: 0.96,
          sticky: false,
        },
      );
      // Left-click: detail popup bound to the marker. Opens on demand.
      m.on("click", (ev) => _onLandmarkLeftClick(ev, lm));
      // Right-click: toggle filter membership. L.DomEvent keeps the browser's
      // native context menu from appearing over the map.
      m.on("contextmenu", (ev) => {
        L.DomEvent.preventDefault(ev.originalEvent);
        L.DomEvent.stopPropagation(ev.originalEvent);
        _onLandmarkRightClick(lm);
      });
      MAP_STATE.landmarkLayer.addLayer(m);
      MAP_STATE.landmarkMarkersByKey.set(lm.key, m);
    }
    // Pins exist now — upgrade any in-chip names to their canonical display
    // form (e.g. "zurich_hb" -> "Zürich HB"), then re-apply active states
    // that hydration set before the pin layer existed.
    _upgradeLandmarkChipNames();
    _renderLandmarkChips();
    _syncLandmarkActiveStates();
  } catch (e) {
    console.warn(
      `[WARN] landmarks_fetch_error: expected=/landmarks reachable, ` +
        `got=${e.message}, fallback=map renders without landmark overlay`,
    );
  }
}

function _onLandmarkLeftClick(ev, lm) {
  // Build a popup with name + category + a hint on how to add to filters.
  // Popup is bound per-click so it can include live "is-active" status
  // without having to manage a single long-lived popup element.
  const active = MAP_STATE.widgetFilters.near_landmark.some(
    (a) => _slugEq(a, lm.name) || _slugEq(a, lm.key),
  );
  const detail = L.popup({
    className: "landmark-popup",
    closeButton: true,
    autoPanPadding: L.point(30, 30),
    maxWidth: 240,
  }).setLatLng(ev.latlng).setContent(`
    <div class="landmark-popup-inner">
      <div class="lp-title">${esc(lm.name)}</div>
      <div class="lp-sub">${esc(lm.category || "landmark")}</div>
      <div class="lp-hint">
        ${
          active
            ? '<span class="lp-active">✓ already in your filter</span>'
            : '<span class="muted">Right-click the pin to add as a "near" filter.</span>'
        }
      </div>
    </div>
  `);
  MAP_STATE.map.openPopup(detail);
}

function _slugEq(a, b) {
  if (a == null || b == null) return false;
  return _landmarkNorm(a) === _landmarkNorm(b);
}

// Word-order-insensitive comparison: split into tokens, sort. Handles the
// "Zürich HB" ↔ "HB Zürich" case the earlier naive compare missed.
function _landmarkNorm(s) {
  if (s == null) return "";
  return String(s)
    .trim()
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")     // strip combining diacritics
    .replace(/[._\-]+/g, " ")
    .replace(/\s+/g, " ")
    .split(" ")
    .filter(Boolean)
    .sort()
    .join(" ");
}

// "Does any name the user typed / LLM emitted match this landmark's known
// identities?" Returns the matched candidate (truthy) or "" (falsy) so
// callers can short-circuit on truthiness.
function _landmarkMatchesAny(lm, candidates) {
  if (!lm || !Array.isArray(candidates) || !candidates.length) return "";
  const haystack = new Set(
    [lm.name, lm.key, ...(Array.isArray(lm.aliases) ? lm.aliases : [])]
      .filter(Boolean)
      .map(_landmarkNorm),
  );
  for (const c of candidates) {
    if (haystack.has(_landmarkNorm(c))) return c;
  }
  return "";
}

function _onLandmarkRightClick(lm) {
  // Add the landmark's display name to near_landmark if not already present;
  // then debounce-resubmit the search with the widget state as override.
  const name = lm.name || lm.key;
  const list = MAP_STATE.widgetFilters.near_landmark.slice();
  const existingMatch = _landmarkMatchesAny(lm, list);
  if (existingMatch) {
    // Second right-click removes — quick toggle affordance.
    const idx = list.findIndex((a) => _slugEq(a, existingMatch));
    if (idx >= 0) list.splice(idx, 1);
    _setLandmarkPinActive(lm.key, false);
  } else {
    list.push(name);
    _setLandmarkPinActive(lm.key, true);
  }
  MAP_STATE.widgetFilters.near_landmark = list;
  MAP_STATE.landmarkListDirty = true;
  _renderWidgetPanel();
  _scheduleWidgetResubmit();
}

function _setLandmarkPinActive(key, isActive) {
  const m = MAP_STATE.landmarkMarkersByKey.get(key);
  if (!m) return;
  const el = m.getElement();
  if (!el) return;
  el.classList.toggle("landmark-is-active", !!isActive);
}

// When landmarks first arrive we may already hold "near_landmark" entries
// that came from hydration before the pin data existed (e.g. "zurich_hb"
// derived from commute_target). Upgrade each entry to the landmark's
// canonical display name so chip labels read cleanly.
function _upgradeLandmarkChipNames() {
  const list = MAP_STATE.widgetFilters.near_landmark || [];
  if (!list.length || !MAP_STATE.landmarkMarkersByKey.size) return;
  const unresolved = [];
  const upgraded = list.map((raw) => {
    for (const m of MAP_STATE.landmarkMarkersByKey.values()) {
      if (_landmarkMatchesAny(m._landmark, [raw])) {
        return m._landmark.name || raw;
      }
    }
    unresolved.push(raw);
    return raw;
  });
  if (unresolved.length) {
    // The user typed (or the LLM emitted) a landmark name that doesn't
    // match any entry in /landmarks. Keep the chip — the backend still
    // honours raw strings in `near_landmark` for BM25 purposes — but flag
    // it so we can spot landmark-data drift or aliasing gaps.
    console.info(
      `[INFO] landmark_chip_unresolved: expected=match in /landmarks aliases, ` +
        `got=${JSON.stringify(unresolved)}, fallback=raw chip + no pin glow`,
    );
  }
  MAP_STATE.widgetFilters.near_landmark = upgraded;
}

// ---- Recenter control ----------------------------------------------------

function _buildRecenterControl() {
  const RecenterCtl = L.Control.extend({
    options: { position: "topright" },
    onAdd: function () {
      const btn = L.DomUtil.create(
        "button",
        "leaflet-bar map-recenter-btn",
      );
      btn.type = "button";
      btn.title = "Recenter on search results";
      btn.setAttribute("aria-label", btn.title);
      btn.innerHTML =
        `<svg viewBox="0 0 24 24" width="16" height="16" ` +
        `fill="none" stroke="currentColor" stroke-width="2" ` +
        `stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">` +
        `<circle cx="12" cy="12" r="3"/><path d="M12 2v3M12 19v3M2 12h3M19 12h3"/>` +
        `</svg>`;
      L.DomEvent.disableClickPropagation(btn);
      btn.addEventListener("click", recenterOnResults);
      btn.disabled = true;            // enabled once a result lands
      return btn;
    },
  });
  return new RecenterCtl();
}

function _setRecenterEnabled(enabled) {
  const btn = document.querySelector(".map-recenter-btn");
  if (!btn) return;
  btn.disabled = !enabled;
  btn.classList.toggle("is-enabled", !!enabled);
}

function recenterOnResults() {
  if (!MAP_STATE.map || !MAP_STATE.resultBounds) return;
  MAP_STATE.map.flyToBounds(MAP_STATE.resultBounds, {
    padding: [40, 40],
    maxZoom: 14,
    duration: 0.7,
  });
}

// ---- Interactions --------------------------------------------------------

function _onClusterClick(ev) {
  const childMarkers = ev.layer.getAllChildMarkers();
  const ids = childMarkers.map((m) => m._listingId).filter(Boolean);
  if (!ids.length) return;
  MAP_STATE.activeAreaFilter = new Set(ids);
  _applyAreaFilterToCards();
  _updateAreaPill(ids.length);
}

function _onMarkerClick(listingId) {
  // If the list tab isn't open, flip to it first so the scroll lands.
  const listPane = document.getElementById("view-list");
  if (listPane && listPane.hidden) {
    setActiveView("list");
  }
  const card = document.querySelector(
    `.listing-card[data-listing-id="${CSS.escape(listingId)}"]`,
  );
  if (!card) return;
  card.scrollIntoView({ behavior: "smooth", block: "center" });
  card.classList.remove("pulse");
  void card.offsetWidth;
  card.classList.add("pulse");
  setTimeout(() => card.classList.remove("pulse"), 1500);
}

function _applyAreaFilterToCards() {
  const cards = document.querySelectorAll(".listing-card[data-listing-id]");
  const visibleSet = MAP_STATE.activeAreaFilter;
  cards.forEach((c) => {
    const id = c.dataset.listingId;
    c.hidden = !!(visibleSet && !visibleSet.has(id));
  });
}

function clearAreaFilter() {
  MAP_STATE.activeAreaFilter = null;
  document
    .querySelectorAll(".listing-card[data-listing-id]")
    .forEach((c) => (c.hidden = false));
  _updateAreaPill(0);
}

function _updateAreaPill(count) {
  const pill = document.getElementById("map-area-pill");
  if (!pill) return;
  if (count > 0) {
    pill.textContent = `${count} in this area · clear`;
    pill.hidden = false;
  } else {
    pill.hidden = true;
  }
}

function _updateMapCounter(n) {
  const el = document.getElementById("map-counter");
  if (!el) return;
  if (n == null) el.textContent = "—";
  else if (n === 0) el.textContent = "no matches";
  else el.textContent = `${n.toLocaleString("de-CH")} on map`;
}

function _updateTabCounts(nOnMap) {
  const listCount = document.getElementById("view-tab-list-count");
  const mapCount = document.getElementById("view-tab-map-count");
  if (mapCount) {
    if (nOnMap > 0) {
      mapCount.textContent = nOnMap.toLocaleString("de-CH");
      mapCount.hidden = false;
    } else {
      mapCount.hidden = true;
    }
  }
  if (listCount) {
    const nCards = document.querySelectorAll(".listing-card[data-listing-id]").length;
    if (nCards > 0) {
      listCount.textContent = String(nCards);
      listCount.hidden = false;
    } else {
      listCount.hidden = true;
    }
  }
}

// ---- View tabs (List / Map toggle) ---------------------------------------

function setActiveView(view) {
  const listBtn = document.getElementById("view-tab-list");
  const mapBtn = document.getElementById("view-tab-map");
  const listPane = document.getElementById("view-list");
  const mapPane = document.getElementById("view-map");
  if (!listBtn || !mapBtn || !listPane || !mapPane) return;
  const showMap = view === "map";
  listBtn.classList.toggle("active", !showMap);
  mapBtn.classList.toggle("active", showMap);
  listBtn.setAttribute("aria-selected", showMap ? "false" : "true");
  mapBtn.setAttribute("aria-selected", showMap ? "true" : "false");
  listPane.hidden = showMap;
  mapPane.hidden = !showMap;
  if (showMap) {
    // Container is now visible — init if we hadn't already, then measure.
    _initMapOnce();
    mapNudgeSize();
    // Drain any searches that happened while the map was hidden.
    if (MAP_STATE.map && MAP_STATE.pendingPoints !== null) {
      const pts = MAP_STATE.pendingPoints;
      const topIds = MAP_STATE.pendingTopIds;
      MAP_STATE.pendingPoints = null;
      MAP_STATE.pendingTopIds = null;
      setTimeout(() => renderMapPoints(pts, topIds), 60);
    }
  }
}

// ---- Fetch ---------------------------------------------------------------

async function fetchAndRenderMap(mapRequestBody, topListingIds) {
  const myToken = ++MAP_STATE.fetchToken;
  _updateMapCounter(null);
  try {
    const r = await fetch("/listings/map", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      credentials: "same-origin",
      body: JSON.stringify(mapRequestBody),
    });
    if (!r.ok) {
      console.warn(
        `[WARN] map_fetch_failed: expected=200 from /listings/map, ` +
          `got=${r.status}, fallback=leave current map state`,
      );
      return;
    }
    const data = await r.json();
    if (myToken !== MAP_STATE.fetchToken) return;
    renderMapPoints(data.points || [], topListingIds);
  } catch (e) {
    console.warn(
      `[WARN] map_fetch_error: expected=/listings/map reachable, ` +
        `got=${e.message}, fallback=leave current map state`,
    );
  }
}

// ---- Widget panel: price slider, feature toggles, landmarks --------------
//
// The panel's DOM exists in demo.html; this section owns all the state and
// event plumbing. Two-way binding:
//   1. Every search result calls hydrateWidgetsFromPlan(plan), which copies
//      non-null fields from meta.query_plan into MAP_STATE.widgetFilters
//      and refreshes the DOM so the UI visibly reflects what the LLM saw.
//   2. Any user change in the panel updates MAP_STATE.widgetFilters and
//      schedules a debounced re-submit via resubmitSearchWithWidgets().
//      The backend merges the override on top of a fresh LLM extraction.

function _wireWidgetPanel() {
  const priceMin = document.getElementById("mf-price-min");
  const priceMax = document.getElementById("mf-price-max");
  const rangeMin = document.getElementById("mf-price-range-min");
  const rangeMax = document.getElementById("mf-price-range-max");
  const roomsMin = document.getElementById("mf-rooms-min");
  const roomsMax = document.getElementById("mf-rooms-max");
  const areaMin  = document.getElementById("mf-area-min");
  const areaMax  = document.getElementById("mf-area-max");
  const areaRangeMin = document.getElementById("mf-area-range-min");
  const areaRangeMax = document.getElementById("mf-area-range-max");
  const collapse = document.getElementById("map-filters-collapse");
  const reset    = document.getElementById("mf-reset");
  const toptg    = document.getElementById("map-toptoggle");

  if (priceMin) priceMin.addEventListener("input", _onPriceMinInput);
  if (priceMax) priceMax.addEventListener("input", _onPriceMaxInput);
  if (rangeMin) rangeMin.addEventListener("input", _onRangeMinInput);
  if (rangeMax) rangeMax.addEventListener("input", _onRangeMaxInput);
  if (roomsMin) roomsMin.addEventListener("input", _onRoomsMinInput);
  if (roomsMax) roomsMax.addEventListener("input", _onRoomsMaxInput);
  if (areaMin)  areaMin.addEventListener("input", _onAreaMinInput);
  if (areaMax)  areaMax.addEventListener("input", _onAreaMaxInput);
  if (areaRangeMin) areaRangeMin.addEventListener("input", _onAreaRangeMinInput);
  if (areaRangeMax) areaRangeMax.addEventListener("input", _onAreaRangeMaxInput);

  document.querySelectorAll(".mf-toggle[data-field]").forEach((btn) => {
    btn.addEventListener("click", () => _onToggleClick(btn));
  });

  if (collapse) collapse.addEventListener("click", () => {
    const panel = document.getElementById("map-filters");
    if (!panel) return;
    const isCollapsed = panel.classList.toggle("collapsed");
    collapse.setAttribute("aria-expanded", String(!isCollapsed));
  });

  if (reset) reset.addEventListener("click", _onWidgetReset);
  if (toptg) toptg.addEventListener("click", toggleOnlyTop);
}

function hydrateWidgetsFromPlan(plan) {
  // Reset widgetFilters to what the LLM extracted so the panel's visual
  // state always mirrors the last backend plan. Any in-flight user edits
  // that are waiting on the debounce timer are discarded — intentionally.
  const soft = (plan && plan.soft_preferences) || {};
  const near = Array.isArray(soft.near_landmark) ? soft.near_landmark.slice() : [];

  // A query like "near zurich hb" almost always extracts as soft.commute_target
  // rather than soft.near_landmark. Both signals point to the same landmark
  // key, so auto-promote commute_target into a chip if the user hasn't
  // already named that landmark explicitly. This way the REFINE panel shows
  // a visible chip AND the corresponding landmark pin glows "active",
  // without the user having to right-click the map.
  if (soft.commute_target) {
    const ct = soft.commute_target;
    const marker = MAP_STATE.landmarkMarkersByKey.get(ct);
    const displayName = marker?._landmark?.name || ct.replace(/_/g, " ");
    if (!marker) {
      // Landmarks may not have loaded yet (first paint race) or ct may be a
      // key we don't ship (landmarks.json drift). Either way we still get
      // a chip onto the panel; _upgradeLandmarkChipNames will revisit once
      // /landmarks returns. Announce the fallback so we can spot truly
      // unknown keys in the console.
      console.info(
        `[INFO] hydrate_commute_target: expected=marker for ct='${ct}', ` +
          `got=not-loaded-or-unknown, fallback='${displayName}' chip text`,
      );
    }
    const alreadyPresent =
      (marker && !!_landmarkMatchesAny(marker._landmark, near)) ||
      near.some((n) => _slugEq(n, displayName));
    if (!alreadyPresent) near.push(displayName);
  }

  MAP_STATE.widgetFilters = {
    min_price:       plan?.min_price ?? null,
    max_price:       plan?.max_price ?? null,
    min_rooms:       plan?.min_rooms ?? null,
    max_rooms:       plan?.max_rooms ?? null,
    min_area:        plan?.min_area ?? null,
    max_area:        plan?.max_area ?? null,
    bathroom_shared: plan?.bathroom_shared ?? null,
    kitchen_shared:  plan?.kitchen_shared ?? null,
    has_cellar:      plan?.has_cellar ?? null,
    near_landmark:   near,
  };
  // Response reflects the merged plan the server actually ran with, so
  // any "dirty" user edits we had queued are now accounted for. Clear
  // the flag — the next user click will re-dirty it.
  MAP_STATE.landmarkListDirty = false;
  _renderWidgetPanel();
  _syncLandmarkActiveStates();
}

function _renderWidgetPanel() {
  const f = MAP_STATE.widgetFilters;
  const priceMin = document.getElementById("mf-price-min");
  const priceMax = document.getElementById("mf-price-max");
  const rangeMin = document.getElementById("mf-price-range-min");
  const rangeMax = document.getElementById("mf-price-range-max");
  const roomsMin = document.getElementById("mf-rooms-min");
  const roomsMax = document.getElementById("mf-rooms-max");
  const areaMin  = document.getElementById("mf-area-min");
  const areaMax  = document.getElementById("mf-area-max");
  const areaRangeMin = document.getElementById("mf-area-range-min");
  const areaRangeMax = document.getElementById("mf-area-range-max");
  if (priceMin) priceMin.value = f.min_price != null ? String(f.min_price) : "";
  if (priceMax) priceMax.value = f.max_price != null ? String(f.max_price) : "";
  if (rangeMin) rangeMin.value = String(f.min_price ?? 0);
  if (rangeMax) rangeMax.value = String(f.max_price ?? _PRICE_RANGE_MAX);
  if (roomsMin) roomsMin.value = f.min_rooms != null ? String(f.min_rooms) : "";
  if (roomsMax) roomsMax.value = f.max_rooms != null ? String(f.max_rooms) : "";
  if (areaMin)  areaMin.value  = f.min_area != null ? String(f.min_area) : "";
  if (areaMax)  areaMax.value  = f.max_area != null ? String(f.max_area) : "";
  if (areaRangeMin) areaRangeMin.value = String(Math.min(f.min_area ?? 0, _AREA_RANGE_MAX));
  if (areaRangeMax) areaRangeMax.value = String(Math.min(f.max_area ?? _AREA_RANGE_MAX, _AREA_RANGE_MAX));
  _updatePriceRangeFill();
  _updateAreaRangeFill();
  _renderFeatureToggles();
  _renderLandmarkChips();
}

function _updatePriceRangeFill() {
  const fill = document.getElementById("mf-price-range-fill");
  if (!fill) return;
  const f = MAP_STATE.widgetFilters;
  const lo = Math.min(f.min_price ?? 0, _PRICE_RANGE_MAX);
  const hi = Math.min(f.max_price ?? _PRICE_RANGE_MAX, _PRICE_RANGE_MAX);
  const leftPct  = Math.max(0, (lo / _PRICE_RANGE_MAX) * 100);
  const rightPct = Math.max(0, (hi / _PRICE_RANGE_MAX) * 100);
  fill.style.left  = `${leftPct}%`;
  fill.style.width = `${Math.max(0, rightPct - leftPct)}%`;
}

function _updateAreaRangeFill() {
  const fill = document.getElementById("mf-area-range-fill");
  if (!fill) return;
  const f = MAP_STATE.widgetFilters;
  const lo = Math.min(f.min_area ?? 0, _AREA_RANGE_MAX);
  const hi = Math.min(f.max_area ?? _AREA_RANGE_MAX, _AREA_RANGE_MAX);
  const leftPct  = Math.max(0, (lo / _AREA_RANGE_MAX) * 100);
  const rightPct = Math.max(0, (hi / _AREA_RANGE_MAX) * 100);
  fill.style.left  = `${leftPct}%`;
  fill.style.width = `${Math.max(0, rightPct - leftPct)}%`;
}

function _renderFeatureToggles() {
  const f = MAP_STATE.widgetFilters;
  document.querySelectorAll(".mf-toggle[data-field]").forEach((btn) => {
    const field = btn.dataset.field;
    const val = f[field];
    // Tri-state: null=any (default), true=require, false=exclude
    let ap = "false";
    if (val === true) ap = "true";
    else if (val === false) ap = "mixed";
    btn.setAttribute("aria-pressed", ap);
  });
}

function _renderLandmarkChips() {
  const wrap = document.getElementById("mf-landmarks-list");
  if (!wrap) return;
  const names = MAP_STATE.widgetFilters.near_landmark || [];
  if (!names.length) {
    wrap.innerHTML = `<span class="mf-hint">Right-click a landmark on the map to add it here.</span>`;
    return;
  }
  wrap.innerHTML = names
    .map(
      (name, i) => `
      <span class="mf-landmark-chip" data-idx="${i}">
        ${esc(name)}
        <button type="button" class="mf-landmark-chip-remove"
                aria-label="Remove ${esc(name)}">×</button>
      </span>`,
    )
    .join("");
  wrap.querySelectorAll(".mf-landmark-chip-remove").forEach((btn) => {
    btn.addEventListener("click", (ev) => {
      const chip = ev.target.closest(".mf-landmark-chip");
      const idx = chip ? parseInt(chip.dataset.idx, 10) : -1;
      if (Number.isFinite(idx) && idx >= 0) {
        const removed = MAP_STATE.widgetFilters.near_landmark[idx];
        MAP_STATE.widgetFilters.near_landmark.splice(idx, 1);
        MAP_STATE.landmarkListDirty = true;
        _renderLandmarkChips();
        // Un-glow the corresponding map pin via alias-aware lookup.
        if (removed) {
          const lmEntry = Array.from(MAP_STATE.landmarkMarkersByKey.entries())
            .find(([k, m]) => !!_landmarkMatchesAny(m._landmark, [removed]));
          if (lmEntry) _setLandmarkPinActive(lmEntry[0], false);
        }
        _scheduleWidgetResubmit();
      }
    });
  });
}

function _syncLandmarkActiveStates() {
  const names = MAP_STATE.widgetFilters.near_landmark || [];
  MAP_STATE.landmarkMarkersByKey.forEach((m, key) => {
    const matched = _landmarkMatchesAny(m._landmark, names);
    _setLandmarkPinActive(key, !!matched);
  });
}

// ---- Widget input handlers ----------------------------------------------

function _clampPrice(n) {
  if (n == null || !Number.isFinite(n)) return null;
  const v = Math.max(0, Math.min(30000, Math.round(n)));
  return v;
}

function _clampRooms(n) {
  if (n == null || !Number.isFinite(n)) return null;
  return Math.max(0, Math.min(15, n));
}

function _onPriceMinInput(ev) {
  const v = ev.target.value.trim() === "" ? null : _clampPrice(Number(ev.target.value));
  MAP_STATE.widgetFilters.min_price = v;
  const rangeMin = document.getElementById("mf-price-range-min");
  if (rangeMin) rangeMin.value = String(Math.min(v ?? 0, _PRICE_RANGE_MAX));
  _updatePriceRangeFill();
  _scheduleWidgetResubmit();
}

function _onPriceMaxInput(ev) {
  const v = ev.target.value.trim() === "" ? null : _clampPrice(Number(ev.target.value));
  MAP_STATE.widgetFilters.max_price = v;
  const rangeMax = document.getElementById("mf-price-range-max");
  if (rangeMax) rangeMax.value = String(Math.min(v ?? _PRICE_RANGE_MAX, _PRICE_RANGE_MAX));
  _updatePriceRangeFill();
  _scheduleWidgetResubmit();
}

function _onRangeMinInput(ev) {
  let v = Number(ev.target.value);
  // Clamp min to stay ≤ max-100.
  const max = MAP_STATE.widgetFilters.max_price ?? _PRICE_RANGE_MAX;
  if (v >= max) v = Math.max(0, max - 100);
  MAP_STATE.widgetFilters.min_price = v === 0 ? null : v;
  const priceMin = document.getElementById("mf-price-min");
  if (priceMin) priceMin.value = v === 0 ? "" : String(v);
  _updatePriceRangeFill();
  _scheduleWidgetResubmit();
}

function _onRangeMaxInput(ev) {
  let v = Number(ev.target.value);
  const min = MAP_STATE.widgetFilters.min_price ?? 0;
  if (v <= min) v = Math.min(_PRICE_RANGE_MAX, min + 100);
  MAP_STATE.widgetFilters.max_price = v === _PRICE_RANGE_MAX ? null : v;
  const priceMax = document.getElementById("mf-price-max");
  if (priceMax) priceMax.value = v === _PRICE_RANGE_MAX ? "" : String(v);
  _updatePriceRangeFill();
  _scheduleWidgetResubmit();
}

function _onRoomsMinInput(ev) {
  const raw = ev.target.value.trim();
  MAP_STATE.widgetFilters.min_rooms = raw === "" ? null : _clampRooms(Number(raw));
  _scheduleWidgetResubmit();
}

function _onRoomsMaxInput(ev) {
  const raw = ev.target.value.trim();
  MAP_STATE.widgetFilters.max_rooms = raw === "" ? null : _clampRooms(Number(raw));
  _scheduleWidgetResubmit();
}

function _clampArea(n) {
  if (n == null || !Number.isFinite(n)) return null;
  return Math.max(0, Math.min(1500, Math.round(n)));
}

function _onAreaMinInput(ev) {
  const v = ev.target.value.trim() === "" ? null : _clampArea(Number(ev.target.value));
  MAP_STATE.widgetFilters.min_area = v;
  const rangeMin = document.getElementById("mf-area-range-min");
  if (rangeMin) rangeMin.value = String(Math.min(v ?? 0, _AREA_RANGE_MAX));
  _updateAreaRangeFill();
  _scheduleWidgetResubmit();
}

function _onAreaMaxInput(ev) {
  const v = ev.target.value.trim() === "" ? null : _clampArea(Number(ev.target.value));
  MAP_STATE.widgetFilters.max_area = v;
  const rangeMax = document.getElementById("mf-area-range-max");
  if (rangeMax) rangeMax.value = String(Math.min(v ?? _AREA_RANGE_MAX, _AREA_RANGE_MAX));
  _updateAreaRangeFill();
  _scheduleWidgetResubmit();
}

function _onAreaRangeMinInput(ev) {
  let v = Number(ev.target.value);
  const max = MAP_STATE.widgetFilters.max_area ?? _AREA_RANGE_MAX;
  if (v >= max) v = Math.max(0, max - 5);
  MAP_STATE.widgetFilters.min_area = v === 0 ? null : v;
  const areaMin = document.getElementById("mf-area-min");
  if (areaMin) areaMin.value = v === 0 ? "" : String(v);
  _updateAreaRangeFill();
  _scheduleWidgetResubmit();
}

function _onAreaRangeMaxInput(ev) {
  let v = Number(ev.target.value);
  const min = MAP_STATE.widgetFilters.min_area ?? 0;
  if (v <= min) v = Math.min(_AREA_RANGE_MAX, min + 5);
  MAP_STATE.widgetFilters.max_area = v === _AREA_RANGE_MAX ? null : v;
  const areaMax = document.getElementById("mf-area-max");
  if (areaMax) areaMax.value = v === _AREA_RANGE_MAX ? "" : String(v);
  _updateAreaRangeFill();
  _scheduleWidgetResubmit();
}

function _onToggleClick(btn) {
  const field = btn.dataset.field;
  if (!field) return;
  const current = MAP_STATE.widgetFilters[field];
  // Cycle: null -> true -> false -> null
  let next;
  if (current === null || current === undefined) next = true;
  else if (current === true) next = false;
  else next = null;
  MAP_STATE.widgetFilters[field] = next;
  _renderFeatureToggles();
  _scheduleWidgetResubmit();
}

function _onWidgetReset() {
  MAP_STATE.widgetFilters = {
    min_price: null,
    max_price: null,
    min_rooms: null,
    max_rooms: null,
    min_area: null,
    max_area: null,
    bathroom_shared: null,
    kitchen_shared: null,
    has_cellar: null,
    near_landmark: [],
  };
  // User explicitly cleared the list — send [] in the next override so the
  // backend doesn't silently re-add LLM-inferred landmarks.
  MAP_STATE.landmarkListDirty = true;
  _renderWidgetPanel();
  _syncLandmarkActiveStates();
  _scheduleWidgetResubmit();
}

// ---- Debounced widget resubmit ------------------------------------------

function _scheduleWidgetResubmit() {
  _setMfStatus("pending…", "is-loading");
  if (MAP_STATE.widgetDebounceTimer) {
    clearTimeout(MAP_STATE.widgetDebounceTimer);
  }
  MAP_STATE.widgetDebounceTimer = setTimeout(() => {
    MAP_STATE.widgetDebounceTimer = null;
    resubmitSearchWithWidgets();
  }, _WIDGET_DEBOUNCE_MS);
}

function _buildOverridePayload() {
  const f = MAP_STATE.widgetFilters;
  // Null fields are omitted so the backend's merge defers to the LLM. The
  // landmark list is the one exception: if the user has touched it since
  // the last hydration (chip removed / reset) we always send it — including
  // an empty array — so the backend doesn't re-inject the LLM's inference.
  const out = {};
  if (f.min_price       != null) out.min_price       = f.min_price;
  if (f.max_price       != null) out.max_price       = f.max_price;
  if (f.min_rooms       != null) out.min_rooms       = f.min_rooms;
  if (f.max_rooms       != null) out.max_rooms       = f.max_rooms;
  if (f.min_area        != null) out.min_area        = f.min_area;
  if (f.max_area        != null) out.max_area        = f.max_area;
  if (f.bathroom_shared != null) out.bathroom_shared = f.bathroom_shared;
  if (f.kitchen_shared  != null) out.kitchen_shared  = f.kitchen_shared;
  if (f.has_cellar      != null) out.has_cellar      = f.has_cellar;
  if (MAP_STATE.landmarkListDirty || (f.near_landmark && f.near_landmark.length)) {
    out.soft_preferences = { near_landmark: f.near_landmark || [] };
  }
  return out;
}

async function resubmitSearchWithWidgets() {
  // Gate: no active text query yet -> the panel is inert; the user needs to
  // submit the search bar once first. Still run the /listings/map overlay
  // so the map can show pure hard-filter results without any text ranking.
  const q = MAP_STATE.lastQueryText || "";
  const override = _buildOverridePayload();
  if (!q && Object.keys(override).length === 0) {
    _setMfStatus("", "");
    return;
  }
  const myToken = ++MAP_STATE.widgetSubmitToken;
  try {
    // Main ranked list re-issue. Preserves the LLM-extracted plan via the
    // backend merge (non-null override fields win).
    if (q) {
      const fd = new FormData();
      fd.append("query", q);
      fd.append("limit", "25");
      fd.append("offset", "0");
      fd.append("personalize",
        (authState.user && els.personalizeToggle?.checked) ? "true" : "false");
      fd.append("hard_filters_override", JSON.stringify(override));
      const r = await fetch("/listings/search/multi", {
        method: "POST",
        body: fd,
        credentials: "same-origin",
      });
      if (!r.ok) {
        throw new Error(`status=${r.status}`);
      }
      const data = await r.json();
      if (myToken !== MAP_STATE.widgetSubmitToken) return;   // stale
      _applySearchResponse(data, { fromWidget: true });
      return;
    }
    // No text — fall back to map-only refresh using the override as hard filters.
    const mapBody = Object.keys(override).length
      ? { hard_filters: override }
      : null;
    if (mapBody) await fetchAndRenderMap(mapBody, new Set());
    _setMfStatus(
      Object.keys(override).length
        ? `${Object.keys(override).length} filter${Object.keys(override).length === 1 ? "" : "s"} applied`
        : "",
      "",
    );
  } catch (e) {
    console.warn(
      `[WARN] resubmit_widgets_failed: expected=200 from /listings/search/multi, ` +
        `got=${e.message}, fallback=leave current results`,
    );
    _setMfStatus("re-run failed — check console", "is-error");
  }
}

function _setMfStatus(text, cls) {
  const el = document.getElementById("mf-status");
  if (!el) return;
  el.textContent = text || "";
  el.className = `mf-status ${cls || ""}`.trim();
}

// ---------- Pass-2b display helpers ---------------------------------------
// All 4 fields are tri-state (true / false / null for
// UNKNOWN). Renders one chip per field that has a known value; empty string
// when UNKNOWN, so the chip row collapses cleanly for listings whose extractor
// couldn't decide. Used by the listing card, detail drawer, and saved-listings.
function bathroomChips(L) {
  const chips = [];
  if (L.bathroom_count != null) {
    const n = Number(L.bathroom_count);
    chips.push(
      `<span class="chip-enriched" title="from pass-2b extraction">🛁 ${esc(n)} ${
        n === 1 ? "bath" : "baths"
      }</span>`,
    );
  }
  if (L.bathroom_shared === true) {
    chips.push(
      `<span class="chip-enriched chip-shared" title="shared bathroom">🛁 shared</span>`,
    );
  } else if (L.bathroom_shared === false && L.bathroom_count == null) {
    // Surface "private bath" only when we couldn't surface a count — avoid
    // duplicating the 🛁 chip when both are present.
    chips.push(`<span class="chip-enriched" title="private bathroom">🛁 private</span>`);
  }
  if (L.has_cellar === true) {
    chips.push(`<span class="chip-enriched" title="has a cellar / Keller">🗝️ cellar</span>`);
  } else if (L.has_cellar === false) {
    chips.push(
      `<span class="chip-enriched chip-negated" title="no cellar">🚫 cellar</span>`,
    );
  }
  if (L.kitchen_shared === true) {
    chips.push(
      `<span class="chip-enriched chip-shared" title="shared kitchen / WG-Küche">🍳 shared</span>`,
    );
  } else if (L.kitchen_shared === false && L.bathroom_shared !== false) {
    chips.push(`<span class="chip-enriched" title="private kitchen">🍳 private</span>`);
  }
  return chips.join("");
}

// Sanitize listing-description HTML with an allowlist and defense-in-depth.
// Parses into a <template> (inert DOM: scripts / <img> don't fetch or run),
// then (1) drops known-dangerous tags wholesale including their subtree, and
// (2) walks every surviving element, replacing non-allowlisted tags with
// their text content and stripping every attribute on allowed tags.
// Verified against script/iframe/svg-onload/img-onerror/javascript-href/
// event-handler/style-attribute inputs via jsdom.
const _ALLOWED_TAGS = new Set([
  "B", "STRONG", "I", "EM", "U", "BR", "P", "DIV", "SPAN", "UL", "OL", "LI",
]);
const _BLOCK_TAGS = [
  "SCRIPT", "IFRAME", "OBJECT", "EMBED", "STYLE", "LINK", "META", "FORM",
  "INPUT", "BUTTON", "IMG", "SVG", "VIDEO", "AUDIO", "SOURCE", "NOSCRIPT",
  "BASE",
];

function sanitizeDescriptionHtml(html) {
  if (html == null) return "";
  const tpl = document.createElement("template");
  tpl.innerHTML = String(html);

  // Pass 1: drop dangerous elements (and everything inside them) wholesale.
  for (const tag of _BLOCK_TAGS) {
    for (const el of Array.from(tpl.content.querySelectorAll(tag))) {
      el.remove();
    }
  }

  // Pass 2: enforce allowlist + strip attributes. Deepest-first replacement
  // so we never hand a stale parent reference to replaceChild.
  const all = Array.from(tpl.content.querySelectorAll("*"));
  all.sort((a, b) => (b.contains(a) ? -1 : a.contains(b) ? 1 : 0));
  for (const el of all) {
    if (!_ALLOWED_TAGS.has(el.tagName)) {
      const txt = document.createTextNode(el.textContent || "");
      if (el.parentNode) el.parentNode.replaceChild(txt, el);
      continue;
    }
    for (const attr of Array.from(el.attributes)) {
      el.removeAttribute(attr.name);
    }
  }

  return tpl.innerHTML;
}

function setStatus(text, cls) {
  els.status.textContent = text;
  els.status.className = "status " + (cls || "");
}

// ---------- renderers --------------------------------------------------------

function renderHardFilters(plan) {
  if (!plan) {
    els.hardView.innerHTML = '<p class="empty">No query plan in response.</p>';
    return;
  }

  // Hide rows the LLM didn't emit so the panel only shows what's actually
  // constraining the search. The raw response is still available in the
  // "For developers" panel for users who want to see every schema field.
  const rows = [];
  const add = (k, v) => {
    if (v == null || (Array.isArray(v) && v.length === 0)) return;
    rows.push(
      `<div class="kv"><span class="k">${esc(k)}</span>` +
      `<span class="v">${esc(v)}</span></div>`,
    );
  };

  add("city", plan.city ? plan.city.join(", ") : null);
  add("postal_code", plan.postal_code ? plan.postal_code.join(", ") : null);
  add("canton", plan.canton);
  add(
    "price",
    plan.min_price == null && plan.max_price == null
      ? null
      : `${plan.min_price ?? "−∞"} .. ${plan.max_price ?? "+∞"} CHF`,
  );
  add(
    "rooms",
    plan.min_rooms == null && plan.max_rooms == null
      ? null
      : `${plan.min_rooms ?? "−∞"} .. ${plan.max_rooms ?? "+∞"}`,
  );
  add(
    "area",
    plan.min_area == null && plan.max_area == null
      ? null
      : `${plan.min_area ?? "−∞"} .. ${plan.max_area ?? "+∞"} m²`,
  );
  add(
    "floor",
    plan.min_floor == null && plan.max_floor == null
      ? null
      : `${plan.min_floor ?? "−∞"} .. ${plan.max_floor ?? "+∞"}`,
  );
  add(
    "year_built",
    plan.min_year_built == null && plan.max_year_built == null
      ? null
      : `${plan.min_year_built ?? "−∞"} .. ${plan.max_year_built ?? "+∞"}`,
  );
  add("available_after", plan.available_from_after);
  add(
    "object_category",
    plan.object_category ? plan.object_category.join(", ") : null,
  );
  // Pass 2b (bathroom/cellar/kitchen) — show only when the LLM emitted a
  // constraint so the panel stays compact for queries that don't mention them.
  add(
    "bathrooms",
    plan.min_bathrooms == null && plan.max_bathrooms == null
      ? null
      : `${plan.min_bathrooms ?? "−∞"} .. ${plan.max_bathrooms ?? "+∞"}`,
  );
  add(
    "bathroom_shared",
    plan.bathroom_shared == null ? null : plan.bathroom_shared ? "shared" : "private",
  );
  add(
    "has_cellar",
    plan.has_cellar == null ? null : plan.has_cellar ? "required" : "excluded",
  );
  add(
    "kitchen_shared",
    plan.kitchen_shared == null ? null : plan.kitchen_shared ? "shared" : "private",
  );

  let html = rows.join("");

  const features = plan.features || [];
  const featuresExcluded = plan.features_excluded || [];
  const keywords = plan.bm25_keywords || [];

  if (features.length) {
    html += `
    <div class="kv"><span class="k">features (required)</span><span class="v"></span></div>
    <div>${features
      .map((f) => `<span class="tag hard">${esc(f)}</span>`)
      .join("")}</div>`;
  }
  if (featuresExcluded.length) {
    html += `
    <div class="kv" style="margin-top:6px"><span class="k">features_excluded</span><span class="v"></span></div>
    <div>${featuresExcluded
      .map((f) => `<span class="tag hard">¬ ${esc(f)}</span>`)
      .join("")}</div>`;
  }
  if (keywords.length) {
    html += `
    <div class="kv" style="margin-top:6px"><span class="k">bm25_keywords</span><span class="v"></span></div>
    <div>${keywords
      .map((k) => `<span class="tag hard" title="Passed to FTS5 MATCH">${esc(k)}</span>`)
      .join("")}</div>`;
  }

  if (!html) {
    html = '<p class="empty">No hard constraints extracted — ranking is driven entirely by your nice-to-haves.</p>';
  }

  els.hardView.innerHTML = html;
}

function renderSoftPrefs(plan) {
  const soft = plan && plan.soft_preferences;
  if (!soft) {
    els.softView.innerHTML =
      '<p class="empty">You didn\'t ask for any nice-to-haves this time.</p>';
    return;
  }

  const chips = [];
  const addChip = (label, on, cls = "soft") => {
    if (on) chips.push(`<span class="tag ${cls}">${esc(label)}</span>`);
  };

  if (soft.price_sentiment) {
    addChip(`price: ${soft.price_sentiment}`, true);
  }
  addChip("quiet", soft.quiet);
  addChip("near_public_transport", soft.near_public_transport);
  addChip("near_schools", soft.near_schools);
  addChip("near_supermarket", soft.near_supermarket);
  addChip("near_park", soft.near_park);
  addChip("family_friendly", soft.family_friendly);
  if (soft.commute_target) {
    addChip(`commute → ${soft.commute_target}`, true);
  }
  (soft.near_landmark || []).forEach((lm) =>
    addChip(`landmark: ${lm}`, true, "lm"),
  );

  if (chips.length === 0) {
    els.softView.innerHTML =
      '<p class="empty">No nice-to-haves activated for this search.</p>';
    return;
  }

  els.softView.innerHTML = `
    <p class="muted small" style="margin:0 0 6px 0">
      Each wish becomes its own sorted list. Homes that rank high on many lists bubble up to the top.
    </p>
    <div>${chips.join("")}</div>`;
}

function renderPipeline(pipeline, poolSize, returned) {
  if (!pipeline) {
    els.pipelineView.innerHTML =
      '<p class="empty">No search details available.</p>';
    return;
  }

  const pill = (label, on, hint) =>
    `<span class="pill ${on ? "on" : "off"}" title="${esc(hint || "")}">${
      on ? "●" : "○"
    } ${esc(label)}</span>`;

  const softCount = pipeline.soft_rankings || 0;

  els.pipelineView.innerHTML = `
    <div class="pipeline-pills">
      ${pill("Word match", pipeline.bm25, "Checks for the important words from your wish in each home's text.")}
      ${pill(
        "Photo match",
        pipeline.visual,
        "Compares your wish to every home's photos to find the ones that fit the look.",
      )}
      ${pill(
        "Meaning match",
        pipeline.text_embed,
        "Reads each home's description and compares its meaning to your wish, even if the words are different.",
      )}
      ${pill(
        `Your wishes × ${softCount}`,
        softCount > 0,
        "One sorted list per nice-to-have you asked for (quiet, near schools, cheap, and so on).",
      )}
    </div>
    <div class="kv"><span class="k">Homes we looked through</span><span class="v">${esc(
      poolSize ?? "—",
    )}</span></div>
    <div class="kv"><span class="k">Homes we're showing</span><span class="v">${esc(
      returned ?? "—",
    )}</span></div>
    <div class="kv"><span class="k">Mixer setting (RRF k)</span><span class="v">${esc(
      pipeline.rrf_k,
    )}</span></div>`;
}

// Render the per-listing scoring breakdown as normalised bars.
// We normalise per-channel across the current result page so a bar's
// fullness reflects "best in this result set" — readable at a glance.
//
// bm25 special case: when the LLM emitted no bm25_keywords at all, the
// channel has nothing to match against for any listing — we show "n/a
// (no keywords)" instead of "no match" so the user can tell the two apart.
function renderBreakdownBar(listings, queryPlan, pipeline) {
  const keywords = (queryPlan && queryPlan.bm25_keywords) || [];
  const hasKeywords = keywords.length > 0;
  const bm25MissLabel = hasKeywords ? "no match" : "not used";
  const bm25MissTooltip = hasKeywords
    ? `We didn't find any of your key words (${keywords.join(", ")}) in this home's title, description, street, or city. The other checks decide its rank.`
    : "Your search didn't need word matching, so we skipped it here. The other checks decide the rank.";

  const visualOn = pipeline && pipeline.visual;
  const semanticOn = pipeline && pipeline.text_embed;
  const visMissLabel = visualOn ? "no photo" : "turned off";
  const visMissTooltip = visualOn
    ? "This home has no photos we can compare. The other checks still rank it."
    : "The photo-match feature is off on this server. Ask an admin to turn it on.";
  const semMissLabel = semanticOn ? "—" : "turned off";
  const semMissTooltip = semanticOn
    ? "We don't have a meaning-match score for this home. That shouldn't happen for homes that have a description — ask an admin to check."
    : "The meaning-match feature is off on this server. Ask an admin to turn it on.";

  const maxRrf = Math.max(
    0.0001,
    ...listings.map((r) => r.breakdown?.rrf_score ?? 0),
  );
  const maxBm25 = Math.max(
    0.0001,
    ...listings.map((r) => r.breakdown?.bm25_score ?? 0),
  );
  const visualScores = listings
    .map((r) => r.breakdown?.visual_score)
    .filter((v) => v != null);
  const textScores = listings
    .map((r) => r.breakdown?.text_embed_score)
    .filter((v) => v != null);
  const maxVisual = visualScores.length ? Math.max(...visualScores) : 1;
  const minVisual = visualScores.length ? Math.min(...visualScores) : 0;
  const maxText = textScores.length ? Math.max(...textScores) : 1;
  const minText = textScores.length ? Math.min(...textScores) : 0;

  return function (breakdown) {
    if (!breakdown) {
      return '<div class="empty">No breakdown for this listing.</div>';
    }
    const row = (label, cls, val, norm, fmt, tooltip) => {
      const labelCell = `<div class="bar-label" title="${esc(
        tooltip,
      )}">${esc(label)}</div>`;
      if (val == null) {
        // Null → the channel didn't contribute for this listing. `fmt` is
        // the caller-supplied reason text (e.g. "no match", "n/a",
        // "no image"). Putting the tooltip on the value cell too so the
        // hover works no matter where the user mouses.
        return `${labelCell}<div class="bar"></div><div class="bar-val muted" title="${esc(
          tooltip,
        )}">${esc(fmt)}</div>`;
      }
      const pct = Math.max(0, Math.min(1, norm)) * 100;
      return `${labelCell}<div class="bar"><div class="fill ${cls}" style="width:${pct.toFixed(
        1,
      )}%"></div></div><div class="bar-val">${esc(fmt)}</div>`;
    };

    const rrfNorm = (breakdown.rrf_score ?? 0) / maxRrf;
    const bm25Norm = (breakdown.bm25_score ?? 0) / maxBm25;
    const visNorm =
      breakdown.visual_score == null
        ? 0
        : maxVisual === minVisual
        ? 1
        : (breakdown.visual_score - minVisual) / (maxVisual - minVisual);
    const txtNorm =
      breakdown.text_embed_score == null
        ? 0
        : maxText === minText
        ? 1
        : (breakdown.text_embed_score - minText) / (maxText - minText);

    const softLabel = `${breakdown.soft_signals_activated} wish${
      breakdown.soft_signals_activated === 1 ? "" : "es"
    }`;

    return `
      <div class="breakdown">
        <div class="breakdown-title">How we ranked this home</div>
        <div class="breakdown-bars">
          ${row(
            "Final score",
            "rrf",
            breakdown.rrf_score,
            rrfNorm,
            breakdown.rrf_score == null
              ? "no score"
              : breakdown.rrf_score.toFixed(4),
            breakdown.rrf_score == null
              ? "We couldn't build a final score because none of our checks had anything to say about this home."
              : "The big score — all the checks below, mixed together. Higher is better.",
          )}
          ${row(
            "Words match",
            "bm25",
            breakdown.bm25_score,
            bm25Norm,
            breakdown.bm25_score == null
              ? bm25MissLabel
              : breakdown.bm25_score.toFixed(3),
            breakdown.bm25_score == null
              ? bm25MissTooltip
              : `How well the key words from your wish show up in this home's text. Words found: ${keywords.join(", ")}.`,
          )}
          ${row(
            "Photos match",
            "visual",
            breakdown.visual_score,
            visNorm,
            breakdown.visual_score == null
              ? visMissLabel
              : breakdown.visual_score.toFixed(3),
            breakdown.visual_score == null
              ? visMissTooltip
              : "How well the photos of this home match the kind of place you described.",
          )}
          ${row(
            "Meaning match",
            "semantic",
            breakdown.text_embed_score,
            txtNorm,
            breakdown.text_embed_score == null
              ? semMissLabel
              : breakdown.text_embed_score.toFixed(3),
            breakdown.text_embed_score == null
              ? semMissTooltip
              : "How close the meaning of this home's description is to what you asked for, even with different words.",
          )}
          <div class="bar-label" title="How many of your nice-to-haves this home scored well on.">Your wishes</div>
          <div class="bar"><div class="fill soft" style="width:${
            breakdown.soft_signals_activated > 0 ? 100 : 0
          }%"></div></div>
          <div class="bar-val">${esc(softLabel)}</div>
        </div>
      </div>`;
  };
}

function renderHardChecks(checks) {
  if (!checks || checks.length === 0) {
    return '<p class="empty">You didn\'t set any must-haves, so every home is eligible.</p>';
  }
  const rows = checks
    .map(
      (c) => `
    <tr>
      <td class="mark ${c.ok ? "ok" : "miss"}">${c.ok ? "✓" : "✗"}</td>
      <td>${esc(c.label)}</td>
      <td>${esc(c.requested)}</td>
      <td>${esc(c.value)}</td>
    </tr>`,
    )
    .join("");
  return `
    <table class="check-table">
      <thead>
        <tr>
          <th></th>
          <th>must-have</th>
          <th>you asked for</th>
          <th>this home</th>
        </tr>
      </thead>
      <tbody>${rows}</tbody>
    </table>`;
}

function renderKeywordHits(md) {
  const matched = md?.matched_keywords || [];
  const missed = md?.unmatched_keywords || [];
  if (matched.length === 0 && missed.length === 0) {
    return '<p class="empty">Your search didn\'t need specific words.</p>';
  }
  const parts = [];
  if (matched.length) {
    parts.push(`<div class="kw-row">${matched
      .map((k) => `<span class="kw hit">✓ ${esc(k)}</span>`)
      .join("")}</div>`);
  }
  if (missed.length) {
    parts.push(`<div class="kw-row">${missed
      .map((k) => `<span class="kw miss" title="This word wasn't found in the home's text">${esc(k)}</span>`)
      .join("")}</div>`);
  }
  parts.push(
    `<p class="muted small" style="margin:4px 0 0 0">We checked the title, description, street, and city for each word.</p>`,
  );
  return parts.join("");
}

function renderSoftFacts(md) {
  const facts = md?.soft_facts || [];
  if (facts.length === 0) {
    return '<p class="empty">No nice-to-haves to check for this home.</p>';
  }
  return `
    <div class="soft-facts">
      ${facts
        .map(
          (f) => `
        <div class="soft-fact ${esc(f.interpretation)}">
          <span class="dot" title="${esc(f.interpretation)}"></span>
          <div class="label"><b>${esc(f.label)}</b><span>axis: <code>${esc(
            f.axis,
          )}</code></span></div>
          <div class="value">${esc(f.value)}</div>
        </div>`,
        )
        .join("")}
    </div>`;
}

// Tier 3a: per-channel memory bars. Cosines are in [-1, 1]; we map them to
// a symmetric bar where 0 is centered and ±1 fills in each direction. Missing
// channels render with a muted "—" so the user can tell "no data" apart from
// "data says 0".
function renderMemoryChannels(bd) {
  if (!bd) return "";
  const fields = [
    ["Description", "memory_semantic", "semantic",
     "Does this home's description sound like the ones you've loved or saved before?"],
    ["Photos", "memory_visual", "visual",
     "Do the photos of this home look like the photos of homes you've loved or saved?"],
    ["Features", "memory_feature", "feature",
     "Does this home have the features (balcony, elevator, pets, etc.) you usually like?"],
    ["Price", "memory_price", "price",
     "Is this home's price close to the prices of homes you usually look at?"],
    ["Overall", "memory_score", "composite",
     "The overall taste-match score — the average of the ones above. This is what nudges the ranking."],
  ];
  const anyPresent = fields.some(([_, key]) => bd[key] != null);
  if (!anyPresent) {
    return `<p class="memory-header-note empty">
      We don't know your taste for this home yet. Love or save a few homes, then search again with 🪄 <b>Learn from me</b> on.
    </p>`;
  }
  const row = (label, key, cls, tooltip) => {
    const v = bd[key];
    if (v == null) {
      return `<div class="memory-bar-row">
        <div class="label" title="${esc(tooltip)}">${esc(label)}</div>
        <div class="memory-bar"></div>
        <div class="val muted">—</div>
      </div>`;
    }
    // Map [-1, 1] → [0, 100] fill, clamped.
    const pct = Math.max(0, Math.min(100, (v + 1) * 50));
    return `<div class="memory-bar-row">
      <div class="label" title="${esc(tooltip)}">${esc(label)}</div>
      <div class="memory-bar"><div class="fill ${cls}" style="width:${pct.toFixed(1)}%"></div></div>
      <div class="val">${v.toFixed(3)}</div>
    </div>`;
  };
  return `
    <p class="memory-header-note">
      These bars show how well this home matches the homes you've loved, saved, or looked at before. Longer bar = better fit.
    </p>
    <div class="memory-channels">
      ${fields.map(([label, key, cls, tip]) => row(label, key, cls, tip)).join("")}
    </div>`;
}

function renderDetail(res) {
  const listing = res.listing || {};
  const md = res.match_detail;
  const bd = res.breakdown;
  const images = [listing.hero_image_url, ...(listing.image_urls || [])]
    .filter(Boolean)
    .filter((v, i, a) => a.indexOf(v) === i);

  const matchedCount = (md?.matched_keywords || []).length;
  const missedCount = (md?.unmatched_keywords || []).length;
  const memoryActive = bd && (bd.memory_rankings_activated || 0) > 0;

  return `
    <div class="listing-detail">
      <div class="detail-block">
        <h4>Does it match what you asked for? <span class="count">${
          (md?.hard_checks || []).length
        }</span></h4>
        ${renderHardChecks(md?.hard_checks)}

        <h4 style="margin-top:14px">Words we found in this home <span class="count">${matchedCount}/${
          matchedCount + missedCount
        }</span></h4>
        ${renderKeywordHits(md)}

        ${
          memoryActive
            ? `<h4 style="margin-top:14px">How well this fits your taste <span class="count">${
                bd.memory_rankings_activated
              }</span></h4>
               ${renderMemoryChannels(bd)}`
            : ""
        }

        ${
          similarFeatureAvailable === false
            ? ""
            : `<button type="button" class="find-similar-btn" data-similar-id="${esc(
                res.listing_id,
              )}" data-similar-title="${esc(listing.title || res.listing_id)}">
          🔍 Show me homes that look like this
        </button>`
        }
      </div>
      <div class="detail-block">
        <h4>How well it matches your nice-to-haves <span class="count">${
          (md?.soft_facts || []).length
        }</span></h4>
        ${renderSoftFacts(md)}
        ${
          images.length > 1
            ? `<h4 style="margin-top:14px">All photos <span class="count">${images.length}</span></h4>
               <div class="image-strip">${images
                 .map(
                   (u) =>
                     `<img src="${esc(u)}" alt="${esc(
                       listing.title || "",
                     )}" loading="lazy" />`,
                 )
                 .join("")}</div>`
            : ""
        }
      </div>
    </div>`;
}

function renderListings(listings, meta) {
  els.listings.innerHTML = "";
  if (!listings || listings.length === 0) {
    els.resultStatus.innerHTML =
      '<div class="coldstart-hint"><b>No homes matched your must-haves.</b> Try loosening your search — maybe a wider city, a higher price, or fewer required features.</div>';
    return;
  }

  // Tier 5a: dismissed-listings toast. Fires only when personalization
  // actually removed something (anonymous callers never see this).
  const hiddenDismissed = Number(meta.hidden_dismissed || 0);
  const dismissedToast = hiddenDismissed > 0
    ? `<div class="dismissed-toast">
         <span><span class="ico">🙈</span> ${hiddenDismissed} home${
           hiddenDismissed === 1 ? "" : "s"
         } hidden because ${
           hiddenDismissed === 1 ? "it's like one" : "they're like ones"
         } you told us to skip.</span>
         <span class="muted small">Turn off 🪄 Learn from me to see ${
           hiddenDismissed === 1 ? "it" : "them"
         } again.</span>
       </div>`
    : "";

  // Tier 5b: cold-start coaching. Authenticated user, personalize on, but
  // zero memory rankings fired — means they haven't liked / saved enough
  // listings yet to build a profile.
  const pipeline = meta.pipeline || {};
  const authed = !!(authState && authState.user);
  const wantsPersonalize = !!(els.personalizeToggle && els.personalizeToggle.checked);
  const memoryInactive = authed && wantsPersonalize && !pipeline.memory;
  const coldStart = memoryInactive
    ? `<div class="coldstart-hint">
         <b>🪄 Learn from me is on, but we don't know your taste yet.</b>
         Press <b>💖 Love</b> or <b>⭐ Save</b> on at least <b>3 homes</b> you like, then search again. We'll start showing you more of what fits your taste.
       </div>`
    : "";

  els.resultStatus.innerHTML = `
    ${dismissedToast}
    ${coldStart}
    Found <b>${listings.length} homes</b> (out of ${
    meta.candidate_pool_size ?? "?"
  } that passed your must-haves).
    <span class="sort-hint">Best match first · tap any card for the full breakdown</span>
  `;

  const barFactory = renderBreakdownBar(
    listings,
    meta.query_plan || null,
    meta.pipeline || null,
  );

  // Batch-level "is this an unranked random selection?" decision.
  // See `isUnscoredBatch` for the rule and rationale.
  const unscored = isUnscoredBatch(listings, meta);

  dwellTracker.reset();

  listings.forEach((res, idx) => {
    const listing = res.listing || {};
    const images = [listing.hero_image_url, ...(listing.image_urls || [])]
      .filter(Boolean)
      .filter((v, i, a) => a.indexOf(v) === i);
    // "TOP" + gold border only make sense when the ranking is real.
    // Suppress both on an unscored (random / anon default) feed.
    const isTop = !unscored && idx === 0;
    const listingId = String(res.listing_id);
    const isLiked = authState.likedIds.has(listingId);
    const isBookmarked = authState.bookmarkedIds.has(listingId);
    const isDismissed = authState.dismissedIds.has(listingId);
    const memBoost =
      res.breakdown && res.breakdown.memory_rankings_activated > 0;

    const card = document.createElement("div");
    card.className = `listing-card${isTop ? " top" : ""}${
      isDismissed ? " dismissed" : ""
    }${memBoost ? " memory-boosted" : ""}`;
    card.dataset.listingId = listingId;
    card.setAttribute("role", "button");
    card.setAttribute("tabindex", "0");
    card.setAttribute("aria-expanded", "false");
    card.innerHTML = `
      <div class="listing-card-summary">
        <div class="listing-image" data-idx="0" data-count="${images.length}">
          ${
            images.length
              ? `<img src="${esc(images[0])}" alt="${esc(listing.title)}" loading="lazy" />
                ${
                  images.length > 1
                    ? `<button class="img-nav prev" aria-label="Previous photo">‹</button>
                       <button class="img-nav next" aria-label="Next photo">›</button>
                       <div class="img-count">1 / ${images.length}</div>`
                    : ""
                }`
              : '<div class="no-image">No photo</div>'
          }
        </div>
        <div class="listing-body">
          <div class="listing-head">
            <h3 class="listing-title">${esc(listing.title || "(no title)")}</h3>
            ${
              unscored
                ? ""
                : `<div class="rank-score">
              <div class="rank ${isTop ? "top" : ""}">#${idx + 1}${
                    isTop ? '<span class="rank-badge-top">TOP</span>' : ""
                  }</div>
              <div class="score" title="Final RRF score">${res.score.toFixed(
                3,
              )}</div>
            </div>`
            }
          </div>
          ${
            authState.user || memBoost
              ? `${renderListingActions(listingId, { variant: "card" })}${
                  memBoost
                    ? `<div class="memory-badge" title="We picked this one because it fits your taste">✨ picked for you</div>`
                    : ""
                }`
              : ""
          }
          <div class="listing-meta">
            <strong>${chf(listing.price_chf)}</strong>
            · ${listing.rooms == null ? "?" : esc(listing.rooms)} rooms
            · ${
              listing.living_area_sqm == null
                ? "?"
                : esc(listing.living_area_sqm) + " m²"
            }
            · ${esc(listing.city || "")}${
              listing.postal_code ? " " + esc(listing.postal_code) : ""
            }${listing.canton ? ", " + esc(listing.canton) : ""}
            ${
              listing.object_category
                ? `· ${esc(listing.object_category)}`
                : ""
            }
          </div>
          ${renderAddressBlock(listing, { cls: "listing-address summary" })}
          ${renderNearbyLandmarks(listing, { variant: "card", limit: 3 })}
          ${
            listing.description
              ? `<div class="listing-desc">${sanitizeDescriptionHtml(listing.description)}</div>`
              : '<div class="listing-desc empty">No description on file.</div>'
          }
          ${
            (listing.features || []).length
              ? `<div class="listing-features">${listing.features
                  .map((f) => `<span class="feat">${esc(f)}</span>`)
                  .join("")}</div>`
              : ""
          }
          ${(() => {
            const bc = bathroomChips(listing);
            return bc ? `<div class="listing-enriched">${bc}</div>` : "";
          })()}
          ${barFactory(res.breakdown)}
          <div class="reason-line">${esc(res.reason)}</div>
          <div class="listing-links">
            listing_id: <kbd>${esc(res.listing_id)}</kbd>
            ${
              listing.original_listing_url
                ? ` · <a href="${esc(
                    listing.original_listing_url,
                  )}" target="_blank" rel="noopener">source</a>`
                : ""
            }
          </div>
          <div class="expand-hint">Tap for the full breakdown ↓</div>
        </div>
      </div>`;

    // wire image carousel (summary tile)
    if (images.length > 1) {
      const imgEl = card.querySelector(".listing-image img");
      const countEl = card.querySelector(".img-count");
      let imgIdx = 0;
      const go = (delta, ev) => {
        if (ev) ev.stopPropagation();
        imgIdx = (imgIdx + delta + images.length) % images.length;
        imgEl.src = images[imgIdx];
        countEl.textContent = `${imgIdx + 1} / ${images.length}`;
      };
      card
        .querySelector(".img-nav.prev")
        .addEventListener("click", (ev) => go(-1, ev));
      card
        .querySelector(".img-nav.next")
        .addEventListener("click", (ev) => go(1, ev));
    }

    // stop clicks on the source link / listing_id from toggling expansion
    card.querySelectorAll(".listing-links a, kbd").forEach((n) =>
      n.addEventListener("click", (ev) => ev.stopPropagation()),
    );

    // Like / save / dismiss buttons never expand the card.
    wireListingActions(card, { cardEl: card });

    // expand/collapse wiring — detail panel is lazily mounted on first open
    const toggle = () => {
      const already = card.classList.contains("expanded");
      if (already) {
        card.classList.remove("expanded");
        card.setAttribute("aria-expanded", "false");
        const detail = card.querySelector(".listing-detail");
        if (detail) detail.remove();
        card.querySelector(".expand-hint").textContent =
          "Tap for the full breakdown ↓";
      } else {
        card.classList.add("expanded");
        card.setAttribute("aria-expanded", "true");
        card.insertAdjacentHTML("beforeend", renderDetail(res));
        card.querySelector(".expand-hint").textContent =
          "Tap again to close ↑";
        // Implicit positive: card was deliberately expanded.
        postInteraction(listingId, "click");
        // Tier 4: wire the "Find similar" button now that it exists in the DOM.
        const similarBtn = card.querySelector(".find-similar-btn");
        if (similarBtn) {
          similarBtn.addEventListener("click", (ev) => {
            ev.stopPropagation();  // don't collapse the card
            openSimilarModal(
              similarBtn.dataset.similarId,
              similarBtn.dataset.similarTitle,
            );
          });
        }
      }
    };

    card.addEventListener("click", toggle);
    card.addEventListener("keydown", (ev) => {
      if (ev.key === "Enter" || ev.key === " ") {
        ev.preventDefault();
        toggle();
      }
    });

    els.listings.appendChild(card);
    // Start watching for the dwell signal only for logged-in users; it's
    // wasted work for anonymous visitors.
    if (authState.user) dwellTracker.watch(card);
  });
}

// ---------- auth + interactions ---------------------------------------------

async function ensureCsrf() {
  // Called before every state-changing call. Cheap (itsdangerous sign) and
  // refreshed server-side on each /auth/csrf hit, so we simply refetch when
  // we don't have one cached.
  if (authState.csrfToken) return authState.csrfToken;
  const r = await fetch("/auth/csrf", { credentials: "same-origin" });
  if (!r.ok) throw new Error(`/auth/csrf returned ${r.status}`);
  const body = await r.json();
  authState.csrfToken = body.csrf_token;
  return authState.csrfToken;
}

async function authJson(path, init) {
  const opts = Object.assign({ credentials: "same-origin" }, init || {});
  opts.headers = Object.assign({}, opts.headers || {});
  if (opts.body && !opts.headers["content-type"]) {
    opts.headers["content-type"] = "application/json";
  }
  const mutating = ["POST", "PUT", "DELETE", "PATCH"].includes(
    (opts.method || "GET").toUpperCase(),
  );
  // Never send CSRF to /auth/login or /auth/register — the session cookie
  // doesn't exist yet so double-submit can't meaningfully protect those.
  const skipCsrf =
    path === "/auth/login" || path === "/auth/register";
  if (mutating && !skipCsrf) {
    const tok = await ensureCsrf();
    opts.headers["X-CSRF-Token"] = tok;
  }
  const r = await fetch(path, opts);
  return r;
}

async function fetchWhoami() {
  try {
    const r = await fetch("/auth/me", { credentials: "same-origin" });
    if (!r.ok) return null;
    return await r.json();
  } catch {
    return null;
  }
}

async function fetchListSet(path) {
  const r = await fetch(path, { credentials: "same-origin" });
  if (!r.ok) return [];
  const body = await r.json();
  return body.favorites || [];
}

function setAuthUI(user) {
  authState.user = user;
  if (user) {
    els.authAnon.hidden = true;
    els.authUser.hidden = false;
    els.authUsername.textContent = user.username;
  } else {
    els.authAnon.hidden = false;
    els.authUser.hidden = true;
    authState.likedIds.clear();
    authState.bookmarkedIds.clear();
    authState.dismissedIds.clear();
    updateBookmarksCount();
  }
}

function updateBookmarksCount() {
  if (els.favoritesCount) {
    els.favoritesCount.textContent = String(authState.bookmarkedIds.size);
  }
}

async function hydrateAuthState() {
  const user = await fetchWhoami();
  setAuthUI(user);
  if (user) {
    // Prime CSRF so the next like/save click doesn't pay a round-trip.
    try { await ensureCsrf(); } catch { /* non-fatal */ }
    const [likes, bookmarks, dismissed] = await Promise.all([
      fetchListSet("/me/likes"),
      fetchListSet("/me/favorites"),
      fetchDismissedIds(),
    ]);
    authState.likedIds = new Set(likes.map((f) => f.listing_id));
    authState.bookmarkedIds = new Set(bookmarks.map((f) => f.listing_id));
    authState.dismissedIds = new Set(dismissed);
    updateBookmarksCount();
  }
}

async function fetchDismissedIds() {
  try {
    const r = await fetch("/me/dismissed", { credentials: "same-origin" });
    if (!r.ok) return [];
    const body = await r.json();
    return Array.isArray(body) ? body : [];
  } catch {
    return [];
  }
}

function openAuthModal(mode) {
  if (!els.authModal) return;
  setAuthMode(mode);
  if (typeof els.authModal.showModal === "function") {
    els.authModal.showModal();
  } else {
    els.authModal.setAttribute("open", "");
  }
}

function closeAuthModal() {
  if (!els.authModal) return;
  if (typeof els.authModal.close === "function") {
    els.authModal.close();
  } else {
    els.authModal.removeAttribute("open");
  }
}

function setAuthMode(mode) {
  const showField = (name, on) => {
    const el = els.authModal.querySelector(`[data-auth-field="${name}"]`);
    if (!el) return;
    el.hidden = !on;
    // Disable inputs inside hidden wrappers so their `required` attribute
    // doesn't block form submission in the other modes. Browsers run HTML5
    // constraint validation on every non-disabled control regardless of
    // whether an ancestor is `hidden`, and the resulting "please fill in
    // this field" tooltip can't anchor to a hidden input — so the form just
    // fails to submit with no visible feedback. Toggling `disabled` pulls
    // the control out of validation AND out of the submitted form data.
    el.querySelectorAll("input").forEach((inp) => {
      inp.disabled = !on;
    });
  };
  // Reset everything then selectively show.
  ["username", "email", "password", "new-password", "password-hint", "account-actions"].forEach(
    (n) => showField(n, false),
  );
  els.authError.hidden = true;
  els.authError.textContent = "";
  els.authModal.dataset.mode = mode;

  // Don't leak sensitive input across mode switches.
  const pwInput = document.getElementById("auth-password-input");
  const newPwInput = document.getElementById("auth-new-password-input");
  if (pwInput) pwInput.value = "";
  if (newPwInput) newPwInput.value = "";
  // Default the password label - overridden below for account mode.
  const pwLabel = els.authModal.querySelector('label[for="auth-password-input"]');
  if (pwLabel) pwLabel.textContent = "Password";
  // In account mode the username isn't editable; prefill with the current
  // logged-in user's name so the modal feels contextual (even though the
  // username field itself stays hidden in this mode).
  const userInput = document.getElementById("auth-username-input");
  if (userInput && mode !== "account") userInput.value = "";

  if (mode === "login") {
    els.authModalTitle.textContent = "Log in";
    showField("username", true);
    showField("password", true);
    document.getElementById("auth-password-input").autocomplete = "current-password";
    els.authSubmit.textContent = "Log in";
  } else if (mode === "register") {
    els.authModalTitle.textContent = "Create your account";
    showField("username", true);
    showField("email", true);
    showField("password", true);
    showField("password-hint", true);
    document.getElementById("auth-password-input").autocomplete = "new-password";
    els.authSubmit.textContent = "Sign up";
  } else if (mode === "account") {
    els.authModalTitle.textContent = "⚙ Account";
    showField("password", true);
    showField("new-password", true);
    showField("account-actions", true);
    document.getElementById("auth-password-input").autocomplete = "current-password";
    if (pwLabel) pwLabel.textContent = "Current password";
    els.authSubmit.textContent = "Change password";
  }

  // Tabs only make sense when we're choosing between sign-in and sign-up.
  // In "account" mode (reached only while logged in) they would offer to
  // switch to unrelated flows, so hide the whole bar.
  if (els.authTabs) {
    els.authTabs.hidden = mode === "account";
    els.authTabs.querySelectorAll(".tab").forEach((t) => {
      t.classList.toggle("active", t.dataset.authMode === mode);
    });
  }
}

function setAuthError(msg) {
  els.authError.hidden = false;
  els.authError.textContent = msg;
}

async function handleAuthSubmit(ev) {
  ev.preventDefault();
  const mode = els.authModal.dataset.mode || "login";
  const username = document.getElementById("auth-username-input").value.trim();
  const password = document.getElementById("auth-password-input").value;
  els.authError.hidden = true;
  els.authSubmit.disabled = true;
  try {
    if (mode === "login") {
      const r = await authJson("/auth/login", {
        method: "POST",
        body: JSON.stringify({ username, password }),
      });
      if (r.status === 429) {
        setAuthError("Too many tries. Wait a few minutes, then try again.");
        return;
      }
      if (!r.ok) {
        setAuthError("That username or password isn't right.");
        return;
      }
      const user = await r.json();
      setAuthUI(user);
      // Refresh CSRF (cookie rotated) and the liked/saved sets.
      authState.csrfToken = null;
      await ensureCsrf();
      const [likes, bookmarks, dismissed] = await Promise.all([
        fetchListSet("/me/likes"),
        fetchListSet("/me/favorites"),
        fetchDismissedIds(),
      ]);
      authState.likedIds = new Set(likes.map((f) => f.listing_id));
      authState.bookmarkedIds = new Set(bookmarks.map((f) => f.listing_id));
      authState.dismissedIds = new Set(dismissed);
      updateBookmarksCount();
      closeAuthModal();
    } else if (mode === "register") {
      const email = document.getElementById("auth-email-input").value.trim();
      const r = await authJson("/auth/register", {
        method: "POST",
        body: JSON.stringify({ username, email, password }),
      });
      if (r.status === 409) {
        setAuthError("That name or email is already taken. Try a different one.");
        return;
      }
      if (!r.ok) {
        const body = await r.json().catch(() => ({}));
        setAuthError(formatValidationError(body));
        return;
      }
      const user = await r.json();
      setAuthUI(user);
      authState.csrfToken = null;
      await ensureCsrf();
      closeAuthModal();
    } else if (mode === "account") {
      const newPassword = document.getElementById("auth-new-password-input").value;
      if (!newPassword) {
        setAuthError("Type your new password.");
        return;
      }
      const r = await authJson("/auth/change-password", {
        method: "POST",
        body: JSON.stringify({
          current_password: password,
          new_password: newPassword,
        }),
      });
      if (r.status === 401) {
        setAuthError("Your current password isn't right.");
        return;
      }
      if (!r.ok) {
        const body = await r.json().catch(() => ({}));
        setAuthError(formatValidationError(body));
        return;
      }
      // Session was rotated server-side - refresh CSRF.
      authState.csrfToken = null;
      await ensureCsrf();
      closeAuthModal();
    }
  } catch (e) {
    setAuthError(`Can't reach the server: ${e.message}`);
  } finally {
    els.authSubmit.disabled = false;
  }
}

function formatValidationError(body) {
  if (!body) return "Something went wrong.";
  if (typeof body.detail === "string") return body.detail;
  if (Array.isArray(body.detail) && body.detail.length) {
    const first = body.detail[0];
    if (first && first.msg) {
      const loc = (first.loc || []).slice(-1)[0] || "input";
      return `${loc}: ${first.msg}`;
    }
  }
  return "Validation failed.";
}

async function handleLogout() {
  try {
    await authJson("/auth/logout", { method: "POST" });
  } catch {
    // ignore - we clear the UI regardless
  }
  authState.csrfToken = null;
  setAuthUI(null);
}

async function handleDeleteAccount() {
  const password = document.getElementById("auth-password-input").value;
  if (!password) {
    setAuthError("Type your current password to confirm.");
    return;
  }
  if (!window.confirm("Really delete your account, your saved homes, and everything we learned about your taste? This can't be undone.")) {
    return;
  }
  try {
    const r = await authJson("/auth/delete-account", {
      method: "POST",
      body: JSON.stringify({ password }),
    });
    if (r.status === 401) {
      setAuthError("That password isn't right.");
      return;
    }
    if (!r.ok) {
      setAuthError("Couldn't delete your account.");
      return;
    }
    authState.csrfToken = null;
    setAuthUI(null);
    closeAuthModal();
  } catch (e) {
    setAuthError(`Can't reach the server: ${e.message}`);
  }
}

async function postInteraction(listingId, kind, value) {
  if (!authState.user) return;  // silently ignored for anonymous
  const r = await authJson("/me/interactions", {
    method: "POST",
    body: JSON.stringify(
      value == null
        ? { listing_id: listingId, kind }
        : { listing_id: listingId, kind, value },
    ),
  });
  if (!r.ok) {
    // Throw so optimistic-UI callers can roll back. Dwell beacons and other
    // fire-and-forget writes already ignore the returned promise, so a
    // throw here is harmless for them.
    const text = await r.text().catch(() => "");
    throw new Error(`interaction write failed: HTTP ${r.status} ${text}`);
  }
}

// Generic optimistic toggle used by both the "like" and the "bookmark"
// buttons. The two differ only in (1) which Set tracks client state, (2)
// which "kind" pair we write to the server, and (3) how the button re-paints.
async function _toggleInteraction({
  listingId,
  buttonEl,
  stateSet,
  positiveKind,
  negativeKind,
  render,
  onAfterChange,
}) {
  if (!authState.user) {
    openAuthModal("login");
    setAuthError("Log in so we can learn from your choices.");
    return;
  }
  const wasOn = stateSet.has(listingId);
  const kind = wasOn ? negativeKind : positiveKind;
  if (wasOn) stateSet.delete(listingId);
  else stateSet.add(listingId);
  render(buttonEl, !wasOn);
  if (onAfterChange) onAfterChange();
  try {
    await postInteraction(listingId, kind);
  } catch {
    // Rollback optimistic UI.
    if (wasOn) stateSet.add(listingId);
    else stateSet.delete(listingId);
    render(buttonEl, wasOn);
    if (onAfterChange) onAfterChange();
  }
}

function toggleLike(listingId, buttonEl) {
  return _toggleInteraction({
    listingId,
    buttonEl,
    stateSet: authState.likedIds,
    positiveKind: "like",
    negativeKind: "unlike",
    render: renderLikeButton,
  });
}

function toggleBookmark(listingId, buttonEl) {
  return _toggleInteraction({
    listingId,
    buttonEl,
    stateSet: authState.bookmarkedIds,
    positiveKind: "bookmark",
    negativeKind: "unbookmark",
    render: renderBookmarkButton,
    onAfterChange: updateBookmarksCount,
  });
}

function toggleDismiss(listingId, buttonEl, cardEl) {
  if (!authState.user) {
    openAuthModal("login");
    setAuthError("Log in to hide homes you don't want to see.");
    return;
  }
  const wasDismissed = authState.dismissedIds.has(listingId);
  // Optimistic UI toggle; rolled back on server failure.
  const apply = (dismissed) => {
    if (dismissed) {
      authState.dismissedIds.add(listingId);
      if (cardEl) {
        cardEl.classList.add("dismissed");
        // Keep the card focusable so the user can still click the undo button.
        cardEl.setAttribute("aria-hidden", "false");
      }
    } else {
      authState.dismissedIds.delete(listingId);
      if (cardEl) {
        cardEl.classList.remove("dismissed");
        cardEl.setAttribute("aria-hidden", "false");
      }
    }
    renderDismissButton(buttonEl, dismissed);
  };
  apply(!wasDismissed);
  const kind = wasDismissed ? "undismiss" : "dismiss";
  postInteraction(listingId, kind).catch((err) => {
    apply(wasDismissed);
    // Surface the failure so the silent snap-back doesn't look like a bug.
    // Most common cause: running server was started before the ``undismiss``
    // kind was added to the backend schema; it now rejects with 422.
    const verb = kind === "dismiss" ? "hide" : "bring back";
    setStatus(`Couldn't ${verb} (try reloading)`, "err");
    console.warn(`${kind} failed`, err);
  });
}

function renderDismissButton(buttonEl, dismissed) {
  if (!buttonEl) return;
  buttonEl.classList.toggle("dismissed", !!dismissed);
  buttonEl.setAttribute("aria-pressed", dismissed ? "true" : "false");
  buttonEl.title = dismissed
    ? "Bring it back"
    : "Hide this home (we won't show it again)";
  buttonEl.textContent = dismissed ? "↩️ Bring back" : "🙈 Hide";
}

function renderLikeButton(buttonEl, liked) {
  if (!buttonEl) return;
  buttonEl.classList.toggle("liked", !!liked);
  buttonEl.setAttribute("aria-pressed", liked ? "true" : "false");
  buttonEl.title = liked
    ? "Tap to un-love it (we'll stop looking for similar homes)"
    : "I love this one! (we'll show you more like it)";
  buttonEl.textContent = liked ? "💖 Loved" : "♡ Love";
}

// ---------- shared interaction-button helpers ------------------------------
//
// Every listing presented in the UI (main cards, detail modal, look-alike
// tiles, favorites drawer) should expose the same three interaction controls
// to a logged-in user: Love, Save, Hide. Anonymous users see nothing.
// Rendering + wiring were previously inlined only in `renderListings`; this
// helper pair consolidates both so every render surface stays in lock-step.
//
// Visual variants:
//   card     full pill buttons with text + icon (used on the main result
//            cards and in the detail modal header; action is the primary
//            secondary action of the surface)
//   compact  icon-only pills, 28 px tall (used on tiles + drawer rows where
//            real estate is tight)
//
// Safety: `stopPropagation()` on every click prevents the button from
// triggering the surface's own click handler (card expand, modal open).
// Wiring is idempotent (marker via a WeakSet) so calling it twice on the
// same DOM subtree never double-binds.

const _wiredActionContainers = new WeakSet();

function renderListingActions(listingId, { variant = "card" } = {}) {
  if (!authState || !authState.user) return "";
  const id = String(listingId);
  const isLiked = authState.likedIds.has(id);
  const isBookmarked = authState.bookmarkedIds.has(id);
  const isDismissed = authState.dismissedIds.has(id);
  const compact = variant === "compact";

  const btn = (action, state, labelOn, labelOff, titleOn, titleOff, iconOn, iconOff) => {
    const active = !!state;
    const classActive =
      action === "like" ? " liked"
      : action === "save" ? " saved"
      : action === "dismiss" ? " dismissed"
      : "";
    const body = compact
      ? (active ? iconOn : iconOff)
      : (active ? `${iconOn} ${labelOn}` : `${iconOff} ${labelOff}`);
    return `<button type="button" class="listing-action ${action}-btn${active ? classActive : ""}"
              aria-pressed="${active}" data-action="${action}"
              title="${esc(active ? titleOn : titleOff)}">${body}</button>`;
  };

  const love = btn(
    "like", isLiked,
    "Loved", "Love",
    "Tap to un-love it (we'll stop looking for similar homes)",
    "I love this one! (we'll show you more like it)",
    "💖", "♡",
  );
  const save = btn(
    "save", isBookmarked,
    "Saved", "Save",
    "Take it off your favorites",
    "Save it to your favorites (we'll show more like it)",
    "⭐", "☆",
  );
  const hide = btn(
    "dismiss", isDismissed,
    "Bring back", "Hide",
    "Bring it back",
    "Hide this home (we won't show it again)",
    "↩️", "🙈",
  );

  return `<div class="listing-actions variant-${variant}" data-listing-id="${esc(id)}">
    ${love}${save}${hide}
  </div>`;
}

function wireListingActions(root, { cardEl = null, onDismissed = null } = {}) {
  if (!root) return;
  root.querySelectorAll(".listing-actions").forEach((box) => {
    if (_wiredActionContainers.has(box)) return;
    _wiredActionContainers.add(box);
    const listingId = box.dataset.listingId;
    if (!listingId) return;
    const card = cardEl ?? box.closest(".listing-card");
    const likeBtn = box.querySelector('[data-action="like"]');
    const saveBtn = box.querySelector('[data-action="save"]');
    const dismissBtn = box.querySelector('[data-action="dismiss"]');
    if (likeBtn) {
      likeBtn.addEventListener("click", (ev) => {
        ev.stopPropagation();
        toggleLike(listingId, likeBtn);
      });
    }
    if (saveBtn) {
      saveBtn.addEventListener("click", (ev) => {
        ev.stopPropagation();
        toggleBookmark(listingId, saveBtn);
      });
    }
    if (dismissBtn) {
      dismissBtn.addEventListener("click", (ev) => {
        ev.stopPropagation();
        toggleDismiss(listingId, dismissBtn, card);
        if (onDismissed) onDismissed(listingId);
      });
    }
  });
}

function renderBookmarkButton(buttonEl, saved) {
  if (!buttonEl) return;
  buttonEl.classList.toggle("saved", !!saved);
  buttonEl.setAttribute("aria-pressed", saved ? "true" : "false");
  buttonEl.title = saved
    ? "Take it off your favorites"
    : "Save it to your favorites (we'll show more like it)";
  buttonEl.textContent = saved ? "⭐ Saved" : "☆ Save";
}

async function openFavorites() {
  if (!authState.user) return;
  const favs = await fetchListSet("/me/favorites");
  authState.bookmarkedIds = new Set(favs.map((f) => f.listing_id));
  updateBookmarksCount();
  const host = els.favoritesList;
  if (!favs.length) {
    host.innerHTML =
      '<p class="empty-state"><span class="empty-ico">💭</span>No saved homes yet. Tap <b>⭐ Save</b> on any card to add it here.</p>';
  } else {
    host.innerHTML = favs.map(renderFavoriteRow).join("");
    // Wire the inline Love / Save / Hide buttons on each row. Must fire
    // BEFORE the delegated "click row → open detail" below, because the
    // action buttons call stopPropagation() — relying on DOM ordering would
    // be fragile. Wiring is idempotent (WeakSet marker).
    wireListingActions(host);
    // Wire click → detail modal. Using event delegation on the list host
    // means we don't pay an addEventListener per card (matters if the
    // drawer ever holds hundreds of bookmarks).
    host.onclick = (ev) => {
      // Let the action buttons' own stopPropagation take effect first.
      if (ev.target.closest(".listing-actions")) return;
      const card = ev.target.closest(".fav-card");
      if (!card) return;
      const lid = card.dataset.listingId;
      if (!lid) return;
      openListingDetail(lid);
    };
  }
  if (typeof els.favoritesModal.showModal === "function") {
    els.favoritesModal.showModal();
  } else {
    els.favoritesModal.setAttribute("open", "");
  }
}

function renderFavoriteRow(f) {
  const price = chf(f.price_chf);
  const rooms = f.rooms == null ? "— rooms" : `${f.rooms} rooms`;
  const area = f.area_sqm == null ? null : `${f.area_sqm} m²`;
  const place = [f.city, f.canton].filter(Boolean).join(", ");
  // Show the three most telling feature chips; anything beyond feels noisy
  // in a compact row.
  const chips = (f.features || []).slice(0, 3);
  return `
    <div class="fav-card" role="button" tabindex="0"
         data-listing-id="${esc(f.listing_id)}"
         aria-label="Open details for ${esc(f.title || f.listing_id)}">
      <div class="fav-thumb">
        ${
          f.hero_image_url
            ? `<img src="${esc(f.hero_image_url)}" alt="${esc(f.title || "saved home")}" loading="lazy" />`
            : '<div class="fav-thumb-empty">No photo</div>'
        }
      </div>
      <div class="fav-body">
        <div class="fav-title">${esc(f.title || "(no title)")}</div>
        <div class="fav-meta">
          <strong>${price}</strong>
          <span>· ${esc(rooms)}</span>
          ${area ? `<span>· ${esc(area)}</span>` : ""}
          ${
            f.object_category
              ? `<span>· ${esc(f.object_category)}</span>`
              : ""
          }
        </div>
        ${place ? `<div class="fav-place muted small">${esc(place)}</div>` : ""}
        ${
          chips.length
            ? `<div class="fav-chips">${chips
                .map((c) => `<span class="chip-sm">${esc(c)}</span>`)
                .join("")}</div>`
            : ""
        }
        ${renderListingActions(f.listing_id, { variant: "compact" })}
        <div class="fav-footer muted small">
          Saved ${esc(f.saved_at.slice(0, 10))} · <kbd>${esc(f.listing_id)}</kbd>
        </div>
      </div>
    </div>
  `;
}

function closeFavorites() {
  if (typeof els.favoritesModal.close === "function") {
    els.favoritesModal.close();
  } else {
    els.favoritesModal.removeAttribute("open");
  }
}

// ---------- listing-detail modal --------------------------------------------

async function openListingDetail(listingId) {
  if (!els.detailModal) return;
  // Show the modal first with a loading placeholder so the click feels
  // instant; we fill in the real content when the fetch resolves.
  els.detailBody.innerHTML = '<p class="muted">Loading…</p>';
  els.detailTitle.textContent = "Listing";
  // `showModal()` throws InvalidStateError on an already-open <dialog>, which
  // is exactly what happens if another flow (e.g. clicking a similar-card
  // from the look-alike grid) re-enters here while the detail modal is still
  // open. Re-render in place in that case. Also reset the body scroll so the
  // user doesn't land mid-way through the previous listing's description.
  if (!els.detailModal.open) {
    if (typeof els.detailModal.showModal === "function") {
      els.detailModal.showModal();
    } else {
      els.detailModal.setAttribute("open", "");
    }
  }
  if (els.detailBody) els.detailBody.scrollTop = 0;

  let data;
  try {
    const r = await fetch(`/listings/${encodeURIComponent(listingId)}`, {
      credentials: "same-origin",
    });
    if (!r.ok) {
      els.detailBody.innerHTML = `<p class="empty">Couldn't load this home (error ${r.status}).</p>`;
      return;
    }
    data = await r.json();
  } catch (e) {
    els.detailBody.innerHTML = `<p class="empty">Can't reach the server: ${esc(e.message)}</p>`;
    return;
  }
  renderListingDetail(data);
}

function closeDetail() {
  if (!els.detailModal) return;
  if (typeof els.detailModal.close === "function") {
    els.detailModal.close();
  } else {
    els.detailModal.removeAttribute("open");
  }
}

// ---------- Tier 4: DINOv2 "find similar" modal ----------------------------
// Image-to-image reverse search. Fetches /listings/{id}/similar and renders
// the results as a tiled grid. Each card is clickable to open the listing
// detail modal (stack-style: closing Similar returns to the source result).

async function openSimilarModal(listingId, sourceTitle) {
  if (!els.similarModal) return;
  els.similarTitle.textContent = sourceTitle
    ? `🔍 Homes that look like "${sourceTitle}"`
    : "🔍 Homes that look like this";
  els.similarBody.innerHTML = '<p class="muted">Looking for homes that look like this…</p>';
  if (typeof els.similarModal.showModal === "function") {
    els.similarModal.showModal();
  } else {
    els.similarModal.setAttribute("open", "");
  }

  let data;
  try {
    const r = await fetch(
      `/listings/${encodeURIComponent(listingId)}/similar?k=12`,
      { credentials: "same-origin" },
    );
    if (r.status === 503) {
      // Feature confirmed off on this server (usually: DINOv2 store not
      // built yet). Remember it and hide the button on every card so the
      // next click never reaches this branch.
      similarFeatureAvailable = false;
      hideAllSimilarButtons();
      console.warn(
        "[WARN] look_alike_feature_off: " +
          "expected=/listings/{id}/similar returns 2xx, got=HTTP 503, " +
          "fallback=hide the 'Show me homes that look like this' button for this session",
      );
      els.similarBody.innerHTML = `<p class="empty">The look-alike-homes feature isn't set up on this server yet, so we'll hide this option for now.</p>`;
      return;
    }
    if (!r.ok) {
      els.similarBody.innerHTML = `<p class="empty">Couldn't load look-alikes (error ${r.status}).</p>`;
      return;
    }
    similarFeatureAvailable = true;
    data = await r.json();
  } catch (e) {
    els.similarBody.innerHTML = `<p class="empty">Can't reach the server: ${esc(e.message)}</p>`;
    return;
  }

  const results = data.results || [];
  if (!results.length) {
    const note = (data.meta && data.meta.note) || "No homes with similar-looking photos found.";
    els.similarBody.innerHTML = `<p class="empty">${esc(note)}</p>`;
    return;
  }

  const meta = data.meta || {};
  const metaLine = `<p class="muted small" style="margin:0 0 10px 0">
    We found ${esc(meta.k_returned ?? results.length)} look-alikes (out of ${esc(meta.k_requested ?? "?")} we checked). Tap any one to see it in full.
  </p>`;

  const cards = results.map((r) => {
    const L = r.listing || {};
    const img = L.hero_image_url || (L.image_urls || [])[0] || "";
    // `cosine` is now the raw DINOv2 cosine in [-1, 1]. A positive value gets
    // rendered as "match X%"; 0.0 (result outside the image index) is hidden
    // so we never show a misleading "match 0%".
    const cos = typeof r.cosine === "number" ? r.cosine : 0;
    const cosineChip = cos > 0
      ? `<div class="cosine">match ${(cos * 100).toFixed(0)}%</div>`
      : "";
    return `<div class="similar-card" data-listing-id="${esc(r.listing_id)}">
      ${img ? `<img src="${esc(img)}" alt="${esc(L.title || "")}" loading="lazy" />` : ""}
      <div class="body">
        <div class="title">${esc(L.title || r.listing_id)}</div>
        <div class="meta">${esc(chf(L.price_chf))} · ${esc(L.rooms ?? "?")} rooms · ${esc(L.city || "")}</div>
        ${cosineChip}
        ${renderListingActions(r.listing_id, { variant: "compact" })}
      </div>
    </div>`;
  }).join("");

  els.similarBody.innerHTML = metaLine + `<div class="similar-list">${cards}</div>`;

  // Wire the interaction buttons on each tile FIRST so their click handlers
  // call stopPropagation() before the tile's own click (which would close the
  // modal and open the detail view).
  wireListingActions(els.similarBody);

  els.similarBody.querySelectorAll(".similar-card").forEach((node) => {
    node.addEventListener("click", () => {
      const otherId = node.dataset.listingId;
      if (!otherId) return;
      // Close the look-alike modal before navigating into the result so the
      // detail dialog sits at the top of the dialog stack (not buried behind
      // the similar-results grid, which locks scroll + pointer events to
      // the back layer). See openListingDetail for the matching guard that
      // lets the detail modal be re-rendered in place without throwing
      // InvalidStateError on `.showModal()`.
      closeSimilarModal();
      openListingDetail(otherId);
    });
  });
}

function closeSimilarModal() {
  if (!els.similarModal) return;
  if (typeof els.similarModal.close === "function") {
    els.similarModal.close();
  } else {
    els.similarModal.removeAttribute("open");
  }
}

// ---------- taste / profile-summary modal -----------------------------------

async function openTasteModal() {
  if (!els.tasteModal || !authState.user) return;
  els.tasteBody.innerHTML = '<p class="muted">Loading…</p>';
  if (typeof els.tasteModal.showModal === "function") {
    els.tasteModal.showModal();
  } else {
    els.tasteModal.setAttribute("open", "");
  }
  let summary;
  try {
    const r = await fetch("/me/profile", { credentials: "same-origin" });
    if (!r.ok) {
      els.tasteBody.innerHTML = `<p class="empty">Could not load profile (HTTP ${r.status}).</p>`;
      return;
    }
    summary = await r.json();
  } catch (e) {
    els.tasteBody.innerHTML = `<p class="empty">Network error: ${esc(e.message)}</p>`;
    return;
  }
  renderTasteSummary(summary);
}

function closeTaste() {
  if (!els.tasteModal) return;
  if (typeof els.tasteModal.close === "function") {
    els.tasteModal.close();
  } else {
    els.tasteModal.removeAttribute("open");
  }
}

function renderTasteSummary(s) {
  const stats = s.stats || { likes: 0, bookmarks: 0, dismissals: 0 };
  const liked = s.liked_features || [];
  const avoided = s.avoided_features || [];
  const price = s.price_range_chf;

  // Cold-start copy: direct the user to the actions that unlock personalization.
  const coldStart = `
    <p class="taste-cold">
      Not enough activity yet to personalize your results.
      Like (♡) or save (☆) a few listings you find interesting and your taste will start showing up here.
      <br><span class="muted small">(${esc(s.positive_count)} positive interaction${s.positive_count === 1 ? "" : "s"} so far; need at least 3.)</span>
    </p>
  `;

  const tagRow = (items, tone) =>
    items.length
      ? `<div class="taste-tags">${items
          .map(
            (it) =>
              `<span class="taste-chip ${tone}" title="confidence ${Math.round(Math.abs(it.weight) * 100)}%">${esc(it.label)}</span>`,
          )
          .join("")}</div>`
      : `<p class="muted small">Nothing confidently inferred yet on this axis.</p>`;

  const statsRow = `
    <div class="taste-stats">
      <div><strong>${stats.likes}</strong><span class="muted small">likes</span></div>
      <div><strong>${stats.bookmarks}</strong><span class="muted small">saved</span></div>
      <div><strong>${stats.dismissals}</strong><span class="muted small">dismissed</span></div>
    </div>
  `;

  const priceRow = price
    ? `
      <section class="taste-section">
        <h3>Typical price</h3>
        <p class="taste-price">Around <strong>${chf(price.mid_chf)}</strong>/month
          <span class="muted small">(${chf(price.low_chf)} – ${chf(price.high_chf)})</span></p>
      </section>`
    : "";

  // The "You tend to avoid" section is intentionally hidden for now.
  // The backend still computes and returns ``avoided_features`` so we can
  // turn it back on by re-adding its <section> block - no server change.
  const body = s.is_cold_start
    ? `${statsRow}${coldStart}`
    : `
      ${statsRow}
      <section class="taste-section">
        <h3>You tend to prefer</h3>
        ${tagRow(liked, "liked")}
      </section>
      ${priceRow}
      <p class="muted small taste-caveat">
        Derived from your recent likes, saves, dwell time, and dismissals
        (last 180 days). Toggle <b>Personalize</b> off in the topbar to
        compare against the anonymous ranking.
      </p>
    `;

  els.tasteBody.innerHTML = body;
}

// ---------- Attach-a-photo (inline with the search bar) --------------------
// The user can attach a photo to the main search. On submit, the /demo form
// POSTs multipart to /listings/search/multi with both the text and the file;
// the server adds a DINOv2 image-similarity ranking to the existing RRF
// fusion so text + photo jointly drive the top-K.

const _attachedImage = { file: null, previewUrl: null };

function _attachedImageFile() {
  return _attachedImage.file;
}

function _acceptAttachedImage(file) {
  if (!file) return;
  if (!/^image\/(jpeg|png|webp)$/i.test(file.type)) {
    setStatus(`Unsupported type ${file.type || "?"} — use JPEG, PNG or WEBP.`, "err");
    return;
  }
  if (file.size > 8 * 1024 * 1024) {
    setStatus(`File is ${(file.size / (1024 * 1024)).toFixed(1)} MB; max is 8 MB.`, "err");
    return;
  }
  _clearAttachedImage();
  const url = URL.createObjectURL(file);
  _attachedImage.file = file;
  _attachedImage.previewUrl = url;
  const chipWrap = document.getElementById("search-image-chip-wrap");
  const thumb = document.getElementById("search-image-chip-thumb");
  const nameEl = document.getElementById("search-image-chip-name");
  const attachBtn = document.querySelector(".search-attach-btn");
  if (thumb) thumb.src = url;
  if (nameEl) {
    const kb = Math.round(file.size / 1024);
    nameEl.textContent = `${file.name} · ${kb} KB`;
  }
  if (chipWrap) chipWrap.hidden = false;
  if (attachBtn) attachBtn.classList.add("has-image");
  setStatus("Photo attached — the ranker will blend it with your text", "ok");
}

function _clearAttachedImage() {
  if (_attachedImage.previewUrl) {
    URL.revokeObjectURL(_attachedImage.previewUrl);
  }
  _attachedImage.file = null;
  _attachedImage.previewUrl = null;
  const chipWrap = document.getElementById("search-image-chip-wrap");
  const thumb = document.getElementById("search-image-chip-thumb");
  const nameEl = document.getElementById("search-image-chip-name");
  const fileInput = document.getElementById("search-image-input");
  const attachBtn = document.querySelector(".search-attach-btn");
  if (chipWrap) chipWrap.hidden = true;
  if (thumb) thumb.removeAttribute("src");
  if (nameEl) nameEl.textContent = "—";
  if (fileInput) fileInput.value = "";
  if (attachBtn) attachBtn.classList.remove("has-image");
}

function renderListingDetail(L) {
  els.detailTitle.textContent = L.title || "(no title)";
  const images = [L.hero_image_url, ...(L.image_urls || [])]
    .filter(Boolean)
    .filter((v, i, a) => a.indexOf(v) === i);
  const addressBlock = renderAddressBlock(L, { cls: "listing-address detail" });
  const metaParts = [
    L.price_chf != null ? `<strong>${chf(L.price_chf)}</strong>` : "",
    L.rooms != null ? `${esc(L.rooms)} rooms` : "",
    L.living_area_sqm != null ? `${esc(L.living_area_sqm)} m²` : "",
    L.object_category ? esc(L.object_category) : "",
    L.available_from ? `available ${esc(L.available_from)}` : "",
  ].filter(Boolean);

  els.detailBody.innerHTML = `
    <div class="detail-media">
      ${
        images.length
          ? `<img class="detail-hero" src="${esc(images[0])}" alt="${esc(L.title || "home")}" />
             ${
               images.length > 1
                 ? `<div class="detail-carousel">
                      <button type="button" class="img-nav prev" aria-label="Previous photo">‹</button>
                      <button type="button" class="img-nav next" aria-label="Next photo">›</button>
                      <div class="img-count">1 / ${images.length}</div>
                    </div>`
                 : ""
             }`
          : '<div class="detail-no-image">No photo available</div>'
      }
    </div>
    <div class="detail-meta">${metaParts.join(" · ")}</div>
    ${renderListingActions(L.id, { variant: "card" })}
    ${addressBlock}
    ${renderNearbyLandmarks(L, { variant: "card", limit: 8 })}
    ${
      (L.features || []).length
        ? `<div class="detail-features">${L.features
            .map((f) => `<span class="chip-sm">${esc(f)}</span>`)
            .join("")}</div>`
        : ""
    }
    ${(() => {
      const bc = bathroomChips(L);
      return bc ? `<div class="detail-enriched">${bc}</div>` : "";
    })()}
    ${
      L.description
        ? `<div class="detail-desc">${sanitizeDescriptionHtml(L.description)}</div>`
        : '<p class="muted small">No description available.</p>'
    }
    <div class="detail-footer">
      <kbd>${esc(L.id)}</kbd>
      ${
        L.original_listing_url
          ? ` · <a href="${esc(L.original_listing_url)}" target="_blank" rel="noopener">open on the original site ↗</a>`
          : ""
      }
    </div>
  `;

  // Wire the optional image carousel.
  if (images.length > 1) {
    const hero = els.detailBody.querySelector(".detail-hero");
    const countEl = els.detailBody.querySelector(".img-count");
    let idx = 0;
    const go = (delta) => {
      idx = (idx + delta + images.length) % images.length;
      hero.src = images[idx];
      countEl.textContent = `${idx + 1} / ${images.length}`;
    };
    els.detailBody.querySelector(".img-nav.prev")
      .addEventListener("click", () => go(-1));
    els.detailBody.querySelector(".img-nav.next")
      .addEventListener("click", () => go(1));
  }

  // Wire the Love / Save / Hide buttons in the modal. No cardEl — this
  // surface has no sibling card to dim, so toggleDismiss just updates the
  // button state + server interaction. Modal stays open either way; the
  // user closes it explicitly with × or the backdrop.
  wireListingActions(els.detailBody);
}

async function clearHistory() {
  if (!authState.user) return;
  if (
    !window.confirm(
      "Erase everything we've learned about your taste (loves, saves, and hides)? This can't be undone.",
    )
  ) {
    return;
  }
  try {
    const r = await authJson("/me/interactions", { method: "DELETE" });
    if (!r.ok) return;
    authState.likedIds.clear();
    authState.bookmarkedIds.clear();
    authState.dismissedIds.clear();
    updateBookmarksCount();
    closeFavorites();
  } catch {
    // ignore
  }
}

// ---------- dwell tracking --------------------------------------------------

const DWELL_VISIBLE_MS = 5000;
const dwellTracker = {
  timers: new WeakMap(),
  observer: null,
  init() {
    if (this.observer || !("IntersectionObserver" in window)) return;
    this.observer = new IntersectionObserver(
      (entries) => {
        for (const entry of entries) {
          const card = entry.target;
          const listingId = card.dataset.listingId;
          if (!listingId) continue;
          const visible = entry.isIntersecting && entry.intersectionRatio >= 0.5;
          if (visible) {
            if (this.timers.has(card)) continue;
            const id = window.setTimeout(() => {
              if (card.dataset.dwellFired === "1") return;
              card.dataset.dwellFired = "1";
              // Fire-and-forget: swallow failures so they don't pollute
              // the console. Dwell is a weak signal; losing one is fine.
              postInteraction(listingId, "dwell", DWELL_VISIBLE_MS / 1000)
                .catch(() => { /* best effort */ });
            }, DWELL_VISIBLE_MS);
            this.timers.set(card, id);
          } else {
            const id = this.timers.get(card);
            if (id != null) {
              window.clearTimeout(id);
              this.timers.delete(card);
            }
          }
        }
      },
      { threshold: [0.5] },
    );
  },
  watch(card) {
    if (!this.observer) this.init();
    if (this.observer) this.observer.observe(card);
  },
  reset() {
    // Called before rendering a new result set - stop observing old cards.
    if (this.observer) this.observer.disconnect();
    this.observer = null;
    this.timers = new WeakMap();
  },
};

// ---------- data flow --------------------------------------------------------

// Homepage feed shown before the user searches. Pulls a small batch from
// `/listings/default`, which is cheap (no LLM, no BM25/visual/text-embed)
// and is personalised server-side when the caller is authenticated + past
// cold-start. We render via `renderListings` and then override the status
// banner it writes — the "Found N homes out of ? that passed your
// must-haves" copy doesn't make sense when no must-haves were applied.
async function loadDefaultFeed() {
  // Don't clobber an existing result set (user could have searched very
  // quickly before auth hydration finished).
  if (els.listings.children.length > 0) return;

  let data;
  try {
    const r = await fetch("/listings/default?limit=12", {
      credentials: "same-origin",
    });
    if (!r.ok) return;
    data = await r.json();
  } catch (e) {
    console.warn("default feed load failed", e);
    return;
  }

  const listings = data.listings || [];
  if (!listings.length) return;

  // Keep the query-plan meta panel hidden — there is no "what we
  // understood" to show for a no-query default feed.
  els.metaPanel.hidden = true;

  renderListings(listings, data.meta || {});

  const personalized = !!(data.meta && data.meta.personalized);
  const authed = !!(authState && authState.user);
  const n = listings.length;
  const plural = n === 1 ? "" : "s";
  const headline = personalized
    ? `<b>Recommended for you</b> \u00b7 ${n} home${plural} picked from what you've liked, saved, or hidden.`
    : authed
    ? `<b>Start here</b> \u00b7 ${n} home${plural}. <span class="muted small">Like or save a few, then come back \u2014 we'll tune this to your taste.</span>`
    : `<b>Start here</b> \u00b7 ${n} home${plural}. <span class="muted small">Sign in to get picks based on your taste.</span>`;
  els.resultStatus.innerHTML = headline;

  setStatus(personalized ? "Picks for you" : "Ready", "ok");
}

async function runQuery(query, limit) {
  setStatus("Thinking…", "loading");
  els.listings.innerHTML = "";
  els.resultStatus.textContent = "";
  els.metaPanel.hidden = true;
  // Cancel any pending widget-driven resubmit before firing a fresh text
  // search. Otherwise a debounce that the user kicked off ~300ms before
  // hitting Enter would land AFTER this search and clobber the new plan.
  if (MAP_STATE.widgetDebounceTimer) {
    clearTimeout(MAP_STATE.widgetDebounceTimer);
    MAP_STATE.widgetDebounceTimer = null;
  }
  // Bumping the token so any /listings/search/multi call still in flight
  // from the widget path loses the stale race in _applySearchResponse.
  MAP_STATE.widgetSubmitToken++;

  const personalize =
    !!(authState.user && els.personalizeToggle && els.personalizeToggle.checked);

  // Always POST as multipart. The /listings/search/multi endpoint handles
  // all three modes (text-only, image-only, text+image) — and when there's
  // no file attached it falls through to the same ranker the JSON /listings
  // endpoint uses, so nothing regresses for text-only queries.
  const attachedFile = _attachedImageFile();
  const fd = new FormData();
  fd.append("query", query || "");
  fd.append("limit", String(limit));
  fd.append("offset", "0");
  fd.append("personalize", personalize ? "true" : "false");
  if (attachedFile) fd.append("file", attachedFile);

  if (attachedFile) {
    setStatus(query ? "Reading your photo + text…" : "Reading your photo…", "loading");
  }

  let response;
  try {
    response = await fetch("/listings/search/multi", {
      method: "POST",
      body: fd,
      credentials: "same-origin",
    });
  } catch (e) {
    setStatus("Can't reach the server", "err");
    els.resultStatus.innerHTML = `<div class="error">Couldn't reach the server: ${esc(
      e.message,
    )}. Is the server running?</div>`;
    return;
  }

  if (!response.ok) {
    const body = await response.text();
    setStatus(`Error ${response.status}`, "err");
    els.resultStatus.innerHTML = `<div class="error">The server ran into a problem (error ${
      response.status
    }):\n${esc(body)}</div>`;
    return;
  }

  let data;
  try {
    data = await response.json();
  } catch (e) {
    setStatus("Couldn't read the answer", "err");
    els.resultStatus.innerHTML = `<div class="error">Got a response, but couldn't read it: ${esc(
      e.message,
    )}</div>`;
    return;
  }

  // Stash the text the user typed so widget-panel re-submits can preserve
  // the full RRF ranking pipeline (backend re-runs the LLM extraction then
  // merges widget-state on top). Empty string is fine — image-only queries
  // won't trigger widget resubmits anyway.
  MAP_STATE.lastQueryText = query || "";
  _applySearchResponse(data, { fromWidget: false, fallbackQuery: query });
}

// ---------- Response handler (shared between form-submit + widget-resubmit) ---
//
// One funnel so every code path that receives a ListingsResponse renders the
// same set of panels, feeds the map overlay with the right top-N set, and
// hydrates the widget panel from the LLM's extracted plan. `opts.fromWidget`
// distinguishes widget-driven resubmits so the status-bar message can say
// "filters applied" instead of "Found N homes" (less noisy for small nudges).
function _applySearchResponse(data, opts) {
  opts = opts || {};
  const n = data.listings?.length ?? 0;
  const meta = data.meta || {};
  if (opts.fromWidget) {
    _setMfStatus(
      n === 0 ? "no homes match" : `${n} home${n === 1 ? "" : "s"} match`,
      "",
    );
  } else {
    setStatus(`Found ${n} home${n === 1 ? "" : "s"}`, "ok");
  }

  els.metaPanel.hidden = false;
  els.rawQuery.textContent = `"${meta.query ?? opts.fallbackQuery ?? ""}"`;
  renderHardFilters(meta.query_plan || null);
  renderSoftPrefs(meta.query_plan || null);
  renderPipeline(meta.pipeline, meta.candidate_pool_size, meta.returned);
  els.rawJson.textContent = JSON.stringify(data, null, 2);
  if (els.memIndicator) {
    els.memIndicator.hidden = !(meta.pipeline && meta.pipeline.memory);
  }
  renderListings(data.listings, meta);

  // Map overlay: the top-N set is exactly the ranker's picks in data.listings,
  // so the map can draw them as bright "top match" markers while the other
  // filter-matched rows appear as faded dots. We reuse meta.query_plan as the
  // hard_filters payload so the map's SQL filter matches the backend's.
  const topIds = new Set(
    (Array.isArray(data.listings) ? data.listings : []).map((l) => String(l.listing_id)),
  );
  const plan = meta.query_plan;
  if (plan && typeof plan === "object") {
    fetchAndRenderMap({ hard_filters: plan }, topIds);
  } else if (opts.fallbackQuery) {
    // No plan emitted (image-only query) — fall back to the NL path so the
    // map at least shows corpus-wide coverage rather than going blank.
    fetchAndRenderMap({ query: opts.fallbackQuery }, topIds);
  }

  // Two-way bind the widget panel to the LLM's extraction. On widget-driven
  // re-submits this overwrites the in-flight widget state with the merged
  // plan coming back from the server — intentional, so the visible panel
  // stays honest to what actually ran.
  if (plan && typeof plan === "object") {
    hydrateWidgetsFromPlan(plan);
  }
}

// ---------- wiring -----------------------------------------------------------

// Boot path: init the map module once the DOM + Leaflet CDN are ready so
// search results can populate it even while the user is still looking at
// the List view. When the user first flips to the Map tab, invalidateSize
// forces a tile paint (the #map container reported 0 height while hidden,
// and Leaflet caches that measurement — we have to re-measure explicitly).
function _wireMapTabsAndBoot() {
  const listBtn = document.getElementById("view-tab-list");
  const mapBtn = document.getElementById("view-tab-map");
  if (listBtn) listBtn.addEventListener("click", () => setActiveView("list"));
  if (mapBtn) mapBtn.addEventListener("click", () => setActiveView("map"));
  _initMapOnce();
  // Widget panel lives in #view-map but its DOM is parsed at page load, so
  // the listener binding can happen even while the map tab is hidden.
  _wireWidgetPanel();
  _wireHelpDrawer();
}

// ---------- Quick-tour help drawer ----------------------------------------
// Slim user guide shown on demand from the topbar "?" button. Uses a
// native <dialog>, so ESC + focus-trap come free. "Try this →" chips
// pre-fill the main search bar with a worked example and submit it.

function _wireHelpDrawer() {
  const openBtn = document.getElementById("help-btn");
  const dialog = document.getElementById("help-drawer");
  const closeBtn = document.getElementById("help-drawer-close");
  if (!openBtn || !dialog) return;

  // First-visit nudge. Toggle the CSS pulse on the Guide button the first
  // time a given browser sees the site, then persist a flag so repeat
  // visitors don't get pulsed at. Also honour `prefers-reduced-motion`
  // — no pulse for users who've opted out of animation.
  const FIRST_VISIT_KEY = "datathon2026_guide_seen";
  const reduceMotion = window.matchMedia?.(
    "(prefers-reduced-motion: reduce)"
  ).matches;
  try {
    const seen = window.localStorage.getItem(FIRST_VISIT_KEY);
    if (!seen && !reduceMotion) {
      openBtn.classList.add("is-fresh");
    }
  } catch (e) {
    // localStorage can be blocked (private mode, strict CSP); pulse anyway.
    console.info(
      `[INFO] help_first_visit: expected=localStorage, got=${e.message}, ` +
        `fallback=always-pulse until next reload`,
    );
    if (!reduceMotion) openBtn.classList.add("is-fresh");
  }
  const markSeen = () => {
    openBtn.classList.remove("is-fresh");
    try {
      window.localStorage.setItem(FIRST_VISIT_KEY, "1");
    } catch (_e) {
      // Same swallow as above; silent fallback is fine here since we'll
      // re-check on the next page load.
    }
  };

  openBtn.addEventListener("click", () => {
    markSeen();
    if (typeof dialog.showModal === "function") {
      dialog.showModal();
    } else {
      // Very old browsers: fall back to a plain show + manual backdrop.
      console.warn(
        `[WARN] help_drawer: expected=<dialog>.showModal, ` +
          `got=no-API, fallback=dialog.open=true (no backdrop)`,
      );
      dialog.open = true;
    }
  });
  if (closeBtn) {
    closeBtn.addEventListener("click", () => dialog.close());
  }
  // Backdrop click closes — detect by checking whether the click landed on
  // the <dialog> element itself (not a child). The dialog fills the edge of
  // the viewport; its drawer inner is narrower, so x-clicks outside the
  // inner fall on the dialog.
  dialog.addEventListener("click", (ev) => {
    if (ev.target === dialog) dialog.close();
  });
  // "Try this →" chips drop a worked example into the search bar and fire
  // the same submit path the user would.
  dialog.querySelectorAll(".help-try[data-query]").forEach((btn) => {
    btn.addEventListener("click", () => {
      const q = btn.getAttribute("data-query") || "";
      if (!q) return;
      if (els.query) {
        els.query.value = q;
        els.query.focus();
      }
      dialog.close();
      // Let the dialog-close animation settle before dispatching submit;
      // otherwise the transition can stutter on slower machines.
      setTimeout(() => {
        if (els.form && typeof els.form.requestSubmit === "function") {
          els.form.requestSubmit();
        } else if (els.form) {
          els.form.submit();
        }
      }, 80);
    });
  });
}

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", _wireMapTabsAndBoot);
} else {
  _wireMapTabsAndBoot();
}

els.form.addEventListener("submit", (ev) => {
  ev.preventDefault();
  const q = els.query.value.trim();
  const hasImage = !!_attachedImageFile();
  // Accept either a text query or a photo; the server needs at least one.
  if (!q && !hasImage) {
    setStatus("Type something or attach a photo first", "err");
    return;
  }
  const lim = parseInt(els.limit.value, 10) || 25;
  runQuery(q, lim);
});

els.examples.forEach((btn) => {
  btn.addEventListener("click", () => {
    els.query.value = btn.dataset.example || "";
    els.form.requestSubmit();
  });
});

// ---------- auth + modal wiring ---------------------------------------------

document.querySelectorAll("[data-auth-open]").forEach((btn) => {
  btn.addEventListener("click", () => openAuthModal(btn.dataset.authOpen));
});
document.querySelectorAll("[data-auth-close]").forEach((btn) => {
  btn.addEventListener("click", closeAuthModal);
});
document.querySelectorAll("[data-fav-close]").forEach((btn) => {
  btn.addEventListener("click", closeFavorites);
});
document.querySelectorAll("[data-detail-close]").forEach((btn) => {
  btn.addEventListener("click", closeDetail);
});
document.querySelectorAll("[data-similar-close]").forEach((btn) => {
  btn.addEventListener("click", closeSimilarModal);
});
document.querySelectorAll("[data-taste-close]").forEach((btn) => {
  btn.addEventListener("click", closeTaste);
});
// Wire the inline attach-a-photo chip.
if (els.searchImageInput) {
  els.searchImageInput.addEventListener("change", (ev) => {
    const f = ev.target.files && ev.target.files[0];
    if (f) _acceptAttachedImage(f);
  });
}
if (els.searchImageChipClear) {
  els.searchImageChipClear.addEventListener("click", _clearAttachedImage);
}

if (els.authTabs) {
  els.authTabs.querySelectorAll(".tab").forEach((tab) => {
    tab.addEventListener("click", () => setAuthMode(tab.dataset.authMode));
  });
}

if (els.authModalForm) {
  els.authModalForm.addEventListener("submit", handleAuthSubmit);
}
if (els.logoutBtn) {
  els.logoutBtn.addEventListener("click", handleLogout);
}
if (els.deleteAccountBtn) {
  els.deleteAccountBtn.addEventListener("click", handleDeleteAccount);
}
if (els.favoritesBtn) {
  els.favoritesBtn.addEventListener("click", openFavorites);
}
if (els.tasteBtn) {
  els.tasteBtn.addEventListener("click", openTasteModal);
}
if (els.clearHistoryBtn) {
  els.clearHistoryBtn.addEventListener("click", clearHistory);
}

// Close dialogs on backdrop click.
[els.authModal, els.favoritesModal, els.detailModal, els.tasteModal].forEach((dlg) => {
  if (!dlg) return;
  dlg.addEventListener("click", (ev) => {
    if (ev.target === dlg) {
      if (typeof dlg.close === "function") dlg.close();
      else dlg.removeAttribute("open");
    }
  });
});

// Keyboard activation for saved-listing cards (click works automatically;
// role="button" + tabindex="0" makes Enter/Space the A11y standard).
if (els.favoritesList) {
  els.favoritesList.addEventListener("keydown", (ev) => {
    if (ev.key !== "Enter" && ev.key !== " ") return;
    const card = ev.target.closest(".fav-card");
    if (!card) return;
    ev.preventDefault();
    const lid = card.dataset.listingId;
    if (lid) openListingDetail(lid);
  });
}

hydrateAuthState()
  .catch((e) => { console.warn("auth hydrate failed", e); })
  .finally(() => { loadDefaultFeed().catch(() => {}); });

// Compact the sticky search panel once the user has scrolled past the hero
// label. A single rAF-throttled `scroll` listener toggles `.scrolled`; the
// collapse animation itself is CSS. Threshold (~72px) is the height of the
// big label so the transition kicks in just as it would scroll off anyway.
(function initCompactSearchBar() {
  const panel = document.querySelector(".search-panel");
  if (!panel) return;
  const THRESHOLD = 72;
  let pending = false;
  const update = () => {
    pending = false;
    panel.classList.toggle("scrolled", window.scrollY > THRESHOLD);
  };
  window.addEventListener("scroll", () => {
    if (pending) return;
    pending = true;
    requestAnimationFrame(update);
  }, { passive: true });
  update();
})();

setStatus("Ready", "ok");
