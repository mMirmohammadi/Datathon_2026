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

// Pass-2b display helpers. All 4 fields are tri-state (true / false / null for
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

  // Every row: label + value or '—' (null/empty). Never hide nulls — the user
  // wants to see exactly what the LLM did and didn't emit.
  const rows = [];
  const add = (k, v) => {
    const isNull = v == null || (Array.isArray(v) && v.length === 0);
    rows.push(
      `<div class="kv"><span class="k">${esc(k)}</span><span class="v ${
        isNull ? "null" : ""
      }">${isNull ? "—" : esc(v)}</span></div>`,
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

  html += `
    <div class="kv"><span class="k">features (required)</span><span class="v">${
      features.length ? "" : "—"
    }</span></div>
    <div>${
      features
        .map((f) => `<span class="tag hard">${esc(f)}</span>`)
        .join("") || ""
    }</div>
    <div class="kv" style="margin-top:6px"><span class="k">features_excluded</span><span class="v">${
      featuresExcluded.length ? "" : "—"
    }</span></div>
    <div>${
      featuresExcluded
        .map((f) => `<span class="tag hard">¬ ${esc(f)}</span>`)
        .join("") || ""
    }</div>
    <div class="kv" style="margin-top:6px"><span class="k">bm25_keywords</span><span class="v">${
      keywords.length ? "" : "—"
    }</span></div>
    <div>${
      keywords
        .map((k) => `<span class="tag hard" title="Passed to FTS5 MATCH">${esc(k)}</span>`)
        .join("") || ""
    }</div>`;

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

  dwellTracker.reset();

  listings.forEach((res, idx) => {
    const listing = res.listing || {};
    const images = [listing.hero_image_url, ...(listing.image_urls || [])]
      .filter(Boolean)
      .filter((v, i, a) => a.indexOf(v) === i);
    const isTop = idx === 0;
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
            <div class="rank-score">
              <div class="rank ${isTop ? "top" : ""}">#${idx + 1}${
                isTop ? '<span class="rank-badge-top">TOP</span>' : ""
              }</div>
              <div class="score" title="Final RRF score">${res.score.toFixed(
                3,
              )}</div>
            </div>
          </div>
          ${
            authState.user || memBoost
              ? `<div class="listing-actions">
            ${
              authState.user
                ? `<button type="button" class="listing-action like-btn${
                    isLiked ? " liked" : ""
                  }" aria-pressed="${isLiked}" data-action="like"
                    title="${
                      isLiked
                        ? "Tap to un-love it (we'll stop looking for similar homes)"
                        : "I love this one! (we'll show you more like it)"
                    }">${isLiked ? "💖 Loved" : "♡ Love"}</button>
            <button type="button" class="listing-action save-btn${
              isBookmarked ? " saved" : ""
            }" aria-pressed="${isBookmarked}" data-action="save"
                    title="${
                      isBookmarked
                        ? "Take it off your favorites"
                        : "Save it to your favorites (we'll show more like it)"
                    }">${isBookmarked ? "⭐ Saved" : "☆ Save"}</button>
            <button type="button" class="listing-action dismiss-btn${
              isDismissed ? " dismissed" : ""
            }" aria-pressed="${isDismissed}" data-action="dismiss"
                    title="${
                      isDismissed
                        ? "Bring it back"
                        : "Hide this home (we won't show it again)"
                    }">${isDismissed ? "↩️ Bring back" : "🙈 Hide"}</button>`
                : ""
            }
            ${
              memBoost
                ? `<div class="memory-badge" title="We picked this one because it fits your taste">✨ picked for you</div>`
                : ""
            }
          </div>`
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
    const likeBtn = card.querySelector('[data-action="like"]');
    const saveBtn = card.querySelector('[data-action="save"]');
    const dismissBtn = card.querySelector('[data-action="dismiss"]');
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
      });
    }

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
    if (el) el.hidden = !on;
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
    // Wire click → detail modal. Using event delegation on the list host
    // means we don't pay an addEventListener per card (matters if the
    // drawer ever holds hundreds of bookmarks).
    host.onclick = (ev) => {
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
      </div>
    </div>`;
  }).join("");

  els.similarBody.innerHTML = metaLine + `<div class="similar-list">${cards}</div>`;

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
    ${addressBlock}
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

async function runQuery(query, limit) {
  setStatus("Thinking…", "loading");
  els.listings.innerHTML = "";
  els.resultStatus.textContent = "";
  els.metaPanel.hidden = true;

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

  const n = data.listings?.length ?? 0;
  setStatus(`Found ${n} home${n === 1 ? "" : "s"}`, "ok");

  const meta = data.meta || {};
  els.metaPanel.hidden = false;
  els.rawQuery.textContent = `"${meta.query ?? query}"`;
  renderHardFilters(meta.query_plan || null);
  renderSoftPrefs(meta.query_plan || null);
  renderPipeline(meta.pipeline, meta.candidate_pool_size, meta.returned);
  els.rawJson.textContent = JSON.stringify(data, null, 2);
  if (els.memIndicator) {
    els.memIndicator.hidden = !(meta.pipeline && meta.pipeline.memory);
  }
  renderListings(data.listings, meta);
}

// ---------- wiring -----------------------------------------------------------

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

hydrateAuthState().catch((e) => {
  console.warn("auth hydrate failed", e);
});

setStatus("Ready", "ok");
