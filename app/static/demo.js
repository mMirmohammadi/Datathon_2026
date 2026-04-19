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
      '<p class="empty">LLM did not activate any soft preferences for this query.</p>';
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
      '<p class="empty">LLM extracted the soft object but activated no keys.</p>';
    return;
  }

  els.softView.innerHTML = `
    <p class="muted small" style="margin:0 0 6px 0">
      Each activated key becomes its own listing ranking; rankings are fused with the other channels via RRF.
    </p>
    <div>${chips.join("")}</div>`;
}

function renderPipeline(pipeline, poolSize, returned) {
  if (!pipeline) {
    els.pipelineView.innerHTML =
      '<p class="empty">No pipeline telemetry in response.</p>';
    return;
  }

  const pill = (label, on, hint) =>
    `<span class="pill ${on ? "on" : "off"}" title="${esc(hint || "")}">${
      on ? "●" : "○"
    } ${esc(label)}</span>`;

  const softCount = pipeline.soft_rankings || 0;

  els.pipelineView.innerHTML = `
    <div class="pipeline-pills">
      ${pill("BM25", pipeline.bm25, "SQLite FTS5 lexical relevance (input-order channel)")}
      ${pill(
        "Visual (SigLIP-2)",
        pipeline.visual,
        "Requires LISTINGS_VISUAL_ENABLED=1 and the image store loaded",
      )}
      ${pill(
        "Semantic (Arctic-Embed)",
        pipeline.text_embed,
        "Requires LISTINGS_TEXT_EMBED_ENABLED=1 and the 1024-d matrix loaded",
      )}
      ${pill(
        `Soft rankings × ${softCount}`,
        softCount > 0,
        "One ranking per activated soft preference key",
      )}
    </div>
    <div class="kv"><span class="k">RRF k</span><span class="v">${esc(
      pipeline.rrf_k,
    )}</span></div>
    <div class="kv"><span class="k">Pool after hard filters</span><span class="v">${esc(
      poolSize ?? "—",
    )}</span></div>
    <div class="kv"><span class="k">Returned</span><span class="v">${esc(
      returned ?? "—",
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
  const bm25MissLabel = hasKeywords ? "no match" : "n/a";
  const bm25MissTooltip = hasKeywords
    ? `None of the LLM-extracted BM25 keywords (${keywords.join(", ")}) appear in this listing's title/description/street/city. BM25 contributes nothing for this listing; it ranks via the other active channels.`
    : "The LLM did not emit any BM25 keywords for this query, so the BM25 channel is inert — the ranker falls back to input order (listing_id). Other channels do the actual ranking.";

  const visualOn = pipeline && pipeline.visual;
  const semanticOn = pipeline && pipeline.text_embed;
  const visMissLabel = visualOn ? "no image" : "channel off";
  const visMissTooltip = visualOn
    ? "This listing has no photo in the image index (10k of 25,546 listings have none). Visual scoring skips it; other channels still rank it."
    : "Visual channel is disabled on this server (LISTINGS_VISUAL_ENABLED=0). Turn it on and restart to get SigLIP-2 scores.";
  const semMissLabel = semanticOn ? "—" : "channel off";
  const semMissTooltip = semanticOn
    ? "The listing's description embedding is missing. The Arctic-Embed matrix covers all 25,546 listings, so this should not happen — if you see this, check the signals table."
    : "Semantic channel is disabled on this server (LISTINGS_TEXT_EMBED_ENABLED=0). Turn it on and restart to get Arctic-Embed scores.";

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

    const softLabel = `${breakdown.soft_signals_activated} ranking${
      breakdown.soft_signals_activated === 1 ? "" : "s"
    }`;

    return `
      <div class="breakdown">
        <div class="breakdown-title">Scoring breakdown</div>
        <div class="breakdown-bars">
          ${row(
            "RRF (final)",
            "rrf",
            breakdown.rrf_score,
            rrfNorm,
            breakdown.rrf_score == null
              ? "no score"
              : breakdown.rrf_score.toFixed(4),
            breakdown.rrf_score == null
              ? "No fused score — this path is reached only when every channel was inert (e.g. the filter endpoint). For the normal /listings flow, this never triggers."
              : "Final fused score. Sorted by this. Units: sum of 1/(60 + rank_c) across channels.",
          )}
          ${row(
            "BM25",
            "bm25",
            breakdown.bm25_score,
            bm25Norm,
            breakdown.bm25_score == null
              ? bm25MissLabel
              : breakdown.bm25_score.toFixed(3),
            breakdown.bm25_score == null
              ? bm25MissTooltip
              : `SQLite FTS5 bm25() score (sign-flipped so higher = better). Keywords hit: ${keywords.join(", ")}.`,
          )}
          ${row(
            "Visual",
            "visual",
            breakdown.visual_score,
            visNorm,
            breakdown.visual_score == null
              ? visMissLabel
              : breakdown.visual_score.toFixed(3),
            breakdown.visual_score == null
              ? visMissTooltip
              : "Max cosine of query vs. this listing's image embeddings (SigLIP-2 Giant, 1536-d).",
          )}
          ${row(
            "Semantic",
            "semantic",
            breakdown.text_embed_score,
            txtNorm,
            breakdown.text_embed_score == null
              ? semMissLabel
              : breakdown.text_embed_score.toFixed(3),
            breakdown.text_embed_score == null
              ? semMissTooltip
              : "Cosine of query vs. description embedding (Snowflake Arctic-Embed-L v2, 1024-d).",
          )}
          <div class="bar-label" title="Number of soft-preference rankings that joined RRF this turn.">Soft</div>
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
    return '<p class="empty">No hard constraints requested — every listing in the pool is eligible.</p>';
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
          <th>constraint</th>
          <th>requested</th>
          <th>this listing</th>
        </tr>
      </thead>
      <tbody>${rows}</tbody>
    </table>`;
}

function renderKeywordHits(md) {
  const matched = md?.matched_keywords || [];
  const missed = md?.unmatched_keywords || [];
  if (matched.length === 0 && missed.length === 0) {
    return '<p class="empty">No BM25 keywords requested for this query.</p>';
  }
  const parts = [];
  if (matched.length) {
    parts.push(`<div class="kw-row">${matched
      .map((k) => `<span class="kw hit">✓ ${esc(k)}</span>`)
      .join("")}</div>`);
  }
  if (missed.length) {
    parts.push(`<div class="kw-row">${missed
      .map((k) => `<span class="kw miss" title="Requested but not in this listing's text">${esc(k)}</span>`)
      .join("")}</div>`);
  }
  parts.push(
    `<p class="muted small" style="margin:4px 0 0 0">Substring check over title, description, street, and city (same fields FTS5 indexes).</p>`,
  );
  return parts.join("");
}

function renderSoftFacts(md) {
  const facts = md?.soft_facts || [];
  if (facts.length === 0) {
    return '<p class="empty">LLM did not activate any soft preference axes.</p>';
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

function renderDetail(res) {
  const listing = res.listing || {};
  const md = res.match_detail;
  const images = [listing.hero_image_url, ...(listing.image_urls || [])]
    .filter(Boolean)
    .filter((v, i, a) => a.indexOf(v) === i);

  const matchedCount = (md?.matched_keywords || []).length;
  const missedCount = (md?.unmatched_keywords || []).length;

  return `
    <div class="listing-detail">
      <div class="detail-block">
        <h4>Hard-filter checks <span class="count">${
          (md?.hard_checks || []).length
        }</span></h4>
        ${renderHardChecks(md?.hard_checks)}

        <h4 style="margin-top:14px">BM25 keyword hits <span class="count">${matchedCount}/${
          matchedCount + missedCount
        }</span></h4>
        ${renderKeywordHits(md)}
      </div>
      <div class="detail-block">
        <h4>Soft-preference signals for this listing <span class="count">${
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
    els.resultStatus.textContent =
      "No listings matched the hard filters for this query.";
    return;
  }
  els.resultStatus.innerHTML = `
    <b>${listings.length}</b> listings (of ${
    meta.candidate_pool_size ?? "?"
  } after hard-filter gate).
    <span class="sort-hint">Sorted by RRF score (highest first) · click any card for the full match breakdown</span>
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
                    ? `<button class="img-nav prev" aria-label="Previous">‹</button>
                       <button class="img-nav next" aria-label="Next">›</button>
                       <div class="img-count">1 / ${images.length}</div>`
                    : ""
                }`
              : '<div class="no-image">No image</div>'
          }
        </div>
        <div class="listing-body">
          <div class="listing-head">
            <h3 class="listing-title">${esc(listing.title || "(untitled)")}</h3>
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
                        ? "Remove like (stops boosting similar listings)"
                        : "Like this listing · boosts similar listings in your results"
                    }">${isLiked ? "♥ Liked" : "♡ Like"}</button>
            <button type="button" class="listing-action save-btn${
              isBookmarked ? " saved" : ""
            }" aria-pressed="${isBookmarked}" data-action="save"
                    title="${
                      isBookmarked
                        ? "Remove from your Saved list"
                        : "Save to your Saved list · also boosts similar listings (stronger than a like)"
                    }">${isBookmarked ? "★ Saved" : "☆ Save"}</button>
            <button type="button" class="listing-action dismiss-btn${
              isDismissed ? " dismissed" : ""
            }" aria-pressed="${isDismissed}" data-action="dismiss"
                    title="${
                      isDismissed
                        ? "Bring this listing back (removes the negative signal)"
                        : "Hide and record a negative signal"
                    }">${isDismissed ? "↶ Undo" : "✕ Not for me"}</button>`
                : ""
            }
            ${
              memBoost
                ? `<div class="memory-badge" title="This listing was boosted by your saved / liked / dwelled history">✨ personalized</div>`
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
          <div class="expand-hint">click for hard-check table, keyword hits, and soft-signal values ↓</div>
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
          "click for hard-check table, keyword hits, and soft-signal values ↓";
      } else {
        card.classList.add("expanded");
        card.setAttribute("aria-expanded", "true");
        card.insertAdjacentHTML("beforeend", renderDetail(res));
        card.querySelector(".expand-hint").textContent =
          "click again to collapse ↑";
        // Implicit positive: card was deliberately expanded.
        postInteraction(listingId, "click");
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
    els.authModalTitle.textContent = "Sign in";
    showField("username", true);
    showField("password", true);
    document.getElementById("auth-password-input").autocomplete = "current-password";
    els.authSubmit.textContent = "Sign in";
  } else if (mode === "register") {
    els.authModalTitle.textContent = "Create account";
    showField("username", true);
    showField("email", true);
    showField("password", true);
    showField("password-hint", true);
    document.getElementById("auth-password-input").autocomplete = "new-password";
    els.authSubmit.textContent = "Create account";
  } else if (mode === "account") {
    els.authModalTitle.textContent = "Account";
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
        setAuthError("Too many login attempts. Wait a few minutes and retry.");
        return;
      }
      if (!r.ok) {
        setAuthError("Invalid credentials.");
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
        setAuthError("That username or email is already taken.");
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
        setAuthError("Enter a new password to update.");
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
        setAuthError("Current password is wrong.");
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
    setAuthError(`Network error: ${e.message}`);
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
    setAuthError("Enter your current password to confirm.");
    return;
  }
  if (!window.confirm("Permanently delete your account and all saved listings?")) {
    return;
  }
  try {
    const r = await authJson("/auth/delete-account", {
      method: "POST",
      body: JSON.stringify({ password }),
    });
    if (r.status === 401) {
      setAuthError("Password is wrong.");
      return;
    }
    if (!r.ok) {
      setAuthError("Could not delete account.");
      return;
    }
    authState.csrfToken = null;
    setAuthUI(null);
    closeAuthModal();
  } catch (e) {
    setAuthError(`Network error: ${e.message}`);
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
    setAuthError("Sign in to personalize your results.");
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
    setAuthError("Sign in to dismiss listings.");
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
    setStatus(`${kind} failed (restart server?)`, "err");
    console.warn(`${kind} failed`, err);
  });
}

function renderDismissButton(buttonEl, dismissed) {
  if (!buttonEl) return;
  buttonEl.classList.toggle("dismissed", !!dismissed);
  buttonEl.setAttribute("aria-pressed", dismissed ? "true" : "false");
  buttonEl.title = dismissed
    ? "Bring this listing back (removes the negative signal)"
    : "Hide and record a negative signal";
  buttonEl.textContent = dismissed ? "↶ Undo" : "✕ Not for me";
}

function renderLikeButton(buttonEl, liked) {
  if (!buttonEl) return;
  buttonEl.classList.toggle("liked", !!liked);
  buttonEl.setAttribute("aria-pressed", liked ? "true" : "false");
  buttonEl.title = liked
    ? "Remove like (stops boosting similar listings)"
    : "Like this listing · boosts similar listings in your results";
  buttonEl.textContent = liked ? "♥ Liked" : "♡ Like";
}

function renderBookmarkButton(buttonEl, saved) {
  if (!buttonEl) return;
  buttonEl.classList.toggle("saved", !!saved);
  buttonEl.setAttribute("aria-pressed", saved ? "true" : "false");
  buttonEl.title = saved
    ? "Remove from your Saved list"
    : "Save to your Saved list · also boosts similar listings (stronger than a like)";
  buttonEl.textContent = saved ? "★ Saved" : "☆ Save";
}

async function openFavorites() {
  if (!authState.user) return;
  const favs = await fetchListSet("/me/favorites");
  authState.bookmarkedIds = new Set(favs.map((f) => f.listing_id));
  updateBookmarksCount();
  const host = els.favoritesList;
  if (!favs.length) {
    host.innerHTML =
      '<p class="muted">No saved listings yet. Click the ☆ on any card to save it to this list.</p>';
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
            ? `<img src="${esc(f.hero_image_url)}" alt="${esc(f.title || "saved listing")}" loading="lazy" />`
            : '<div class="fav-thumb-empty">No image</div>'
        }
      </div>
      <div class="fav-body">
        <div class="fav-title">${esc(f.title || "(untitled listing)")}</div>
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
  if (typeof els.detailModal.showModal === "function") {
    els.detailModal.showModal();
  } else {
    els.detailModal.setAttribute("open", "");
  }

  let data;
  try {
    const r = await fetch(`/listings/${encodeURIComponent(listingId)}`, {
      credentials: "same-origin",
    });
    if (!r.ok) {
      els.detailBody.innerHTML = `<p class="empty">Could not load listing (HTTP ${r.status}).</p>`;
      return;
    }
    data = await r.json();
  } catch (e) {
    els.detailBody.innerHTML = `<p class="empty">Network error: ${esc(e.message)}</p>`;
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

function renderListingDetail(L) {
  els.detailTitle.textContent = L.title || "(untitled listing)";
  const images = [L.hero_image_url, ...(L.image_urls || [])]
    .filter(Boolean)
    .filter((v, i, a) => a.indexOf(v) === i);
  const place = [
    L.street,
    [L.postal_code, L.city].filter(Boolean).join(" "),
    L.canton,
  ]
    .filter(Boolean)
    .join(" · ");
  const metaParts = [
    L.price_chf != null ? `<strong>${chf(L.price_chf)}</strong>` : "",
    L.rooms != null ? `${esc(L.rooms)} rooms` : "",
    L.living_area_sqm != null ? `${esc(L.living_area_sqm)} m²` : "",
    L.object_category ? esc(L.object_category) : "",
    L.available_from ? `avail. ${esc(L.available_from)}` : "",
  ].filter(Boolean);

  els.detailBody.innerHTML = `
    <div class="detail-media">
      ${
        images.length
          ? `<img class="detail-hero" src="${esc(images[0])}" alt="${esc(L.title || "listing")}" />
             ${
               images.length > 1
                 ? `<div class="detail-carousel">
                      <button type="button" class="img-nav prev" aria-label="Previous">‹</button>
                      <button type="button" class="img-nav next" aria-label="Next">›</button>
                      <div class="img-count">1 / ${images.length}</div>
                    </div>`
                 : ""
             }`
          : '<div class="detail-no-image">No image on file</div>'
      }
    </div>
    <div class="detail-meta">${metaParts.join(" · ")}</div>
    ${place ? `<div class="detail-place muted">${esc(place)}</div>` : ""}
    ${
      (L.features || []).length
        ? `<div class="detail-features">${L.features
            .map((f) => `<span class="chip-sm">${esc(f)}</span>`)
            .join("")}</div>`
        : ""
    }
    ${
      L.description
        ? `<div class="detail-desc">${sanitizeDescriptionHtml(L.description)}</div>`
        : '<p class="muted small">No description on file.</p>'
    }
    <div class="detail-footer">
      <kbd>${esc(L.id)}</kbd>
      ${
        L.original_listing_url
          ? ` · <a href="${esc(L.original_listing_url)}" target="_blank" rel="noopener">open source listing ↗</a>`
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
      "Wipe all interaction history (likes, saves, dismissals, dwell)?",
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
  setStatus("extracting…", "loading");
  els.listings.innerHTML = "";
  els.resultStatus.textContent = "";
  els.metaPanel.hidden = true;

  const personalize =
    !!(authState.user && els.personalizeToggle && els.personalizeToggle.checked);
  let response;
  try {
    response = await fetch("/listings", {
      method: "POST",
      headers: { "content-type": "application/json" },
      credentials: "same-origin",
      body: JSON.stringify({ query, limit, offset: 0, personalize }),
    });
  } catch (e) {
    setStatus("network error", "err");
    els.resultStatus.innerHTML = `<div class="error">Fetch failed: ${esc(
      e.message,
    )}. Is the FastAPI server running on this origin?</div>`;
    return;
  }

  if (!response.ok) {
    const body = await response.text();
    setStatus(`HTTP ${response.status}`, "err");
    els.resultStatus.innerHTML = `<div class="error">Server returned ${
      response.status
    }:\n${esc(body)}</div>`;
    return;
  }

  let data;
  try {
    data = await response.json();
  } catch (e) {
    setStatus("bad JSON", "err");
    els.resultStatus.innerHTML = `<div class="error">Could not parse response as JSON: ${esc(
      e.message,
    )}</div>`;
    return;
  }

  setStatus(`ok · ${data.listings?.length ?? 0} results`, "ok");

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
  if (!q) return;
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
if (els.clearHistoryBtn) {
  els.clearHistoryBtn.addEventListener("click", clearHistory);
}

// Close dialogs on backdrop click.
[els.authModal, els.favoritesModal, els.detailModal].forEach((dlg) => {
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

setStatus("ready", "ok");
