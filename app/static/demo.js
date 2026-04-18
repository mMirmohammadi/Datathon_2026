// Demo frontend for the Datathon 2026 listings harness.
// Calls POST /listings and renders the extracted query plan + ranking breakdown.
// No silent fallbacks: every missing/unexpected field is shown as such.

const els = {
  form: document.getElementById("search-form"),
  query: document.getElementById("query"),
  limit: document.getElementById("limit"),
  status: document.getElementById("status"),
  statusInline: document.getElementById("status-inline"),
  metaPanel: document.getElementById("meta-panel"),
  rawQuery: document.getElementById("raw-query"),
  hardView: document.getElementById("hard-filters-view"),
  softView: document.getElementById("soft-prefs-view"),
  pipelineView: document.getElementById("pipeline-view"),
  rawJson: document.getElementById("raw-json"),
  listings: document.getElementById("listings"),
  resultStatus: document.getElementById("result-status"),
  examples: document.querySelectorAll(".chip"),
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
  if (els.statusInline) {
    els.statusInline.textContent = text;
    els.statusInline.className = "status-inline " + (cls || "");
  }
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

  listings.forEach((res, idx) => {
    const listing = res.listing || {};
    const images = [listing.hero_image_url, ...(listing.image_urls || [])]
      .filter(Boolean)
      .filter((v, i, a) => a.indexOf(v) === i);
    const isTop = idx === 0;

    const card = document.createElement("div");
    card.className = `listing-card${isTop ? " top" : ""}`;
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
  });
}

// ---------- data flow --------------------------------------------------------

async function runQuery(query, limit) {
  setStatus("extracting…", "loading");
  els.listings.innerHTML = "";
  els.resultStatus.textContent = "";
  els.metaPanel.hidden = true;

  let response;
  try {
    response = await fetch("/listings", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ query, limit, offset: 0 }),
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

setStatus("ready", "ok");
