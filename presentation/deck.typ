// =============================================================================
// Robin — Hybrid Listing Search and Ranking
// Datathon 2026 pitch deck · typst 0.14+
// =============================================================================

#set document(title: "Robin — Hybrid Listing Search and Ranking")

// --- palette (aligned with app/static/demo.css) ------------------------------

#let bg      = rgb("#FEF8F2") // warm cream
#let card    = rgb("#FFFFFF")
#let ink     = rgb("#1E293B")
#let ink-2   = rgb("#334155")
#let muted   = rgb("#64748B")
#let muted-2 = rgb("#94A3B8")
#let border  = rgb("#E5E7EB")
#let border-2 = rgb("#F1F5F9")

#let coral   = rgb("#FB7185")
#let orange  = rgb("#FB923C")
#let amber   = rgb("#F59E0B")
#let indigo  = rgb("#6366F1")
#let purple  = rgb("#8B5CF6")
#let green   = rgb("#10B981")
#let red     = rgb("#EF4444")
#let blue    = rgb("#60A5FA")
#let pink    = rgb("#F472B6")

#set page(
  width: 33.867cm,
  height: 19.05cm,
  margin: 0pt,
  fill: bg,
)

#set text(
  font: ("Helvetica Neue", "Helvetica", "Arial"),
  size: 16pt,
  fill: ink,
  lang: "en",
)
#set par(leading: 0.62em)

#show raw: set text(font: ("Menlo", "Monaco", "Courier New"))

// bullet list — tighter, no fluff
#set list(marker: text(fill: orange, weight: 700, "•"), indent: 0pt, body-indent: 8pt, spacing: 0.75em)

// --- slide helpers -----------------------------------------------------------

#let slide-n = counter("slide-n")
#let slide-total = 12

#let header-strip = {
  set text(size: 10pt, fill: muted, tracking: 0pt)
  grid(
    columns: (1fr, auto),
    align: (left + horizon, right + horizon),
    [#text(weight: 700, fill: ink)[robinreal] #h(4pt) · hybrid search & ranking],
    [Datathon 2026],
  )
  v(-4pt)
  line(length: 100%, stroke: 0.4pt + border)
}

#let footbar = {
  set text(size: 9pt, fill: muted-2, tracking: 0pt)
  grid(
    columns: (1fr, auto),
    align: (left, right),
    [],
    [#context [#slide-n.display() / #slide-total]],
  )
}

#let slide(kicker: none, title: none, body) = {
  slide-n.step()
  page(
    margin: (left: 2.2cm, right: 2.2cm, top: 1.1cm, bottom: 0.9cm),
    header: header-strip,
    footer: footbar,
    {
      v(14pt)
      if kicker != none {
        set text(size: 10pt, fill: orange, weight: 700, tracking: 1.5pt)
        upper(kicker)
        v(4pt)
      }
      if title != none {
        set par(leading: 0.4em)
        text(size: 26pt, weight: 700, fill: ink, tracking: -0.3pt)[#title]
        v(14pt)
      }
      body
    },
  )
}

#let cover-slide(body) = {
  slide-n.step()
  page(
    margin: (left: 2.2cm, right: 2.2cm, top: 2cm, bottom: 1.4cm),
    fill: ink,
    body,
  )
}

// reusable atoms --------------------------------------------------------------

#let chip(txt, color: orange) = box(
  fill: color.lighten(85%),
  stroke: 0.5pt + color.lighten(55%),
  inset: (x: 8pt, y: 3pt),
  radius: 999pt,
  text(size: 10pt, weight: 600, fill: color.darken(15%), tracking: 0pt, txt),
)

#let kv(k, v) = grid(
  columns: (auto, 1fr),
  column-gutter: 8pt,
  row-gutter: 4pt,
  text(fill: muted, weight: 500, size: 11pt, tracking: 0pt)[#k],
  text(fill: ink, weight: 600, size: 11pt, tracking: 0pt)[#v],
)

// simple panel box (used sparingly for tables / code)
#let panel(body, tone: border-2) = block(
  width: 100%,
  inset: 12pt,
  radius: 8pt,
  fill: tone,
  stroke: 0.5pt + border,
  body,
)

#let head(body) = block(
  below: 8pt,
  text(size: 11pt, fill: orange, weight: 700, tracking: 1.2pt)[#upper(body)],
)

// =============================================================================
// 1 · Cover
// =============================================================================

#cover-slide({
  set text(fill: rgb("#FEF8F2"))
  v(0.6cm)
  set text(size: 10pt, fill: orange, weight: 700, tracking: 3pt)
  upper("Datathon 2026 · robinreal")
  v(18pt)
  set par(leading: 0.45em)
  set text(size: 56pt, weight: 800, fill: rgb("#FEF8F2"), tracking: -1.2pt)
  [Robin — hybrid listing]
  linebreak()
  [search and ranking.]
  v(20pt)
  set par(leading: 0.65em)
  set text(size: 16pt, fill: rgb("#CBD5E1"), weight: 400, tracking: 0pt)
  [Natural-language queries over 22,819 Swiss rental listings, four languages, four fused retrieval channels, eight-signal blend, opt-in personalization.]

  v(1fr)
  line(length: 2cm, stroke: 2pt + orange)
  v(10pt)
  set text(size: 11pt, fill: rgb("#94A3B8"), tracking: 0pt)
  [Team Robin · April 2026]
})

// =============================================================================
// 2 · Problem
// =============================================================================

#slide(kicker: "Problem", title: [Queries mix two incompatible intents.])[
  #grid(
    columns: (1fr, 1fr),
    column-gutter: 26pt,
    [
      #head("Two layers in every query")
      - *Hard constraints* — must not be violated (rooms, price, city, required features, commute ceiling).
      - *Soft preferences* — influence ranking only (_bright, modern, quiet, family-friendly_).
      - Hard violations are the highest-weighted failure mode in the brief.

      #v(6pt)
      #head("Example")
      #panel(tone: card)[
        #set text(size: 12pt)
        _"3-room #text(fill: ink, weight: 700)[bright] apartment
        #text(fill: ink, weight: 700)[in Zurich] #text(fill: orange, weight: 700)[under CHF 2,800]
        with #text(fill: ink, weight: 700)[balcony]."_
      ]
    ],
    [
      #head("Why it is hard")
      - Vague terms: _bright, modern, family-friendly, quiet_ — no DB column captures them.
      - Conflicting prefs: _cheap but central_, _quiet and lively_.
      - Multilingual: DE / FR / IT / EN, often within one sentence.
      - Dataset: 48 % of rows have no city / canton, ~97 % of feature flags NULL outside one source, descriptions are HTML.
    ],
  )
]

// =============================================================================
// 3 · Pipeline
// =============================================================================

#slide(kicker: "Pipeline", title: [Query → gate → fused retrieval → blend.])[
  #let node(t, s, c: orange) = box(
    inset: (x: 10pt, y: 8pt),
    radius: 6pt,
    fill: c.lighten(88%),
    stroke: 0.6pt + c.lighten(40%),
    {
      set par(leading: 0.4em)
      text(size: 11pt, weight: 700, fill: c.darken(15%), tracking: 0pt)[#t]
      linebreak()
      text(size: 9pt, fill: muted, tracking: 0pt)[#s]
    },
  )
  #let arr = text(size: 14pt, fill: orange, weight: 700)[ → ]

  #v(4pt)
  #align(center, grid(
    columns: (auto,) * 9,
    column-gutter: 4pt,
    align: horizon,
    node("Query", "natural language", c: ink),
    arr,
    node("Claude 4.6", "QueryPlan · strict JSON", c: coral),
    arr,
    node("SQL gate", "hard filters", c: red),
    arr,
    node("Retrieve", "4 channels · RRF k=60", c: orange),
    arr,
    node("Rank", "8-signal blend · reason", c: amber),
  ))

  #v(16pt)
  #grid(
    columns: (1fr, 1fr),
    column-gutter: 20pt,
    [
      #head("Retrieval channels (fused via RRF)")
      - *BM25* — SQLite FTS5, `unicode61` + diacritic-folded. Preserves DE domain terms (_Attika, Minergie, Altbau_).
      - *Dense text* — Snowflake Arctic-Embed-L v2, 256-d Matryoshka, cosine.
      - *Visual* — SigLIP-2 base, 5 image scores per listing.
      - *Soft signals* — up to 10 rankings (price-sentiment, quiet, near-transit, schools, parks, commute target, landmarks).
    ],
    [
      #head("Design rules")
      - SQL is a *gate*, not a channel. BM25 / dense / visual / soft are intersected inside the allowed set before fusion.
      - A listing violating a hard filter cannot appear in the output.
      - One Claude call per query; everything else is deterministic on precomputed indices.
      - Every fallback emits `[WARN] expected=X got=Y fallback=Z`.
    ],
  )
]

// =============================================================================
// 4 · Hard-filter gate
// =============================================================================

#slide(kicker: "Gate", title: [Hard filters are a gate, not a ranking.])[
  #grid(
    columns: (1.1fr, 1fr),
    column-gutter: 24pt,
    [
      #head("Architecture")
      - Retrieval universe = rows passing the SQL hard filter.
      - BM25 / dense / visual / soft all operate inside that set.
      - Fusion (RRF, k=60) runs on the intersection; violations are structurally unreachable.

      #v(4pt)
      #panel(tone: card)[
        #set text(font: "Menlo", size: 10pt)
        ```
        ┌─ SQL hard-filter set (allowed) ─┐
        │   BM25  ∪  dense / visual /    │ → RRF → blend → top-K
        │         soft-signal rankings   │
        └────────────────────────────────┘
             (everything outside is excluded)
        ```
      ]

      #head("Relaxation ladder (when gate empties)")
      - price ± 10 % → drop city → drop canton → radius × 1.5 → drop rarest feature.
      - Applied step announced in `meta.relaxations`.
    ],
    [
      #head("Supported hard filters")
      - `city`, `postal_code`, `canton`, `city_slug`
      - `min_price`, `max_price`
      - `min_rooms`, `max_rooms`
      - `features` (12 boolean flags)
      - `latitude`, `longitude`, `radius_km`
      - `offer_type`, `object_category`
      - `sort_by`, `limit`, `offset`

      #v(10pt)
      #head("Query robustness")
      - Fuzzy city via RapidFuzz (_Zürich / Zurich / typos_).
      - Claude timeout → regex fallback (rooms / CHF / city), logged.
      - `clarification_needed=true` → empty response + `meta.clarification`.
    ],
  )
]

// =============================================================================
// 5 · Query understanding
// =============================================================================

#slide(kicker: "Layer 1", title: [Query understanding — strict JSON, grounded spans.])[
  #grid(
    columns: (1.05fr, 1fr),
    column-gutter: 22pt,
    [
      #head("Claude 4.6 · forced tool use · strict: true")
      - Constrained decoding → valid JSON, no parse retries.
      - Every field carries a `source_span` (the phrase that justified it) → reduces hallucinated constraints.
      - System prompt padded past cache threshold → 90 % cost / 85 % latency reduction on cache hits.
      - 5 s timeout + regex fallback; fallback path emits `[WARN]`.
      - Classification rule: explicit operators (_unter / max / bis / sous / fino a_) → `hard`; hedges (_ideally / gerne / plutôt / pas trop_) → `soft`.
    ],
    [
      #head("QueryPlan (abridged)")
      #panel(tone: card)[
        #set text(size: 10pt, font: "Menlo")
        ```
        {
          lang: "de" | "fr" | "it" | "en",
          city_slug:  "zurich",
          rent_chf:   { max: 2800, modality: "hard" },
          rooms:      { min: 3,    modality: "hard" },
          features:   [{ name: "balcony",
                         polarity: "required" }],
          landmarks:  [{ text: "ETH",
                         max_minutes: 20 }],
          rewrites:   [ … 3 paraphrases … ],
          bm25_keywords: [ literal tokens ],
          clarification_needed: false,
          confidence: 0.94
        }
        ```
      ]
    ],
  )
]

// =============================================================================
// 6 · Ranking
// =============================================================================

#slide(kicker: "Layer 3", title: [N-way RRF fusion + 8-signal linear blend.])[
  #grid(
    columns: (1fr, 1fr),
    column-gutter: 22pt,
    [
      #head("Fusion")
      - Up to ~12 rankings per request: BM25 · Arctic-Embed · SigLIP visual · 8–10 soft-signal rankings.
      - Reciprocal Rank Fusion, k = 60. Score-scale agnostic, zero tuning.
      - Pool size 300, pagination applied after fusion.

      #v(4pt)
      #head("Blend")
      - Percentile-normalised inside the candidate pool.
      - Weights live in `scoring_config.py`.
      - Reason string built from the same signals — not a post-hoc LLM paraphrase.

      #v(4pt)
      #head("Reason template")
      #panel(tone: card)[
        #set text(size: 11pt, style: "italic", fill: ink-2)
        _"Matched hard filters; text match; visual match (0.24); semantic match (0.61); 6 soft preferences."_
      ]
    ],
    [
      #head("Blend weights")
      #table(
        columns: (1fr, auto),
        stroke: none,
        inset: (x: 6pt, y: 5pt),
        align: (left, right),
        table.hline(stroke: 0.5pt + border),
        text(size: 10pt, fill: muted, weight: 600, tracking: 0pt)[Signal],
        text(size: 10pt, fill: muted, weight: 600, tracking: 0pt)[Weight],
        table.hline(stroke: 0.5pt + border),
        [Cross-encoder rerank (bge-reranker-v2-m3)], [*0.30*],
        [BM25 percentile],              [0.15],
        [Dense cosine percentile],      [0.10],
        [Feature match (flag ∨ text)],  [0.10],
        [Price fit (triangle)],         [0.10],
        [Geo / landmark fit],           [0.08],
        [Image quality fit (SigLIP-2)], [0.07],
        [Freshness],                    [0.05],
        [Negative penalty],             [−0.15],
        table.hline(stroke: 0.5pt + border),
      )
    ],
  )
]

// =============================================================================
// 7 · Enrichment
// =============================================================================

#slide(kicker: "Layer 2", title: [Enrichment — fix the data before you rank.])[
  #grid(
    columns: (1.2fr, 1fr),
    column-gutter: 22pt,
    [
      #head("Offline passes")
      - *Canton closure*: `reverse_geocoder` (1,611) · PLZ majority vote (1,502) · GPT-5.4-nano with Nominatim cross-check (overrode 3 errors).
      - *Transit matrix*: r5py on cleaned Swiss GTFS, 4 × 10 GB JVM workers, Haversine ≤ 40 km pre-filter.
      - *Landmarks*: 30 curated + 15 GPT-mined, Nominatim-resolved.
      - *Text features*: DE / FR / IT / EN regex + ±5-token NegEx — fills the `balcony / parking / elevator / …` flags missing on 21 k rows.
      - *Image scores*: SigLIP-2 positive / negative prompt pairs over ~30 k images (brightness, modernity, view, spaciousness, family).
    ],
    [
      #head("Numbers")
      - Canton coverage: *87.2 % → 99.68 %* (`UNKNOWN` rows 3,177 → 0).
      - Commute matrix: *125,396 rows*, 21,973 listings × 45 landmarks.
      - Landmark gazetteer: *45* entries.
      - LLM cost for canton top-up: *CHF 0.02*.
      - r5py wall time: *91 min*.

      #v(8pt)
      #head("Non-goals")
      - No Nominatim bulk geocoding (TOS).
      - No DEM / BFS demographics (cut list).
      - No cross-encoder LLM judge at query time.
    ],
  )
]

// =============================================================================
// 8 · Multimodal
// =============================================================================

#slide(kicker: "Multimodal", title: [Image signals — extract, score, cross-check.])[
  #grid(
    columns: (1fr, 1fr),
    column-gutter: 22pt,
    [
      #head("5 SigLIP-2 scores per listing")
      - Brightness · natural-light vs dim interiors.
      - Modernity · kitchen / bathroom age.
      - View · greenery, cityscape, mountain, water.
      - Spaciousness · perceived room size, open plan.
      - Family-friendly · child cues in layout.

      #v(4pt)
      #head("Text × image cross-check")
      - Description claims "_bright / lichtdurchflutet_" + image brightness < 0.2 → score penalty.
      - Disagreement is itself a ranking signal.
    ],
    [
      #head("Image assets")
      - *70,548* hero images picked offline; *617* floor plans classified.
      - Per-listing score dict loaded at startup; query-time carries zero image inference.

      #v(8pt)
      #head("Fallbacks (never silent)")
      - Missing image → Pillow-luminance proxy + `[WARN]`.
      - SigLIP disabled (`LISTINGS_VISUAL_ENABLED=0`) → channel drops, BM25 + text-embed + soft still run.
    ],
  )
]

// =============================================================================
// 9 · Personalization
// =============================================================================

#slide(kicker: "Bonus", title: [Personalization — five memory rankings, opt-in.])[
  #grid(
    columns: (1.1fr, 1fr),
    column-gutter: 22pt,
    [
      #head("Auth and interaction stream")
      - argon2id hashes · HttpOnly + `SameSite=Strict` cookies · CSRF double-submit · login rate-limit · session rotation on login / password change.
      - Event kinds: `bookmark / unbookmark`, `like / unlike`, `click`, `dwell`, `dismiss / undismiss`. 180-day sliding window.
      - Cold-start gate: `positive_count < 3` → positive channels skipped with `[WARN]`; dismissal demotion still fires.
      - Anonymous path is bit-for-bit identical to the pre-personalization pipeline (pinned by a regression test).

      #v(6pt)
      #head("Event weights")
      #panel(tone: card)[
        #set text(size: 11pt)
        bookmark *+5* · like *+3* · dwell ≥ 5 s *+2* · click *+1* \
        dismiss *−2* · unlike / unbookmark / undismiss = exact inverses
      ]
    ],
    [
      #head("Memory rankings (fed into RRF)")
      #table(
        columns: (auto, 1fr),
        stroke: none,
        inset: (x: 4pt, y: 5pt),
        align: (left, left),
        table.hline(stroke: 0.5pt + border),
        text(size: 10pt, fill: muted, weight: 600, tracking: 0pt)[Channel],
        text(size: 10pt, fill: muted, weight: 600, tracking: 0pt)[Score],
        table.hline(stroke: 0.5pt + border),
        [1 · semantic taste], [cos(candidate, positives)],
        [2 · visual taste],   [cos(img, mean(img positives))],
        [3 · feature taste],  [12-d dot product],
        [4 · price fit],      [−|log p − μ| / max(σ, 0.05)],
        [5 · dismissal],      [dismissed + 0.85-sim neighbours → back],
        table.hline(stroke: 0.5pt + border),
      )
      #v(6pt)
      - Dismissed listings get a hard drop after fusion, not just a demotion.
    ],
  )
]

// =============================================================================
// 10 · Engineering
// =============================================================================

#slide(kicker: "Engineering", title: [Reliability, latency, debuggability.])[
  #grid(
    columns: (1fr, 1fr, 1fr),
    column-gutter: 16pt,
    [
      #head("Tests")
      - *295 passing*, 2 skipped, 11 s default suite.
      - 59 new tests for auth / memory; 0 pre-existing regressed.
      - Smoke suite against the real 25 k-row bundle.
    ],
    [
      #head("Latency")
      - Single `/listings` on 25 k rows, no ML: *~20 ms*.
      - Full pipeline p50 target: *< 2.5 s*.
      - BM25 + soft + memory channels available without torch loaded.
    ],
    [
      #head("Operations")
      - One migration file = single source of schema truth (idempotent ALTERs).
      - ML loaders env-gated (`LISTINGS_VISUAL_ENABLED`, `LISTINGS_TEXT_EMBED_ENABLED`).
      - Every fallback logs `[WARN]` with expected / got / fallback.
    ],
  )

  #v(16pt)
  #grid(
    columns: (1fr, 1fr),
    column-gutter: 16pt,
    [
      #head("Security (personalization)")
      - Generic 401 on login (no existence oracle).
      - Sessions rotate on login + password change, 30-day sliding / 90-day absolute.
      - Login rate-limit: 10 per-username / 20 per-IP per 5 min.
      - Password hashing: argon2id (time=3, mem=64 MiB, parallelism=4).
    ],
    [
      #head("Known non-goals")
      - No OAuth / SSO, no email verification / password reset.
      - No OJP live commute (r5py matrix precomputed instead).
      - No cross-encoder LLM judge at query time.
      - No A/B eval harness yet.
    ],
  )
]

// =============================================================================
// 11 · Worked example
// =============================================================================

#slide(kicker: "Example", title: [One query, end to end.])[
  #panel(tone: ink)[
    #set text(fill: rgb("#FEF8F2"), size: 12pt, font: "Menlo")
    _"günstige ruhige 3-Zimmer-Wohnung in Zürich, nahe Schulen, nahe ETH, 25 Min zum HB"_
  ]

  #v(8pt)
  #grid(
    columns: (1fr, 1.2fr),
    column-gutter: 22pt,
    [
      #head("Extracted QueryPlan")
      #panel(tone: card)[
        #set text(size: 10pt, font: "Menlo")
        ```
        city_slug: "zurich"
        rooms:     { min: 3, max: 3, hard }
        rent_chf:  { max: null, soft: "cheap" }
        soft:      quiet · near_public_transport
                 · near_schools
        commute_target: zurich_hb
        landmarks: [{ text: "ETH",
                      max_minutes: 25 }]
        ```
      ]

      #v(8pt)
      #head("Pipeline")
      #panel(tone: card)[
        #set text(size: 10pt, font: "Menlo")
        ```
        bm25: true · visual: true · text_embed: true
        soft_rankings: 6 · rrf_k: 60
        fused in ~20 ms (no ML)
        ~2.5 s p50 with full ML stack
        ```
      ]
    ],
    [
      #head("Top-3 results")
      #let row(rank, id, price, reason) = block(
        width: 100%,
        inset: 10pt,
        radius: 8pt,
        fill: card,
        stroke: (left: 3pt + orange, rest: 0.5pt + border),
        {
          grid(
            columns: (auto, 1fr, auto),
            column-gutter: 10pt,
            text(size: 18pt, weight: 800, fill: orange, tracking: 0pt)[\##rank],
            [
              #text(size: 11pt, font: "Menlo")[id=#id]
              #v(-2pt)
              #text(size: 10pt, fill: muted, style: "italic", tracking: 0pt)[#reason]
            ],
            text(weight: 700, size: 12pt, tracking: 0pt)[CHF #price],
          )
        },
      )
      #row("1", "10211",     "2,042", "Matched hard filters; 6 soft preferences activated.")
      #v(6pt)
      #row("2", "69b40cfc…", "1,744", "Matched hard filters; text match; 6 soft preferences.")
      #v(6pt)
      #row("3", "695fbad9…", "2,199", "Matched hard filters; text match; 6 soft preferences.")
    ],
  )
]

// =============================================================================
// 12 · Summary
// =============================================================================

#slide(kicker: "Summary", title: [What ships.])[
  #grid(
    columns: (1fr, 1fr, 1fr),
    column-gutter: 18pt,
    [
      #head("Retrieval")
      - 4 fused channels (BM25, Arctic-Embed, SigLIP, soft-signal set).
      - SQL hard-filter as a gate.
      - 8-signal linear blend, percentile-normalised.
      - Templated reason on every ranked listing.
    ],
    [
      #head("Data")
      - 22,819 listings, 4 languages.
      - Canton coverage 99.68 %.
      - 125 k precomputed transit travel-times.
      - 45 landmarks · 30 k image scores · text-derived features across 21 k rows.
    ],
    [
      #head("Bonus")
      - argon2id auth + CSRF + rate-limit.
      - 5 memory rankings appended to RRF.
      - Dismissed listings hard-dropped on the personalized path.
      - Anonymous path unchanged bit-for-bit.
    ],
  )

  #v(18pt)
  #grid(
    columns: (auto, 1fr),
    column-gutter: 14pt,
    align: horizon,
    chip(color: orange)[295 tests passing],
    chip(color: indigo)[0 silent fallbacks],
  )
  #v(6pt)
  #grid(
    columns: (auto, 1fr),
    column-gutter: 14pt,
    align: horizon,
    chip(color: amber)[p50 ~20 ms no-ML · < 2.5 s full],
    chip(color: green)[Docker · public HTTPS · live demo],
  )
]
