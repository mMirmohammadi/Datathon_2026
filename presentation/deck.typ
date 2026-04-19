// =============================================================================
// Robin · Hackathon report
// Datathon 2026 · typst 0.14+
// =============================================================================

#set document(title: "Robin · Datathon 2026 hackathon report")

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

// bullet list, tight
#set list(marker: text(fill: orange, weight: 700, "•"), indent: 0pt, body-indent: 8pt, spacing: 0.8em)

// --- slide helpers -----------------------------------------------------------

#let slide-n = counter("slide-n")
#let slide-total = 13

#let header-strip = {
  set text(size: 10pt, fill: muted, tracking: 0pt)
  grid(
    columns: (1fr, auto),
    align: (left + horizon, right + horizon),
    [#text(weight: 700, fill: ink)[robinreal] #h(4pt) · hackathon report],
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
  column-gutter: 10pt,
  row-gutter: 6pt,
  text(fill: muted, weight: 500, size: 11pt, tracking: 0pt)[#k],
  text(fill: ink, weight: 600, size: 11pt, tracking: 0pt)[#v],
)

#let stat(label, value, color: orange) = block(
  width: 100%,
  inset: 10pt,
  radius: 8pt,
  fill: card,
  stroke: (left: 2pt + color, rest: 0.5pt + border),
  {
    set par(leading: 0.45em)
    text(size: 10pt, fill: muted, tracking: 0pt)[#label]
    linebreak()
    text(size: 15pt, weight: 700, fill: ink, tracking: -0.2pt)[#value]
  },
)

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
  [Robin: a hybrid search]
  linebreak()
  [and ranking system.]
  v(20pt)
  set par(leading: 0.65em)
  set text(size: 16pt, fill: rgb("#CBD5E1"), weight: 400, tracking: 0pt)
  [Hackathon report on a smart system for rental-listing.]

  v(1fr)
  line(length: 2cm, stroke: 2pt + orange)
  v(10pt)
  set text(size: 11pt, fill: rgb("#94A3B8"), tracking: 0pt)
  [RobinReal · April 2026]
})

// =============================================================================
// 2 · Task + data reality
// =============================================================================

#slide(kicker: "The brief", title: [Three required steps, one optional bonus.])[
  #grid(
    columns: (1.1fr, 1fr),
    column-gutter: 28pt,
    [
      #head("What the brief asked for")
      - *Step 1.* Extract hard filters from a natural-language query.
      - *Step 2.* Retrieve candidates that satisfy every hard constraint.
      - *Step 3.* Rank the retrieved set by soft preferences.
      - *Bonus.* Personalize ranking from past interactions.

      #v(16pt)
      #panel(tone: card)[
        #set text(size: 11pt, fill: ink-2, style: "italic")
        _The evaluation weighs hard-filter precision first, then technical depth, feature width, creativity, demo quality, and failure analysis._
      ]
    ],
    [
      #head("Data reality we audited")
      #stat([listings · sources], [22,819 · 4])
      #v(6pt)
      #stat([SRED share with no city, canton, or features], [48 %], color: red)
      #v(6pt)
      #stat([canton known before enrichment], [35 %], color: amber)
      #v(6pt)
      #stat([language mix], [DE 71 · FR 22 · IT 4 · EN 0.4], color: indigo)
    ],
  )
]

// =============================================================================
// 3 · Pipeline
// =============================================================================

#slide(kicker: "Pipeline", title: [Query, gate, fused retrieval, reason.])[
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
    node("Query", "text ± image", c: ink),
    arr,
    node("LLM", "HardFilters, strict JSON", c: coral),
    arr,
    node("SQL gate", "hard filters", c: red),
    arr,
    node("Retrieve", "RRF (k=60) over N rankings", c: orange),
    arr,
    node("Rank", "templated reason string", c: amber),
  ))

  #v(22pt)
  #head("Rankings that feed RRF, one design rule")
  - *BM25.* SQLite FTS5 over `title + description + street + city`.
  - *Dense text.* Snowflake Arctic-Embed-L v2, 1024 dim, L2 normalised.
  - *Visual text-to-image.* SigLIP-2 Giant, max cosine per listing.
  - *Soft rankings.* One best-first list per activated soft preference.
  - *SQL as a gate.* RRF fuses only inside the allowed set.
]

// =============================================================================
// 4 · Step 1 · Query understanding
// =============================================================================

#slide(kicker: "Step 1 · extract hard filters", title: [One LLM call, strict JSON schema, temperature 0.])[
  #grid(
    columns: (1.05fr, 1fr),
    column-gutter: 22pt,
    [
      - Structured outputs with `response_format = json_schema` and `strict = true`. One shot, no retries.
      - Schema enumerates cantons, feature keys, and object categories, so the model cannot emit values outside the allowed set.
      - System prompt encodes hard-vs-soft cue tables in DE / FR / IT / EN and bans inferring features from adjectives (_"modern"_ does NOT imply `new_build`).
      - Sub-city neighbourhoods (_Oerlikon, Plainpalais_) emit both the neighbourhood and its parent city.
      - On any failure the extractor logs `[WARN]` and re-raises. No silent defaults.
    ],
    [
      #head("HardFilters (abridged)")
      #panel(tone: card)[
        #set text(size: 10pt, font: "Menlo")
        ```
        {
          city:              ["zurich"],
          canton:            null,
          max_price:         2800,
          min_rooms:         3.0,
          max_rooms:         3.0,
          features:          ["balcony"],
          features_excluded: null,
          bm25_keywords:     ["bright"],
          soft_preferences: {
            price_sentiment:  "cheap",
            near_public_transport: true,
            commute_target:   "zurich_hb",
            near_landmark:    ["ETH"]
          }
        }
        ```
      ]
    ],
  )
]

// =============================================================================
// 5 · Step 2 · Hard filter as a gate
// =============================================================================

#slide(kicker: "Step 2 · retrieve candidates", title: [Hard filters as a gate, feeding into scoring channels.])[
  #grid(
    columns: (1.1fr, 1fr),
    column-gutter: 24pt,
    [
      - Retrieval universe = the first 300 rows passing the SQL hard filter (`HYBRID_POOL = 300`).
      - BM25 order, dense text, visual, soft and (when present) memory rankings are all computed inside that pool.
      - RRF (k=60) fuses every ranking the turn produces, so a hard violation is structurally unreachable.
      - City match is ASCII-folded on both sides: the extractor emits a canonical slug (_Zuerich_ → `zurich`, _Genf_ → `geneva`), SQL keys on `city_slug`.
      - Nothing is silent. Unknown scrape source, missing visual index, empty gate: each prints a `[WARN]` and the affected channel steps aside.
    ],
    [
      #head("Hard filter fields")
      #panel(tone: card)[
        #set text(size: 10.5pt, fill: ink-2)
        `city`, `postal_code`, `canton` \
        `min_price`, `max_price` \
        `min_rooms`, `max_rooms` \
        `min_area`, `max_area` \
        `min_floor`, `max_floor` \
        `min_year_built`, `max_year_built` \
        `available_from_after` \
        `features` · `features_excluded` (12 flags) \
        `object_category` \
        `min_bathrooms`, `max_bathrooms` \
        `bathroom_shared`, `has_cellar`, `kitchen_shared` \
        `bm25_keywords`, `soft_preferences`
      ]
    ],
  )
]

// =============================================================================
// 6 · Step 3 · Fusion and blend
// =============================================================================

#slide(kicker: "Step 3 · rank by relevance", title: [One fusion step: reciprocal-rank over every signal we produced.])[
  #grid(
    columns: (1fr, 1fr),
    column-gutter: 22pt,
    [
      - *RRF is the ranker.* Each ranking contributes `1 / (k + rank)` per listing (`k = 60`), scores are summed, top-K is sorted once.
      - No weighted linear blend and no cross-encoder re-ranker. Relative channel weight is implicit in how many rankings each channel contributes.
      - Pagination applies after fusion; the candidate pool (300) is kept intact for the reason builder.
      - Dismissed listings take a hard drop after fusion on the personalized path.
      - The reason string on each card is a template over the same per-channel scores, not an LLM paraphrase.

      #v(8pt)
      #panel(tone: card)[
        #set text(size: 11pt, style: "italic", fill: ink-2)
        _"Matched hard filters; text match; visual match (0.24); semantic match (0.61); 6 soft preferences."_
      ]
    ],
    [
      #head("Rankings fused on a typical turn")
      #table(
        columns: (1fr, auto),
        stroke: none,
        inset: (x: 6pt, y: 5pt),
        align: (left, right),
        table.hline(stroke: 0.5pt + border),
        text(size: 10pt, fill: muted, weight: 600, tracking: 0pt)[Source],
        text(size: 10pt, fill: muted, weight: 600, tracking: 0pt)[Rankings],
        table.hline(stroke: 0.5pt + border),
        [BM25 (FTS5 input order)],              [1],
        [SigLIP-2 text-to-image cosine],        [1],
        [Arctic-Embed dense cosine],            [1],
        [DINOv2 image-to-image (if photo sent)],[0 or 1],
        [Soft preferences (one per activated)], [0 to ~9],
        [Memory (personalized, opt-in)],        [0 to 5],
        table.hline(stroke: 0.5pt + border),
      )
      #v(6pt)
      #text(size: 10pt, fill: muted)[_Max observed on a rich turn: ~13 rankings._]
    ],
  )
]

// =============================================================================
// 7 · Going beyond · offline enrichment
// =============================================================================

#slide(kicker: "Beyond the dataset", title: [What we built into the data.])[
  #grid(
    columns: (1.3fr, 1fr),
    column-gutter: 22pt,
    [
      - *Canton closure.* `reverse_geocoder` plus a PLZ majority vote, with gpt-5.4-nano residuals cross-checked against Nominatim.
      - *Transit matrix.* r5py over cleaned Swiss GTFS, four JVM workers at 10 GB each, Haversine 40 km pre-filter.
      - *Landmarks.* 30 hand-curated plus 15 mined from descriptions, Nominatim-resolved, chain-store names pruned.
      - *Text features.* DE / FR / IT / EN regex with a 3-token negation lookback (_kein, ohne, pas de, sans, senza_) over 12 feature flags, `floor`, `year_built`, agency fields.
      - *Image index.* SigLIP-2 Giant over all images (70,548 main + 617 floorplans), plus a DINOv2 ViT-L/14-reg global-descriptor index (1024 dim, GeM pooled) for image-to-image retrieval.
    ],
    // [
      // #head("Numbers")
      // #stat([canton coverage], [87.2 % → 99.68 %], color: green)
    //   #v(6pt)
    //   #stat([commute matrix rows], [125,396], color: indigo)
    //   #v(6pt)
    //   #stat([LLM cost for canton top-up], [CHF 0.02], color: amber)
    //   #v(6pt)
    //   #stat([r5py wall time], [91 min], color: orange)
    // ],
  )
]

// =============================================================================
// 8 · Multimodal
// =============================================================================

#slide(kicker: "Multimodal", title: [Two image channels, both joined via RRF.])[
  #grid(
    columns: (1fr, 1fr),
    column-gutter: 24pt,
    [
      #head("Text-to-image · SigLIP-2 Giant")
      - SigLIP text tower encodes the query once; cosine against each candidate's images, max per listing.
      - The ranking joins RRF with BM25 and Arctic. Empty-text turns skip it (encoder output too neutral).
      - Matrix memory-mapped fp32, shape 70,548 × 1536.
    ],
    [
      #head("Image-to-image · DINOv2 ViT-L/14-reg")
      - A pasted or uploaded photo is encoded with the same 1024-d GeM-pooled descriptor and RRF-fused with every other channel.
      - The matching photo is reordered to position 0 on the card so the user sees _why_ it matched.

      #v(10pt)
      #head("Index assets")
      #stat([main · floorplan images], [70,548 · 617])
    ],
  )
]

// =============================================================================
// 9 · Personalization (bonus)
// =============================================================================

#slide(kicker: "Bonus · personalization", title: [Personalization with memory rankings appended to RRF.])[
  #grid(
    columns: (1.05fr, 1fr),
    column-gutter: 22pt,
    [
      // - Security with argon2id hashes, HttpOnly and `SameSite=Strict` cookies, CSRF double-submit, login rate-limit, session rotation.
      - Event kinds: `bookmark`, `like`, `click`, `dwell`, `dismiss`, and their inverses. 180-day sliding window.
      - Cold-start gate: fewer than 3 positives skips the positive-taste channels. Dismissal still fires.
      - Anonymous path is bit-for-bit identical to the pre-personalization pipeline (pinned by a regression test).
      - Dismissed listings take a hard drop after fusion, not just a demotion.

      #v(8pt)
      #panel(tone: card)[
        #set text(size: 11pt)
        bookmark *+5* · like *+3* · dwell ≥ 5 s *+2* · click *+1* · dismiss *−2*
      ]
    ],
    [
      #head("Memory rankings fed into RRF")
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
        [5 · dismissal],      [dismissed plus 0.85-sim neighbours down],
        table.hline(stroke: 0.5pt + border),
      )
    ],
  )
]

// =============================================================================
// 10 · Engineering
// =============================================================================

#slide(kicker: "Engineering", title: [Reliable, Fast, Strong, Explainable.])[
  #grid(
    columns: (1fr, 1fr, 1fr),
    column-gutter: 16pt,
    stat([tests passing], [295 · 2 skipped · 11 s], color: orange),
    stat([no-ML latency on 25 k rows], [~20 ms], color: amber),
    stat([full-pipeline p50 target], [under 2.5 s], color: green),
  )

  #v(14pt)
  - One migration file is the single source of schema truth (idempotent ALTERs).
  - ML loaders are env-gated: `LISTINGS_VISUAL_ENABLED`, `LISTINGS_TEXT_EMBED_ENABLED`.
  - Every fallback logs `[WARN]` with expected, got, and fallback fields.
  - Sessions rotate on login and password change. Sliding 30-day, absolute 90-day expiry.
  - Login rate-limit: 10 per username and 20 per IP per 5 minutes. argon2id at time=3, memory=64 MiB.

  #v(12pt)
  #panel(tone: card)[
    #set text(size: 10.5pt, fill: muted)
    *Explicit non-goals.* No OAuth or SSO. No password reset. No live OJP at query time (r5py matrix instead). No cross-encoder LLM judge at query time. No A/B evaluation harness.
  ]
]

// =============================================================================
// 11 · Worked example
// =============================================================================

#slide(kicker: "Worked example", title: [One query, end to end.])[
  #panel(tone: ink)[
    #set text(fill: rgb("#FEF8F2"), size: 12pt, font: "Menlo")
    _"günstige ruhige 3-Zimmer-Wohnung in Zürich, nahe Schulen, nahe ETH, 25 Min zum HB"_
  ]

  #v(10pt)
  #grid(
    columns: (1fr, 1.2fr),
    column-gutter: 22pt,
    [
      #head("Extracted HardFilters")
      #panel(tone: card)[
        #set text(size: 10pt, font: "Menlo")
        ```
        city:       ["zurich"]
        min_rooms:  3.0
        max_rooms:  3.0
        soft_preferences: {
          price_sentiment:       "cheap",
          quiet:                 true,
          near_public_transport: true,
          near_schools:          true,
          commute_target:        "zurich_hb",
          near_landmark:         ["ETH"]
        }
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
      #row("1", "10211",     "2,042", "Matched hard filters; 5 soft preferences.")
      #v(6pt)
      #row("2", "69b40cfc…", "1,744", "Matched hard filters; text match; semantic match (0.54); 4 soft preferences.")
      #v(6pt)
      #row("3", "695fbad9…", "2,199", "Matched hard filters; text match; visual match (0.21); 4 soft preferences.")
    ],
  )
]

// =============================================================================
// 12 · Summary
// =============================================================================

#slide(kicker: "Summary", title: [What ships.])[
  - *Retrieval.* BM25, Arctic dense, SigLIP text-to-image, DINOv2 image-to-image, and up to ~9 soft-preference rankings, all behind a SQL hard-filter gate.
  - *Ranking.* N-way reciprocal-rank fusion, `k = 60`, pool size 300, templated reason on every listing.
  - *Data.* 22,819 listings · canton 99.68 % · 125 k r5py transit times · 45 landmarks · 70,548 indexed images.
  - *Personalization.* 5 memory rankings (opt-in), derived from a 180-day interaction window. Dismissed listings hard-dropped.
  - *Engineering.* 295 tests · 0 silent fallbacks · Docker · public HTTPS · live demo.

  #v(28pt)
  #grid(
    columns: (auto, auto, auto, auto),
    column-gutter: 10pt,
    row-gutter: 10pt,
    align: horizon,
    chip(color: orange)[295 tests passing],
    chip(color: indigo)[0 silent fallbacks],
    chip(color: amber)[p50 ~20 ms no-ML],
    chip(color: green)[public HTTPS · live demo],
  )
]

// =============================================================================
// 13 · Thank you
// =============================================================================

#cover-slide({
  set text(fill: rgb("#FEF8F2"))
  v(0.6cm)
  set text(size: 10pt, fill: orange, weight: 700, tracking: 3pt)
  upper("Datathon 2026 · team robin")
  v(1fr)
  set par(leading: 0.4em)
  set text(size: 96pt, weight: 800, fill: rgb("#FEF8F2"), tracking: -2pt)
  [Thank you!]
  v(22pt)
  set par(leading: 0.65em)
  set text(size: 18pt, fill: rgb("#CBD5E1"), weight: 400, tracking: 0pt)
  [Questions?]
  v(1fr)
  line(length: 2cm, stroke: 2pt + orange)
  v(10pt)
  set text(size: 11pt, fill: rgb("#94A3B8"), tracking: 0pt)
  [RobinReal · April 2026]
})
