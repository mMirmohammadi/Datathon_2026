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
#let slide-total = 19

#let header-strip = {
  set text(size: 10pt, fill: muted, tracking: 0pt)
  grid(
    columns: (1fr, auto),
    align: (left + horizon, right + horizon),
    [#text(weight: 700, fill: ink)[robinreal] #h(4pt) · hackathon report #h(4pt) · E27],
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

// --- hero query + highlight helper -------------------------------------------
// Each chunk: (text, list-of-category-names). The first category in the list
// is the "primary" used in all-on mode. When `focus` is an array of category
// names, any chunk that shares a category with `focus` lights up in the color
// of the first matched category; the rest go muted. When `focus` is `none`,
// every chunk renders in its primary color (the hero-query slide).

#let cat-color = (
  hard:         orange,
  bm25:         purple,
  arctic:       indigo,
  siglip:       pink,
  dinov2:       coral,
  soft:         green,
  landmarks:    blue,
  multilingual: amber,
  memory:       red,
  rrf:          ink-2,
)

#let hero-chunks = (
  ("Bright, airy",      ("siglip", "arctic")),
  (" 3.5-room ",        ("hard",)),
  ("Altbau",            ("bm25", "arctic")),
  (" in ",              ()),
  ("Zurich",            ("hard",)),
  (" with ",            ()),
  ("balcony",           ("hard",)),
  (", ",                ()),
  ("no garage",         ("hard",)),
  (", ",                ()),
  ("family-friendly",   ("soft",)),
  (", ",                ()),
  ("quiet",             ("soft",)),
  (", ",                ()),
  ("near ETH",          ("landmarks", "bm25")),
  (", ",                ()),
  ("max 25 min to HB",  ("landmarks", "bm25")),
  (", ",                ()),
  ("under 3200 CHF",    ("hard",)),
  (".",                 ()),
)

#let hero-query(focus: none, size: 14pt) = {
  set text(size: size)
  for chunk in hero-chunks {
    let t = chunk.at(0)
    let cats = chunk.at(1)
    if focus == none {
      if cats.len() == 0 {
        text(fill: muted, weight: 400)[#t]
      } else {
        let c = cat-color.at(cats.at(0))
        text(fill: c.darken(15%), weight: 700)[#t]
      }
    } else {
      let matched = cats.find(c => focus.contains(c))
      if matched != none {
        let c = cat-color.at(matched)
        text(fill: c.darken(15%), weight: 700)[#t]
      } else {
        text(fill: muted-2, weight: 400)[#t]
      }
    }
  }
}

// Reference interior — place `_OPE006.jpeg` next to this file (`presentation/`).
#let ref-photo(height: 5.2cm) = block(
  width: 100%,
  height: height,
  radius: 8pt,
  clip: true,
  stroke: 0.5pt + border,
  image("_OPE006.jpeg", width: 100%, height: 100%, fit: "cover"),
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
  [E27: a hybrid search]
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
// 2 · Hero query
// =============================================================================

#slide(kicker: "The query", title: [Hero query map])[
  #panel(tone: card)[
    #hero-query(size: 19pt)
  ]
  #v(12pt)
  #grid(
    columns: (1fr, 1.3fr),
    column-gutter: 22pt,
    [
      #head("Add a photo")
      #ref-photo()
    ],
    [
      #head("What each slide covers")
      #set text(size: 10.5pt)
      #grid(
        columns: (auto, 1fr),
        column-gutter: 10pt,
        row-gutter: 5pt,
        align: (right + horizon, left + horizon),
        chip([1], color: orange),    [Must-haves you will not skip],
        chip([2], color: purple),    [Words that appear in ads],
        chip([3], color: indigo),    [Sense of the full description],
        chip([4], color: pink),      [What listing photos actually show],
        chip([5], color: coral),     [Similar look to your photo],
        chip([6], color: green),     [Wishes, not strict requirements],
        chip([7], color: blue),     [Trip time to named places],
        chip([8], color: amber),    [Same query in four languages],
        chip([9], color: red),      [Learns from saves and skips],
        chip([10], color: ink-2),   [Fair blend of every signal],
      )
    ],
  )
]

// =============================================================================
// 3 · Pipeline
// =============================================================================

#slide(kicker: "Pipeline", title: [Query to ranking])[
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

  #v(18pt)
  #align(center, grid(
    columns: (auto,) * 9,
    column-gutter: 4pt,
    align: horizon,
    node("Query", "words or photo", c: ink),
    arr,
    node("Model", "reads into rules", c: coral),
    arr,
    node("Database", "drops bad matches", c: red),
    arr,
    node("Retrieve", "merge many signals", c: orange),
    arr,
    node("Explain", "short reason per home", c: amber),
  ))

  #v(40pt)
  #align(center, {
    set par(leading: 0.6em)
    set text(size: 14pt, fill: ink-2, style: "italic")
    [Must-haves filter the list first.]
    linebreak()
    v(2pt)
    [Then merge softer signals fairly.]
  })
]

// =============================================================================
// 4 · Capability 1 · Hard SQL gate
// =============================================================================

#slide(kicker: "1 · Must-haves", title: [Rules before ranking])[
  #panel(tone: card)[
    #hero-query(focus: ("hard",), size: 15pt)
  ]
  #v(16pt)
  #grid(
    columns: (1.05fr, 1fr),
    column-gutter: 22pt,
    [
      - Text turns into search rules.
      - One normal spelling per city.
      - Only qualifying homes pass through.
    ],
    [
      #head("Example output")
      #panel(tone: card)[
        #set text(size: 10.5pt, fill: ink-2)
        City Zurich · rooms 3.5 · max price 3200 \
        Must have balcony · must not have garage
      ]
    ],
  )
]

// =============================================================================
// 5 · Capability 2 · BM25 lexical
// =============================================================================

#slide(kicker: "2 · Exact words", title: [Words in listings])[
  #panel(tone: card)[
    #hero-query(focus: ("bm25",), size: 15pt)
  ]
  #v(16pt)
  #grid(
    columns: (1.05fr, 1fr),
    column-gutter: 22pt,
    [
      - Find your words in listings.
      - Fold accents for easier matching.
      - Only your own words count.
    ],
    [
      #head("Example")
      #panel(tone: card)[
        #set text(size: 11pt, fill: ink-2)
        Keywords: old building, university, main station
      ]
      #v(8pt)
      #stat([Indexed text rows], [25,546], color: purple)
    ],
  )
]

// =============================================================================
// 6 · Capability 3 · Arctic dense semantic
// =============================================================================

#slide(kicker: "3 · Meaning", title: [Beyond exact words])[
  #panel(tone: card)[
    #hero-query(focus: ("arctic",), size: 15pt)
  ]
  #v(16pt)
  #grid(
    columns: (1.05fr, 1fr),
    column-gutter: 22pt,
    [
      - Each home one text vector.
      - Your query becomes a vector too.
      - Nearer text means higher rank.
    ],
    [
      #head("Scale")
      #stat([Listing vectors], [25k], color: indigo)
      #v(6pt)
      #stat([Vector size], [1024 numbers], color: indigo)
    ],
  )
]

// =============================================================================
// 7 · Capability 4 · SigLIP text → image
// =============================================================================

#slide(kicker: "4 · Text to photos", title: [Words meet pictures])[
  #panel(tone: card)[
    #hero-query(focus: ("siglip",), size: 15pt)
  ]
  #v(12pt)
  #grid(
    columns: (1fr, 1.05fr),
    column-gutter: 22pt,
    align: top,
    [
      - One joint space for both.
      - Best photo scores each listing.
      - No text means skip this.

      #v(10pt)
      #stat([Photos indexed], [70k+], color: pink)
    ],
    [
      #head("Matched on \"bright, airy\"")
      #block(
        clip: true,
        radius: 8pt,
        stroke: 0.5pt + border,
        image("4.jpg", height: 6.8cm, fit: "cover"),
      )
      #v(4pt)
      #text(size: 9.5pt, fill: muted, style: "italic")[
        A real listing photo the text tower ranks up for this query.
      ]
    ],
  )
]

// =============================================================================
// 8 · Capability 5 · DINOv2 image → image
// =============================================================================

#slide(kicker: "5 · Photo search", title: [Find similar looks])[
  #panel(tone: card)[
    #grid(
      columns: (1fr, auto),
      column-gutter: 14pt,
      align: horizon,
      hero-query(focus: (), size: 14pt),
      text(size: 14pt, weight: 700, fill: coral.darken(15%))[+ photo],
    )
  ]
  #v(16pt)
  #grid(
    columns: (0.9fr, 1.1fr),
    column-gutter: 22pt,
    align: top,
    [
      #ref-photo(height: 6cm)
    ],
    [
      - Match your photo to all.
      - Show best photo first.
      - Balance text and photo fairly.

      #v(10pt)
      #stat([Quality check], [self-match at top], color: coral)
    ],
  )
]

// =============================================================================
// 9 · Capability 6 · Soft rankings
// =============================================================================

#slide(kicker: "6 · Nice-to-haves", title: [Soft preferences])[
  #panel(tone: card)[
    #hero-query(focus: ("soft",), size: 15pt)
  ]
  #v(16pt)
  #grid(
    columns: (1.05fr, 1fr),
    column-gutter: 22pt,
    [
      - One ranked list per wish.
      - Facts precomputed for every home.
      - Missing data does not hurt.
    ],
    [
      #head("Examples")
      #panel(tone: card)[
        #set text(size: 10.5pt, fill: ink-2)
        Quiet → farther from busy roads \
        Family → schools and play nearby \
        Cheap → price vs local typical
      ]
    ],
  )
]

// =============================================================================
// 10 · Capability 7 · Commute + landmarks
// =============================================================================

#slide(kicker: "7 · Places & travel", title: [Time to places])[
  #panel(tone: card)[
    #hero-query(focus: ("landmarks",), size: 15pt)
  ]
  #v(16pt)
  #grid(
    columns: (1.05fr, 1fr),
    column-gutter: 22pt,
    [
      - Real schedules when we can.
      - Many named places nationwide.
      - Else rough walk-time guess.
    ],
    [
      #head("Data")
      #stat([Trip rows], [125k+], color: blue)
      #v(6pt)
      #stat([Named places], [45], color: blue)
    ],
  )
]

// =============================================================================
// 11 · Capability 8 · Multilingual
// =============================================================================

#slide(kicker: "8 · Languages", title: [Speak your language])[
  #panel(tone: card)[
    #hero-query(size: 13.5pt)
  ]
  #v(12pt)
  #grid(
    columns: (1fr,) * 3,
    column-gutter: 14pt,
    [
      #head("Deutsch")
      #panel(tone: card)[
        #set text(size: 10.5pt, style: "italic", fill: ink-2)
        "Helle, luftige 3.5-Zimmer-Altbau in Zürich mit Balkon, ohne Garage, familienfreundlich, ruhig, nahe ETH, max 25 Min zum HB, unter 3200 CHF."
      ]
    ],
    [
      #head("Français")
      #panel(tone: card)[
        #set text(size: 10.5pt, style: "italic", fill: ink-2)
        "Appartement Altbau lumineux 3.5 pièces à Zurich avec balcon, sans garage, familial, calme, proche ETH, max 25 min de la HB, moins de 3200 CHF."
      ]
    ],
    [
      #head("Italiano")
      #panel(tone: card)[
        #set text(size: 10.5pt, style: "italic", fill: ink-2)
        "Appartamento Altbau luminoso 3.5 locali a Zurigo con balcone, senza garage, adatto alle famiglie, tranquillo, vicino ETH, max 25 min dalla HB, meno di 3200 CHF."
      ]
    ],
  )
  #v(14pt)
  #grid(
    columns: (1fr, 1fr, 1fr),
    column-gutter: 14pt,
    align: top,
    [
      #head("Understanding")
      #set text(size: 10.5pt)
      One model, four languages.
    ],
    [
      #head("Meaning & photos")
      #set text(size: 10.5pt)
      Same models everywhere.
    ],
    [
      #head("Word search")
      #set text(size: 10.5pt)
      Strip accents; text still matches.
    ],
  )
]

// =============================================================================
// 12 · Capability 9 · Personalization (bonus)
// =============================================================================

#slide(kicker: "9 · You", title: [Learns your taste])[
  #panel(tone: card)[
    #hero-query(focus: (), size: 13.5pt)
  ]
  #v(12pt)
  #grid(
    columns: (1fr, 1fr),
    column-gutter: 22pt,
    [
      #head("Person A")
      #panel(tone: card)[
        #set text(size: 10.5pt, fill: ink-2)
        Saved modern city flats; hid suburban houses. \
        #v(4pt)
        #text(weight: 700, fill: red.darken(10%))[Top pick:] loft near center, 3100
      ]
    ],
    [
      #head("Person B")
      #panel(tone: card)[
        #set text(size: 10.5pt, fill: ink-2)
        Liked family homes near parks; hid shared rooms. \
        #v(4pt)
        #text(weight: 700, fill: red.darken(10%))[Top pick:] flat by playground, 2980
      ]
    ],
  )
  #v(14pt)
  #grid(
    columns: (auto, 1fr, auto, 1fr),
    column-gutter: 10pt,
    row-gutter: 6pt,
    align: left + horizon,
    chip([text taste], color: red), text(size: 10pt)[likes vs past saves],
    chip([photo taste], color: red),   text(size: 10pt)[style of photos you liked],
    chip([features], color: red),      text(size: 10pt)[balcony, lift, etc.],
    chip([price habit], color: red),   text(size: 10pt)[typical rent you pick],
  )
  #v(8pt)
  #set text(size: 10pt, fill: muted, style: "italic")
  Skipped homes stay hidden when you are signed in.
]

// =============================================================================
// 13 · Capability 10 · RRF payoff
// =============================================================================

#slide(kicker: "10 · Merge", title: [One fair blend])[
  #grid(
    columns: (1fr, 1.15fr),
    column-gutter: 24pt,
    align: top,
    [
      #head("How merge works")
      #panel(tone: card)[
        #set text(size: 12pt, fill: ink-2)
        Each signal votes with rank. \
        Higher rank anywhere helps. \
        Sum votes; sort; show page.
      ]
      #v(12pt)
      #head("Signals in one query")
      #set text(size: 10.5pt)
      #table(
        columns: (1fr, auto),
        stroke: none,
        inset: (x: 4pt, y: 5pt),
        align: (left, right),
        table.hline(stroke: 0.5pt + border),
        text(fill: muted, weight: 600, tracking: 0pt)[Kind],
        text(fill: muted, weight: 600, tracking: 0pt)[Count],
        table.hline(stroke: 0.5pt + border),
        [Word match],        [1],
        [Meaning],           [1],
        [Text → photos],     [1],
        [Photo → photos],    [1],
        [Nice-to-haves],     [few],
        [Your history],      [few],
        table.hline(stroke: 0.5pt + border),
        [*Rough total*],     [*~13 max*],
        table.hline(stroke: 0.5pt + border),
      )
    ],
    [
      #head("Example top three")
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
              #text(size: 9.5pt, fill: muted, style: "italic", tracking: 0pt)[#reason]
            ],
            text(weight: 700, size: 12pt, tracking: 0pt)[CHF #price],
          )
        },
      )
      #row("1", "10211",     "2,980", "Must-haves ok; words; photos; meaning; soft cues.")
      #v(6pt)
      #row("2", "69b40cfc…", "3,100", "Must-haves ok; words; photos; soft cues.")
      #v(6pt)
      #row("3", "695fbad9…", "2,850", "Must-haves ok; photos; meaning; soft cues.")
    ],
  )
]

// =============================================================================
// 14 · Summary
// =============================================================================

#slide(kicker: "Summary", title: [What we built])[
  // QR card pinned to the top-right corner of the slide body so the 2×
  // image has room without pushing the slide onto a second page.
  #place(
    top + right,
    dx: 0pt,
    dy: -4pt,
    block(
      fill: card,
      stroke: 0.5pt + border,
      radius: 10pt,
      inset: 8pt,
      width: 10.8cm,
      {
        set text(tracking: 0pt)
        set par(leading: 0.4em)
        align(center, image("QR.svg", width: 10cm))
        v(2pt)
        align(center, text(
          size: 10pt,
          fill: orange,
          weight: 700,
          tracking: 1.2pt,
        )[#upper("Scan to try it")])
      },
    ),
  )

  - Hard rules first. All else second.
  - Cover words, sense, photos, habits.
  - Everything merges in one stage.

  #v(24pt)
  #grid(
    columns: (auto, auto),
    column-gutter: 10pt,
    row-gutter: 10pt,
    align: horizon,
    chip(color: orange)[10 ideas],
    chip(color: purple)[1 merge],
    chip(color: indigo)[warns, never hides],
    chip(color: green)[live demo online],
  )
]

// =============================================================================
// 15 · Beyond the dataset · enrichment
// =============================================================================

#slide(kicker: "Beyond the data", title: [Richer data])[
  #set text(size: 13pt, fill: ink-2)
  The raw dataset had holes and missing signals. We closed them, on purpose.
  #v(14pt)
  #grid(
    columns: (1.1fr, 1fr),
    column-gutter: 22pt,
    align: top,
    [
      #set par(leading: 0.65em)
      - *Every listing knows its canton.* \
        We geocoded the address; for the few rows without coordinates, we used the postal code to infer the canton.
      - *Real commute minutes.* \
        We precomputed door-to-door travel time by rail and bus for every listing using the official Swiss transit schedule.
      - *Places people actually ask about.* \
        45 Swiss train stations, universities and lakes — each hand-verified on a map before entering the system.
      - *Features understood in four languages.* \
        We parsed the description in DE, FR, IT and EN to flag balcony, elevator, parking, fireplace and 8 more.
      - *Every photo made searchable.* \
        70,548 listing photos encoded as vectors so queries can match by words or by look-alike photo.
    ],
    [
      #grid(
        columns: (1fr, 1fr),
        column-gutter: 10pt,
        row-gutter: 10pt,
        stat([Canton coverage], [*99.68%* · 0 unknown], color: green),
        stat([Commute rows], [*125,396* trips], color: blue),
        stat([Named places], [*45* verified], color: amber),
        stat([Photo vectors], [*70,548* encoded], color: pink),
      )
      #v(10pt)
      #panel(tone: card)[
        #set text(size: 10pt, fill: ink-2)
        #set par(leading: 0.65em)
        *Why it matters.* \
        Every signal above is measured and sourced — so when the ranker says _"14 min to HB by transit"_ or _"balcony: present"_, it is reading a fact we computed, not a guess.
      ]
    ],
  )
]

// =============================================================================
// 16-18 · Live demo · full-bleed screenshots with overlay label
// =============================================================================

// Full-bleed demo slide: image fills the whole page (fit: contain preserves
// aspect), a compact dark "bubble" carries the kicker + title + caption, and a
// small counter sits in the opposite corner. `pos` picks which corner holds
// the label; dx/dy fine-tune the offset away from the slide edge.
#let demo-slide(
  path,
  kicker: none,
  title: none,
  caption: none,
  pos: bottom + left,
  dx: 1cm,
  dy: -1cm,
  counter-pos: bottom + right,
  counter-dx: -0.6cm,
  counter-dy: -0.5cm,
) = {
  slide-n.step()
  page(
    margin: 0pt,
    fill: bg,
    {
      place(
        center + horizon,
        image(path, width: 100%, height: 100%, fit: "contain"),
      )
      place(
        pos,
        dx: dx,
        dy: dy,
        block(
          fill: ink,
          stroke: (left: 3pt + orange, rest: 0.5pt + ink-2),
          radius: 10pt,
          inset: (x: 16pt, y: 12pt),
          width: 11.5cm,
          {
            set par(leading: 0.4em)
            if kicker != none {
              set text(size: 9pt, fill: orange, weight: 700, tracking: 2pt)
              upper(kicker)
              linebreak()
              v(3pt)
            }
            if title != none {
              text(
                size: 20pt,
                weight: 800,
                fill: rgb("#FEF8F2"),
                tracking: -0.3pt,
              )[#title]
              linebreak()
              v(4pt)
            }
            if caption != none {
              set text(size: 10pt, fill: rgb("#CBD5E1"))
              caption
            }
          },
        ),
      )
      place(
        counter-pos,
        dx: counter-dx,
        dy: counter-dy,
        context text(size: 9pt, fill: muted, tracking: 0pt)[
          #slide-n.display() / #slide-total
        ],
      )
    },
  )
}

// Slide 16 — result card. Top-right keeps the photo, the listing details,
// and the "How we ranked this home" breakdown bars fully visible; the label
// sits over the top of the right-hand query-plan panel (already shown full
// on slide 2, so no information is lost).
#demo-slide(
  "1.png",
  kicker: "Live demo · 1",
  title: [Top match],
  caption: [
    Same hero query + reference photo. \
    The top home the system returns.
  ],
  pos: top + right,
  dx: -1cm,
  dy: 1cm,
  counter-pos: bottom + right,
)

// Slide 17 — reason panel. Top-right places the label over the query-plan
// panel (which we already showed on slide 2), keeping the fact rows + taste
// bars fully visible.
#demo-slide(
  "2.png",
  kicker: "Live demo · 2",
  title: [Why it ranks here],
  caption: [
    Fact-by-fact checks, matched words, \
    soft cues and your taste bars.
  ],
  pos: top + right,
  dx: -1cm,
  dy: 1cm,
  counter-pos: bottom + right,
)

// Slide 18 — map. Bottom-left overlays the leaflet attribution strip only,
// leaving every pin, highlighted canton and landmark visible.
#demo-slide(
  "3.png",
  kicker: "Live demo · 3",
  title: [Every option on the map],
  caption: [
    All ranked matches plus named places nearby — \
    pan, filter, and refine live.
  ],
)

// =============================================================================
// 19 · Thank you
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
