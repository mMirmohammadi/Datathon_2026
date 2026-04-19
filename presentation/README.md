# Pitch deck — Robin hybrid listing search & ranking

12-slide deck for the robinreal Datathon 2026 jury. Bullet-driven, palette matched to `app/static/demo.css`.

## Build

```bash
typst compile deck.typ deck.pdf
typst watch deck.typ deck.pdf   # live reload while editing
```

`brew install typst` (macOS). Uses Helvetica Neue + Menlo from the OS.

## Slide index

| # | Kicker | Title |
|---|---|---|
| 1 | Datathon 2026 · robinreal | Robin — hybrid listing search and ranking. |
| 2 | Problem | Queries mix two incompatible intents. |
| 3 | Pipeline | Query → gate → fused retrieval → blend. |
| 4 | Gate | Hard filters are a gate, not a ranking. |
| 5 | Layer 1 | Query understanding — strict JSON, grounded spans. |
| 6 | Layer 3 | N-way RRF fusion + 8-signal linear blend. |
| 7 | Layer 2 | Enrichment — fix the data before you rank. |
| 8 | Multimodal | Image signals — extract, score, cross-check. |
| 9 | Bonus | Personalization — five memory rankings, opt-in. |
| 10 | Engineering | Reliability, latency, debuggability. |
| 11 | Example | One query, end to end. |
| 12 | Summary | What ships. |

## Palette (from `app/static/demo.css`)

| Token | Hex | Use |
|---|---|---|
| `bg` | `#FEF8F2` | slide background (warm cream) |
| `ink` | `#1E293B` | body text |
| `muted` | `#64748B` | labels / footer |
| `orange` | `#FB923C` | primary accent, kickers, bullets |
| `coral` | `#FB7185` | secondary accent |
| `amber` | `#F59E0B` | stats highlight |
| `indigo` | `#6366F1` | `hide` / negative action tone |
| `purple` | `#8B5CF6` | memory layer |
| `green` | `#10B981` | positive state |
| `red` | `#EF4444` | `love` / hard-filter callout |

Cover uses `#1E293B` ink as background + cream text.
