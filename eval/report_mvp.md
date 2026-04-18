# MVP eval report

_Queries: 15_

## Headline metrics

| Metric | Value |
|---|---|
| mean HF-P (overall) | 1.000 |
| mean CSR (strict)   | 1.000 |
| coverage (≥5 hits)  | 0.867 |
| p50 latency         | 6287 ms |
| p95 latency         | 8212 ms |
| mean confidence     | 0.859 |

## By stratum

| Stratum | n | HF-P | CSR | Coverage | p50 ms |
|---|---|---|---|---|---|
| clear_hard | 3 | 1.000 | 1.000 | 1.000 | 6547 |
| soft_heavy | 3 | 1.000 | 1.000 | 1.000 | 6224 |
| multilingual | 3 | 1.000 | 1.000 | 0.667 | 6242 |
| landmark | 2 | 1.000 | 1.000 | 1.000 | 5969 |
| adversarial | 4 | 1.000 | 1.000 | 0.750 | 6660 |

## Per-query details

### `c1` · clear_hard · en
> 3-room bright apartment in Zurich under 2800 CHF with balcony, close to public transport

- listings: **10**  ·  HF-P 1.00  ·  CSR 1  ·  latency 6537 ms  ·  confidence 0.95
- top 3:
  1. `212466` · score 0.644 · Zürich · 3.0 rms · CHF 2500 · ['balcony', 'elevator', 'private_laundry']
     — Matches 3 rooms · Zürich · CHF 2500 · balcony, elevator, private_laundry. BM25 rank 2/23.
  2. `59685` · score 0.625 · Zürich · 3.0 rms · CHF 1800 · ['balcony']
     — Matches 3 rooms · Zürich · CHF 1800 · balcony. BM25 rank 3/23.
  3. `27103` · score 0.609 · Zürich · 3.0 rms · CHF 1170 · ['balcony']
     — Matches 3 rooms · Zürich · CHF 1170 · balcony. BM25 rank 1/23. Soft-match: bright.

### `c2` · clear_hard · de
> Helle 3.5-Zimmer-Wohnung in Zürich, nah am Bahnhof, max 2800 CHF

- listings: **10**  ·  HF-P 1.00  ·  CSR 1  ·  latency 6547 ms  ·  confidence 0.92
- top 3:
  1. `212038` · score 0.670 · Zürich · 3.5 rms · CHF 1921 · ['balcony']
     — Matches 3.5 rooms · Zürich · CHF 1921 · balcony. BM25 rank 2/58.
  2. `58962` · score 0.656 · Zürich · 3.5 rms · CHF 2480 · []
     — Matches 3.5 rooms · Zürich · CHF 2480. BM25 rank 4/58.
  3. `212997` · score 0.650 · Zürich · 3.5 rms · CHF 2408 · ['balcony', 'pets_allowed']
     — Matches 3.5 rooms · Zürich · CHF 2408 · balcony, pets_allowed. BM25 rank 1/58. Soft-match: hell.

### `c3` · clear_hard · en
> 2 to 3.5 room apartment in Basel, no basement, with elevator

- listings: **10**  ·  HF-P 1.00  ·  CSR 1  ·  latency 7071 ms  ·  confidence 0.95
- top 3:
  1. `69af71010ad89f01c7f09fab` · score 0.670 · Basel · 2.5 rms · CHF 1755 · ['elevator']
     — Matches 2.5 rooms · Basel · CHF 1755 · elevator. BM25 rank 1/42.
  2. `60104` · score 0.640 · Basel · 2.5 rms · CHF 1520 · ['elevator']
     — Matches 2.5 rooms · Basel · CHF 1520 · elevator. BM25 rank 2/42.
  3. `60115` · score 0.626 · Basel · 2.5 rms · CHF 1280 · ['elevator']
     — Matches 2.5 rooms · Basel · CHF 1280 · elevator. BM25 rank 3/42.

### `s1` · soft_heavy · en 🔄 relaxed
> Modern studio in Geneva for June move-in, quiet area, nice views if possible

- listings: **10**  ·  HF-P 1.00  ·  CSR 1  ·  latency 6025 ms  ·  confidence 0.85
- relaxations: ["Dropped city=['Genève'] (kept canton='GE')", "Dropped canton='GE'"]
- top 3:
  1. `1016` · score 0.655 · Riaz · 1.5 rms · CHF 1050 · []
     — Matches 1.5 rooms · Riaz · CHF 1050. BM25 rank 4/67.
  2. `212124` · score 0.648 · Thusis · 1.5 rms · CHF 790 · ['balcony', 'private_laundry']
     — Matches 1.5 rooms · Thusis · CHF 790 · balcony, private_laundry. BM25 rank 5/67. Soft-match: modern.
  3. `59692` · score 0.644 · Rheineck · 1.0 rms · CHF 850 · ['parking']
     — Matches 1 rooms · Rheineck · CHF 850 · parking. BM25 rank 2/67. Soft-match: modern.

### `s2` · soft_heavy · en
> Bright family-friendly flat in Winterthur, not too expensive, ideally with parking

- listings: **8**  ·  HF-P 1.00  ·  CSR 1  ·  latency 6224 ms  ·  confidence 0.88
- top 3:
  1. `213211` · score 0.676 · Winterthur · 4.0 rms · CHF 2145 · ['balcony', 'parking', 'child_friendly', 'pets_allowed']
     — Matches 4 rooms · Winterthur · CHF 2145 · balcony, parking, child_friendly. BM25 rank 2/8. Price fits moderate sentiment.
  2. `59901` · score 0.638 · Winterthur · 4.5 rms · CHF 3625 · ['balcony', 'parking', 'child_friendly', 'wheelchair_accessible']
     — Matches 4.5 rooms · Winterthur · CHF 3625 · balcony, parking, child_friendly. BM25 rank 1/8.
  3. `212660` · score 0.637 · Winterthur · 1.5 rms · CHF 1685 · ['elevator', 'parking', 'garage', 'child_friendly']
     — Matches 1.5 rooms · Winterthur · CHF 1685 · elevator, parking, garage. BM25 rank 3/8. Price fits moderate sentiment.

### `s3` · soft_heavy · fr 🔄 relaxed
> Cherche studio moderne à Genève, plutôt calme, autour de 1500 CHF

- listings: **10**  ·  HF-P 1.00  ·  CSR 1  ·  latency 8212 ms  ·  confidence 0.75
- relaxations: ["Dropped city=['Genève'] (kept canton='GE')", "Dropped canton='GE'"]
- top 3:
  1. `212124` · score 0.731 · Thusis · 1.5 rms · CHF 790 · ['balcony', 'private_laundry']
     — Matches 1.5 rooms · Thusis · CHF 790 · balcony, private_laundry. BM25 rank 2/68. Price fits moderate sentiment. Soft-match: moderne.
  2. `858` · score 0.727 · Gachnang · 1.0 rms · CHF 990 · []
     — Matches 1 rooms · Gachnang · CHF 990. BM25 rank 3/68. Price fits moderate sentiment. Soft-match: moderne.
  3. `1016` · score 0.721 · Riaz · 1.5 rms · CHF 1050 · []
     — Matches 1.5 rooms · Riaz · CHF 1050. BM25 rank 8/68. Price fits moderate sentiment. Soft-match: calme.

### `m1` · multilingual · it 🔄 relaxed
> Cerco un bilocale a Lugano con balcone, max 2000 CHF

- listings: **5**  ·  HF-P 1.00  ·  CSR 1  ·  latency 6242 ms  ·  confidence 0.93
- relaxations: ['Expanded price ±10% (price=(None,2000) → (None,2200))', "Dropped city=['Lugano'] (kept canton='TI')"]
- top 3:
  1. `212411` · score 0.633 · Caslano · 2.0 rms · CHF 1250 · ['balcony', 'elevator', 'parking', 'garage']
     — Matches 2 rooms · Caslano · CHF 1250 · balcony, elevator, parking. BM25 rank 1/5.
  2. `213486` · score 0.633 · Caslano · 2.0 rms · CHF 1250 · ['balcony', 'elevator', 'parking', 'garage']
     — Matches 2 rooms · Caslano · CHF 1250 · balcony, elevator, parking. BM25 rank 2/5.
  3. `2579` · score 0.417 · Massagno · 2.0 rms · CHF 1520 · ['balcony', 'elevator', 'wheelchair_accessible']
     — Matches 2 rooms · Massagno · CHF 1520 · balcony, elevator, wheelchair_accessible. BM25 rank 3/5.

### `m2` · multilingual · de
> 4.5-Zimmer-Wohnung in Bern mit Lift, unter 3500 Fr., möglichst modern

- listings: **9**  ·  HF-P 1.00  ·  CSR 1  ·  latency 5899 ms  ·  confidence 0.95
- top 3:
  1. `213117` · score 0.629 · Bern · 4.5 rms · CHF 1895 · ['balcony', 'elevator', 'parking']
     — Matches 4.5 rooms · Bern · CHF 1895 · balcony, elevator, parking. BM25 rank 1/9. Soft-match: modern.
  2. `5122` · score 0.629 · Bern · 4.5 rms · CHF 3490 · ['balcony', 'elevator', 'child_friendly', 'pets_allowed']
     — Matches 4.5 rooms · Bern · CHF 3490 · balcony, elevator, child_friendly. BM25 rank 2/9. Soft-match: modern.
  3. `212823` · score 0.529 · Bern · 4.5 rms · CHF 2200 · ['elevator', 'parking']
     — Matches 4.5 rooms · Bern · CHF 2200 · elevator, parking. BM25 rank 4/9. Soft-match: modern.

### `m3` · multilingual · fr
> appartement 2 pièces à Lausanne avec balcon, maximum 2200 CHF

- listings: **1**  ·  HF-P 1.00  ·  CSR 1  ·  latency 6271 ms  ·  confidence 0.95
- top 3:
  1. `213385` · score 0.450 · Lausanne · 2.0 rms · CHF 1400 · ['balcony', 'elevator', 'pets_allowed']
     — Matches 2 rooms · Lausanne · CHF 1400 · balcony, elevator, pets_allowed. BM25 rank 1/1.

### `l1` · landmark · en
> affordable student accommodation near ETH Zurich, modern kitchen preferred

- listings: **10**  ·  HF-P 1.00  ·  CSR 1  ·  latency 5598 ms  ·  confidence 0.85
- top 3:
  1. `10095` · score 0.764 · Zürich · 1.0 rms · CHF 1900 · ['balcony', 'elevator']
     — Matches 1 rooms · Zürich · CHF 1900 · balcony, elevator. BM25 rank 1/100. Price fits cheap sentiment. Soft-match: student.
  2. `212672` · score 0.757 · Zürich · 1.5 rms · CHF 750 · []
     — Matches 1.5 rooms · Zürich · CHF 750. BM25 rank 2/100. Price fits cheap sentiment.
  3. `212772` · score 0.747 · Zürich · 1.0 rms · CHF 1000 · []
     — Matches 1 rooms · Zürich · CHF 1000. BM25 rank 5/100. Price fits cheap sentiment.

### `l2` · landmark · en
> apartment close to Zurich Hauptbahnhof, 2-3 rooms, around 2500 CHF

- listings: **10**  ·  HF-P 1.00  ·  CSR 1  ·  latency 6341 ms  ·  confidence 0.88
- top 3:
  1. `212965` · score 0.767 · Zürich · 2.0 rms · CHF 2220 · ['balcony']
     — Matches 2 rooms · Zürich · CHF 2220 · balcony. BM25 rank 3/100. Price fits moderate sentiment.
  2. `212191` · score 0.720 · Zürich · 2.5 rms · CHF 2134 · ['balcony', 'elevator']
     — Matches 2.5 rooms · Zürich · CHF 2134 · balcony, elevator. BM25 rank 13/100. Price fits moderate sentiment.
  3. `213162` · score 0.717 · Zürich · 2.0 rms · CHF 2089 · []
     — Matches 2 rooms · Zürich · CHF 2089. BM25 rank 6/100. Price fits moderate sentiment.

### `a1` · adversarial · en 🔄 relaxed
> 5 rooms in Geneva under CHF 500

- listings: **1**  ·  HF-P 1.00  ·  CSR 1  ·  latency 9838 ms  ·  confidence 0.95
- relaxations: ['Expanded price ±10% (price=(None,500) → (None,550))', "Dropped city=['Genève'] (kept canton='GE')", "Dropped canton='GE'"]
- top 3:
  1. `58528` · score 0.450 · Windisch · 5.0 rms · CHF 185 · ['elevator', 'parking']
     — Matches 5 rooms · Windisch · CHF 185 · elevator, parking. BM25 rank 1/1.

### `a2` · adversarial · en ❓ clarify ⚠ warn
> nice flat

- listings: **10**  ·  HF-P 1.00  ·  CSR 1  ·  latency 6063 ms  ·  confidence 0.20
- warnings: ['low_confidence_plan: confidence=0.20 (regex fallback likely used; check [WARN] logs)']
- clarification: Where are you looking (city or canton), and what's your rough budget?
- top 3:
  1. `1007` · score 0.649 · Cugy · 3.5 rms · CHF 2200 · ['balcony', 'parking', 'garage', 'private_laundry']
     — Matches 3.5 rooms · Cugy · CHF 2200 · balcony, parking, garage. BM25 rank 6/71.
  2. `10077` · score 0.644 · Oberengstringen · 2.5 rms · CHF 1500 · ['balcony']
     — Matches 2.5 rooms · Oberengstringen · CHF 1500 · balcony. BM25 rank 2/71.
  3. `10042` · score 0.639 · Sitten · 3.5 rms · CHF 1640 · []
     — Matches 3.5 rooms · Sitten · CHF 1640. BM25 rank 3/71.

### `a3` · adversarial · en
> Zuerich 3 room balkony under 2800

- listings: **10**  ·  HF-P 1.00  ·  CSR 1  ·  latency 6287 ms  ·  confidence 0.95
- top 3:
  1. `212337` · score 0.662 · Zürich · 3.0 rms · CHF 1906 · ['balcony']
     — Matches 3 rooms · Zürich · CHF 1906 · balcony. BM25 rank 1/23.
  2. `212466` · score 0.625 · Zürich · 3.0 rms · CHF 2500 · ['balcony', 'elevator', 'private_laundry']
     — Matches 3 rooms · Zürich · CHF 2500 · balcony, elevator, private_laundry. BM25 rank 3/23.
  3. `59685` · score 0.607 · Zürich · 3.0 rms · CHF 1800 · ['balcony']
     — Matches 3 rooms · Zürich · CHF 1800 · balcony. BM25 rank 4/23.

### `a4` · adversarial · en 🔄 relaxed
> big loft with mountain view in St. Moritz, no ground floor, with fireplace

- listings: **10**  ·  HF-P 1.00  ·  CSR 1  ·  latency 7034 ms  ·  confidence 0.92
- relaxations: ["Dropped city=['St. Moritz'] (kept canton='GR')", "Dropped canton='GR'", "Dropped required_features=['fireplace']"]
- top 3:
  1. `4676` · score 0.665 · Au · 2.5 rms · CHF 1515 · ['balcony', 'elevator', 'parking', 'garage']
     — Matches 2.5 rooms · Au · CHF 1515 · balcony, elevator, parking. BM25 rank 2/30.
  2. `400` · score 0.629 · Geneva · 6.0 rms · CHF 7700 · ['balcony', 'elevator']
     — Matches 6 rooms · Geneva · CHF 7700 · balcony, elevator. BM25 rank 1/30.
  3. `212792` · score 0.610 · Dulliken · 4.5 rms · CHF 2320 · ['balcony', 'elevator', 'parking', 'child_friendly']
     — Matches 4.5 rooms · Dulliken · CHF 2320 · balcony, elevator, parking. BM25 rank 6/30.
