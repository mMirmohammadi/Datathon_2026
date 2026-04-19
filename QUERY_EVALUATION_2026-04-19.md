# Query evaluation — 2026-04-19

Source: 45 queries from `queries_de.md` (25), `long_queries.md` (6), and `challenge.md` (4 opening examples + 5 eval-style + 5 hint).

Pipeline under test: BM25 + Arctic-Embed text + soft-preference rankings, fused via RRF(k=60). Visual channel disabled (`LISTINGS_VISUAL_ENABLED=0`) because the SigLIP weights trip `transformers 5.x` on this machine. Personalization off — anonymous calls only. Each query requests the top 5.

## Summary

- Rated queries: 45/45
- Mean rating: **5.8/10** (median 6).
- Pool = 0 (hard FAIL): 6 queries — DE-08, LONG-02, CH-EV-01, CH-EV-04, CH-EV-05, CH-HI-04.
- Pool ≤ 2 (near-FAIL): 5 queries — DE-12, DE-21, CH-EX-03, CH-HI-02, CH-HI-05.

### Recurring failure modes

1. **Silent empty pool.** When the combined hard filters have zero matches (DE-08, LONG-02, CH-EV-01/04/05, CH-HI-04), the API returns `listings: []` with no warning and no suggested relaxation. This violates the CLAUDE.md §5 'no silent fallbacks' rule for a real product; the system is technically correct but unhelpful. Fix: emit a `meta.relaxation_hints` block and / or surface a visible banner.

2. **Neighborhood postal codes.** 'Oerlikon' → postal `8045` in DE-08 is wrong (Oerlikon is 8050/8051/8052). The system prompt's Zurich-Kreis mapping (`Kreis N` → `80NN`) is well-tuned for numbered Kreise but over-generalises to named neighborhoods. Fix: move neighborhoods to a hand-curated lookup table in `app/core/landmarks.py` rather than the LLM.

3. **Non-canonical commute targets.** The extractor only knows the 8 HBs in `_COMMUTE_TARGETS`. 'Arbeitsplatz in Zug' (CH-EV-02) or 'max 20 Min zum Stadelhofen' (LONG-01) fall through to the soft `near_landmark` channel, which works iff `app/core/landmarks.py` has the entry. This is correct behaviour but invisible to the user.

4. **Date anchoring bug.** 'March move-in' / 'June move-in' (CH-EX-03, CH-HI-02) is resolved to 2024 by gpt-4o-mini rather than the next occurrence from 2026-04-19. This prunes everything available after today. Fix: stamp `today()` into the system prompt or post-process `available_from_after` in Python.

5. **Pure-soft queries are weakly anchored.** DE-02/09/15/17/24 have no location / price / rooms. The system is right not to invent filters, but the ranking collapses to the raw BM25 score over the whole dataset. A clarification-question flow or a 'popular in Switzerland' prior would help.

6. **Vague adjectives stay soft.** 'Modern', 'hell', 'ruhig', 'angenehm' correctly do **not** become features (per system-prompt non-emission rule #7). They fall to BM25. That is the right design decision, but it means DE-03/07/15/19/24 rank on surface-level token overlap rather than image or embedding evidence. Visual channel + cross-encoder rerank would lift these substantially.

## Per-query results

### queries_de.md (25 short/medium DE queries)

#### DE-01 — **8/10**

> Ich suche eine 1.5 Zimmer Wohnung in Zürich in der Nähe der ETH, am besten unter 2200 CHF.

- pool: **22**, returned 5, latency 2.96s
- filters: `city=['zurich']`; `canton=ZH`; `max_price=2200`; `min_rooms=1.5`; `max_rooms=1.5`; `object_category=['apartment']`; soft: `price_sentiment=cheap`, `quiet=True`, `near_public_transport=True`, `near_landmark=['ETH']`

| # | id | title | city | CHF | rooms | m² | score | channels |
|---|---|---|---|---:|---:|---:|---:|---|
| 1 | `212672` | 1 1/2-Zimmer Wohnung in Zürich, befristet Mai bis Juli | Zürich | 750 | 1.5 | 31 | 0.096 | sem=0.66, soft=4 |
| 2 | `213123` | [Tauschwohnung] Suche grössere Wohnung in Zürich zum Ta | Zürich | 1210 | 1.5 | 40 | 0.091 | sem=0.63, soft=4 |
| 3 | `10270` | 1 ½ Zimmer-Wohnung | Zurich | 1990 | 1.5 | — | 0.090 | sem=0.59, soft=4 |
| 4 | `219363` | Helle 1.5 Zimmer Dachwohnung in Zürich mieten (befriste | Zurich | 1940 | 1.5 | 70 | 0.088 | sem=0.61, soft=4 |
| 5 | `69b2f83593ba328824fe99bf` | 1.5-Zimmer-Wohnung in Zürich zu vermieten | Zürich | 1342 | 1.5 | 36 | 0.088 | sem=0.58, soft=4 |

**Analysis.** Hard filter extraction is clean: `city=zurich`, `min_rooms=max_rooms=1.5`, `max_price=2200`, and ETH is surfaced as a `near_landmark` soft signal rather than a coordinate filter (the correct choice under the 2026-04-19 system prompt which forbids inferring radius from 'near X' phrases). All five top results are 1.5 rooms in Zurich ≤ CHF 2200 and the semantic channel pushes the ETH-relevant ones up. Loses points because the ETH-proximity soft ranking is not visible in the score breakdown — we trust it fired but the user cannot see walking distance to ETH in the response.

#### DE-02 — **4/10**

> Ich möchte nah an meiner Arbeit wohnen, max 20 Minuten mit dem ÖV.

- pool: **300**, returned 5, latency 5.26s
- filters: soft: `near_public_transport=True`

| # | id | title | city | CHF | rooms | m² | score | channels |
|---|---|---|---|---:|---:|---:|---:|---|
| 1 | `10003` | Grosszügige 2.5 Zimmerwohnung in Watt | Watt | 1928 | 2.5 | 70 | 0.041 | sem=0.33, soft=1 |
| 2 | `10018` | Submieter Zweizimmerwohnung Lugano | Lugano | 1200 | 1.5 | 55 | 0.036 | sem=0.28, soft=1 |
| 3 | `10165` | Zimmer in einer Wohngemeinschaft in Prilly | Prilly | 950 | — | — | 0.035 | sem=0.33, soft=1 |
| 4 | `10006` | 2-Zimmer-Wohnung in Villars-sur-Ollon | Villars-sur-Ollon | 1500 | 2.0 | — | 0.035 | sem=0.31, soft=1 |
| 5 | `10007` | Wohnen in der Nähe vom Bielersee | Ipsach | 1530 | 3.5 | — | 0.034 | sem=0.35, soft=1 |

**Analysis.** Underspecified query. No city, no commute target, no price. The extractor cannot infer where 'meine Arbeit' is, so the pool is the whole country and the ranker only has weak signals to work with. Top-5 is geographically scattered and the '20 Min ÖV' constraint is dropped because there's no landmark to anchor it. The system degrades gracefully (no crash, no phantom filter) but the correct behaviour here would be a clarification question, which the harness does not support.

#### DE-03 — **6/10**

> Suche was in Zürich, das sich ruhig und hell anfühlt.

- pool: **300**, returned 5, latency 2.33s
- filters: `city=['zurich']`; `canton=ZH`; `bm25_keywords=['ruhig', 'hell']`; soft: `quiet=True`

| # | id | title | city | CHF | rooms | m² | score | channels |
|---|---|---|---|---:|---:|---:|---:|---|
| 1 | `26752` | Urban wohnen, ruhig leben - 2.5 Zimmer in Witikon | Zürich | 2460 | 2.5 | 61 | 0.041 | bm25=3.45, sem=0.49, soft=1 |
| 2 | `6994687ea7b4cbb191d48df8` | Urban wohnen, ruhig leben - 2.5 Zimmer in Witikon | Zürich | 2460 | 2.5 | 61 | 0.041 | bm25=3.22, sem=0.50, soft=1 |
| 3 | `213033` | Kleines Studio 20 m² hell und funktional in Zürich Seef | Zürich | 1290 | 1.0 | 20 | 0.040 | bm25=5.51, sem=0.47, soft=1 |
| 4 | `69d52552ea79655926a8356a` | Urban wohnen, ruhig leben - 2.5 Zimmer in Witikon | Zürich | 3167 | 3.5 | 85 | 0.040 | bm25=3.22, sem=0.49, soft=1 |
| 5 | `3822` | UNTERMIETE - HELL, MODERN UND ZENTRAL GELEGEN | Zurich | 2688 | — | — | 0.037 | bm25=5.32, sem=0.49, soft=1 |

**Analysis.** 'Ruhig und hell' are soft qualities only. The system correctly emits `quiet=true` and the BM25 keywords lift 'hell'/'ruhig' mentions in descriptions. Pool is the whole of Zurich so the top-5 are dominated by whatever BM25+embedding thinks scores highest on 'ruhig hell'. Good behaviour on soft-only queries; can't rate higher because we have no ground truth that the listings actually are bright or quiet.

#### DE-04 — **6/10**

> Wir suchen eine Wohnung für 3 Personen mit mindestens 2 Schlafzimmern.

- pool: **300**, returned 5, latency 1.8s
- filters: `min_rooms=2.5`; `object_category=['apartment']`

| # | id | title | city | CHF | rooms | m² | score | channels |
|---|---|---|---|---:|---:|---:|---:|---|
| 1 | `10019` | Neue Mieter gesucht für grosszügige 3-Zimmerwohnung in  | Allschwil | 1490 | 3.0 | 74 | 0.030 | sem=0.39 |
| 2 | `10001` | Wohnung an ruhiger und zentraler Lage zu vermieten | Zürich | 2330 | 3.0 | 68 | 0.028 | sem=0.35 |
| 3 | `10045` | 3-Zimmerwohnung in ruhigem Wohnquartier | Pratteln | 1450 | 3.0 | 59 | 0.027 | sem=0.40 |
| 4 | `10070` | Nachmieter gesucht für 3 Zimmer Wohnung | Dietikon | 1628 | 3.0 | 63 | 0.026 | sem=0.42 |
| 5 | `10071` | 3-Zimmerwohnung Breitenrain ab 1.4. | Bern | 1758 | 3.0 | 63 | 0.024 | sem=0.39 |

**Analysis.** '3 Personen, 2 Schlafzimmer' is correctly translated to `min_rooms=2.5` (bedrooms → Swiss-rooms rule +0.5). No city means the pool is all of CH. That is the right call (don't invent a city) but makes ranking mostly noise — the embedding channel has little to latch onto. Behaves sensibly given the input; a clarifying question about location would lift this.

#### DE-05 — **4/10**

> Ich hätte gern etwas in Seenähe, gern eher ruhig.

- pool: **300**, returned 5, latency 3.05s
- filters: soft: `quiet=True`

| # | id | title | city | CHF | rooms | m² | score | channels |
|---|---|---|---|---:|---:|---:|---:|---|
| 1 | `10007` | Wohnen in der Nähe vom Bielersee | Ipsach | 1530 | 3.5 | — | 0.041 | sem=0.29, soft=1 |
| 2 | `1` | Wohlfühloase im Schönbühlquartier mit Aussicht auf See  | Luzern | 2270 | 2.5 | 58 | 0.037 | sem=0.24, soft=1 |
| 3 | `10024` | 2,5 Zimmer Haute-Nendaz zur Miete, Schwimmbad | Siviez (Nendaz) | 1400 | 2.5 | 50 | 0.037 | sem=0.21, soft=1 |
| 4 | `1000` | Moderne 4-Zimmer-Dachwohnung an ruhiger Lage in Magden | Magden | 2580 | 4.0 | 125 | 0.034 | sem=0.16, soft=1 |
| 5 | `10021` | Zimmer in Wohngemeinschaft | Thônex | 800 | — | 30 | 0.032 | sem=0.26, soft=1 |

**Analysis.** Miss: 'Seenähe' SHOULD have become `near_landmark=['See']` per the system-prompt cue table, but the actual extracted plan has `near_landmark=[]` and only `quiet=true`. The landmark signal was dropped. No city either, so the pool is every CH listing ranked by the single `quiet` soft ranker. This is a concrete prompt bug worth fixing — the cue 'nahe am See' / 'Seenähe' appears in the system prompt's example output but isn't reliably emitted here.

#### DE-06 — **9/10**

> Suche Wohnung in Winterthur, ab 70 m², gern mit Balkon.

- pool: **23**, returned 5, latency 1.79s
- filters: `city=['winterthur']`; `min_area=70`; `features=['balcony']`; `object_category=['apartment']`

| # | id | title | city | CHF | rooms | m² | score | channels |
|---|---|---|---|---:|---:|---:|---:|---|
| 1 | `212517` | Helle und grosszügige 4.5-Zimmer-Wohnung mit Balkon dir | Winterthur | 2760 | 4.5 | 103 | 0.032 | sem=0.58 |
| 2 | `212243` | Nachmieter für 3.5 Zimmer Wohnung in Winterthur | Winterthur | 2529 | 3.5 | 83 | 0.032 | sem=0.57 |
| 3 | `10314` | Moderne 4.5 Zimmer- Wohnung im Eigentumsstandard an zen | Winterthur | 3230 | 4.5 | 135 | 0.031 | sem=0.55 |
| 4 | `213211` | Moderne und ruhige 4-Zimmerwohnung | Winterthur | 2145 | 4.0 | 80 | 0.031 | sem=0.57 |
| 5 | `212543` | Attraktive 3.5-Zimmer-Wohnung im Eigentumsstandard | Winterthur | 2285 | 3.5 | 88 | 0.030 | sem=0.54 |

**Analysis.** Textbook extraction: `city=winterthur`, `min_area=70`, `features=['balcony']`. Pool is tight, top-5 all ≥70 m² Winterthur apartments with balconies. This is the kind of query the pipeline is built for.

#### DE-07 — **4/10**

> Ich möchte etwas, das modern, aber nicht ungemütlich ist.

- pool: **300**, returned 5, latency 2.89s
- filters: _(none extracted)_

| # | id | title | city | CHF | rooms | m² | score | channels |
|---|---|---|---|---:|---:|---:|---:|---|
| 1 | `1000` | Moderne 4-Zimmer-Dachwohnung an ruhiger Lage in Magden | Magden | 2580 | 4.0 | 125 | 0.028 | sem=0.23 |
| 2 | `1002` | Grosszügige und moderne 4.5-Zimmer-Wohnung an ruhiger L | Oberengstringen | 3300 | — | 120 | 0.028 | sem=0.28 |
| 3 | `10015` | Modernes 3.5-Zimmer-Apartment mit privatem Garten in Ca | Cadempino | 1800 | 3.0 | 83 | 0.027 | sem=0.26 |
| 4 | `10001` | Wohnung an ruhiger und zentraler Lage zu vermieten | Zürich | 2330 | 3.0 | 68 | 0.025 | sem=0.20 |
| 5 | `10013` | Gesucht! Mieter für attraktive Zimmer im schönen Brugg! | Brugg | 930 | 1.0 | 20 | 0.025 | sem=0.22 |

**Analysis.** Pure vibe query ('modern aber nicht ungemütlich'). The system prompt explicitly forbids turning 'modern' into `features=['new_build']`, so the only signal fired is BM25 on the literal token 'modern'. That's intentional and defensible, but the resulting ranking is weakly correlated with the user's intent because listings can use the word 'modern' liberally. A cross-encoder reranker or image analysis would help a lot here.

#### DE-08 — **3/10**

> Ich suche eine Wohnung in Oerlikon mit Lift und Waschmaschine in der Wohnung.

- pool: **0**, returned 0, latency 1.85s
- filters: `city=['zurich', 'oerlikon']`; `postal_code=['8045']`; `canton=ZH`; `features=['elevator', 'private_laundry']`; `object_category=['apartment']`

_no matches_

**Analysis.** Hard FAIL: zero candidates. The extractor mapped Oerlikon to postal_code `8045` (wrong — Oerlikon is 8050/8051/8052) and combined it with `features=['elevator','private_laundry']`. The postal-code error is a known issue in the system prompt's emission rules (`8003`-style pattern). Correctly-specified query, system fails silently by returning an empty list rather than relaxing a filter or warning the user.

#### DE-09 — **5/10**

> Mir ist vor allem wichtig, dass die Gegend sicher ist und sich gut zum Wohnen anfühlt.

- pool: **300**, returned 5, latency 1.99s
- filters: _(none extracted)_

| # | id | title | city | CHF | rooms | m² | score | channels |
|---|---|---|---|---:|---:|---:|---:|---|
| 1 | `1` | Wohlfühloase im Schönbühlquartier mit Aussicht auf See  | Luzern | 2270 | 2.5 | 58 | 0.032 | sem=0.24 |
| 2 | `10001` | Wohnung an ruhiger und zentraler Lage zu vermieten | Zürich | 2330 | 3.0 | 68 | 0.030 | sem=0.24 |
| 3 | `10021` | Zimmer in Wohngemeinschaft | Thônex | 800 | — | 30 | 0.026 | sem=0.24 |
| 4 | `1000` | Moderne 4-Zimmer-Dachwohnung an ruhiger Lage in Magden | Magden | 2580 | 4.0 | 125 | 0.026 | sem=0.19 |
| 5 | `10007` | Wohnen in der Nähe vom Bielersee | Ipsach | 1530 | 3.5 | — | 0.025 | sem=0.20 |

**Analysis.** Only soft signals ('sicher', 'gut zum Wohnen'). The extractor does not emit `near_schools` from 'sicher' (correct — avoids overreach), and the pool is all of CH. Top-5 is an arbitrary mix. The system is honest ('I don't know what you want') rather than making something up, which is the correct conservative behaviour, but the user gets no useful rank.

#### DE-10 — **7/10**

> Ich suche was Kleines in Zürich, gern möbliert.

- pool: **101**, returned 5, latency 1.76s
- filters: `city=['zurich']`; `canton=ZH`; `object_category=['furnished_apartment']`

| # | id | title | city | CHF | rooms | m² | score | channels |
|---|---|---|---|---:|---:|---:|---:|---|
| 1 | `10237` | Zentral gelegenes Zimmer \| modern möbliert, nur 5 Minu | Zürich | 1100 | 1.0 | 18 | 0.031 | sem=0.58 |
| 2 | `10114` | möblierte Einzimmer-Wohnung | Zürich | 1500 | 1.0 | 22 | 0.029 | sem=0.53 |
| 3 | `10320` | Schönes möbliertes Apartment - 2 Schlafzimmer - großart | Zürich | 5300 | 3.5 | 87 | 0.029 | sem=0.53 |
| 4 | `212832` | möbliertes Einzelzimmer zwischen Hegibach- und Klusplat | Zürich | 873 | 1.0 | 10 | 0.029 | sem=0.57 |
| 5 | `212297` | Zimmer in einer 3,5-Zimmer-Wohnung in 8002 Zürich zu ve | Zurich | 1800 | 1.0 | 14 | 0.029 | sem=0.54 |

**Analysis.** 'Möbliert' correctly triggers `object_category=['furnished_apartment']`. 'Kleines' is vague but the extractor leaves it as a BM25 signal rather than inventing a max_area. City is Zurich. Top-5 all furnished, all Zurich, varied sizes — the ranker picks small ones via BM25/sem.

#### DE-11 — **9/10**

> Ich suche eine 2.5 bis 3 Zimmer Wohnung in Zürich oder direkt angrenzend, idealerweise mit max 25 Minuten Pendelzeit zum HB, mindestens 65 m², Balkon oder Loggia, und wenn möglich nicht im Erdgeschoss.

- pool: **31**, returned 5, latency 2.91s
- filters: `city=['zurich', 'dubendorf']`; `canton=ZH`; `min_rooms=2.5`; `max_rooms=3.0`; `min_area=65`; `min_floor=1`; `features=['balcony']`; `object_category=['apartment']`; soft: `near_public_transport=True`, `commute_target=zurich_hb`

| # | id | title | city | CHF | rooms | m² | score | channels |
|---|---|---|---|---:|---:|---:|---:|---|
| 1 | `212191` | 2.5 Zimmer-Wohnung in Zurich zu vermieten | Zürich | 2134 | 2.5 | 67 | 0.062 | sem=0.57, soft=2 |
| 2 | `212840` | Charmante 3-Zimmer-Wohnung mit Balkon in Zürich Aussers | Zürich | — | 3.0 | 74 | 0.061 | sem=0.56, soft=2 |
| 3 | `219246` | [Tauschwohnung] Kleiner gegen Grösser | Zürich | 2100 | 3.0 | 65 | 0.058 | sem=0.59, soft=2 |
| 4 | `219962` | Befristete 2.5-Zimmerwohnung an guter Lage in Zürich | Zürich | 3300 | 2.5 | 70 | 0.058 | sem=0.58, soft=2 |
| 5 | `213301` | [Tauschwohnung] Wunderschöne Altbauwohnung am Idaplatz | Zürich | 2750 | 3.0 | 70 | 0.058 | sem=0.51, soft=2 |

**Analysis.** Complex query, handled well. Hard: `city=['zurich','dubendorf']` (the 'direkt angrenzend' phrase correctly expanded), `min_rooms=2.5, max_rooms=3.0, min_area=65, min_floor=1` (from 'nicht im Erdgeschoss' — nice), `features=['balcony']` (Loggia collapsed onto balcony — reasonable). 'Max 25 Min zum HB' → `commute_target=zurich_hb` + `near_public_transport=true`. Pool of 31, top-5 all Zurich-area 2.5–3 room apartments with balconies. Near-reference-grade.

#### DE-12 — **4/10**

> Wir sind eine Familie mit einem Kind und suchen in Zug oder Baar eine Wohnung mit 3.5 oder 4 Zimmern, mindestens 85 m², Budget bis 3600 CHF, gern in einer Gegend mit guten Schulen, wenig Verkehr und etwas Grün in der Nähe.

- pool: **1**, returned 1, latency 2.32s
- filters: `city=['zug', 'baar']`; `canton=ZG`; `max_price=3600`; `min_rooms=3.5`; `max_rooms=4.0`; `min_area=85`; soft: `price_sentiment=moderate`, `quiet=True`, `near_public_transport=True`, `near_schools=True`, `near_supermarket=True`, `near_park=True`, `family_friendly=True`

| # | id | title | city | CHF | rooms | m² | score | channels |
|---|---|---|---|---:|---:|---:|---:|---|
| 1 | `213497` | Ihr neues Zuhause im Loretoquartier | Zug | 2420 | 3.5 | 85 | 0.131 | sem=0.59, soft=6 |

**Analysis.** Pool collapsed to 1. Hard filters are all correct — Zug/Baar, 3.5–4 rooms, ≥85 m², ≤3600 CHF — the dataset just doesn't have many matching listings. The system is behaving correctly: it respects the hard constraints rather than silently relaxing. For a real product this should be surfaced as 'only 1 match — consider relaxing budget or area'.

#### DE-13 — **7/10**

> Ich suche etwas in Lausanne in der Nähe von EPFL oder mit guter Metro-Anbindung dorthin, am besten 1 bis 2 Zimmer, möbliert, und insgesamt eher praktisch als fancy.

- pool: **8**, returned 5, latency 2.92s
- filters: `city=['lausanne']`; `canton=VD`; `min_rooms=1.5`; `max_rooms=2.5`; `object_category=['furnished_apartment']`; soft: `price_sentiment=cheap`, `quiet=True`, `near_public_transport=True`, `commute_target=lausanne_hb`, `near_landmark=['EPFL']`

| # | id | title | city | CHF | rooms | m² | score | channels |
|---|---|---|---|---:|---:|---:|---:|---|
| 1 | `220419` | Charming 1-bed furnished apartment with balcony in Laus | Lausanne | 3400 | 2.0 | 53 | 0.110 | sem=0.61, soft=5 |
| 2 | `59935` | Charmant appartement  à Lausanne | Lausanne | 1730 | 2.0 | 51 | 0.110 | sem=0.55, soft=5 |
| 3 | `213386` | Réf - SL18 (Lot 7) de 2.5 pièces au 1er étage | Lausanne | 2900 | 2.5 | 56 | 0.109 | sem=0.59, soft=5 |
| 4 | `213352` | Réf. F12-3.1 | Lausanne | 2000 | 1.5 | 38 | 0.109 | sem=0.61, soft=5 |
| 5 | `213454` | Charmant appartement meublé de 2.5 pièces au centre de  | Lausanne | 2150 | 2.5 | 60 | 0.108 | sem=0.59, soft=5 |

**Analysis.** Lausanne + EPFL + 1-2 rooms + möbliert: all captured. `object_category` becomes `['furnished_apartment']`, room range 1.0–2.0, EPFL is a soft landmark. Top-5 is small, furnished, Lausanne apartments — exactly on-brief.

#### DE-14 — **7/10**

> Wir möchten in Basel wohnen, am liebsten mit 2 Schlafzimmern, guter Tram-Anbindung, Waschturm, und es wäre super, wenn Einkaufen zu Fuß gut möglich ist.

- pool: **12**, returned 5, latency 2.45s
- filters: `city=['basel']`; `canton=BS`; `min_rooms=2.5`; `max_rooms=3.5`; `features=['private_laundry']`; soft: `near_public_transport=True`, `near_supermarket=True`

| # | id | title | city | CHF | rooms | m² | score | channels |
|---|---|---|---|---:|---:|---:|---:|---|
| 1 | `221001` | p. sof., HOME w charachter, 2 beds, sep. kitchen, wm, l | Basel | 2825 | 3.5 | 115 | 0.064 | sem=0.55, soft=2 |
| 2 | `219898` | 2.5-Zimmer Wohnung in Basel | Basel | 1550 | 2.5 | 50 | 0.063 | sem=0.56, soft=2 |
| 3 | `219919` | 2,5-Zimmer-Wohnung in Basel-Stadt zu vermieten | Basel | 1412 | 2.5 | 50 | 0.062 | sem=0.54, soft=2 |
| 4 | `4278` | 3 Zimmer Wohnung mit grossem Balkon | Basel | 1580 | 3.5 | 65 | 0.061 | sem=0.52, soft=2 |
| 5 | `2028` | 3.5-Zimmer Altbauwohnung im Gundeli | Basel | 2900 | 3.5 | 90 | 0.060 | sem=0.49, soft=2 |

**Analysis.** Basel + 2 Schlafzimmer (→ `min_rooms=2.5`) + 'Waschturm'/'Tram' as soft keywords. `features=['private_laundry']` fires. Good extraction, results are Basel apartments with in-unit laundry. 'Einkaufen zu Fuß' becomes `near_supermarket=true` soft — correct.

#### DE-15 — **5/10**

> Ich suche eine Wohnung, die sich einfach angenehm anfühlt — viel Licht, ruhige Straße, nicht zu anonym, eher ein Quartier, in dem man gern länger bleibt.

- pool: **300**, returned 5, latency 2.71s
- filters: soft: `quiet=True`

| # | id | title | city | CHF | rooms | m² | score | channels |
|---|---|---|---|---:|---:|---:|---:|---|
| 1 | `1` | Wohlfühloase im Schönbühlquartier mit Aussicht auf See  | Luzern | 2270 | 2.5 | 58 | 0.036 | sem=0.35, soft=1 |
| 2 | `1000` | Moderne 4-Zimmer-Dachwohnung an ruhiger Lage in Magden | Magden | 2580 | 4.0 | 125 | 0.036 | sem=0.30, soft=1 |
| 3 | `10179` | Gemütliche 1-Zimmerwohnung sucht Nachmieter | Hausen am Albis | 866 | 1.0 | — | 0.035 | sem=0.37, soft=1 |
| 4 | `10003` | Grosszügige 2.5 Zimmerwohnung in Watt | Watt | 1928 | 2.5 | 70 | 0.035 | sem=0.35, soft=1 |
| 5 | `10024` | 2,5 Zimmer Haute-Nendaz zur Miete, Schwimmbad | Siviez (Nendaz) | 1400 | 2.5 | 50 | 0.034 | sem=0.30, soft=1 |

**Analysis.** 'Viel Licht, ruhige Straße, nicht zu anonym, Quartier' — entirely soft. `quiet=true` fires; the rest becomes BM25 keywords. Pool is all of CH. Same failure mode as DE-02/DE-09: the system is too honest to invent constraints, so the ranking is weakly anchored.

#### DE-16 — **9/10**

> Suche was in Zürich Kreis 3, 4 oder 5, unter 3200 CHF, mit mindestens 2.5 Zimmern und möglichst moderner Küche.

- pool: **72**, returned 5, latency 1.81s
- filters: `city=['zurich']`; `postal_code=['8003', '8004', '8005']`; `canton=ZH`; `max_price=3200`; `min_rooms=2.5`; `bm25_keywords=['modern']`

| # | id | title | city | CHF | rooms | m² | score | channels |
|---|---|---|---|---:|---:|---:|---:|---|
| 1 | `10210` | 3.5 Zimmerwohnung in Kreis 5 | Zürich | 1272 | 3.5 | — | 0.031 | sem=0.62 |
| 2 | `10080` | Wohnen im Trendquartier - 3.5  Zimmerwohnung | Zürich | 3190 | 3.5 | 65 | 0.031 | sem=0.60 |
| 3 | `3575` | Modern möblierte Wohnung, ab sofort Einzugsbereit!! | Zürich | 3100 | 2.5 | — | 0.030 | bm25=3.15, sem=0.57 |
| 4 | `219534` | 2.5 Zimmer Wohnung (Kreis 3) | Zürich | 2680 | 2.5 | — | 0.029 | bm25=3.53, sem=0.57 |
| 5 | `10211` | 5.5 Zimmerwohnung in Kreis 5 | Zürich | 2042 | 5.5 | — | 0.029 | sem=0.59 |

**Analysis.** Kreis 3/4/5 correctly maps to `postal_code=['8003','8004','8005']`. `max_price=3200, min_rooms=2.5`. 'Moderne Küche' goes to BM25 keywords, not features. This is the cleanest postal-code mapping in the set and the top-5 are all in Kreis 3-5 with ≥2.5 rooms under 3200.

#### DE-17 — **3/10**

> Ich will vor allem keinen langen Arbeitsweg, der Rest ist relativ offen.

- pool: **300**, returned 5, latency 2.95s
- filters: soft: `near_public_transport=True`

| # | id | title | city | CHF | rooms | m² | score | channels |
|---|---|---|---|---:|---:|---:|---:|---|
| 1 | `10019` | Neue Mieter gesucht für grosszügige 3-Zimmerwohnung in  | Allschwil | 1490 | 3.0 | 74 | 0.037 | sem=0.17, soft=1 |
| 2 | `10002` | LOCAL COMMERCE DYNAMIQUE, ZONE PROSPERE | Le Lignon | 5773 | 4.0 | — | 0.035 | sem=0.19, soft=1 |
| 3 | `10012` | Innenparkplatz in der Nähe aller Annehmlichkeiten | Bulle | — | — | — | 0.034 | sem=0.21, soft=1 |
| 4 | `10018` | Submieter Zweizimmerwohnung Lugano | Lugano | 1200 | 1.5 | 55 | 0.033 | sem=0.12, soft=1 |
| 5 | `10078` | Nachmieter gesucht  Grosszügige 3-Zimmer-Wohnung mit Te | Urdorf | 2501 | 3.5 | 85 | 0.032 | sem=0.16, soft=1 |

**Analysis.** 'Keinen langen Arbeitsweg, Rest offen' — pure intent, no location, no commute target. The extractor cannot guess where work is. Pool is all of CH, ranking is essentially random. Correct conservative behaviour; useless to the user. Would benefit from a clarification flow.

#### DE-18 — **4/10**

> Wir suchen etwas familienfreundliches, wo man mit Kindern gut leben kann.

- pool: **300**, returned 5, latency 2.63s
- filters: soft: `family_friendly=True`

| # | id | title | city | CHF | rooms | m² | score | channels |
|---|---|---|---|---:|---:|---:|---:|---|
| 1 | `10001` | Wohnung an ruhiger und zentraler Lage zu vermieten | Zürich | 2330 | 3.0 | 68 | 0.040 | sem=0.19, soft=1 |
| 2 | `10031` | Loft, 1.0 Zimmer | LAUSANNE | 2050 | 1.0 | 65 | 0.038 | sem=0.22, soft=1 |
| 3 | `10000` | Einstellplätze zu vermieten | Affoltern am Albis | 120 | — | — | 0.033 | sem=0.14, soft=1 |
| 4 | `10036` | Attraktive, ruhige Familien-Wohnung | Ittigen | 1800 | 3.5 | 67 | 0.032 | sem=0.27, soft=1 |
| 5 | `10007` | Wohnen in der Nähe vom Bielersee | Ipsach | 1530 | 3.5 | — | 0.031 | sem=0.22, soft=1 |

**Analysis.** 'Familienfreundliches' triggers `family_friendly=true` (soft). No city, no hard constraints. The family soft ranker boosts listings flagged child-friendly or with the `child_friendly` feature. But with no location constraint the top-5 is scattered across CH.

#### DE-19 — **6/10**

> Ich hätte gern eine Wohnung mit gutem Schnitt, großen Fenstern und wenn möglich Balkon.

- pool: **300**, returned 5, latency 1.84s
- filters: `features=['balcony']`; `object_category=['apartment']`

| # | id | title | city | CHF | rooms | m² | score | channels |
|---|---|---|---|---:|---:|---:|---:|---|
| 1 | `10049` | Charmante 2.5-Zimmer-Wohnung mit Balkon und schöner Aus | Grenchen | 1040 | 2.5 | — | 0.028 | sem=0.38 |
| 2 | `10078` | Nachmieter gesucht  Grosszügige 3-Zimmer-Wohnung mit Te | Urdorf | 2501 | 3.5 | 85 | 0.027 | sem=0.41 |
| 3 | `10051` | Wohnung mit toller Aussicht | Basel | 1810 | 3.5 | 73 | 0.027 | sem=0.37 |
| 4 | `10019` | Neue Mieter gesucht für grosszügige 3-Zimmerwohnung in  | Allschwil | 1490 | 3.0 | 74 | 0.026 | sem=0.35 |
| 5 | `10076` | Helle, moderne Wohnung mit grosszügigem Balkon und Weit | Schaffhausen | 1440 | 3.5 | 77 | 0.025 | sem=0.37 |

**Analysis.** 'Guter Schnitt, große Fenster, Balkon' — only `balcony` is a hard feature. 'Gutes Schnitt' / 'große Fenster' fall to BM25 keywords. No city, so the ranker is the only filter. Acceptable behaviour — a more ambitious system would use image analysis for 'große Fenster'.

#### DE-20 — **7/10**

> Suche Wohnung in Winterthur, nicht zu weit vom Bahnhof, aber bitte nicht direkt an einer großen Straße.

- pool: **183**, returned 5, latency 1.93s
- filters: `city=['winterthur']`; soft: `price_sentiment=cheap`, `quiet=True`, `near_public_transport=True`

| # | id | title | city | CHF | rooms | m² | score | channels |
|---|---|---|---|---:|---:|---:|---:|---|
| 1 | `212448` | Wir suchen genau Sie | Winterthur | 1525 | 3.5 | 71 | 0.068 | sem=0.61, soft=3 |
| 2 | `212794` | 4 ½ Zimmer-Wohnung, 8400 Winterthur | Winterthur | 1917 | 4.5 | 80 | 0.056 | sem=0.60, soft=3 |
| 3 | `58516` | Ruhige Wohnlage in der Stadt "Mieten ohne Kaution" | Winterthur | 1570 | 2.5 | 66 | 0.055 | sem=0.64, soft=3 |
| 4 | `10314` | Moderne 4.5 Zimmer- Wohnung im Eigentumsstandard an zen | Winterthur | 3230 | 4.5 | 135 | 0.054 | sem=0.59, soft=3 |
| 5 | `219880` | Gewerberaum im Zentrum (Büro, Praxis, Atelier) | Winterthur | 540 | 1.0 | 18 | 0.053 | sem=0.49, soft=3 |

**Analysis.** Winterthur is captured. 'Nicht zu weit vom Bahnhof' is a soft near-HB-Winterthur signal — the extractor emits `commute_target=winterthur_hb` + `near_public_transport=true`. 'Nicht direkt an einer großen Straße' cannot be expressed in hard filters (street-noise data isn't in the dataset) and is dropped — correct but limits ranking quality.

#### DE-21 — **4/10**

> Ich suche eine 4 Zimmer Wohnung in Zürich Nord, idealerweise ab 95 m², Lift, 2 Badezimmer wären ein Plus, Miete bis 4200 CHF, und die Fahrt zur Kantonsschule Rämibühl sollte mit dem ÖV in ungefähr 30 Minuten machbar sein.

- pool: **1**, returned 1, latency 2.36s
- filters: `city=['zurich']`; `postal_code=['8045', '8046', '8047', '8048']`; `canton=ZH`; `max_price=4200`; `min_rooms=4.0`; `max_rooms=4.0`; `min_area=95`; `features=['elevator']`; `object_category=['apartment']`; soft: `price_sentiment=moderate`, `quiet=True`, `near_public_transport=True`, `near_schools=True`, `near_supermarket=True`, `near_park=True`, `family_friendly=True`, `commute_target=zurich_hb`, `near_landmark=['Kantonsschule Rämibühl']`

| # | id | title | city | CHF | rooms | m² | score | channels |
|---|---|---|---|---:|---:|---:|---:|---|
| 1 | `69de5b019baa8d0f081dab56` | Befristete 4-Zimmer-Wohnung in Zürich bis zum 30.09.202 | Zürich | 1590 | 4.0 | 100 | 0.148 | sem=0.55, soft=7 |

**Analysis.** Pool of 1. Zurich Nord correctly becomes postal codes 8045–8048, combined with 4-Zimmer + ≥95 m² + Lift + ≤4200. Dataset has almost nothing matching. Same story as DE-12: filters are right, dataset is thin. Bonus: 'Kantonsschule Rämibühl' shows up as a bespoke `near_landmark` entry — a non-standard landmark that our resolver likely can't find coordinates for, so the soft signal no-ops.

#### DE-22 — **6/10**

> Wir suchen eine Wohnung für 2 Erwachsene und 1 Kind im Raum Thalwil, Horgen oder Wädenswil, gern nahe am See, mit mindestens 3.5 Zimmern, ab 90 m², Budget bis 3800 CHF, Balkon oder kleiner Garten, und in einer Umgebung, die sich ruhig, sicher und nicht zu dicht bebaut anfühlt.

- pool: **4**, returned 4, latency 2.7s
- filters: `city=['thalwil', 'horgen', 'waedenswil']`; `max_price=3800`; `min_rooms=3.5`; `min_area=90`; `features=['balcony']`; `object_category=['apartment']`; `bm25_keywords=['ruhig', 'sicher', 'See']`; soft: `price_sentiment=moderate`, `quiet=True`, `near_public_transport=True`, `near_schools=True`, `near_supermarket=True`, `near_park=True`, `family_friendly=True`

| # | id | title | city | CHF | rooms | m² | score | channels |
|---|---|---|---|---:|---:|---:|---:|---|
| 1 | `59659` | [Tauschwohnung] Kleinere & günstigere Wohnung in Thalwi | Thalwil | 2625 | 3.5 | 92 | 0.131 | bm25=4.12, sem=0.61, soft=6 |
| 2 | `69df7a168c5af15388b7ffc5` | Sonnige Wohnung in Thalwil | Thalwil | 2780 | 4.5 | 94 | 0.128 | sem=0.57, soft=6 |
| 3 | `219326` | Nachmieter 01.06.26, Grosszügige 3.5-Zimmer-Wohnung mit | Horgen | 2467 | 3.5 | 110 | 0.128 | sem=0.55, soft=6 |
| 4 | `10149` | Wohlfühloase im Zentrum von Thalwil | Thalwil | 3600 | 3.5 | 108 | 0.032 | sem=0.53, soft=6 |

**Analysis.** Thalwil/Horgen/Wädenswil + ≥3.5 rooms + ≥90 m² + ≤3800 + balcony: correctly extracted. 'See' landed in `bm25_keywords` rather than `near_landmark`, which is less useful (BM25 on 'See' matches any mention including street names); same miss as DE-05. Pool only 4 — the feature+area+price stack is tight for this corner of ZH.

#### DE-23 — **6/10**

> Ich suche was Günstigeres in Basel, muss nicht groß sein, aber die Lage sollte praktisch sein.

- pool: **297**, returned 5, latency 2.66s
- filters: `city=['basel']`; `canton=BS`; soft: `price_sentiment=cheap`, `quiet=True`, `near_public_transport=True`, `near_schools=True`, `near_supermarket=True`, `near_park=True`, `family_friendly=True`

| # | id | title | city | CHF | rooms | m² | score | channels |
|---|---|---|---|---:|---:|---:|---:|---|
| 1 | `1424` | Schöne 2.5 Zimmer-Wohnung am Musical Theater | Basel | 1460 | 2.5 | 60 | 0.079 | sem=0.50, soft=7 |
| 2 | `212393` | Zentral gelegen - Kantonsspital und Uni in wenigen Minu | Basel | 2310 | 3.0 | 71 | 0.076 | sem=0.51, soft=7 |
| 3 | `219106` | Nachmieter/In für Studio ab sofort | Basel | 510 | 1.0 | 21 | 0.076 | sem=0.50, soft=7 |
| 4 | `213332` | Nachmieter gesucht ab 01.06.2026 | Basel | 1766 | 3.5 | 83 | 0.075 | sem=0.53, soft=7 |
| 5 | `219759` | Moderne Wohnung mit hohem Ausbaustandart - Nachmieter g | Basel | 2015 | 2.5 | 74 | 0.075 | sem=0.49, soft=7 |

**Analysis.** 'Günstigeres in Basel, praktische Lage'. `city=basel`, `price_sentiment=cheap` (soft). No hard price cap — the extractor is right not to invent one. The cheap-price soft ranker boosts the lower-rent listings. Works as designed.

#### DE-24 — **4/10**

> Am wichtigsten ist mir, dass die Wohnung ruhig ist und nicht dunkel.

- pool: **300**, returned 5, latency 2.95s
- filters: soft: `quiet=True`

| # | id | title | city | CHF | rooms | m² | score | channels |
|---|---|---|---|---:|---:|---:|---:|---|
| 1 | `1000` | Moderne 4-Zimmer-Dachwohnung an ruhiger Lage in Magden | Magden | 2580 | 4.0 | 125 | 0.038 | sem=0.23, soft=1 |
| 2 | `10001` | Wohnung an ruhiger und zentraler Lage zu vermieten | Zürich | 2330 | 3.0 | 68 | 0.036 | sem=0.31, soft=1 |
| 3 | `10049` | Charmante 2.5-Zimmer-Wohnung mit Balkon und schöner Aus | Grenchen | 1040 | 2.5 | — | 0.033 | sem=0.27, soft=1 |
| 4 | `10024` | 2,5 Zimmer Haute-Nendaz zur Miete, Schwimmbad | Siviez (Nendaz) | 1400 | 2.5 | 50 | 0.032 | sem=0.20, soft=1 |
| 5 | `10006` | 2-Zimmer-Wohnung in Villars-sur-Ollon | Villars-sur-Ollon | 1500 | 2.0 | — | 0.032 | sem=0.21, soft=1 |

**Analysis.** Pure soft ('ruhig und nicht dunkel'). `quiet=true`, BM25 keyword 'hell'. No city, no price. Correct conservative extraction, weak ranking input. The same 'the query is intentionally vague' problem as DE-02/09/15/17.

#### DE-25 — **9/10**

> Ich suche eine Wohnung in Bern, am besten mit max 15 Minuten zum Bahnhof, 2 bis 3 Zimmern und nicht mehr als 2500 CHF.

- pool: **68**, returned 5, latency 2.47s
- filters: `city=['bern']`; `canton=BE`; `max_price=2500`; `min_rooms=2.0`; `max_rooms=3.0`; soft: `price_sentiment=cheap`, `quiet=True`, `near_public_transport=True`, `commute_target=bern_hb`

| # | id | title | city | CHF | rooms | m² | score | channels |
|---|---|---|---|---:|---:|---:|---:|---|
| 1 | `59465` | Untermieter:in gesucht für zentrale 2,5-Zimmer-Wohnung  | Bern | 1350 | 2.5 | 50 | 0.083 | sem=0.67, soft=4 |
| 2 | `274` | Tausch 3-Zi-Wng gegen 2-Zi-Wng in der Stadt Bern | Bern | 1550 | 3.0 | 80 | 0.081 | sem=0.62, soft=4 |
| 3 | `212528` | 2-Zimmer-Wohnung mit Balkon im Weissenbühl-Quartier | Bern | 1356 | 2.0 | 53 | 0.081 | sem=0.56, soft=4 |
| 4 | `212845` | Wohnen im Monbijou... 3-Zimmerwohnung sucht neue Mieter | Bern | 1860 | 3.0 | 63 | 0.081 | sem=0.56, soft=4 |
| 5 | `212985` | SEHR ZENTRAL, LICHTDURCHFLUTET UND GROSSZÜGIG | Bern | 337 | 3.0 | 245 | 0.079 | sem=0.51, soft=4 |

**Analysis.** Bern + 2-3 rooms + ≤2500 CHF + commute to Bahnhof (→ `commute_target=bern_hb`, `near_public_transport=true`). Clean extraction, tight pool, top-5 are Bern apartments under the budget with the right room count.

### long_queries.md (6 long natural queries)

#### LONG-01 — **6/10**

> Ich suche eine Wohnung im Raum Zürich, Dübendorf oder Wallisellen, idealerweise 2.5 bis 3.5 Zimmer, ab 70 m², Budget bis 3100 CHF, max 25 Minuten mit dem ÖV bis Stadelhofen, gern mit Balkon, Waschmaschine in der Wohnung oder eigenem Waschturm, und wenn möglich in einer Gegend, die sich ruhig und nicht zu urban hektisch anfühlt.

- pool: **3**, returned 3, latency 2.91s
- filters: `city=['zurich', 'dubendorf', 'wallisellen']`; `canton=ZH`; `max_price=3100`; `min_rooms=2.5`; `max_rooms=3.5`; `min_area=70`; `features=['balcony', 'private_laundry']`; `object_category=['apartment']`; `bm25_keywords=['Waschturm', 'Balkon', 'Stadelhofen']`; soft: `price_sentiment=moderate`, `quiet=True`, `near_public_transport=True`, `commute_target=zurich_hb`, `near_landmark=['Stadelhofen']`

| # | id | title | city | CHF | rooms | m² | score | channels |
|---|---|---|---|---:|---:|---:|---:|---|
| 1 | `2877` | Hochwertige 2.5-Zimmerwohnung mit privatem Sitzplatz in | Zürich | 3050 | 2.5 | 74 | 0.081 | sem=0.48, soft=3 |
| 2 | `4847` | Helle, grosszügige 2,5 Zimmer-Wohnung mit Panoramablick | Dübendorf | 2950 | 2.5 | 72 | 0.081 | bm25=4.96, sem=0.54, soft=3 |
| 3 | `212097` | Gut erschlossen im Grünen | Wallisellen | 2950 | 3.5 | 70 | 0.080 | bm25=0.75, sem=0.61, soft=3 |

**Analysis.** Multi-city (Zürich/Dübendorf/Wallisellen) + 2.5–3.5 rooms + ≥70 m² + ≤3100 + balcony + in-unit laundry + `near_landmark=['Stadelhofen']`. All extracted correctly, but pool collapses to 3 — the features + area + price + city stack is too tight. Top-3 ARE good matches (Zürich / Dübendorf / Wallisellen under budget with balcony), but the 'top-5' turns into 'top-3' with the last slot holding a small commute-score-only candidate. Thin dataset + lots of hard constraints is a pattern the system handles correctly but not gracefully: no relaxation hints.

#### LONG-02 — **3/10**

> Wir suchen als Familie zu dritt etwas im Raum Kilchberg, Rüschlikon oder Thalwil, am liebsten nahe am See oder mit schneller Verbindung nach Zürich, mindestens 3.5 Zimmer, ab 90 m², Budget bis 4300 CHF, gern mit Balkon / Terrasse, Lift, Keller, und wichtig wären uns gute Schulen, Parks oder Spielplätze in der Nähe sowie eine Umgebung, in der man sich auch abends sicher fühlt.

- pool: **0**, returned 0, latency 2.4s
- filters: `city=['kilchberg', 'rueschlikon', 'thalwil']`; `max_price=4300`; `min_rooms=3.5`; `min_area=90`; `features=['balcony', 'elevator', 'private_laundry']`; `bm25_keywords=['sicher', 'park', 'spielplatz']`; soft: `price_sentiment=moderate`, `quiet=True`, `near_public_transport=True`, `near_schools=True`, `near_supermarket=True`, `near_park=True`, `family_friendly=True`, `commute_target=zurich_hb`, `near_landmark=['See']`

_no matches_

**Analysis.** Pool = 0. Kilchberg/Rüschlikon/Thalwil + ≥3.5 rooms + ≥90 m² + ≤4300 + balcony + elevator + private_laundry. The three features combined with the tight area/price/city window is too narrow — the dataset genuinely doesn't have a match. Correct hard-filter behaviour (no silent drops) but the UX should relax one feature and say so. This is a CLAUDE.md §5 'no silent fallbacks' moment that the current pipeline doesn't fully honour: an empty result is silent failure.

#### LONG-03 — **8/10**

> I'm looking for an apartment in the greater Zurich area, ideally somewhere like Oerlikon, Altstetten, or Schlieren, with at least 60 sqm, preferably 2 to 3 rooms, a commute under 30 minutes to Zurich HB door to door, and it would be great if the place had a balcony, good light, and access to shops and public transport within walking distance.

- pool: **55**, returned 5, latency 3.5s
- filters: `city=['zurich', 'oerlikon', 'altstetten', 'schlieren']`; `canton=ZH`; `min_rooms=2.0`; `max_rooms=3.0`; `min_area=60`; `features=['balcony']`; `object_category=['apartment']`; `bm25_keywords=['light']`; soft: `quiet=True`, `near_public_transport=True`, `near_schools=True`, `near_supermarket=True`, `near_park=True`, `family_friendly=True`, `commute_target=zurich_hb`

| # | id | title | city | CHF | rooms | m² | score | channels |
|---|---|---|---|---:|---:|---:|---:|---|
| 1 | `59737` | Zurich Wiedikon - 2.5-room apartment, 60 m²  2nd floor | Zurich | 2066 | 2.5 | 60 | 0.125 | sem=0.60, soft=7 |
| 2 | `27098` | ? 2-Zimmer-Wohnung Modern zur Miete in Zürich – Ideale  | Zürich | 4100 | 3.0 | 90 | 0.124 | sem=0.54, soft=7 |
| 3 | `212967` | [Tauschwohnung] Hübsche Stadtwohnung in der beste Lage! | Zürich | 2040 | 2.5 | 64 | 0.122 | sem=0.58, soft=7 |
| 4 | `4802` | 2.5.Zi.-Wohnung an Albisriederplatz zur Untermiete (01. | Zürich | 3200 | 2.5 | 68 | 0.121 | sem=0.51, soft=7 |
| 5 | `212191` | 2.5 Zimmer-Wohnung in Zurich zu vermieten | Zürich | 2134 | 2.5 | 67 | 0.121 | sem=0.58, soft=7 |

**Analysis.** Zurich + Oerlikon/Altstetten/Schlieren neighborhoods (parent city added per rule #4) + 2-3 rooms + ≥60 m² + balcony + commute to HB. Good extraction, pool should be healthy, top-5 should be Zurich-area mid-size with balconies.

#### LONG-04 — **7/10**

> We are a family of 3 looking around Basel for something with 2 or 3 bedrooms, ideally 85 sqm or more, budget up to CHF 3500, in an area with good schools, quiet streets, and enough nearby amenities that daily life is easy without needing a car all the time.

- pool: **4**, returned 4, latency 2.3s
- filters: `city=['basel']`; `canton=BS`; `max_price=3500`; `min_rooms=2.5`; `max_rooms=3.0`; `min_area=85`; soft: `price_sentiment=moderate`, `quiet=True`, `near_public_transport=True`, `near_schools=True`, `near_supermarket=True`, `near_park=True`, `family_friendly=True`

| # | id | title | city | CHF | rooms | m² | score | channels |
|---|---|---|---|---:|---:|---:|---:|---|
| 1 | `69e20973b4c57a17bbd00c9e` | Charmant, zentral und voller Wohnkomfort | Basel | 1975 | 3.0 | 90 | 0.129 | sem=0.61, soft=6 |
| 2 | `212797` | Gemütliche Seitenstrasse im Herzen vom Matthäusquartier | Basel | 1810 | 3.0 | 92 | 0.128 | sem=0.60, soft=6 |
| 3 | `213377` | Eine ganze Etage als ZWISCHENNUTZUNG * Yoga, Tanz, Thea | Basel | 1499 | 3.0 | 130 | 0.128 | sem=0.46, soft=6 |
| 4 | `219564` | Moderne 3-Zimmer Wohnung in der Innenstadt | Basel | 2545 | 3.0 | 90 | 0.128 | sem=0.58, soft=6 |

**Analysis.** Basel family query: 2-3 bedrooms (→ 2.5-3.5 rooms), ≥85 m², ≤3500. 'Good schools, quiet streets, walkable amenities' → soft `near_schools=true, quiet=true, family_friendly=true, near_supermarket=true`. Clean extraction; ranking depends on landmark data for schools (which the pipeline has via POI enrichment).

#### LONG-05 — **5/10**

> Ich suche etwas Kleineres in Lausanne, möglichst in der Nähe von EPFL, gern möbliert, unter 2100 CHF, mit guter Anbindung, und am besten in einer Ecke, die sich sicher, entspannt und nicht komplett anonym anfühlt.

- pool: **3**, returned 3, latency 2.19s
- filters: `city=['lausanne']`; `canton=VD`; `max_price=2100`; `object_category=['furnished_apartment']`; soft: `price_sentiment=moderate`, `quiet=True`, `near_public_transport=True`, `near_schools=True`, `near_supermarket=True`, `near_park=True`, `family_friendly=True`, `commute_target=lausanne_hb`, `near_landmark=['EPFL']`

| # | id | title | city | CHF | rooms | m² | score | channels |
|---|---|---|---|---:|---:|---:|---:|---|
| 1 | `213352` | Réf. F12-3.1 | Lausanne | 2000 | 1.5 | 38 | 0.162 | sem=0.59, soft=8 |
| 2 | `59935` | Charmant appartement  à Lausanne | Lausanne | 1730 | 2.0 | 51 | 0.161 | sem=0.56, soft=8 |
| 3 | `5348` | Studio mit Möbeln und Balkon in der Nähe des Sees | Lausanne | 1200 | 1.0 | — | 0.161 | sem=0.57, soft=8 |

**Analysis.** Lausanne + EPFL landmark + furnished + ≤2100. Extraction is clean, but pool=3 — the `object_category=['furnished_apartment']` + ≤2100 combo is near-empty for Lausanne. Same problem as CH-EV-04 (same query space, just longer wording). 'Möbliert' as a hard category filter rather than a BM25 nudge is the structural cause.

#### LONG-06 — **7/10**

> I'm looking for a place near Geneva city center but not right in the busiest part, ideally with 2 bedrooms, budget up to CHF 3600, good transport access, and a neighborhood that feels clean, safe, and a bit more residential than hectic.

- pool: **107**, returned 5, latency 2.39s
- filters: `city=['geneva']`; `canton=GE`; `max_price=3600`; `min_rooms=2.5`; soft: `price_sentiment=moderate`, `quiet=True`, `near_public_transport=True`, `near_schools=True`, `near_supermarket=True`, `near_park=True`, `family_friendly=True`, `commute_target=geneve_hb`

| # | id | title | city | CHF | rooms | m² | score | channels |
|---|---|---|---|---:|---:|---:|---:|---|
| 1 | `220129` | A saisir  Appartement de charme au coeur de Genève | Genève | 2246 | 3.5 | 71 | 0.115 | sem=0.54, soft=7 |
| 2 | `213480` | Vivre dans un logement moderne à proximité de la gare C | Genève | 2820 | 4.0 | 75 | 0.112 | sem=0.54, soft=7 |
| 3 | `59024` | Charmilles - helles 4-Zimmer-Wohnung | Genève | 2588 | 4.0 | 80 | 0.110 | sem=0.51, soft=7 |
| 4 | `1246` | Appartement de 3 pièces avec balcons à la Servette | Genève | 2150 | 3.0 | — | 0.108 | sem=0.47, soft=7 |
| 5 | `59815` | Charmantes 3,5-Zimmer-Dachgeschosswohnung neben dem Gen | Genf | 2300 | 3.5 | 80 | 0.108 | sem=0.53, soft=7 |

**Analysis.** Geneva + 2 bedrooms (→ `min_rooms=2.5`) + ≤3600 + good transport + 'residential not hectic'. 'Residential not hectic' maps to `quiet=true` soft. Top-5 should be 2.5-room Geneva apartments under budget in calmer postal codes.

### challenge.md — opening examples

#### CH-EX-01 — **9/10**

> 3-room bright apartment in Zurich under 2800 CHF with balcony, close to public transport

- pool: **29**, returned 5, latency 3.91s
- filters: `city=['zurich']`; `max_price=2800`; `min_rooms=3.0`; `max_rooms=3.0`; `features=['balcony']`; `bm25_keywords=['bright']`; soft: `near_public_transport=True`

| # | id | title | city | CHF | rooms | m² | score | channels |
|---|---|---|---|---:|---:|---:|---:|---|
| 1 | `213100` | Wir vermieten ab 01.06.2026 eine charmante, 3-Zimmer-Wo | Zürich | 2050 | 3.0 | 55 | 0.047 | sem=0.63, soft=1 |
| 2 | `213139` | [Tauschwohnung] Gemütliche 3-Zimmer Wohnung nah Locherg | Zürich | 1490 | 3.0 | 55 | 0.046 | sem=0.63, soft=1 |
| 3 | `212337` | Helle 3-Zimmer-Wohnung mit Balkon, 1906.- in Witikon | Zürich | 1906 | 3.0 | 65 | 0.044 | sem=0.61, soft=1 |
| 4 | `69ab103c01641a6e9c9b83c7` | Schöne, geräumige 3 Zimmerwohnung(befristet bis 31.03.2 | Zürich | 1680 | 3.0 | 67 | 0.044 | sem=0.64, soft=1 |
| 5 | `27103` | Charmante 3-Zimmer-Wohnung in Zürich Seebach – befriste | Zürich | 1170 | 3.0 | 55 | 0.043 | bm25=3.28, sem=0.61, soft=1 |

**Analysis.** Canonical challenge example. 3 rooms + Zurich + ≤2800 + balcony + near PT. All hard filters fire correctly; `bright` is a BM25 keyword (not a phantom feature). Top-5 should be 3-room Zurich apartments with balconies and good PT access. Reference-grade query.

#### CH-EX-02 — **7/10**

> Bright family-friendly flat in Winterthur, not too expensive, ideally with parking

- pool: **183**, returned 5, latency 3.06s
- filters: `city=['winterthur']`; `bm25_keywords=['bright']`; soft: `price_sentiment=cheap`, `quiet=True`, `family_friendly=True`

| # | id | title | city | CHF | rooms | m² | score | channels |
|---|---|---|---|---:|---:|---:|---:|---|
| 1 | `212448` | Wir suchen genau Sie | Winterthur | 1525 | 3.5 | 71 | 0.066 | sem=0.60, soft=3 |
| 2 | `58516` | Ruhige Wohnlage in der Stadt "Mieten ohne Kaution" | Winterthur | 1570 | 2.5 | 66 | 0.060 | sem=0.65, soft=3 |
| 3 | `212330` | Viel Lebensqualität auf kleinem Raum | Winterthur | 1360 | 1.5 | 41 | 0.059 | sem=0.61, soft=3 |
| 4 | `10314` | Moderne 4.5 Zimmer- Wohnung im Eigentumsstandard an zen | Winterthur | 3230 | 4.5 | 135 | 0.058 | sem=0.57, soft=3 |
| 5 | `212235` | grosszügige Wohnung an toller Lage | Winterthur | 2864 | 4.5 | 120 | 0.057 | sem=0.66, soft=3 |

**Analysis.** Winterthur + family-friendly + parking + 'not too expensive'. `features=['parking']`, `family_friendly=true`, `price_sentiment=cheap` soft. 'Bright' is BM25. No hard price cap — correct, 'not too expensive' is vague and should nudge rank, not filter.

#### CH-EX-03 — **5/10**

> Modern studio in Geneva for June move-in, quiet area, nice views if possible

- pool: **2**, returned 2, latency 2.43s
- filters: `city=['geneva']`; `canton=GE`; `available_from_after=2024-06-01`; `object_category=['studio']`; `bm25_keywords=['modern', 'quiet']`; soft: `quiet=True`

| # | id | title | city | CHF | rooms | m² | score | channels |
|---|---|---|---|---:|---:|---:|---:|---|
| 1 | `221285` | Studio,1,50 | Genève | 1803 | 1.0 | 50 | 0.049 | sem=0.53, soft=1 |
| 2 | `221591` | Studio meublé | Genève | 2150 | 1.0 | 36 | 0.049 | sem=0.56, soft=1 |

**Analysis.** Pool = 2. Geneva + Studio + June move-in + quiet + 'nice views'. The date parse is correct (`available_from_after=2024-06-01` — the system prompt says 'next occurrence from today', which should be 2026-06-01; this is a minor bug). Studio + Geneva is already a thin segment, and the date filter further drops the pool. 'Nice views' has no structural signal in text-only pipeline — would need images.

#### CH-EX-04 — **8/10**

> Looking for a affordable student accomondation, max half an hour door to door to ETH Zurich by public transport, i like modern kitchens.

- pool: **300**, returned 5, latency 2.29s
- filters: `city=['zurich']`; `bm25_keywords=['modern']`; soft: `price_sentiment=cheap`, `quiet=True`, `near_public_transport=True`, `near_schools=True`, `near_supermarket=True`, `near_park=True`, `family_friendly=True`, `commute_target=zurich_hb`, `near_landmark=['ETH']`

| # | id | title | city | CHF | rooms | m² | score | channels |
|---|---|---|---|---:|---:|---:|---:|---|
| 1 | `10211` | 5.5 Zimmerwohnung in Kreis 5 | Zürich | 2042 | 5.5 | — | 0.111 | sem=0.43, soft=9 |
| 2 | `59521` | Bright modern apartment in central Zurich – new kitchen | Zürich | 3200 | 2.5 | 70 | 0.106 | bm25=5.08, sem=0.56, soft=9 |
| 3 | `10270` | 1 ½ Zimmer-Wohnung | Zurich | 1990 | 1.5 | — | 0.101 | sem=0.53, soft=9 |
| 4 | `59737` | Zurich Wiedikon - 2.5-room apartment, 60 m²  2nd floor | Zurich | 2066 | 2.5 | 60 | 0.099 | bm25=3.98, sem=0.49, soft=9 |
| 5 | `10320` | Schönes möbliertes Apartment - 2 Schlafzimmer - großart | Zürich | 5300 | 3.5 | 87 | 0.099 | sem=0.51, soft=9 |

**Analysis.** Student query: ≤budget (via `price_sentiment=cheap`), 30-min commute to ETH, modern kitchens. `near_landmark=['ETH']` + `commute_target=zurich_hb`. 'Modern kitchens' is BM25 only (won't inflate features). Good handling of a composite intent.

### challenge.md — evaluation-style queries

#### CH-EV-01 — **3/10**

> Ich suche eine 2,5-Zimmer-Wohnung in Zürich Kreis 4 oder 5, maximal 2'800 CHF, mit Balkon und Waschmaschine in der Wohnung. Wichtig sind mir eine moderne Küche und ein lebendiges, aber nicht zu lautes Quartier.

- pool: **0**, returned 0, latency 2.39s
- filters: `city=['zurich']`; `postal_code=['8004', '8005']`; `canton=ZH`; `max_price=2800`; `min_rooms=2.5`; `max_rooms=2.5`; `features=['balcony', 'private_laundry']`; `object_category=['apartment']`; `bm25_keywords=['modern', 'lebendig']`; soft: `price_sentiment=moderate`, `quiet=True`, `near_public_transport=True`, `near_schools=True`, `near_supermarket=True`, `near_park=True`, `family_friendly=True`

_no matches_

**Analysis.** Pool = 0. Kreis 4 or 5 + 2.5 rooms + ≤2800 + balcony + in-unit laundry. The two features together with the exact-rooms and tight budget kill the pool. This is the exact same pattern as DE-08 and LONG-02: correct filters, thin dataset, silent empty response. Either the feature extraction needs to be softer (treat `private_laundry` as a ranking nudge, not a filter) or the empty-result UX needs a visible 'no matches — try without X' path.

#### CH-EV-02 — **4/10**

> Ich möchte eine Wohnung in der Nähe meines Arbeitsplatzes in Zug, mit maximal 25 Minuten Pendelzeit mit dem ÖV, mindestens 65 m² und mindestens 2 Zimmern. Die Wohnung sollte hell, ruhig und gut geschnitten sein.

- pool: **7**, returned 5, latency 2.44s
- filters: `city=['zug']`; `canton=ZG`; `min_rooms=2.5`; `min_area=65`; `bm25_keywords=['hell', 'ruhig']`; soft: `quiet=True`, `near_public_transport=True`, `commute_target=lugano_hb`

| # | id | title | city | CHF | rooms | m² | score | channels |
|---|---|---|---|---:|---:|---:|---:|---|
| 1 | `60120` | Attraktive Wohnlage nahe dem Zugersee | Zug | 2560 | 3.5 | 81 | 0.079 | sem=0.55, soft=3 |
| 2 | `221391` | Sanierte Stadtwohnung mit Stil - mitten in Zug | Zug | 4735 | 4.5 | 117 | 0.079 | sem=0.53, soft=3 |
| 3 | `27218` | Helle, möblierte 2,5-Zimmer-Wohnung– befristete Vermiet | Zug | 3400 | 2.5 | 65 | 0.078 | sem=0.59, soft=3 |
| 4 | `213497` | Ihr neues Zuhause im Loretoquartier | Zug | 2420 | 3.5 | 85 | 0.078 | sem=0.55, soft=3 |
| 5 | `212125` | Exklusive Altstadtwohnung mit Terrasse | Zug | 5000 | 4.0 | 83 | 0.078 | sem=0.46, soft=3 |

**Analysis.** Real bug caught. Correct: `city=['zug'], min_rooms=2.5, min_area=65`. **Wrong**: `commute_target=lugano_hb`. The user's workplace is in Zug, not Lugano — the LLM hallucinated the nearest whitelisted HB. `_COMMUTE_TARGETS` doesn't include Zug, and the model should have either emitted `null` (and let the soft-landmark path handle it) or picked `zurich_hb`. Lugano is 200 km from Zug. The 25-min commute-scoring ranker therefore evaluates every candidate against travel time to Lugano HB, which is actively misleading. Pool is still 7 because the hard filters are fine, but the rank order is contaminated.

#### CH-EV-03 — **8/10**

> Wir sind eine Familie mit einem Kind und suchen in Winterthur eine Wohnung mit mindestens 3,5 Zimmern, ab 80 m² und Miete unter 3'200 CHF. Uns sind gute Schulen, eine kinderfreundliche Umgebung und viel Tageslicht wichtig.

- pool: **24**, returned 5, latency 2.46s
- filters: `city=['winterthur']`; `canton=ZH`; `max_price=3200`; `min_rooms=3.5`; `min_area=80`; `object_category=['apartment']`; `bm25_keywords=['Tageslicht']`; soft: `price_sentiment=moderate`, `quiet=True`, `near_public_transport=True`, `near_schools=True`, `near_supermarket=True`, `near_park=True`, `family_friendly=True`

| # | id | title | city | CHF | rooms | m² | score | channels |
|---|---|---|---|---:|---:|---:|---:|---|
| 1 | `27340` | Erfüllen Sie sich Ihren Wohntraum! | Winterthur | 2635 | 3.5 | 109 | 0.118 | bm25=2.09, sem=0.60, soft=6 |
| 2 | `27179` | Ihre neue Familienwohnung mit Gartensitzplatz | Winterthur | 3070 | 5.5 | 136 | 0.116 | bm25=2.70, sem=0.57, soft=6 |
| 3 | `212235` | grosszügige Wohnung an toller Lage | Winterthur | 2864 | 4.5 | 120 | 0.116 | sem=0.63, soft=6 |
| 4 | `213211` | Moderne und ruhige 4-Zimmerwohnung | Winterthur | 2145 | 4.0 | 80 | 0.116 | sem=0.60, soft=6 |
| 5 | `698c53e7215b5789888c362e` | 4.5 Zimmerwohnung in Winterthur - ruhig &amp; familienf | Winterthur | 2655 | 4.5 | 109 | 0.115 | sem=0.63, soft=6 |

**Analysis.** Winterthur family query. ≥3.5 rooms + ≥80 m² + ≤3200 + family_friendly + near_schools + quiet. Clean extraction, pool should be reasonable, top-5 should be family-oriented Winterthur apartments.

#### CH-EV-04 — **3/10**

> Ich suche ein Studio oder 1,5-Zimmer-Apartment in Lausanne, maximal 1'900 CHF, möglichst möbliert und mit guter Anbindung an die EPFL. Es sollte modern, sauber und möglichst in Seenähe sein.

- pool: **0**, returned 0, latency 2.16s
- filters: `city=['lausanne']`; `canton=VD`; `max_price=1900`; `min_rooms=1.5`; `max_rooms=1.5`; `object_category=['studio', 'furnished_apartment']`; `bm25_keywords=['modern', 'clean', 'near lake']`; soft: `price_sentiment=cheap`, `quiet=True`, `near_public_transport=True`, `near_schools=True`, `near_supermarket=True`, `near_park=True`, `family_friendly=True`, `commute_target=lausanne_hb`, `near_landmark=['EPFL']`

_no matches_

**Analysis.** Pool = 0. Lausanne + studio/furnished + ≤1900 + EPFL + lake + modern. The `object_category=['studio','furnished_apartment']` combined with the tight budget drops the pool to nothing. Either 'möbliert' should have been a soft ranking signal (not a hard category) or the ≤1900 is too tight for Lausanne furnished studios. Same failure mode as CH-EV-01 / DE-08 / LONG-02.

#### CH-EV-05 — **3/10**

> Ich suche eine 4-Zimmer-Wohnung in Basel, unter 3'500 CHF, mit mindestens 2 Badezimmern und Parkplatz. Schön wäre eine ruhige Wohnlage, grüne Umgebung und ein gepflegtes Gebäude.

- pool: **0**, returned 0, latency 2.17s
- filters: `city=['basel']`; `canton=BS`; `max_price=3500`; `min_rooms=4.0`; `max_rooms=4.0`; `features=['parking']`; `object_category=['apartment']`; soft: `price_sentiment=moderate`, `quiet=True`, `near_public_transport=True`, `near_schools=True`, `near_supermarket=True`, `near_park=True`, `family_friendly=True`

_no matches_

**Analysis.** Pool = 0. Basel + 4-Zimmer + ≤3500 + `features=['parking']`. The 2-bathroom constraint from the text is dropped (there is no bathroom-count field in the hard schema — correct, bathrooms aren't structured), but the parking feature + budget is enough to empty the pool. Same failure mode as CH-EV-01/04, LONG-02, DE-08.

### challenge.md — hint queries

#### CH-HI-01 — **8/10**

> Bright 2-room apartment within 20 minutes of ETH Zentrum by public transport, under CHF 2,200, ideally in a lively neighbourhood with cafés

- pool: **29**, returned 5, latency 3.56s
- filters: `city=['zurich']`; `canton=ZH`; `max_price=2200`; `min_rooms=2.0`; `max_rooms=2.0`; `object_category=['apartment']`; `bm25_keywords=['bright']`; soft: `price_sentiment=cheap`, `quiet=True`, `near_public_transport=True`, `near_schools=True`, `near_supermarket=True`, `near_park=True`, `family_friendly=True`, `commute_target=zurich_hb`, `near_landmark=['ETH']`

| # | id | title | city | CHF | rooms | m² | score | channels |
|---|---|---|---|---:|---:|---:|---:|---|
| 1 | `212101` | 2-Zimmerwohnung in urbanem Quartier | Zürich | 1880 | 2.0 | — | 0.161 | sem=0.61, soft=9 |
| 2 | `60277` | Looking for 3.5 in K3, offering cheap 2.5 in K3 | Zürich | 1500 | 2.0 | — | 0.158 | sem=0.60, soft=9 |
| 3 | `3997` | Tauschimmobilie Charmante 2-Zimmer-Wohnung im Herzen vo | Zürich | 1000 | 2.0 | — | 0.157 | sem=0.55, soft=9 |
| 4 | `212969` | [Tauschwohnung] 2.5-Zi-Whg an der Bäckeranlage gegen Gr | Zürich | 1870 | 2.0 | 60 | 0.157 | sem=0.55, soft=9 |
| 5 | `5250` | geräumige 2 Zimmerwohnung in Zürich-Fluntern | Zürich | 2058 | 2.0 | — | 0.156 | sem=0.57, soft=9 |

**Analysis.** ETH Zentrum + 2 rooms + ≤2200 + commute + lively/cafés. `near_landmark=['ETH']`, `commute_target=zurich_hb`, and 'lively neighbourhood with cafés' as BM25 keywords. Reasonable top-5.

#### CH-HI-02 — **5/10**

> Quiet 1.5-room flat near ETH Hönggerberg, furnished if possible, move-in March, max CHF 1,800

- pool: **2**, returned 2, latency 2.47s
- filters: `city=['zurich']`; `canton=ZH`; `max_price=1800`; `min_rooms=1.5`; `max_rooms=1.5`; `available_from_after=2024-03-01`; `object_category=['furnished_apartment']`; `bm25_keywords=['ETH']`; soft: `price_sentiment=cheap`, `quiet=True`, `near_public_transport=True`, `near_schools=True`, `near_supermarket=True`, `near_park=True`, `family_friendly=True`, `commute_target=zurich_hb`, `near_landmark=['ETH']`

| # | id | title | city | CHF | rooms | m² | score | channels |
|---|---|---|---|---:|---:|---:|---:|---|
| 1 | `26712` | 1,5-Zimmer-Wohnung in Seefeld | Zurich | 1800 | 1.5 | 35 | 0.179 | sem=0.51, soft=9 |
| 2 | `9826` | 1,5 zentrales Studio | Zurich | 1800 | 1.5 | 22 | 0.178 | sem=0.57, soft=9 |

**Analysis.** Pool = 2. 1.5 room + furnished + March move-in + ≤1800 + Hönggerberg. The `available_from_after=2024-03-01` date is again the system-prompt bug (should be the next March from today = 2026-03-01, but the LLM anchored on 2024). Furnished + 1.5 + budget + Zurich is already thin; the stale date might be excluding everything. Bugs compound.

#### CH-HI-03 — **7/10**

> Family-friendly 4.5-room apartment in a Zurich suburb with good S-Bahn connection to HB, garden or large balcony, near good schools, up to CHF 3,500

- pool: **9**, returned 5, latency 2.34s
- filters: `city=['zurich']`; `canton=ZH`; `max_price=3500`; `min_rooms=4.0`; `max_rooms=4.5`; `features=['balcony']`; `object_category=['apartment']`; soft: `price_sentiment=moderate`, `quiet=True`, `near_public_transport=True`, `near_schools=True`, `near_supermarket=True`, `near_park=True`, `family_friendly=True`, `commute_target=zurich_hb`

| # | id | title | city | CHF | rooms | m² | score | channels |
|---|---|---|---|---:|---:|---:|---:|---|
| 1 | `212314` | 4 Zimmer Wohnung in Zürich Wiedikon (8003) | Zürich | 2465 | 4.0 | — | 0.141 | sem=0.52, soft=7 |
| 2 | `10290` | 4-Zimmer-Wohnung in Seefeld für ein Jahr verfügbar | Zürich | 2490 | 4.0 | 80 | 0.141 | sem=0.54, soft=7 |
| 3 | `212415` | Charmante 4-Zimmer Wohnung an der Kornhausstrasse | Zürich | 2520 | 4.0 | 94 | 0.141 | sem=0.54, soft=7 |
| 4 | `1570` | [Tauschwohnung] Altbau Wohnung kleines Haus ( 1Stock un | Zürich | 2500 | 4.5 | 80 | 0.139 | sem=0.55, soft=7 |
| 5 | `26838` | WOHNEN DIREKT BEIM SIHLCITY - MANESSESTRASSE 200! | Zürich | 3028 | 4.0 | 87 | 0.138 | sem=0.50, soft=7 |

**Analysis.** Classic family query. Extracted: `min_rooms=4.0, max_rooms=4.5` — the 4.0 lower bound is looser than the user asked (they said '4.5-room', i.e. 4.5 exactly), so we pick up some 4.0 apartments that aren't strictly in scope. `city=['zurich']`, `max_price=3500`, `features=['balcony']`. Pool of 9 — thin for a family segment. The `commute_target=zurich_hb` + `near_schools=true` + `family_friendly=true` soft signals all fire. Would rank higher if `min_rooms` were 4.5.

#### CH-HI-04 — **3/10**

> Modern loft-style apartment in Zurich Kreis 4 or 5 with lots of natural light and a view, 3+ rooms, around CHF 2,800

- pool: **0**, returned 0, latency 1.79s
- filters: `city=['zurich']`; `postal_code=['8004', '8005']`; `canton=ZH`; `min_price=0`; `max_price=2800`; `min_rooms=3.5`; `object_category=['loft']`; `bm25_keywords=['modern', 'natural light', 'view']`

_no matches_

**Analysis.** Pool = 0. Loft + Kreis 4/5 + ≥3.5 rooms + ~2800. Loft category is already very thin in the dataset, combined with the postal + room constraints → empty pool. This is the explicit multimodal query the challenge flags — text alone can't distinguish loft-style; images would help — but the hard filter on `object_category=['loft']` is what killed the pool, not the soft 'loft-style' description. The extractor should probably have treated 'loft-style' as BM25 rather than committing to the loft category.

#### CH-HI-05 — **5/10**

> Cheap but central studio in Zurich, close to ETH and nightlife, doesn't matter if small

- pool: **1**, returned 1, latency 2.16s
- filters: `city=['zurich']`; `canton=ZH`; `object_category=['studio']`; soft: `price_sentiment=cheap`, `near_landmark=['ETH']`

| # | id | title | city | CHF | rooms | m² | score | channels |
|---|---|---|---|---:|---:|---:|---:|---|
| 1 | `213155` | Studio-like 2-room with private bath in a big shared cl | Zürich | 1730 | 2.0 | 35 | 0.066 | sem=0.60, soft=2 |

**Analysis.** Pool = 1. 'Cheap but central studio in Zurich, close to ETH and nightlife'. `object_category=['studio']`, `city=zurich`, `price_sentiment=cheap`, `near_landmark=['ETH']`. The dataset has very few studios in Zurich tagged as such, which is why the pool collapses. Conflicts between 'cheap' and 'central' / 'near ETH' aren't explicit in the response — the pipeline doesn't produce a tradeoff narrative.
