⏺ "Go Beyond the Dataset" — Enrichment & Ranking Plan                                                                                                                                                                                         
                                                                                           
  The gap
                                                                                                                                                                                                                                              
  The raw dataset answers structural queries (Zurich / 3 rooms / <2800 CHF / has balcony). It can't answer the 80% of real queries that mix soft, vague, and relational intent: quiet, bright, modern, family-friendly, 30-min to ETH, near   
  good schools, not too expensive, nice view. Enrichment exists to close that gap. Everything below maps 1:1 to user phrases seen in the slides.                                                                                              
                                                                                                                                                                                                                                              
  Enrichment layers — ranked by marginal NDCG per engineer-hour                                                                                                                                                                               
                                                                                                                                                                                                                                              
  ┌─────┬──────────────────────────────────────────────────────────────────────────────────────────────────────────┬────────────────────┬───────────────────────────────────────────────────┬───────────────────────────────────────────┐     
  │  #  │                                                  Layer                                                   │       Effort       │               Answers which intent                │                  Source                   │  
  ├─────┼──────────────────────────────────────────────────────────────────────────────────────────────────────────┼────────────────────┼───────────────────────────────────────────────────┼───────────────────────────────────────────┤     
  │ 1   │ Per-segment price baselines (median by canton×rooms and PLZ-prefix×rooms)                                │ 30 min             │ "not too expensive", "affordable", "cheap"        │ own data                                  │  
  ├─────┼──────────────────────────────────────────────────────────────────────────────────────────────────────────┼────────────────────┼───────────────────────────────────────────────────┼───────────────────────────────────────────┤  
  │ 2   │ SBB GTFS stops → nearest-stop distance, stop type (train/tram/bus), lines count                          │ 2h                 │ "close to public transport", "good connections"   │ opentransportdata.swiss (free, no key)    │     
  ├─────┼──────────────────────────────────────────────────────────────────────────────────────────────────────────┼────────────────────┼───────────────────────────────────────────────────┼───────────────────────────────────────────┤     
  │ 3   │ Landmark gazetteer (~150 entries: ETH, EPFL, UZH, all unis, major stations, airports, lakes, old-town    │ 1h                 │ "near ETH", "close to Altstadt", "walking         │ Nominatim (one-off, cached JSON)          │     
  │     │ centroids, major employers)                                                                              │                    │ distance to Roche"                                │                                           │     
  ├─────┼──────────────────────────────────────────────────────────────────────────────────────────────────────────┼────────────────────┼───────────────────────────────────────────────────┼───────────────────────────────────────────┤  
  │ 4   │ Commute proxy: nearest-station distance × 60 km/h rail baseline → city-center travel time estimate       │ 1h                 │ "commute to city center"                          │ derived from #2                           │     
  ├─────┼──────────────────────────────────────────────────────────────────────────────────────────────────────────┼────────────────────┼───────────────────────────────────────────────────┼───────────────────────────────────────────┤  
  │ 5   │ Multilingual description embeddings (bge-m3 OR OpenAI) over a "listing card"                             │ 1h compute, big    │ soft-vague queries at scale ("cozy", "modern",    │ local/API                                 │     
  │     │ (title+city+features+desc-head)                                                                          │ unlock             │ "charming")                                       │                                           │ 
  ├─────┼──────────────────────────────────────────────────────────────────────────────────────────────────────────┼────────────────────┼───────────────────────────────────────────────────┼───────────────────────────────────────────┤     
  │ 6   │ Description NLP for features with DE/FR/IT/EN regex + negation guard, writing feat_*_txt                 │ half-day           │ recovers feature flags for all 11k SRED rows      │ own data + rules                          │ 
  ├─────┼──────────────────────────────────────────────────────────────────────────────────────────────────────────┼────────────────────┼───────────────────────────────────────────────────┼───────────────────────────────────────────┤     
  │ 7   │ OSM POI density within 300m/1km: supermarkets, schools, kindergartens, restaurants, parks, playgrounds,  │ half-day           │ "family-friendly", "good schools", "lively",      │ Overpass API or geofabrik.de CH extract   │     
  │     │ gyms, clinics                                                                                            │                    │ "walkable"                                        │ (~600MB)                                  │
  ├─────┼──────────────────────────────────────────────────────────────────────────────────────────────────────────┼────────────────────┼───────────────────────────────────────────────────┼───────────────────────────────────────────┤     
  │ 8   │ Noise/quiet proxy: distance to motorway/primary road + rail line (OSM)                                   │ 1h                 │ "quiet area"                                      │ OSM (reuses #7 extract)                   │
  ├─────┼──────────────────────────────────────────────────────────────────────────────────────────────────────────┼────────────────────┼───────────────────────────────────────────────────┼───────────────────────────────────────────┤     
  │ 9   │ Floor-plan classifier + hero selection (CLIP zero-shot or Claude vision)                                 │ 2h                 │ correctness guard for all image signals           │ local/API                                 │
  ├─────┼──────────────────────────────────────────────────────────────────────────────────────────────────────────┼────────────────────┼───────────────────────────────────────────────────┼───────────────────────────────────────────┤     
  │ 10  │ Claude Vision on top-20 at rerank time (brightness, modernity, view, kitchen quality, open plan) — not   │ 1h wiring          │ "bright", "modern kitchen", "nice view"           │ Anthropic API                             │
  │     │ offline                                                                                                  │                    │                                                   │                                           │     
  ├─────┼──────────────────────────────────────────────────────────────────────────────────────────────────────────┼────────────────────┼───────────────────────────────────────────────────┼───────────────────────────────────────────┤
  │ 11  │ Real SBB routing for top-20 (Journey Planner API, origin=listing, destinations=user landmarks)           │ 2h                 │ "30 min door-to-door to ETH" exactly              │ opentransportdata.swiss                   │     
  ├─────┼──────────────────────────────────────────────────────────────────────────────────────────────────────────┼────────────────────┼───────────────────────────────────────────────────┼───────────────────────────────────────────┤     
  │ 12  │ SwissTopo DEM 25m → elevation + local relief                                                             │ 2h                 │ "nice view" (weak — better fused with #10)        │ swisstopo (free download)                 │
  ├─────┼──────────────────────────────────────────────────────────────────────────────────────────────────────────┼────────────────────┼───────────────────────────────────────────────────┼───────────────────────────────────────────┤     
  │ 13  │ BFS demographics per PLZ (median income, family rate)                                                    │ half-day           │ "good neighborhood", family-fit nuance            │ bfs.admin.ch                              │
  └─────┴──────────────────────────────────────────────────────────────────────────────────────────────────────────┴────────────────────┴───────────────────────────────────────────────────┴───────────────────────────────────────────┘     
                  
  Everything above row 10 is either offline+cached or low-volume on-demand. No per-query API cost explosion.                                                                                                                                  
                  
  How this becomes "the best match"                                                                                                                                                                                                           
                  
  Retrieval → rerank pipeline:                                                                                                                                                                                                                
  query → LLM plan (hard + soft + landmarks + commute)
        → SQL hard filter (with filled canton/city from the previous script)
        → BM25 ∪ dense embeddings → RRF top 200                                                                                                                                                                                               
        → linear-blend rerank on ~12 normalized signals (weights tuned on organizer eval set)                                                                                                                                                 
        → top 20: on-demand SBB routing + Claude Vision on hero                                                                                                                                                                               
        → Pareto / MMR layer → final top-N with explanations                                                                                                                                                                                  
                                                                                                                                                                                                                                              
  Solving conflicting preferences (slide 10's "cheap AND central AND quiet"):                                                                                                                                                                 
  - Compute a Pareto frontier over the top-50 on the 3 axes the query mentions (price, centrality, noise, size, quality). Non-dominated listings only.                                                                                        
  - Surface three strategies in the response: prioritize price, balanced, prioritize lifestyle — this directly mirrors slide 10 and lets the user pick their tradeoff instead of the system pretending there's one answer.                    
  - MMR diversification when top-K scores cluster: force spread across city / price band / size so the user isn't shown 10 variants of the same building.                                                                                     
  - Clarification chips when the LLM marks a query as genuinely ambiguous ("nice flat"): return empty + 3 chips. Better empty than wrong.                                                                                                     
                                                                                                                                                                                                                                              
  Execution order                                                                                                                                                                                                                             
                                                                                                                                                                                                                                              
  1. Price baselines (30 min) — highest NDCG/hour.                                                                                                                                                                                            
  2. SBB GTFS stops + BallTree (2h).
  3. Landmark gazetteer (1h).                                                                                                                                                                                                                 
  4. Description NLP for features, esp. SRED (half-day) — largest coverage uplift on the 11k blind rows.
  5. Multilingual embeddings (1h compute, big win on soft queries).                                                                                                                                                                           
  6. OSM POI counts (half-day).
  7. Floor-plan classifier + hero pick (2h).                                                                                                                                                                                                  
  8. Claude Vision on-demand for top-K (1h).
  9. Real SBB routing on-demand for top-K (2h).                                                                                                                                                                                               
  10. Pareto + MMR + clarification (half-day).                                                                                                                                                                                                
  11. Ablation on eval set — turn each signal off, measure NDCG@10 delta; cut anything that doesn't move the needle. Critical, don't skip.                                                                                                    
                                                                                                                                                                                                                                              
  Cut list if time is short (drop in this order)
                                                                                                                                                                                                                                              
  DEM/elevation → demographics → self-hosted routing (use proxy #4) → per-photo room classification (hero + floor-plan filter is enough).                                                                                                     
   
  Tools/services to line up now                                                                                                                                                                                                               
                  
  - opentransportdata.swiss — GTFS download + Journey Planner API (free, no key for low volume)                                                                                                                                               
  - Overpass API or geofabrik.de CH extract for OSM
  - Nominatim public — only for the ~150 landmark one-off; don't use it at request time                                                                                                                                                       
  - Anthropic API — query planning + vision reranker                                                                                                                                                                                          
  - Embeddings — local bge-m3 (free, 2GB weights) or OpenAI text-embedding-3-large (no weights in container, fits Fly.io better)                                                                                                              
                                                                                                                                                                                                                                              
  What to measure                                                                                                                                                                                                                             
                                                                                                                                                                                                                                              
  Hard-filter precision and NDCG@10 on the organizer-provided eval set, gated in CI. Run the full ablation at least twice: after layer 5 (text-only baseline) and after layer 10 (multimodal). If adding a signal doesn't improve NDCG, delete
   it — complexity without measured gain is a jury red flag, not a strength.
                                                                                                                                                                                                                                              
  Want me to start implementing? I'd recommend: price baselines + SBB stops + landmark gazetteer first — all three are ≤ half a day combined and unlock the biggest chunk of realistic queries.

⏺ Cost & rate limit breakdown                                                                                                                                                                                                                 
                                                                                                                                                                                                                                              
  Tier 1 — Free forever, no per-call limit (local / downloaded)                                                                                                                                                                               
                                                                                                                                                                                                                                              
  ┌───────────────────────────────────────────────┬───────────────────┬───────────────────────────────────────────────────────────────────────────────┐                                                                                       
  │                     Tool                      │       Size        │                                     Notes                                     │                                                                                       
  ├───────────────────────────────────────────────┼───────────────────┼───────────────────────────────────────────────────────────────────────────────┤                                                                                       
  │ reverse_geocoder (already used)               │ ~2 MB GeoNames DB │ Free local KNN. No limits.                                                    │
  ├───────────────────────────────────────────────┼───────────────────┼───────────────────────────────────────────────────────────────────────────────┤                                                                                       
  │ GTFS stops.txt (SBB network)                  │ ~5 MB             │ Download once, BallTree locally. No runtime API cost.                         │                                                                                       
  ├───────────────────────────────────────────────┼───────────────────┼───────────────────────────────────────────────────────────────────────────────┤                                                                                       
  │ CH OSM extract (geofabrik.de)                 │ ~600 MB           │ ODbL-licensed, free. Query locally with osmium/pyrosm — no rate limit at all. │                                                                                       
  ├───────────────────────────────────────────────┼───────────────────┼───────────────────────────────────────────────────────────────────────────────┤                                                                                       
  │ SwissTopo swissALTI3D (DEM)                   │ 1–2 GB            │ Free since 2021. Download once.                                               │
  ├───────────────────────────────────────────────┼───────────────────┼───────────────────────────────────────────────────────────────────────────────┤                                                                                       
  │ BFS (Swiss federal statistics)                │ <100 MB           │ Open data.                                                                    │
  ├───────────────────────────────────────────────┼───────────────────┼───────────────────────────────────────────────────────────────────────────────┤                                                                                       
  │ bge-m3 multilingual embeddings                │ 2.3 GB weights    │ Free, local. CPU works; GPU ~20× faster.                                      │
  ├───────────────────────────────────────────────┼───────────────────┼───────────────────────────────────────────────────────────────────────────────┤                                                                                       
  │ CLIP ViT-B/32                                 │ ~350 MB weights   │ Free, local.                                                                  │
  ├───────────────────────────────────────────────┼───────────────────┼───────────────────────────────────────────────────────────────────────────────┤                                                                                       
  │ Swiss Post PLZ CSV (for postal_code backfill) │ <10 MB            │ Free.                                                                         │
  └───────────────────────────────────────────────┴───────────────────┴───────────────────────────────────────────────────────────────────────────────┘                                                                                       
                  
  These cost zero at query time. The only cost is disk/RAM and one-time download.                                                                                                                                                             
                  
  Tier 2 — Free but rate-limited (use for one-off enrichment, never at query time)                                                                                                                                                            
                  
  ┌──────────────────────┬───────────────────────────────────────────────────────────────────────────────────────────────────┬───────────────────────────────────────────────────────────────────────────────────────────────────────────┐    
  │         Tool         │                                               Limit                                               │                                            Practical takeaway                                             │ 
  ├──────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────┤    
  │ SBB OJP (Journey     │ Free tier: 50 req/min, 20,000 req/day per API key. Paid: CHF 500/mo → 2,500 req/min, 1M/day; CHF  │ Free tier is plenty for hackathon demo (call only for top-5 commute of each user query → 4,000            │ 
  │ Planner)             │ 1,000/mo → 5,000 req/min, 2M/day.                                                                 │ queries/day budget). Register an API key.                                                                 │ 
  ├──────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────┤    
  │ Nominatim (public)   │ Absolute max 1 req/s, policy may change without notice, "not for commercial heavy use".           │ Only use for the one-off ~150 landmark gazetteer (~3 min total). Never at query time. Self-host if you    │
  │                      │                                                                                                   │ need more.                                                                                                │    
  ├──────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────┤
  │ Overpass API         │ Slot-based queue, ~1M req/day across ~30k users, 180s query timeout, 512 MB memory default, HTTP  │ Fine for small ad-hoc queries. For 22,819-listing POI enrichment, don't use Overpass — download the       │    
  │ (public)             │ 429 on overuse.                                                                                   │ geofabrik CH extract and query locally.                                                                   │    
  └──────────────────────┴───────────────────────────────────────────────────────────────────────────────────────────────────┴───────────────────────────────────────────────────────────────────────────────────────────────────────────┘
                                                                                                                                                                                                                                              
  Tier 3 — Paid per call (LLM & embeddings)

  ┌────────────────────────────────────────────┬──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┬───────────────────────────────────────────────────────────┐   
  │                  Service                   │                                                            Price                                                             │                     Where you use it                      │
  ├────────────────────────────────────────────┼──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┼───────────────────────────────────────────────────────────┤   
  │ Claude Sonnet 4.6 (query plan,             │ $3 / MTok input, $15 / MTok output, $0.30 / MTok cached input (90% off), Batch API halves both. Images counted as input      │ Query planning + optional vision rerank + optional        │
  │ explanations, vision)                      │ tokens (~640 tokens for a typical 800×600 listing photo).                                                                    │ LLM-polished explanations.                                │
  ├────────────────────────────────────────────┼──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┼───────────────────────────────────────────────────────────┤   
  │ OpenAI text-embedding-3-small              │ $0.02 / MTok                                                                                                                 │ Alternative to bge-m3 if you don't want 2.3 GB weights in │
  │                                            │                                                                                                                              │  your container.                                          │   
  ├────────────────────────────────────────────┼──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┼───────────────────────────────────────────────────────────┤
  │ OpenAI text-embedding-3-large              │ $0.13 / MTok                                                                                                                 │ Higher quality, still cheap.                              │   
  └────────────────────────────────────────────┴──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┴───────────────────────────────────────────────────────────┘   
   
  Per-query cost model (with warm prompt cache)                                                                                                                                                                                               
                  
  ┌────────────────────────────────────────────────────────────────────────────────────────────┬────────────────┐                                                                                                                             
  │                                         Component                                          │ Cost per query │
  ├────────────────────────────────────────────────────────────────────────────────────────────┼────────────────┤
  │ Claude query-plan call (2k token cached system prompt + 100 token user + 200 token output) │ ~$0.004        │
  ├────────────────────────────────────────────────────────────────────────────────────────────┼────────────────┤
  │ Query embedding (OpenAI small, or local bge-m3)                                            │ ~$0            │                                                                                                                             
  ├────────────────────────────────────────────────────────────────────────────────────────────┼────────────────┤                                                                                                                             
  │ Hard SQL filter + BM25 + FAISS rerank (all local)                                          │ $0             │                                                                                                                             
  ├────────────────────────────────────────────────────────────────────────────────────────────┼────────────────┤                                                                                                                             
  │ SBB OJP routing for top-5 commute (free tier)                                              │ $0             │
  ├────────────────────────────────────────────────────────────────────────────────────────────┼────────────────┤                                                                                                                             
  │ Claude Vision on top-5 hero images (~640 tokens each, cached prompt, 150-token output)     │ ~$0.02         │
  ├────────────────────────────────────────────────────────────────────────────────────────────┼────────────────┤                                                                                                                             
  │ Claude Vision on top-10 hero images                                                        │ ~$0.04         │
  ├────────────────────────────────────────────────────────────────────────────────────────────┼────────────────┤                                                                                                                             
  │ Optional LLM-polished reasons (top-5, 120-token output each)                               │ ~$0.009        │
  └────────────────────────────────────────────────────────────────────────────────────────────┴────────────────┘                                                                                                                             
                  
  Typical end-to-end query:                                                                                                                                                                                                                   
  - Text-only rerank: ~$0.004 per query
  - + Vision on top-5: ~$0.025 per query                                                                                                                                                                                                      
  - + Vision on top-10 + LLM reasons: ~$0.05 per query
                                                                                                                                                                                                                                              
  Running 1,000 test/demo queries in the heaviest configuration = ~$50. Well inside hackathon credits.                                                                                                                                        
                                                                                                                                                                                                                                              
  One-off enrichment cost (runs once, cached forever)                                                                                                                                                                                         
                                                                                                                                                                                                                                              
  ┌───────────────────────────────────────────────────────────────────────┬──────────────────────────────────────────────────────┐                                                                                                            
  │                                 Step                                  │                         Cost                         │
  ├───────────────────────────────────────────────────────────────────────┼──────────────────────────────────────────────────────┤
  │ Reverse-geocode DB (already written)                                  │ $0 (local)                                           │
  ├───────────────────────────────────────────────────────────────────────┼──────────────────────────────────────────────────────┤
  │ Embed all 22,819 listings with bge-m3 locally                         │ $0, ~30 min CPU                                      │                                                                                                            
  ├───────────────────────────────────────────────────────────────────────┼──────────────────────────────────────────────────────┤                                                                                                            
  │ OR embed with OpenAI text-embedding-3-small (22k × ~300 tokens)       │ ~$0.14 one-time                                      │                                                                                                            
  ├───────────────────────────────────────────────────────────────────────┼──────────────────────────────────────────────────────┤                                                                                                            
  │ OR embed with text-embedding-3-large                                  │ ~$0.89 one-time                                      │
  ├───────────────────────────────────────────────────────────────────────┼──────────────────────────────────────────────────────┤                                                                                                            
  │ CLIP scoring for all images (local)                                   │ $0, compute-bound                                    │
  ├───────────────────────────────────────────────────────────────────────┼──────────────────────────────────────────────────────┤                                                                                                            
  │ Landmark gazetteer (~150 Nominatim calls at 1 req/s)                  │ $0, ~3 min                                           │
  ├───────────────────────────────────────────────────────────────────────┼──────────────────────────────────────────────────────┤                                                                                                            
  │ SBB GTFS stops + BallTree build                                       │ $0                                                   │
  ├───────────────────────────────────────────────────────────────────────┼──────────────────────────────────────────────────────┤                                                                                                            
  │ OSM POI density for 22k listings (local query over geofabrik extract) │ $0, ~1h                                              │
  ├───────────────────────────────────────────────────────────────────────┼──────────────────────────────────────────────────────┤                                                                                                            
  │ Claude Vision batch scoring all hero images (if you want it offline)  │ 22k × $0.004 = ~$88 — don't do this, do it on-demand │
  └───────────────────────────────────────────────────────────────────────┴──────────────────────────────────────────────────────┘                                                                                                            
                  
  Total one-off budget: under $1 if you use OpenAI embeddings; zero if you use bge-m3 locally.                                                                                                                                                
                  
  Pragmatic recommendation                                                                                                                                                                                                                    
                  
  - Register an OJP API key for the 20k/day free tier → plenty for the demo.                                                                                                                                                                  
  - Never hit Nominatim or Overpass at query time. Use them for offline one-offs only.
  - Cache the Claude query plan per (query-string) and vision results per (listing_id, prompt) — repeated queries during the demo cost zero.                                                                                                  
  - Set a daily spend cap on the Anthropic account (e.g., $20) so a runaway peer-testing load can't surprise you.                                                                                                                             
  - Budget for the hackathon: ~$1 enrichment + ~$30 for realistic 1k-query demo traffic + safety margin = under $50 total Anthropic spend.                                                                                                    
                                                                                                                                                                                                                                              
  Sources                                                                                                                                                                                                                                     
                                                                                                                                                                                                                                              
  - https://opentransportdata.swiss/en/limits-and-costs/                                                                                                                                                                                      
  - https://operations.osmfoundation.org/policies/nominatim/
  - https://wiki.openstreetmap.org/wiki/Overpass_API                                                                                                                                                                                          
  - https://platform.claude.com/docs/en/about-claude/pricing