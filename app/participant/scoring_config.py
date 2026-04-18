"""Central place for the MVP ranking weights + tunable constants.

Percentile-normalized signals compose linearly. Positive weights sum to 1.0
before the negative penalty. Edit here (not in ranking.py) to tune.
"""
from __future__ import annotations

# --- blend weights (see baseline_mvp.md §3) ---
W_BM25 = 0.40          # BM25F percentile within candidate pool
W_FEATURE_MATCH = 0.20 # fraction of preferred features present
W_PRICE_FIT = 0.20     # triangle fit to candidate pool price distribution
W_FRESHNESS = 0.10     # linear on available_from
W_NEGATIVE_PENALTY = -0.10  # applied if negative keyword / flag match

# --- freshness window (days) ---
FRESHNESS_FULL_DAYS = 60    # ≤ this many days away → score 1.0
FRESHNESS_ZERO_DAYS = 365   # ≥ this many days away → score 0.0

# --- rapidfuzz thresholds for fuzzy negative-keyword match ---
NEGATIVE_FUZZY_RATIO = 85   # 0-100; above this a token in text is treated as a hit

# --- ranking output count ---
# How many candidates we rank into the final response (caller chooses limit).
# We rank up to this many, then paginate.
MAX_RANKED = 100

# --- price-fit triangle centers as quantile of candidate pool ---
PRICE_SENTIMENT_Q = {
    "cheap": 0.25,
    "moderate": 0.50,
    "premium": 0.75,
}
