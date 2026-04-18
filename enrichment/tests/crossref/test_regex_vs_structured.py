"""Crossref: regex extractor agreement with the structured feature_* flags.

For each feature, compute TP/FP/FN/TN against the structured label, plus:
  * Recall  = fraction of structured=1 rows the regex also matched
  * Precision (vs. noisy label) = TP / (TP + FP) — measures agreement, NOT true
    precision. See note below.

Only non-SRED rows are measured (SRED has no structured feature flags per
REPORT.md §7 L145).

## Why precision gate is relaxed

The plan's original §D.3 §3 target (precision ≥ 0.95) presumed a clean ground
truth. In practice, the structured flag is NOISY:

  * `feature_fireplace=0` ≠ "no fireplace" — it often means "lister didn't tick
    the fireplace box". An in-repo spot-check (Mar 2026) found 428/14010 (3.1%)
    of `feature_fireplace=0` rows literally say "Kamin" / "Cheminée" in the
    description text. Those are counted as regex FPs against the structured
    label but are correct extractions from the text.
  * Same pattern applies to child_friendly (REPORT §7 L149: "advertising claim
    flag — rarely set to False explicitly").

## Gate we CAN assert

  * **Recall** (regex catches structured=1 rows) — structured=1 is reliable
    (lister actively claimed the feature), so recall is a fair quality metric.
  * **Prevalence sanity** (regex match-rate within 0.5x..3.0x of structured=1
    prevalence) — catches patterns that catastrophically over- or under-match.

The confusion matrix is printed on every run for diagnostic visibility.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
import yaml

from enrichment.common.langdet import guess_lang, strip_html
from enrichment.common.text_extract import find_first_match

PATTERNS_DIR = Path(__file__).resolve().parents[2] / "patterns"


def _load_patterns() -> tuple[dict, dict]:
    with (PATTERNS_DIR / "features.yaml").open() as f:
        features = yaml.safe_load(f)
    with (PATTERNS_DIR / "negation.yaml").open() as f:
        neg = yaml.safe_load(f)
    return features, neg


def _langs(spec: dict) -> dict[str, list[str]]:
    if "all" in spec:
        all_pats = spec["all"]
        return {"de": all_pats, "fr": all_pats, "it": all_pats, "en": all_pats}
    return {k: spec.get(k, []) for k in ("de", "fr", "it", "en")}


@pytest.fixture(scope="module")
def non_sred_rows(base_db) -> list[tuple[str, str, int, int, int, int, int]]:
    """Every non-SRED row: (listing_id, description_clean, f_balcony, f_elevator,
    f_parking, f_garage, f_fireplace). Null structured flags → -1.
    """
    conn = sqlite3.connect(str(base_db))
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute("""
            SELECT listing_id, title, description,
                   feature_balcony, feature_elevator, feature_parking,
                   feature_garage, feature_fireplace
            FROM listings
            WHERE scrape_source != 'SRED';
        """).fetchall()
    finally:
        conn.close()

    out = []
    for r in rows:
        desc = strip_html(f"{r['title'] or ''}\n{r['description'] or ''}")
        fb = r["feature_balcony"] if r["feature_balcony"] is not None else -1
        fe = r["feature_elevator"] if r["feature_elevator"] is not None else -1
        fp = r["feature_parking"] if r["feature_parking"] is not None else -1
        fg = r["feature_garage"] if r["feature_garage"] is not None else -1
        ff = r["feature_fireplace"] if r["feature_fireplace"] is not None else -1
        out.append((r["listing_id"], desc, fb, fe, fp, fg, ff))
    return out


@pytest.mark.parametrize("feat,col_idx,min_recall,prev_lo,prev_hi", [
    # col_idx is the position in non_sred_rows after (listing_id, desc):
    # 2=balcony, 3=elevator, 4=parking, 5=garage, 6=fireplace.
    # Gates are (min_recall, prev_ratio_low, prev_ratio_high).
    # prev_ratio = regex_positive_rate / structured_positive_rate.
    #
    # Why recall gates differ per feature:
    #   balcony: ~81% of struct=1 rows mention balcony/Balkon/terrasse in text
    #   elevator: ONLY ~30% of struct=1 rows mention lift/Aufzug in text.
    #     The other 70% ticked the structured box without writing prose.
    #     (Measured in-repo Mar 2026: 2,638 / 3,763 = 70.1% have no elevator
    #      keyword anywhere in description. Our regex cannot recover those.)
    #   fireplace: ~50% of struct=1 rows mention Kamin/Cheminée in text.
    # Gates are set BELOW the empirical ceiling so they remain regression-sensitive.
    ("balcony",   2, 0.70, 0.50, 3.0),
    ("elevator",  3, 0.25, 0.30, 3.0),
    ("fireplace", 6, 0.45, 0.50, 3.0),
])
def test_regex_agreement_with_structured_flag(
    non_sred_rows, feat: str, col_idx: int,
    min_recall: float, prev_lo: float, prev_hi: float,
):
    features, neg_yaml = _load_patterns()
    langs = _langs(features[feat])
    neg = {k: neg_yaml.get(k, []) for k in ("de", "fr", "it", "en")}

    tp = fp = fn = tn = 0
    for row in non_sred_rows:
        _, desc, *flags = row
        true_flag = flags[col_idx - 2]
        if true_flag == -1:
            continue
        lang = guess_lang(desc)
        if lang == "unk":
            lang = "de"
        hit = find_first_match(desc, langs, lang, neg)
        regex_says_yes = hit is not None and not hit.negated

        if true_flag == 1 and regex_says_yes:
            tp += 1
        elif true_flag == 1 and not regex_says_yes:
            fn += 1
        elif true_flag == 0 and regex_says_yes:
            fp += 1
        else:
            tn += 1

    total = tp + fp + fn + tn
    assert total > 100, f"too few non-SRED rows with {feat} structured flag: {total}"

    precision_vs_noisy = tp / (tp + fp) if (tp + fp) else 1.0
    recall = tp / (tp + fn) if (tp + fn) else 1.0
    struct_pos_rate = (tp + fn) / total
    regex_pos_rate = (tp + fp) / total
    prev_ratio = regex_pos_rate / struct_pos_rate if struct_pos_rate else float("inf")

    print(
        f"\n[{feat}] TP={tp} FP={fp} FN={fn} TN={tn} total={total} "
        f"precision_vs_noisy_label={precision_vs_noisy:.3f} recall={recall:.3f} "
        f"struct_pos_rate={struct_pos_rate:.3f} regex_pos_rate={regex_pos_rate:.3f} "
        f"prev_ratio={prev_ratio:.2f}x "
        f"(gates: recall>={min_recall}, prev_ratio in [{prev_lo}x, {prev_hi}x])",
        flush=True,
    )

    assert recall >= min_recall, (
        f"{feat} recall {recall:.3f} < {min_recall} "
        f"(regex missed {fn}/{tp+fn} structured-positive rows; "
        f"likely pattern too narrow — see langs DE/FR/IT/EN for {feat!r})"
    )
    assert prev_lo <= prev_ratio <= prev_hi, (
        f"{feat} regex prevalence {regex_pos_rate:.3f} "
        f"is {prev_ratio:.2f}x the structured rate {struct_pos_rate:.3f}; "
        f"gate allows [{prev_lo}x, {prev_hi}x]. "
        f"{'Pattern too loose' if prev_ratio > prev_hi else 'Pattern too narrow'}."
    )
