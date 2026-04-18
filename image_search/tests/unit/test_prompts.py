from __future__ import annotations

from image_search.common.prompts import (
    ALL_CLASSES,
    DROPPED_CLASSES,
    FLOORPLAN_CLASSES,
    KEPT_CLASSES,
    MAIN_INDEX_CLASSES,
    PROMPTS,
    flatten,
)


def test_class_sets_are_partitioned():
    assert KEPT_CLASSES.isdisjoint(DROPPED_CLASSES)
    assert frozenset(ALL_CLASSES) == KEPT_CLASSES | DROPPED_CLASSES
    assert FLOORPLAN_CLASSES.issubset(KEPT_CLASSES)
    assert MAIN_INDEX_CLASSES == KEPT_CLASSES - FLOORPLAN_CLASSES


def test_every_class_has_four_languages_with_nonempty_templates():
    for label in ALL_CLASSES:
        per_lang = PROMPTS[label]
        assert set(per_lang.keys()) == {"de", "fr", "it", "en"}, label
        for lang, templates in per_lang.items():
            assert len(templates) >= 3, f"{label}/{lang} has <3 templates"
            for t in templates:
                assert t and isinstance(t, str), f"{label}/{lang} has empty/non-str template"
                assert t.strip() == t, f"{label}/{lang} has un-stripped template {t!r}"


def test_flatten_preserves_order_and_has_ge_12_templates():
    for label in ALL_CLASSES:
        flat = flatten(label)
        assert len(flat) >= 12, f"{label} has only {len(flat)} templates across all langs"
        # language order: de, fr, it, en — flatten should interleave in that order
        de_first = PROMPTS[label]["de"][0]
        en_last = PROMPTS[label]["en"][-1]
        assert flat[0] == de_first
        assert flat[-1] == en_last


def test_total_prompt_count_sanity():
    total = sum(len(flatten(c)) for c in ALL_CLASSES)
    assert total >= 84, f"pipeline has only {total} prompts total — expected ≥84 (7×4×3)"
