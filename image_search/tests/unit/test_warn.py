from __future__ import annotations

from image_search.common.warn import warn


def test_warn_writes_expected_format(capsys):
    warn("dedup_dropped", kept="a.jpg", dropped="b.jpg")
    captured = capsys.readouterr()
    assert captured.err.startswith("[WARN] dedup_dropped:")
    assert "kept='a.jpg'" in captured.err
    assert "dropped='b.jpg'" in captured.err
    assert captured.err.endswith("\n")
    assert captured.out == ""


def test_warn_no_kwargs_still_emits_context(capsys):
    warn("standalone_warning")
    captured = capsys.readouterr()
    assert captured.err.strip() == "[WARN] standalone_warning"


def test_warn_preserves_numeric_types(capsys):
    warn("triage_lowconf", top="logo", score=0.12)
    captured = capsys.readouterr()
    assert "score=0.12" in captured.err
    assert "top='logo'" in captured.err
