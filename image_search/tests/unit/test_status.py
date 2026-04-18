from __future__ import annotations

import json
from pathlib import Path

from image_search.common.status import step


def test_step_prints_header_and_footer(capsys):
    with step("unit_test_step", total=5) as s:
        s["count"] = 5
    captured = capsys.readouterr()
    lines = [ln for ln in captured.out.split("\n") if ln]
    assert any("[STEP] unit_test_step starting" in ln for ln in lines)
    assert any("[STEP] unit_test_step done" in ln and "count=5" in ln for ln in lines)


def test_step_writes_jsonl_log(tmp_path: Path):
    log_path = tmp_path / "status.jsonl"
    with step("embed", total=3, log_path=log_path) as s:
        s["count"] = 3
        s["extra"]["note"] = "ok"
    assert log_path.exists()
    record = json.loads(log_path.read_text().strip())
    assert record["step"] == "embed"
    assert record["count"] == 3
    assert record["total"] == 3
    assert record["extra"] == {"note": "ok"}
    assert record["duration_s"] >= 0


def test_step_logs_warning_count_in_footer(capsys):
    with step("triage") as s:
        s["count"] = 10
        s["warnings"] = 2
    captured = capsys.readouterr()
    assert "warnings=2" in captured.out
