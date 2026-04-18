from pathlib import Path


def test_core_runtime_entrypoints_exist() -> None:
    repo_root = Path(__file__).resolve().parents[1]

    for relative_path in [
        "Dockerfile",
        "docker-compose.yml",
        "README.md",
        "challenge.md",
        "app/main.py",
        "apps_sdk/server/main.py",
        "scripts/mcp_smoke.py",
    ]:
        assert (repo_root / relative_path).exists(), relative_path

    assert (repo_root / "raw_data").exists() or (repo_root / "raw_data.zip").exists()


def test_widget_app_contains_mcp_bridge_logic() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    app_tsx = (repo_root / "apps_sdk" / "web" / "src" / "App.tsx").read_text(encoding="utf-8")

    assert "tool-result" in app_tsx
    assert "addEventListener" in app_tsx
