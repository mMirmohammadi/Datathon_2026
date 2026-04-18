from pathlib import Path


def test_apps_sdk_split_files_exist() -> None:
    repo_root = Path(__file__).resolve().parents[1]

    required_paths = [
        "apps_sdk/server/main.py",
        "apps_sdk/web/package.json",
        "apps_sdk/web/src/App.tsx",
    ]

    for relative_path in required_paths:
        assert (repo_root / relative_path).exists(), relative_path
