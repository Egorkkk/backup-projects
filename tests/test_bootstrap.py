import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_pyproject_tooling_smoke() -> None:
    pyproject_data = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))

    assert pyproject_data["project"]["requires-python"] == ">=3.12"
    assert "ruff" in pyproject_data["tool"]
    assert "black" in pyproject_data["tool"]
    assert "pytest" in pyproject_data["tool"]


def test_makefile_targets_smoke() -> None:
    makefile_content = (ROOT / "Makefile").read_text(encoding="utf-8")

    for target in ("test:", "lint:", "format:"):
        assert f"\n{target}" in f"\n{makefile_content}"
