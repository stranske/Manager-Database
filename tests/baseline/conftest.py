"""Fixtures and catalog loading for the Manager-Database baseline kit.

Provides:
  * the loaded scenario ``catalog`` (via ``baseline_kit.load_catalog``),
  * ``seed_rows_for(seed_name)`` -- a fresh copy of a committed seed variant,
  * ``db_path`` -- a per-test fresh sqlite file path (the seed target),

so each scenario runs against a freshly-seeded, deterministic in-process DB.
"""

from __future__ import annotations

import copy
import functools
import sys
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parents[1]
CATALOG_PATH = HERE / "catalog.yaml"

# Ensure the repo root is importable (mirrors pyproject `pythonpath = ["."]`),
# so `api.managers` / `adapters.base` resolve under the baseline venv too.
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


@functools.lru_cache(maxsize=1)
def _load_catalog_cached():
    from baseline_kit import load_catalog

    return load_catalog(CATALOG_PATH)


def seed_rows_for(seed_name: str) -> list[dict]:
    """Return a deep copy of a named seed variant from the catalog."""
    seeds = _load_catalog_cached()["seeds"]
    if seed_name not in seeds:
        raise KeyError(f"unknown seed variant {seed_name!r}; known: {sorted(seeds)}")
    return copy.deepcopy(seeds[seed_name])


def scenarios() -> list[dict]:
    return list(_load_catalog_cached()["scenarios"])


def scenarios_by_id() -> dict[str, dict]:
    return {s["id"]: s for s in scenarios()}


@pytest.fixture(scope="session")
def catalog():
    return _load_catalog_cached()


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    """A per-test fresh sqlite file path used as the seed target."""
    return tmp_path / "baseline.db"
