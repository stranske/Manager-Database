"""Coverage manifest -- which request knobs the scenarios exercise; emit a report.

Uses the generic ``baseline_kit.CoverageManifest``. For the api-snapshot
modality the "input parameter" space is the request surface of the /managers
endpoints: the chosen ``endpoint`` plus the query/path knobs (``limit``,
``offset``, ``jurisdiction``, ``tag``, ``manager_id``). A parameter is "touched"
when at least one scenario sets it (or, for ``endpoint``, exercises that value).
"""

from __future__ import annotations

import os
from pathlib import Path

from baseline_kit import CoverageManifest, load_catalog

from .conftest import CATALOG_PATH, REPO_ROOT

REPORT_PATH = REPO_ROOT / "docs" / "reports" / "baseline-coverage.md"

# The full request-parameter space the /managers surface accepts.
_ALL_PARAMS = {"endpoint", "limit", "offset", "jurisdiction", "tag", "manager_id"}


def _touched_params(catalog) -> set[str]:
    touched: set[str] = set()
    for scenario in catalog["scenarios"]:
        touched.add("endpoint")  # every scenario picks an endpoint
        for key in scenario.get("params") or {}:
            touched.add(key)
        if "manager_id" in scenario:
            touched.add("manager_id")
    return touched


def _build_manifest() -> CoverageManifest:
    catalog = load_catalog(CATALOG_PATH)
    return CoverageManifest(
        all_keys=set(_ALL_PARAMS),
        touched_keys=_touched_params(catalog),
        priority_params=list(catalog.get("priority_params", [])),
        title="Manager-Database baseline coverage manifest (api-snapshot)",
    )


def test_no_unknown_catalog_params():
    m = _build_manifest()
    assert not m.unknown_catalog_keys, (
        "Scenarios reference request params outside the documented surface: "
        f"{sorted(m.unknown_catalog_keys)}"
    )


def test_priority_params_covered():
    m = _build_manifest()
    assert not m.priority_gaps, "Priority params with no scenario: " + ", ".join(m.priority_gaps)


def test_emit_coverage_report(tmp_path: Path):
    m = _build_manifest()
    report_path = (
        REPORT_PATH
        if os.environ.get("BASELINE_REFRESH_REPORT") == "1"
        else tmp_path / "baseline-coverage.md"
    )
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(m.to_markdown())
    assert report_path.exists()
