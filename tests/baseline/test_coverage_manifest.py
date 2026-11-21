"""Coverage manifest -- which request knobs the scenarios exercise and consume; emit a report.

Uses the generic ``baseline_kit.CoverageManifest``. For the api-snapshot
modality the "input parameter" space is the request surface of the /managers
endpoints: the chosen ``endpoint`` plus the query/path knobs (``limit``,
``offset``, ``jurisdiction``, ``tag``, ``search``, ``name``, ``manager_id``).
A parameter is "touched" when at least one scenario sets it in the catalog and
"read" only when the route handler or filter builder actually consumes it at
runtime.
"""

from __future__ import annotations

import inspect
import os
import tempfile
from pathlib import Path

import pytest
from baseline_kit import CoverageManifest, load_catalog

from . import adapter
from .conftest import CATALOG_PATH, REPO_ROOT, seed_rows_for

REPORT_PATH = REPO_ROOT / "docs" / "reports" / "baseline-coverage.md"

# The full request-parameter space the /managers surface accepts.
_ALL_PARAMS = {
    "endpoint",
    "limit",
    "offset",
    "jurisdiction",
    "tag",
    "search",
    "name",
    "manager_id",
}


def _touched_params(catalog) -> set[str]:
    touched: set[str] = set()
    for scenario in catalog["scenarios"]:
        touched.add("endpoint")  # every scenario picks an endpoint
        for key in scenario.get("params") or {}:
            touched.add(key)
        if "manager_id" in scenario:
            touched.add("manager_id")
    return touched


def _observed_read_params(catalog, db_path: Path) -> set[str]:
    observed: set[str] = set()
    for scenario in catalog["scenarios"]:
        _payload, _metrics, read_keys = adapter.run_scenario_observed(
            scenario,
            seed_rows_for(scenario["seed"]),
            db_path,
        )
        observed.update(read_keys)
    return observed


def _build_manifest(db_path: Path | None = None) -> CoverageManifest:
    catalog = load_catalog(CATALOG_PATH)
    if db_path is None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            read_keys = _observed_read_params(catalog, Path(tmp_dir) / "manifest.db")
    else:
        read_keys = _observed_read_params(catalog, db_path)
    return CoverageManifest(
        all_keys=set(_ALL_PARAMS),
        touched_keys=_touched_params(catalog),
        priority_params=list(catalog.get("priority_params", [])),
        read_keys=read_keys,
        title="Manager-Database baseline coverage manifest (api-snapshot)",
    )


def test_no_unknown_catalog_params(tmp_path: Path):
    from api import managers as managers_module

    sig_params = {
        name
        for name in inspect.signature(managers_module.list_managers).parameters
        if name != "request"
    }
    missing = sorted(sig_params - _ALL_PARAMS)
    assert sig_params <= _ALL_PARAMS, f"list_managers query parameters missing: {missing}"
    m = _build_manifest(tmp_path / "manifest.db")
    assert not m.unknown_catalog_keys, (
        "Scenarios reference request params outside the documented surface: "
        f"{sorted(m.unknown_catalog_keys)}"
    )


def test_priority_params_covered(tmp_path: Path):
    m = _build_manifest(tmp_path / "manifest.db")
    assert not m.priority_gaps, "Priority params with no scenario: " + ", ".join(m.priority_gaps)
    unread_priority = [p for p in m.priority_params if p not in m.read_keys]
    assert not unread_priority, (
        "Priority params not observed read at runtime: " + ", ".join(unread_priority)
    )


def test_emit_coverage_report(tmp_path: Path):
    m = _build_manifest(tmp_path / "manifest.db")
    report_path = (
        REPORT_PATH
        if os.environ.get("BASELINE_REFRESH_REPORT") == "1"
        else tmp_path / "baseline-coverage.md"
    )
    report_path.parent.mkdir(parents=True, exist_ok=True)
    rendered = m.to_markdown()
    report_path.write_text(rendered)
    assert report_path.exists()
    if REPORT_PATH.exists():
        assert REPORT_PATH.read_text() == rendered


def test_manager_search_baseline_records_observed_filter_reads(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    from api import managers as managers_module
    from api.cache import reset_cache_backend

    probe_db = tmp_path / "probe.db"
    adapter.seed_database(probe_db, seed_rows_for("richer"))

    with adapter._seeded_client(probe_db) as client:
        with adapter.observe_route_reads() as search_reads:
            search_resp = client.get("/managers", params={"search": "management"})
        assert search_resp.status_code == 200
        search_body = search_resp.json()
        assert search_body["total"] == 3
        assert [item["manager_id"] for item in search_body["items"]] == [1, 3, 4]

        with adapter.observe_route_reads() as alias_reads:
            alias_resp = client.get("/managers", params={"search": "rentech"})
        assert alias_resp.status_code == 200
        alias_body = alias_resp.json()
        assert alias_body["total"] == 1
        assert [item["name"] for item in alias_body["items"]] == ["Renaissance Technologies"]

        with adapter.observe_route_reads() as name_reads:
            name_resp = client.get("/managers", params={"name": "t"})
        assert name_resp.status_code == 200
        name_body = name_resp.json()
        assert name_body["total"] == 2
        assert [item["manager_id"] for item in name_body["items"]] == [3, 7]

        # Exercise both filters together via the actual /managers route plus an unused control key.
        # Strict narrowing ([1, 3, 4] & [3, 7] -> [3]) demonstrates both filters applied.
        with adapter.observe_route_reads() as combined_reads:
            combined_resp = client.get(
                "/managers",
                params={"search": "management", "name": "t", "unused_control": "ignored"},
            )
        assert combined_resp.status_code == 200
        combined_body = combined_resp.json()
        assert combined_body["total"] == 1
        assert [item["manager_id"] for item in combined_body["items"]] == [3]
        assert [item["name"] for item in combined_body["items"]] == ["TCI Fund Management"]
        assert "unused_control" not in combined_reads
        assert search_reads == {"endpoint", "search"}
        assert alias_reads == {"endpoint", "search"}
        assert name_reads == {"endpoint", "name"}
        assert combined_reads == {"endpoint", "search", "name"}

        # Unused / whitespace-only inputs must register no filter read.
        with adapter.observe_route_reads() as blank_reads:
            blank_resp = client.get(
                "/managers",
                params={"search": "   ", "unused_control": "ignored"},
            )
        assert blank_resp.status_code == 200
        assert blank_resp.json()["total"] == 8
        assert blank_reads == {"endpoint"}

        # Bypassed-key control: when filter construction bypasses ``name``, even a declared
        # query parameter present on the URL and in a scenario catalog must not register as read.
        real_filter_clauses = managers_module._manager_filter_clauses

        def _bypass_name_filter(conn, jurisdiction, tag, search, _name):
            return real_filter_clauses(conn, jurisdiction, tag, search, None)

        monkeypatch.setattr(managers_module, "_manager_filter_clauses", _bypass_name_filter)
        reset_cache_backend()
        with adapter.observe_route_reads() as bypassed_reads:
            bypassed_resp = client.get(
                "/managers",
                params={"search": "management", "name": "t", "unused_control": "ignored"},
            )
        assert bypassed_resp.status_code == 200
        assert [item["manager_id"] for item in bypassed_resp.json()["items"]] == [1, 3, 4]
        assert bypassed_reads == {"endpoint", "search"}
        assert "name" not in bypassed_reads
        assert "unused_control" not in bypassed_reads

        bypassed_catalog = {
            "scenarios": [
                {
                    "id": "bypassed_probe",
                    "endpoint": "list",
                    "seed": "richer",
                    "params": {
                        "search": "management",
                        "name": "t",
                        "bypassed_control": "ignored",
                    },
                }
            ],
            "priority_params": ["search", "name"],
        }
        bypassed_manifest = CoverageManifest(
            all_keys=set(_ALL_PARAMS),
            touched_keys=_touched_params(bypassed_catalog),
            priority_params=bypassed_catalog["priority_params"],
            read_keys=_observed_read_params(bypassed_catalog, tmp_path / "bypassed.db"),
        )
        assert {"search", "name", "bypassed_control"} <= bypassed_manifest.touched_keys
        assert bypassed_manifest.read_keys == {"endpoint", "search"}
        assert "name" not in bypassed_manifest.read_keys
        assert "bypassed_control" not in bypassed_manifest.read_keys
        monkeypatch.undo()
        reset_cache_backend()

        # Compatibility control across existing /managers list filters, detail, and stats
        # without an active read observer.
        compat_list = client.get("/managers", params={"jurisdiction": "us", "tag": "activist"})
        assert compat_list.status_code == 200
        assert compat_list.json()["total"] == 2
        assert [item["manager_id"] for item in compat_list.json()["items"]] == [1, 4]

        compat_detail = client.get("/managers/4")
        assert compat_detail.status_code == 200
        assert compat_detail.json()["manager_id"] == 4
        assert compat_detail.json()["name"] == "Pershing Square Capital Management"

        compat_stats = client.get("/managers/stats")
        assert compat_stats.status_code == 200
        assert compat_stats.json()["total_managers"] == 8

    manifest = _build_manifest(tmp_path / "manifest.db")
    assert manifest.all_keys == _ALL_PARAMS
    assert manifest.touched_keys == _ALL_PARAMS
    assert manifest.read_keys == _ALL_PARAMS
    assert not manifest.priority_gaps
    assert not manifest.unknown_catalog_keys
