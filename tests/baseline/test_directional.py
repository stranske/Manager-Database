"""Directional ("metamorphic") checks on reduced response metrics.

Each catalog ``directionals`` entry asserts a relationship the endpoint contract
guarantees, e.g. a filter narrows the count; pagination caps the page but
preserves the total; a richer seed raises the count / stats total. The metric is
pulled from the adapter's flat reduction of each scenario's response.
"""

from __future__ import annotations

import pytest
from baseline_kit import evaluate_direction

from . import adapter
from .conftest import scenarios_by_id, seed_rows_for

_CATALOG_SCENARIOS = scenarios_by_id()


def _metric(scenario_id: str, key: str, db_path) -> float:
    scenario = _CATALOG_SCENARIOS[scenario_id]
    _payload, metrics = adapter.run_scenario(scenario, seed_rows_for(scenario["seed"]), db_path)
    return float(metrics[key])


def _load_directionals():
    from baseline_kit import load_catalog

    from .conftest import CATALOG_PATH

    return load_catalog(CATALOG_PATH).get("directionals", [])


_DIRECTIONALS = _load_directionals()


@pytest.mark.parametrize("scen", _DIRECTIONALS, ids=[s["id"] for s in _DIRECTIONALS])
def test_directional(scen, record_property, db_path):
    metric = scen["metric"]
    variant = _metric(scen["scenario"], metric, db_path)
    control = _metric(scen["control"], metric, db_path)
    holds = evaluate_direction(scen["direction"], variant, control)
    msg = (
        f"{scen['id']}: {metric} scenario={variant:.6g} "
        f"{scen['direction']} control={control:.6g} -> {holds}"
    )
    record_property("directional", msg)
    if scen.get("enforce"):
        assert holds, "Contract-violating direction -- " + msg
    elif not holds:
        pytest.skip("[report-only] " + msg)
