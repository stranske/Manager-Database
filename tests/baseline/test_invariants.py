"""Structural + economic invariants on every catalog scenario's response."""

from __future__ import annotations

import pytest
from baseline_kit import assert_invariants

from . import invariants
from .conftest import scenarios

_SCENARIOS = scenarios()


@pytest.mark.parametrize("scenario", _SCENARIOS, ids=[s["id"] for s in _SCENARIOS])
def test_scenario_invariants(scenario, db_path):
    assert_invariants(
        invariants.check_scenario(scenario, db_path),
        context=scenario["id"],
    )
