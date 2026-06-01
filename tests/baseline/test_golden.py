"""Golden snapshots of each scenario's JSON response (api-snapshot modality).

Each scenario's normalized ``{"status_code", "json"}`` payload is snapshotted as
YAML via ``baseline_kit.check_snapshot`` (pytest-regressions' ``data_regression``
fixture -- no numpy/pandas). Volatile fields are redacted with the ``exclude``
path syntax so the snapshots are stable run-to-run:

  * ``created_at`` / ``updated_at`` -- bare-key redaction drops these timestamps
    wherever they appear (top-level detail body or nested list items).

The seeded ids are deliberately NOT redacted: the seed pins explicit contiguous
ids, so they are deterministic and part of what we want to pin (ordering,
pagination boundaries). The DB is freshly recreated per scenario, so ids never
drift.

Re-bless an intended change with ``--force-regen``, then INSPECT the updated
YAML under ``test_golden/`` before committing.
"""

from __future__ import annotations

import pytest
from baseline_kit import check_snapshot

from . import adapter
from .conftest import scenarios, seed_rows_for

_SCENARIOS = scenarios()

# Volatile fields redacted from every snapshot (bare-key => any depth).
_EXCLUDE = ("created_at", "updated_at")


@pytest.mark.parametrize("scenario", _SCENARIOS, ids=[s["id"] for s in _SCENARIOS])
def test_response_snapshot(scenario, data_regression, db_path):
    seed_rows = seed_rows_for(scenario["seed"])
    payload, _metrics = adapter.run_scenario(scenario, seed_rows, db_path)
    check_snapshot(data_regression, payload, exclude=_EXCLUDE, basename=scenario["id"])
