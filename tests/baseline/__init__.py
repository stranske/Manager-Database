"""Manager-Database app behavior baseline kit (api-snapshot modality).

Built on the shared ``baseline_kit`` package -- this directory contains only the
app-specific pieces (adapter, catalog, invariant bounds, seed fixture). The
generic harness (snapshot glue, directional engine, invariant assertion,
coverage manifest) is imported from ``baseline_kit``, the same core the
Counter_Risk / TMP / trip-planner kits use.

**This is the fleet's reference kit for the api-snapshot modality.** The rest of
the fleet baselines a *deterministic compute* (a pure function reduced to a flat
metrics dict, golden-mastered with ``check_metrics``). Manager-Database is a
FastAPI service, so its target surface is its HTTP layer: the kit exercises the
``/managers`` endpoints through a Starlette ``TestClient`` against a freshly
seeded, deterministic in-process sqlite database, and snapshots the JSON
responses with ``baseline_kit.check_snapshot`` (YAML via pytest-regressions'
``data_regression`` -- no numpy/pandas).

Target surface (``api/managers.py`` routes, mounted on a minimal FastAPI app):

  * ``GET /managers``        -- paginated list (``limit``/``offset``) with
                                ``jurisdiction``/``tag`` filters.
  * ``GET /managers/{id}``   -- single manager by id (404 when absent).
  * ``GET /managers/stats``  -- universe aggregates (counts, by-jurisdiction,
                                by-tag, with_cik, with_lei).

The adapter returns BOTH the JSON payload (for snapshotting) and a flat
``dict[str, float | int]`` of reduced metrics (for invariants and directional
checks), so the same scenario drives all three layers.
"""
