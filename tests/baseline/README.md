# Manager-Database app behavior baseline kit (api-snapshot modality)

Scenario-driven wiring / sensibility / regression tests built on the shared
**`baseline_kit`** package. Only the app-specific pieces live here.

> **This is the fleet's reference kit for the _api-snapshot_ modality.** The
> rest of the fleet (Counter_Risk, TMP, PAEM, trip-planner, …) baselines a
> *deterministic compute*: a pure function reduced to a flat metrics dict,
> golden-mastered with `check_metrics` (numpy/pandas via `num_regression`).
> Manager-Database is a **FastAPI service**, so its target surface is its HTTP
> layer. The kit drives the `/managers` endpoints through a Starlette
> `TestClient` against a **freshly seeded, deterministic sqlite DB** and
> snapshots the JSON responses with `baseline_kit.check_snapshot`
> (pytest-regressions' `data_regression`, YAML — **no numpy/pandas**).

## Requires

`baseline_kit` (the shared core, **v0.2.0+** — needs the `snapshot` helper) must
be importable. It lives in `stranske/Workflows` under
`packages/app-baseline-kit` and is declared in this repo's `pyproject.toml`
`[project.optional-dependencies].dev` (Pattern A, unpinned `@main`):

```bash
pip install -e ".[app,dev]"   # how CI installs it (resolves the @main URL)
```

`pytest-regressions` is a transitive dependency of `app-baseline-kit`; it is also
declared explicitly in `[dev]` so the lock-presence test stays satisfied. See
[`docs/baseline-kit-dependency.md`](../../docs/baseline-kit-dependency.md) for
the dependency-pattern justification.

## Target surface

The FastAPI `/managers` endpoints in `api/managers.py`, mounted on a **minimal**
app (managers router only — the kit does not import `api.chat.app`, avoiding its
boto3 / langsmith / prometheus import chain):

| Endpoint | What it returns |
|---|---|
| `GET /managers` | paginated list (`limit`/`offset`) + `jurisdiction`/`tag` filters; `{items, total, limit, offset}` envelope |
| `GET /managers/{id}` | a single manager (404 when absent) |
| `GET /managers/stats` | universe aggregates (`total_managers`, `by_jurisdiction`, `by_tag`, `with_cik`, `with_lei`) |

## How it stays deterministic

`adapters.base.connect_db` reads `DB_PATH` from the environment per request and
opens a plain `sqlite3` connection. The adapter seeds a **fresh** sqlite file
(per-test `tmp_path`) from a committed, fixed seed variant, points `DB_PATH` at
it, resets the in-process query cache, and calls the endpoint via `TestClient`.
Same seed + same request ⇒ byte-identical JSON. The two volatile fields
(`created_at` / `updated_at`) are both pinned in the seed *and* redacted by the
golden layer, so they can never wobble.

## Layout

```
adapter.py                # seed sqlite + TestClient -> (payload, flat metrics)  [the only app glue]
catalog.yaml              # seed variants + endpoint/param scenarios + directional checks
invariants.py             # structural + economic invariants -> baseline_kit.InvariantResult
test_golden.py            # JSON-response snapshots via check_snapshot (redacts timestamps)
test_directional.py       # metamorphic checks (filter narrows count, richer seed raises total...)
test_invariants.py        # invariants on every scenario
test_coverage_manifest.py # request-param coverage -> docs/reports/baseline-coverage.md
test_golden/              # blessed YAML snapshots (one per scenario)
```

## Scenario model

A *scenario* names an `endpoint` (`list` | `detail` | `stats`), a `seed` variant
(`base` | `richer`), and the request knobs (`params` for list filters/paging, or
`manager_id` for detail). Each scenario produces **both** a JSON payload (for
snapshotting) and a flat `dict[str, float | int]` of reduced metrics (`count`,
`total`, `total_managers`, `with_cik`, …) for invariants and directional checks.

## Running

```bash
PYTHONHASHSEED=0 pytest tests/baseline/                          # full suite
pytest tests/baseline/test_golden.py --force-regen               # re-bless after an intended change
BASELINE_REFRESH_REPORT=1 pytest tests/baseline/test_coverage_manifest.py  # refresh report
```

After a `--force-regen`, **inspect** the updated YAML under `test_golden/`
before committing.

## Invariants enforced

- **list:** `status==200`; `{items,total,limit,offset}` envelope; `items` is a
  list; `count <= limit` and `count <= total`; ids are integers, unique, and
  **sorted ascending** (`ORDER BY id` ⇒ stable ordering); names non-empty; the
  per-scenario seeded `count`/`total` expectation holds.
- **detail:** found ⇒ `status==200`, echoes the requested id, has a name and the
  required fields; miss ⇒ `status==404`, `{"detail": …}`, no `manager_id`.
- **stats:** `status==200`; `total_managers` equals the seeded row count and the
  scenario expectation; `with_cik`/`with_lei` non-negative and `<= total`; every
  breakdown key lowercased; each breakdown count in `[1, total]`.

## Directional checks

- a `jurisdiction` / `tag` filter **narrows** the count vs the unfiltered list;
- combining two filters narrows **at least as much** as one;
- pagination **caps** the page size below the full count but leaves `total`
  **unchanged**;
- a **richer** seed (more managers) **raises** the list count, the stats
  `total_managers`, and (weakly) `with_cik`.
