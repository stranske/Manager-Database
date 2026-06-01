# app-baseline-kit dependency

Manager-Database uses **Pattern A** (the fleet default; see
`stranske/Workflows/docs/guides/BASELINE_KIT_DEPENDENCY.md`).

## What

`app-baseline-kit` (the `baseline_kit` import) is declared **unpinned `@main`**
in `pyproject.toml` `[project.optional-dependencies].dev`:

```toml
"app-baseline-kit @ git+https://github.com/stranske/Workflows.git#subdirectory=packages/app-baseline-kit"
```

and is **excluded from `requirements.lock`** via:

```toml
[tool.uv.pip]
no-emit-package = ["app-baseline-kit"]
```

`pytest-regressions>=2.8` (a transitive dependency of `app-baseline-kit`, used by
the `data_regression` fixture the api-snapshot golden layer drives) is declared
explicitly in `[dev]` so it is captured in the lock and the lock-presence test
passes.

## Why Pattern A (not B or vendoring)

- **Build backend.** This repo uses the plain `setuptools.build_meta` backend,
  which serializes a PEP 508 `name @ git+url` direct reference in extras metadata
  cleanly. That rules out Pattern B (TPP's lock-only workaround exists only
  because its custom `tp_build_backend` cannot serialize such metadata).
- **CI install path.** CI calls the reusable
  `stranske/Workflows/.github/workflows/reusable-10-ci-python.yml@main`, which
  installs **both** `-r requirements.lock` **and** `-e ".[app,dev]"` in a single
  `uv pip install`. The editable `[dev]` install resolves the `@main` URL, so
  `baseline_kit` is installed at test time. Because the lock and the pyproject
  URL are passed to the *same* `uv` invocation, the lock must **not** carry a
  SHA-pinned `app-baseline-kit` URL — otherwise uv aborts on a cold-cache resolve
  with *"Requirements contain conflicting URLs for package app-baseline-kit."*
  `no-emit-package` keeps the package out of the lock, so the only URL uv sees is
  the unpinned `@main` one. (This is the exact failure class Pension-Data hit and
  fixed by adding `no-emit-package`.)
- **Not vendoring.** The consumed surface is non-trivial (snapshot normalization,
  directional engine, invariants, coverage manifest) and we want to track
  Workflows `main` automatically while the package is unversioned, so vendoring
  (Inv-Man-Intake's approach) is not warranted.

## Knock-on changes

- `tests/test_dependency_version_alignment.py` now subtracts the
  `[tool.uv.pip].no-emit-package` names from the expected-in-lock set and treats
  `name @ git+...` direct references as present-without-a-pinned-version (mirrors
  Counter_Risk).
- `.project_modules.txt` declares `baseline_kit` (the shipped import) and
  `conftest` (the relative import the test-dependency scanner sees) as
  first-party so `scripts/sync_test_dependencies.py --verify` does not flag them.

## Tradeoff

The exact baseline-kit commit used in CI is not recorded in the lock;
reproducibility of *that one package* depends on Workflows `main` HEAD at install
time. Acceptable while the package is unversioned; revisit once it is
tagged/released.
