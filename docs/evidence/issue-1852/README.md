# Issue 1852 verification

Observed on October 26, 2026. PR #1856 and issue #1852 were already merged/closed
when this verification ran. No PR state or workflow file was changed.

- [x] Observe the original PR's final-head hosted Python 3.13/3.14 jobs and coverage.
- [x] Execute the named guard with a 3.12 mutation and byte-identical restoration
  in isolated copies of each real caller, using this runner's working Python 3.14.8.
- [ ] Read the live caller files on November 7, 2026, and confirm zero lines match
  `3.12`, as required by the original issue. Today's check cannot satisfy this
  future persistence acceptance.

## Hosted evidence

[Gate run 40250298739](https://github.com/stranske/Manager-Database/actions/runs/40250298739)
passed for final PR head `fcada2e4dc4625fa71479f056b4aefe70f807cee`.
The job logs identify the PR merge checkout `a3cf567`, incorporating that exact
head into base `eed368f5635526f14325314a8abc373ed296b3ae`.

| Job | Result | Tests | Combined statement/branch coverage |
| --- | --- | --- | --- |
| Python 3.13.16 | success | 2,537 passed, 26 skipped | 90.16% |
| Python 3.14.8 | success | 2,537 passed, 26 skipped | 90.14% |

Both pytest coverage checks exceeded 75%; both separate coverage-minimum steps
also passed (92.68% and 92.67% statement coverage respectively). Job IDs, relevant
step conclusions, checkout evidence, and log excerpts are in [hosted-gate.json](hosted-gate.json).
These results apply to the original PR head, not the subsequent local regression
test changes in this commit.

## Local execution evidence

This runner selects `/opt/hostedtoolcache/Python/3.14.8/x64/bin/python` and has no
repository `.venv`. The prior machine's stalled canonical environment was not
used or repaired. The current runtime executed the repository test suite's
runtime-guard module successfully:

```text
python -m pytest tests/test_ci_python_standard.py -m 'not slow' \
  -o junit_family=xunit1 --junitxml=docs/evidence/issue-1852/runtime-guard.xml
24 passed
```

[runtime-guard.xml](runtime-guard.xml) records each caller's original, mutated,
and restored SHA-256 hashes and the actual assertion message. The updated test
changes only the JSON runtime pin, verifies its exact failure location, restores
the original bytes in `finally`, and reruns the named guard after restoration.

Independent pytest invocations also exercised byte-for-byte copies of the
updated test module and both real callers. A temporary conftest selected the
copied caller directory. Each mutation changed only `["3.13", "3.14"]` to
`["3.12", "3.14"]`; no repository workflow was edited. The receipt binds the test
source hash, selected interpreter, commands, exit codes, and caller hashes:
[deliberate-break.json](deliberate-break.json).

| Stage | Pytest exit code | Raw output |
| --- | --- | --- |
| Baseline named guard | 0 | [baseline.log](baseline.log) |
| CI caller 3.12 mutation | 1 | [ci-break.log](ci-break.log) |
| CI caller exact restoration | 0 | [ci-restored.log](ci-restored.log) |
| Gate caller 3.12 mutation | 1 | [pr-00-gate-break.log](pr-00-gate-break.log) |
| Gate caller exact restoration | 0 | [pr-00-gate-restored.log](pr-00-gate-restored.log) |

The existing zero-3.12-lines regression passes today for both callers. Retain the
November 7 live read as a separate outstanding requirement; merging the original
PR does not establish persistence on that date.

## Formatting and lint

Pinned Black 26.10.1 left the changed test unchanged, and Ruff passed. The initial
full Black invocation stalled in the sandbox's concurrency worker. Checking all
445 tracked Python files serially through Black's CLI populated its normal cache
under `/tmp`; the required repository-wide check then exited successfully with
`445 files would be left unchanged`:

```text
BLACK_CACHE_DIR=/tmp/issue-1852-black-cache \
  black --check --line-length 100 --exclude '(\.workflows-lib|node_modules)' .
```

The successful full check is retained in [format-check.log](format-check.log).
`git diff --check` also passed. No formatter rules or security settings changed.
