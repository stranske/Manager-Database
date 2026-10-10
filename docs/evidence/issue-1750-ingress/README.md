# Manager ingress validation coverage

Related to #1750. This is a bounded chunk of the continuing coverage initiative;
#1750 stays open below 90%. No production code is changed.

Fresh last-500-commit ranking selects `api/managers.py`: nine repair-subject
proxy commits, 16 touches, 25 uncovered statements. This is a history proxy,
not a count of escaped defects. `selection.json` retains the ranking.

Four named tests protect invalid bulk-size configuration with warning/default,
empty CSV header diagnosis, invalid patch CIK rejection before database access,
and malformed Content-Length with actual-body size enforcement. Request tests
invoke the real async handlers using a Starlette Request and a forbidden-storage
sentinel; they do not claim a deployed HTTP server or real PostgreSQL execution.

## Reproduce

```bash
python3 -m pytest tests/test_manager_ingress_boundaries.py -q -o addopts=
python3 docs/evidence/issue-1750-ingress/replay_ingress_boundaries.py --output /tmp/new-manager-ingress-proof
python3 -m pytest tests -q --cov --cov-report=json:coverage.json --junitxml=results.xml
python3 -m black --check .
python3 -m ruff check tests/test_manager_ingress_boundaries.py docs/evidence/issue-1750-ingress/replay_ingress_boundaries.py
git diff --check
```

The replay requires the bound production bytes and a new output directory. Each
of four real production mutations causes exactly one intended named assertion
failure (exit 1); byte-identical restoration passes that case (exit 0). Caller
hashes remain unchanged. The runner restores production in `finally`; timeout,
malformed JUnit, unexpected failures or skipped nodes are incomplete proof.
Run in a dedicated worktree: concurrent writers and uncatchable process death
are outside that cleanup guarantee.

## Complete matched-suite measurement

Baseline: 1,827 passed, 24 failed, 21 skipped. Candidate: 1,831 passed, the same
24 failed, 21 skipped. Every old node keeps its outcome. Both commands exit 1;
the existing UI-auth failures remain visible in `comparison.json`. This is not
a full-suite PASS.

Coverage: 12,356 -> 12,362 covered lines; 86.7818513836213% ->
86.82399213372665%, with identical 116 files, 14,238 statements and 82 excluded
lines. Six new lines are in `api/managers.py`; no covered line regresses. The
invalid-CIK case independently detects a validation bypass but adds no statement
coverage in this suite. No threshold, exclusion or production bytes changed.
Production SHA256: `401a4591cd8965308f5ccffaf2d95144f2fd190a114e223cf3bccd77c0ff8245`.

Full Black checked 390 files; focused Ruff and whitespace checks pass. Local
Python is Anaconda 3.12.2, pytest 9.1.1, coverage 7.16.0, Black 26.5.1 and Ruff
0.16.7. Coverage/Black/Ruff differ from repository pins (7.16.2/26.10.0/0.16.10);
no hosted-environment parity or hosted CI PASS is claimed.

`validation.tar.gz` retains lossless full consoles, JUnit, coverage JSON,
exact argv/cwd/exits, interpreter versions, original source/callers and all eight
mutation phases. `archive-index.json` binds every archive member and current
production/test/replay bytes. `comparison.json` lists all existing failures.
Keepalive owns fresh hosted checks/reviews; closer owns unchanged-head complete
check topology, full active threads/zero-active gate, seven-minute floor, guarded
merge, actual verify:compare and bounded chunk disposition. No deployment,
provider PASS, or repository-wide 90% completion claim is made.
