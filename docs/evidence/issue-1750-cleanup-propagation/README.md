# Committed bulk-import cleanup error — source #1750

One new HTTP regression covers the remaining reachable manager cleanup-error line.
After a successful commit and cache invalidation, restoring PostgreSQL autocommit
raises: the endpoint must return 503 with `Database unavailable`, log the same
exception and still close the connection. A recording connection and in-process
ASGI request exercise production routing; this is not a live PostgreSQL claim.
Production code is unchanged.

Current last-500-commit ranking selects `api/managers.py`: 8 whole-word repair-subject
proxy matches, 17 touches and 2 uncovered statements. This proxy is not an escaped
incident count. Line1441 is the selected reachable error path. Line330 is behind
`_ensure_manager_table`, which already adds that column; bypassing that helper would
manufacture a normal-path precondition. It remains honestly uncovered.

## Observed results

- Complete matched suite: 1956→1957 PASS, identical24UI-auth FAIL and21SKIP. Both
  commands exit1. All2001 old node outcomes are unchanged; one new node passes.
- Covered statements12446→12447 of14309:86.98022224%→86.98721085%. Identical117-file
  source universe and82excluded lines; no covered-line regression. Manager824→825
  of826 statements. The broader initiative remains OPEN below90%.
- Actual function-scoped production mutation removes only error propagation after
  autocommit restoration: the named case fails with HTTP200 instead of503 (REDexit1).
  Byte-identical source restoration passes (GREENexit0). Standard and `python -O`
  replay both enforce named JUnit failures, zero errors/skips and exact source hashes.
- Reusing the standard proof directory fails before mutation and preserves all7
  existing files. Replay uses a private git archive and preserves caller source/test.
- Full repository Black, focused Ruff and `git diff --check` pass. Actual interpreter
  Python3.12.2; hosted dependency parity is not asserted. No deployment/provider PASS.

## Reproduce

```sh
/opt/anaconda3/bin/python3 -m pytest tests/test_manager_bulk_cleanup_failure.py -q -o addopts=
/opt/anaconda3/bin/python3 docs/evidence/issue-1750-cleanup-propagation/replay_cleanup.py --output /tmp/new-cleanup-proof
/opt/anaconda3/bin/python3 -O docs/evidence/issue-1750-cleanup-propagation/replay_cleanup.py --output /tmp/new-cleanup-proof-optimized
/opt/anaconda3/bin/python3 -m pytest tests -q --cov --cov-report=json:coverage.json
```

Use fresh replay output paths. `validation.tar.gz` retains complete baseline/candidate
console, coverageJSON, JUnit and exact commands/exits, both final mutation phase
receipts, reuse refusal, source/test/driver snapshots and ranking. `manifest.json`
binds every archive member and current input. Historical JSON/dialect chunk receipts
and provider CONCERNS are preserved. Matching keepalive owns hosted CI/review; closer
owns complete exact-head topology, all review threads, the seven-minute floor,
guarded merge and actual compare. This PR is related to1750 and does not close it.
