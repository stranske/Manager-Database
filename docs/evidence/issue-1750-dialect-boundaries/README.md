# Manager adapter boundaries — source #1750

The [acceptance follow-up](acceptance.md) records the current focused verification,
eleven additional adapter/replay checks, and a fresh six-case mutation archive.
The full-suite and coverage comparison below describes the original bounded run;
it is not a new full-suite result for the follow-up environment.

Six new named cases cover PostgreSQL array filters, native-array universe upsert,
missing insert identities, JSONB registry updates, preservation of conflict flags,
and missing aggregate results. Production bytes are unchanged. The connection is
a recording double: these tests inspect SQL and parameter contracts, not a real
PostgreSQL server, deployed HTTP endpoint, or database concurrency.

Fresh last-500 source-touching commit ranking still selects `api/managers.py`:
19 whole-word repair-subject proxy matches, 61 touches, and 10 uncovered statements
at baseline. This is a proxy, not a verified escaped-defect count. The next ranked
module has 14 proxy matches. Raw ranking and full results are in the archive.

## Observed results

- Matched full suite: 1,875 → 1,881 PASS, unchanged 24 FAIL and 21 SKIP. Both
  commands exit 1. All 1,920 old node outcomes are unchanged; exactly six new
  nodes pass. Existing UI/auth failures remain visible and separately owned.
- Covered statements: 12,376 → 12,384 of 14,243 (86.8918065% → 86.9479744%).
  Same 116-file source universe, 82 excluded lines, and no covered-line regression.
  Manager coverage increases 816 → 824 of 826; lines 330 and 1441 remain uncovered.
- Six actual function-scoped production mutations each give RED exit 1 with
  exactly the named failing JUnit case; exact-byte restoration gives GREEN exit 0.
  The replay operates on a private archive and preserves caller source/tests.
  Reusing an output directory fails before mutation; all 37 existing proof files
  remain unchanged.
- Full Black: 399 files unchanged; focused Ruff, three replay files together under
  mypy, and `git diff --check` pass. Local interpreter is Python 3.12.2 with its
  installed environment; hosted dependency parity is not asserted.

## Reproduce

```sh
python3 -m pytest tests/test_manager_dialect_boundaries.py -q -o addopts=
python3 docs/evidence/issue-1750-dialect-boundaries/replay_dialect_boundaries.py --output /tmp/new-manager-dialect-proof
python3 -m pytest tests -q --cov --cov-report=json:coverage.json
python3 -m black --check --line-length 100 --exclude '(\.workflows-lib|node_modules)' .
python3 -m ruff check tests/test_manager_dialect_boundaries.py docs/evidence/issue-1750-dialect-boundaries/replay_dialect_boundaries.py
```

Use a fresh replay output path. Commit production edits before replaying: its
archive must match current production bytes. Current test bytes are overlaid in
the private archive. Baseline and candidate use the identical full-suite command,
selection, exclusions, and environment; full-suite PASS is not claimed.

`validation.tar.gz` retains complete console, JUnit, coverage, argv/cwd/process
receipts, ranking, production/test/replay snapshots, and twelve mutation phase
logs/JUnit/receipts. `manifest.json` binds every archive member and current input;
`comparison.json` identifies all new nodes, coverage gains, and limitations.

Broader #1750 remains OPEN below 90%. This chunk is related to #1750 and does not
close it. Matching Codex keepalive owns hosted checks and reviews. The closer owns
complete exact-head check topology, full active-thread bodies and zero-active
gate, seven-minute activity floor, guarded merge, actual `verify:compare`, and
bounded chunk disposition. No provider PASS or merge authorization is claimed.
