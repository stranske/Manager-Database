# Manager legacy decoding boundaries

Related to #1750 after independently accepted merged #1759. The broader coverage
initiative remains open. This test-only chunk protects native database arrays and
maps, malformed JSON and unsupported shapes, legacy delimiter precedence and
empty-part filtering, and ten-column manager rows whose timestamps precede the
new quality-flags column. No production bug was found or production code changed.

Fresh last-500-commit ranking selects `api/managers.py`: nine repair-subject proxy
commits, sixteen touches and fifty-two uncovered statements. `history-500.txt.gz`
and `ranking.json.gz` preserve the input and ranking. Repair subjects are a
prioritization proxy, not a verified escaped-incident count. Base is
`9486daea8baa38032c226c65aabb0e900450c112`; production SHA256 is
`401a4591cd8965308f5ccffaf2d95144f2fd190a114e223cf3bccd77c0ff8245`.

## Validation and limits

Six new named tests pass. Each test was also executed under its own real
`api/managers.py` mutation: RED exit 1 with exactly its named JUnit failure,
byte-identical restoration, then GREEN exit 0 with exactly that named passing
case. Errors, skips and wrong-node results are rejected. The portable driver
archives the checkout into a private tree and restores mutated source in
`finally`; caller source and test hashes stay equal. Reusing an output directory
fails before source mutation.

Complete baseline and candidate use the same command and existing Python 3.12
environment, with distinct output paths:

```sh
python3 -m pytest tests -q --cov --cov-report=json:coverage.json --junitxml=junit.xml
```

Baseline has 1806 PASS, candidate 1812 PASS, with the same 24 UI-auth failures and
21 skips in both. Both full commands exit 1. Exactly six nodes were added; no old
outcome changed. Identical 114-file/14110-statement/82-exclusion source universes
yield coverage 86.7611622962438% to 86.9029057406095%, twenty newly covered manager
statements and no covered-line regressions. This is not full-suite PASS, repository
90% completion, hosted parity, PostgreSQL validation or deployment evidence.

Full-repository Black checks 384 files unchanged; focused Ruff and diff whitespace
checks pass. Complete argv, cwd, actual exits, raw console, JUnit, coverage JSON,
six mutation pairs and formatting output are retained losslessly in
`validation.tar.gz`. `comparison.json` lists exact new nodes, unchanged failure
inventory and coverage deltas. `manifest.json` binds every stored/decoded archive
member plus current source, test and driver bytes. No prior proof was rewritten.

## Replay

Use a development environment with repository test dependencies and `defusedxml`
(the recorded environment supplies 0.7.1). Run from the repository root:

```sh
python3 -m pytest tests/test_manager_legacy_decoding.py -q -o addopts=
python3 docs/evidence/issue-1750-json-decoding/replay_json_boundaries.py --output /tmp/new-manager-json-proof
```

The output directory must be new. The driver derives the checkout from its own
path and uses the same interpreter for child tests; no network or package
installation occurs during replay. The archive uses the current Git snapshot and
overlays only the current test file. Replays after source changes require a new
source identity and acceptance review; retained receipts are revision-bound.

Matching keepalive owns hosted checks and review after PR birth. Reviewed Repo
Merge Verify Closer owns complete expected topology, unchanged head, full active
review-thread bodies, seven-minute review floor, guarded merge and actual
`verify:compare` with bounded chunk disposition. Original provider NON_PASS and
the separate existing UI-auth failures remain visible.

CI correction: the new driver uses a unique module name to avoid a collision with the older evidence replay. Exact mypy 2.4.0 checking now passes that discovery boundary. It retains one identical pre-existing Playwright Page protocol error on the untouched base and candidate; this is not a full typecheck PASS. Raw hosted failure and paired local receipts are in `typecheck-correction.tar.gz`, bound by `typecheck-correction.json`. The original validation archive remains unchanged.
