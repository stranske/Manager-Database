# Stats schema compatibility regression proof

Related coverage initiative #1750; this is one bounded test-only chunk after
merged #1753/#1754. Source remains open below the whole-repository 90% goal.
Baseline main: `1be8228369c5f8b45e85af41acf269f7ad88657b`. Production `api/managers.py` is
byte-identical to baseline; no runtime defect was reproduced.

## Selection

Last 500 main commits rank `api/managers.py` first: 9 repair-subject proxy hits,
16 source-touch commits and 67 uncovered statements; dashboard is next at 5/8/104.
The subject proxy counts fix/repair/regression wording; it is not a verified
escaped-incident count. Full ranking and coverage are retained. This chunk
protects previously unexecuted stats schema fallback and error classification.

## Observed behavior and limits

Four real ASGI HTTP requests cover current physical SQLite schema (legacy scalar
column absent), legacy scalar fallback for empty array/text, and an unrelated
SQL error. Assert exact aggregate JSON, case/whitespace normalization and
deduplication, modern-array precedence, sanitized 503 and exactly-once cleanup.
Three cases use physical SQLite tables; the unrelated-error case uses a connection
double to prove query retry/cleanup control. No PostgreSQL, hosted deployment or
production database trial is claimed.

## Commands and results

`/opt/anaconda3/bin/python3 -m pytest tests/test_manager_stats_schema_fallback.py -q`:
4 passed. The focused API/bulk/cache/write-outage/stats command in
`manager-focused-all.log.gz` and its JUnit report has 132 passed, zero failures/skips.

Identical baseline/candidate full command, with distinct report destinations:

```sh
/opt/anaconda3/bin/python3 -m pytest tests -q --cov --cov-report=json:<report.json> --junitxml=<report.xml>
```

| Measurement | Baseline | Candidate |
| --- | ---: | ---: |
| Passed | 1796 | 1800 |
| Failed | 24 | 24 |
| Skipped | 21 | 21 |
| Total nodes | 1841 | 1845 |
| Configured statements | 14110 | 14110 |
| Covered statements | 12227 | 12241 |
| Statement coverage | 86.65485471296952% | 86.75407512402552% |
| managers covered / statements | 754 / 821 | 768 / 821 |

Both full commands exit 1 because the same 24 existing `tests/test_ui_auth.py`
nodes fail. This is not a full-suite pass. All failing node identities, raw console,
JUnit and coverage JSON are retained; source universe, per-file statement counts,
coverage config and exclusions match. Thresholds, skips and exclusions are unchanged.
Focused Ruff and full Black (379 files) pass, as does `git diff --check`.

## Actual deliberate break and byte restoration

The retained `manager-mutation-driver.py` changes real `api/managers.py`, runs
each named pytest node with a fresh bytecode cache, restores source in finally,
compares bytes and runs the restored node. Formatting the retained driver was
verified AST-equivalent to the executed source.

| Named case | Actual source change | RED | Restored GREEN |
| --- | --- | ---: | ---: |
| current schema | reject missing jurisdiction-column fallback | 1 | 0 |
| legacy empty-array | disable scalar fallback | 1 | 0 |
| legacy empty-text | disable scalar fallback | 1 | 0 |
| unrelated error | retry non-schema database errors | 1 | 0 |

`manager-mutations.json` retains concrete nodes, mutations, exits, source/mutant
hashes and GREEN argv; the retained driver specifies the RED invocation. Temporary
bytecode-cache paths are generated per execution. RED/GREEN console logs are
separate and immutable. Per-case JUnit is the final restored run; RED evidence is
in its separate console capture, not inferred from that overwritten XML.
`manifest.json` stores compressed and decoded SHA256 values for every artifact.
Historical provider verdicts remain unchanged. Keepalive owns hosted checks/review;
closer owns unchanged-head acceptance, full topology/threads, seven-minute floor,
guarded merge and actual comparison before chunk disposition.

## Portable immutable replay recovery

The historical executed driver and its original hash manifest are preserved as
`manager-mutation-driver-original.py.gz` and `manifest-original.json.gz`.
The current driver derives this checkout from its own location, uses the active
Python interpreter and requires a new output directory:

```sh
python docs/evidence/issue-1750-stats-schema/manager-mutation-driver.py --output /tmp/manager-stats-new-proof
```

An existing output path is refused before source is read or mutated. Each RED
and GREEN has its own JUnit report, raw compressed log and exact argv receipt;
bytecode caches are temporary and cleaned after each process. Importing the
helper performs no mutation. The new replay is under `portable-replay/` and
retains every actual four-case RED1/GREEN0 result separately from the original
captures. Six isolated controls prove restoration on launch error, RED timeout,
RED exits0/2 and GREEN timeout, plus refusal of existing output before source read.
Those controls are synthetic; the four-case portable replay uses real pytest
processes and actual production mutations. Focused Black/Ruff/diff checks pass.
The original full-suite coverage comparison still describes the unchanged
runtime source and four stats tests; this recovery changes only retained proof
utility/evidence, and does not claim a new full-suite or hosted CI pass.
