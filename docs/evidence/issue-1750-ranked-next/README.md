# Manager similarity HTTP boundary coverage

Current base: `1fc1474db6f2a67b229e97fdadbb089228d057e9`; related to broad issue #1750. No production diff or independently reproduced application bug.

Current 500-commit ranking selects `api/managers.py`:9 repair-subject proxy commits,16 touches,53 uncovered statements. `history.txt` and `ranking.json` retain the complete ranking inputs. Repair wording is a proxy, not a verified escaped-incident count.

Six new HTTP cases use real SQLite infinity values in each selected/secondary score and prove filtering occurs before limit1. Isolated database error doubles prove sanitized503 for connection/query failures and exactly-once acquired-connection cleanup. They do not claim live PostgreSQL or deployment readiness.

## Actual execution

- New focused suite:6 passing cases, complete console and JUnit retained.
- Authoritative full baseline (`baseline-verified-*`):1800 passed,24 existing UI-auth failures,21 skips; actual exit1.
- Full candidate:1806 passed, the identical24 UI-auth failing nodes,21 skips; actual exit1. No full-suite PASS claim.
- Both have14110 statements,82 excluded lines and identical measured source/statement universes. Statement coverage86.75407512402552% ->86.7611622962438%,12241 ->12242 covered lines; no covered-line regressions. This modest delta does not complete the repository90% initiative. Finite-score and cleanup cases additionally protect already executed decisions.
- Black/Ruff on the test and driver and `git diff --check` exit0; complete argv and console in validation.json and adjacent captures.

The initial `baseline-*` raw files are also preserved. Its shell did not retain the raw pytest exit; the subsequently captured `baseline-verified-*` repeat excluding only the new file is the authoritative baseline for comparison. Every comparison count above comes from the retained JUnit/coverage JSON, not a console proxy.

## Real mutations and portable replay

`python3 docs/evidence/issue-1750-ranked-next/replay.py --output /tmp/a-new-similarity-proof-directory`

Use an interpreter with this repository's test dependencies. Output must not exist. The driver derives the checkout and active interpreter, reserves fresh output before touching source, uses bounded subprocess argv and restores original bytes in finally. It writes its receipt even on failure. Every one of the six named nodes actually failed with its production decision broken (REDexit1), then passed with exact source bytes restored (GREENexit0). Full argv/cwd, mutation anchors, source hashes, raw console and JUnit are in `mutation-run/`. Existing-output invocation actually refused before source mutation, and all prior proof/source hashes stayed identical (`existing-output-refusal.json`).

`manifest.json` hashes every retained artifact other than itself. Immutable raw console/JUnit and large coverage files are losslessly compressed as `.gz`; the manifest includes stored and decoded hashes. Production source bytes match base exactly and are separately hashed in comparison.json.

Matching Codex keepalive owns hosted CI and review after PR opening. Reviewed Repo Merge Verify Closer owns exact-head/full expected contexts/zero active review threads/seven-minute floor/guarded merge and actual verify:compare. Broader #1750 stays open; previous provider dispositions and unrelated UI-auth failures remain unchanged.

## Acceptance verification follow-up

The four infinity cases now contain two finite peers whose ranking reverses between Jaccard and cosine. Each case requests both limit1 and limit2, checking the complete response payload. This verifies filtering before a binding limit, retention of both finite peers, and ranking by the requested basis. The two isolated outage cases continue to verify sanitized503 and exactly-once cleanup after a query failure.

Fresh results in `followup/` are separate from the original full baseline/candidate captures above:

- Focused suite:6 passed; related manager suite:76 passed. Both commands skip slow tests and retain console, JUnit, argv, cwd and actual exit0.
- Replay:all six named cases produced REDexit1 then GREENexit0, with production bytes restored exactly. The driver now explicitly skips slow tests.
- Additional mutation removing `finite_rows[:limit]` truncation:all four score cases failed, then all four passed after exact restoration. Complete phase receipts, console and JUnit are retained.
- Targeted `api.managers` coverage is diagnostic only:510/821 statements,62.119366626065776%. The focused selection cannot replace the retained full-suite measurement or establish the broader90% target; its command explicitly disables the repository-wide coverage floor for this diagnostic run.
- The exact repository Black check (line length100 and the requested exclusions), scoped Ruff check and whitespace check exited0. Multi-file Black initially stalled in this runner. All382 tracked Python files were checked individually under the root configuration using a writable `/tmp` cache; the repository check then verified those files unchanged. The cache environment and successful gate argv are retained in `followup/validation.json`.
- `followup/audit.json` records verification of all50 original stored/decoded artifact hashes, the retained full-suite comparison against raw JUnit and coverage, all fresh mutation results, and the unchanged production hash. The refreshed manifest covers the new captures and updated documentation/driver.

The full-suite counts and coverage comparison above describe the original captures; no fresh full-suite run is claimed for this follow-up. The same24 existing UI authentication failures remain recorded. These SQLite tests and isolated outage doubles do not establish deployment readiness or live PostgreSQL behavior, and do not complete the broader coverage initiative.

Acceptance criteria verified against the tests and retained evidence:

- [x] Tests:HTTP-level similarity coverage filters invalid scores before applying result limits.
- [x] Tests:database errors return sanitized service-unavailable responses, with connection cleanup after query failures.
- [x] Documentation:test results, coverage comparison, production mutation checks and artifact integrity evidence are retained.
- [x] Documentation:existing UI authentication failures, deployment limitations and the open broader coverage initiative are recorded.
