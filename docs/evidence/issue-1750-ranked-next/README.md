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
