# Chat chain lifecycle coverage for issue 1750

The chunk adds eight test cases for fail-closed module loading, constructor cleanup, invoke-only context/payload fidelity (empty and populated context), missing executors, suppressed cleanup errors, missing filing IDs and asynchronous answers. Production `api/chat.py` is unchanged from base `369ea9c399dbbd5b145d71e36cfb6862251a8a6b`.

The fresh 500-source-commit ranking puts `api/managers.py` first (31 repair-subject proxy matches / 61 touches), with only its previously documented structurally unreachable schema-add statement uncovered. `api/chat.py` is the next bounded source (21 proxy matches / 71 touches / 79 initially uncovered statements). These are subject/churn proxies, not escaped-defect counts. The measured gain is 12 previously uncovered chat statements; import-failure checks also add causal regression protection without a measured full-suite gain.

## Validation

Interpreter: `/opt/anaconda3/bin/python3` (Python 3.12.2). No runtime installation, provider calls, workflow changes, exclusion changes or production edits.

| Gate | Result |
| --- | --- |
| `python3 -m pytest tests/test_chat_chain_lifecycle.py -q -o addopts=` | 8 passed |
| Baseline full `python3 -m pytest tests -q --cov --cov-report=json:coverage.json` | 2093 passed, 24 existing UI-auth failures, 21 skipped; exit 1 |
| Candidate same complete suite | 2101 passed, same 24 failures, same 21 skips; exit 1 |
| Old outcome identity comparison | No changed/removed old nodes; exactly 8 new passing nodes |
| Source/exclusion universe | Same 118 files / 14396 statements / 82 exclusions |
| Statement coverage | 87.2464573493% to 87.3298138372%; 12 added covered lines, none lost |
| Eight private production mutations | Each exact named assertion RED exit 1, restored GREEN exit 0; no errors/skips; byte-identical restoration |
| Output reuse | FileExistsError before mutation; all 65 existing proof files unchanged |
| Malformed JUnit identity | Rejected |
| Full repository Black | 422 files unchanged, exit 0 |
| Focused Ruff / `git diff --check` | Exit 0 |

The full suite is **not green**. The pre-existing UI-auth failures are retained with complete consoles and JUnit; no provider PASS, hosted CI success, deployment or repository 90% completion is claimed. Broader issue 1750 stays open.

## Inspect and replay

`comparison.json` contains exact totals, new node identities and added lines; `ranking.json` records selection inputs. `validation.tar.gz` contains both full-suite consoles/JUnit/coverage/argv/cwd/exits, all eight mutant/original source images and sixteen phase logs/JUnit/commands, misuse controls, static-check outputs and bound source/test/driver inputs. Its `manifest.json` lists SHA256 and byte length for all 82 members; `archive.json` binds the archive and current inputs.

Run from a fresh checkout using the selected Python:

```sh
/opt/anaconda3/bin/python3 docs/evidence/issue-1750-chat-lifecycle/replay.py --output /tmp/new-chat-lifecycle-proof
```

The destination must not exist. The replay uses a temporary Git archive, overlays only the current test, scopes unique mutation anchors to actual production functions, validates the exact named assertion and zero errors/skips, restores bytes in finally blocks, and verifies the calling checkout is unchanged. It removes the temporary archive checkout after execution.

Keepalive owns hosted CI and review after ready PR creation. Closer retains complete current exact-head check topology, all active thread bodies/zero-active gate, seven-minute review floor, guarded merge, actual compare and bounded acceptance. This test chunk cannot close the broader coverage initiative.

## Keepalive replay guard follow-up

- [x] First task revalidated: eight focused tests passed; a fresh private replay produced eight intended assertion failures (exit 1) and eight restored passes (exit 0), with no errors/skips and unchanged caller bytes.

`tests/test_chat_lifecycle_replay.py` adds 32 regression cases for the replay contract: all eight named red/green reports, malformed identities and case ownership, errors/skips, unrelated assertions, incorrect process exits, and refusal of existing output files/directories before archive creation or mutation. The combined lifecycle and replay-guard run passed all 40 cases on Python 3.14.8:

```sh
python -m pytest tests/test_chat_chain_lifecycle.py tests/test_chat_lifecycle_replay.py -m 'not slow' -q -o addopts=
PYTEST_ADDOPTS='-m "not slow"' python docs/evidence/issue-1750-chat-lifecycle/replay.py --output /tmp/new-chat-lifecycle-proof
```

The original archive SHA256, all 82 manifest members, the three current source/test/driver bindings, and all sixteen archived phase identities/exits/assertions were verified again. The archive and its bound inputs remain unchanged. Full-suite and coverage totals above describe the original eight-test chunk; this follow-up adds guard cases and does not claim a new full-suite or coverage result. Those acceptance tasks remain pending for the current candidate.

Focused Ruff and whitespace checks passed. Black 26.10.1 checked all 423 current Python files successfully. Its asynchronous worker run stalled in this sandbox, so serial single-file checks populated a writable standard Black cache before the required repository-wide command exited 0:

```sh
BLACK_CACHE_DIR=/tmp/chat-lifecycle-black-cache black --check --line-length 100 --exclude '(\.workflows-lib|node_modules)' .
```

## Keepalive full-suite comparison follow-up

- [x] Revalidated the original eight-test chunk's complete baseline/candidate evidence: 2093/2101 passed, the same 24 UI-auth failures and 21 skips, every old node outcome unchanged, exactly eight new passing nodes, and both full-suite exits 1.

`tests/test_chat_lifecycle_evidence.py` adds 16 cases that independently read the retained full-suite JUnit and command receipts. Before comparing outcomes, its fixture verifies the unchanged archive SHA256, all 82 manifest members and all three current source/test/driver bindings. The comparison checks suite counts against individual outcomes, exact added node identities, complete-suite arguments, report destinations, interpreter and checkout identity. Negative controls reject swapped old outcomes with unchanged aggregate counts, removed/renamed nodes, a skipped new case, a false exit 0, narrowed commands, misbound reports, differing checkouts and malformed JUnit.

The combined lifecycle, replay and evidence tests passed all 56 cases on Python 3.14.8:

```sh
python -m pytest tests/test_chat_chain_lifecycle.py tests/test_chat_lifecycle_replay.py tests/test_chat_lifecycle_evidence.py -m 'not slow' -q -o addopts=
```

The checkbox above applies to the archived original comparison. A current complete-suite attempt stopped making progress after three tests and was interrupted. An isolated diagnostic of `tests/baseline/test_directional.py` timed out after 45 seconds; the stack capture shows the first directional case waiting in Starlette `TestClient.__enter__` through AnyIO. The broader targeted chat run returned 86 passed / 1 failed: `test_deterministic_surface_unaffected_when_zone_disabled` received health status 503 instead of 200. Both that existing test and production `api/chat.py` are byte-identical to the recorded base. Targeted `--cov=api.chat --cov-report=term-missing` reported 70%; it does not establish the repository percentage in the original comparison. Current complete-suite and repository-coverage acceptance remain pending; no new full-suite PASS or coverage gain is claimed.

Focused Ruff and whitespace checks passed. Black checked all 424 current Python files successfully with the required repository-wide command, using `BLACK_CACHE_DIR=/tmp/chat-lifecycle-comparison-black-cache` after serial single-file checks populated that cache.
