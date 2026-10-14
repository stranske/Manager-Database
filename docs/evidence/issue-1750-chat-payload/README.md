# Chat payload boundary coverage for issue #1750

Eight new cases exercise actual `api.chat` normalization functions: a string
answer, a mixed citation list, four unsupported citation containers, an object
with a response fallback and complete metadata, and an object without optional
attributes. No production code changes.

## Selection

`ranking.json` ranks the last 500 source-touching commits by repair-like subject
words, then touches, then currently uncovered statements. This is a repair-history
proxy, not a count of verified escaped incidents. `api/managers.py` ranks first
with one remaining structurally unreachable schema-add line; bypassing the helper
that already adds the column would manufacture coverage. `api/chat.py` is next:
13 proxy matches, 71 touches and 91 missing statements. The selected normalization
paths at lines 777–807 can be exercised without provider calls or hosting changes.

## Results

On base `1291e3f9a53e0586378cf6bf223f24855e829df0`, the complete baseline has
2077 passed, 24 existing UI-auth failures and 21 skips. The candidate has 2085
passed with the identical 24 failures and 21 skips. Every old testcase identity
and outcome is preserved; exactly eight added cases pass. Both full-suite commands
exit 1. This is **not** a full-suite PASS or a repository 90% completion claim.

Statement coverage rises from 12550/14396 (87.17699361%) to 12560/14396
(87.24645735%). All ten added covered lines are in `api/chat.py`; no covered line
regresses. Both captures have the same 118 source files and 82 excluded lines.
Branch coverage is not measured by this existing configuration. Production
`api/chat.py` remains byte-identical to the base, SHA256
`0e11ed9b2fee12c2cc0ce78cb01d85a11708e31c01d89403381af2e73bc71815`.

Each named case fails at its intended assertion with a real private production
mutation (exit 1, one JUnit failure, no error/skip) and passes after exact
restoration (exit 0). The replay uses a disposable Git archive and restores source
in `finally`; caller source/test bytes remain unchanged. Reusing output refuses
before mutation with all 65 proof files unchanged. Seven malformed receipt controls
(wrong name/class/count, skip, error, unrelated assertion and wrong process exit)
are rejected. All 16 actual phase receipts pass the strict validator.

Full-repository Black, focused Ruff and diff checks pass. The archive's 88 members
(including its member manifest) and current source/test/driver bindings were
independently checked. `manifest.json` binds the archive and comparison hashes;
`members.json` inside the archive binds every other stored member's bytes/hash.
Original captured argv, cwd, interpreter version, console, JUnit, coverage JSON,
production mutants/restorations, ranking history and validation receipts are retained.

## Reproduce

Use Python >=3.12 with the repository's development dependencies installed. Select
the actual installed interpreter explicitly; the original absolute interpreter
paths remain historical capture facts in the archive, not a portability requirement.
Run from the repository root, with a fresh output path:

```sh
PYTHON=/path/to/installed/python3
"$PYTHON" -m pytest tests/test_chat_payload_boundaries.py -q -o addopts=
"$PYTHON" docs/evidence/issue-1750-chat-payload/chat_payload_replay.py --output /tmp/new-chat-payload-proof
"$PYTHON" -m pytest tests -q --cov --cov-report=json:/tmp/chat-coverage.json
```

The focused/replay gates require exit 0. The recorded full suite retains the
pre-existing UI-auth failures; its expected current baseline exit is 1. Hosted CI
is a separate validation environment, and local results do not establish parity,
provider approval, deployment or live product acceptance. The broad issue remains
open below 90%; keepalive owns subsequent CI/review, then closer owns exact-head
checks, review closure, seven-minute floor, guarded merge and actual comparison.
