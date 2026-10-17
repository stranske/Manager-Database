# Exact-head review and CI recovery

All four active CodeRabbit findings on initial head 826b88f6 were read in full:

- `r4301875490`: historical cutoff is now serialized in LangSmith trace inputs.
- `r4301875493`: committed causal command uses `.venv/bin/python`; exact machine-specific command stays only in the local automation receipt.
- `r4301875495`: the original all-final suite's single skip was `test_original_replay_after_amendment_postgres`, because MGRDB_PG_TEST_URL was unset. It was not a tracing skip.
- `r4301875498`: the PostgreSQL test restores its exact original row timestamps and deletes the exact inserted row, committing cleanup. It asserts the original seed values afterward.

Actual CI Python 3.13 run 39013062968/job117339587254 exposed a historical evidence binding: `test_chat_lifecycle_evidence.py` compared archived `bound-inputs/api/chat.py` with today's API bytes. The original archive remains byte-for-byte unchanged, including its hard-coded SHA256, manifest, member counts, raw JUnit and tamper checks. A separate `historical-api-chat.py.txt` retains the archived API input, and the archive test binds to that immutable historical copy. The original eight lifecycle regressions remain unchanged and execute the current product API. This does not replace historical captures with current results or relax lifecycle behavior assertions.

Validation: 108 current API/holdings/lifecycle/archive tests passed. All 23 real PostgreSQL chain/bitemporal tests passed with no skips, including the formerly skipped original amendment replay case. Raw JUnit/logs are retained beside this note. The local interpreter is the repository-selected Python 3.12 environment; hosted Python 3.13/full-suite/coverage checks must rerun on the new head. No hosted green or provider/source acceptance is asserted by these local receipts.
