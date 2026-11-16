# Issue #1917 local Form-4 CLI deliberate-break evidence

This records falsification proof for the local ingestion command in
`etl/insider_flow.py` (`run_form4_ingest` → `ingest_insider_for_issuers`). The named
gate is `test_local_form4_command_persists_and_annotates`, which exercises
`main()`, stubbed fetching, seeded SQLite, full run-summary counters,
`api.signals` readback, per-issuer fetch failure tolerance, and rerun idempotency.

## Named gate on restored code

```console
$ python3 -m pytest tests/test_insider_flow.py::test_local_form4_command_persists_and_annotates -q
.                                                                        [100%]
1 passed

$ python3 -m pytest tests/test_insider_flow.py -q
.........                                                                [100%]
9 passed
```

Unit gate documenting the surgical break (no ingestion call):

```console
$ python3 -m pytest tests/test_insider_flow.py::test_local_form4_skip_ingest_documents_deliberate_break_regression -q
.                                                                        [100%]
1 passed
```

## Deliberate break

In `etl/insider_flow.py`, `run_form4_ingest` temporarily replaced the
`ingest_insider_for_issuers` call with a zero-row stub so the CLI resolved
issuers but never fetched or persisted Form-4 transactions.

Running the named gate failed for the expected reason:

```console
$ python3 -m pytest tests/test_insider_flow.py::test_local_form4_command_persists_and_annotates -q
F                                                                        [100%]
=========================== short test summary info ============================
FAILED tests/test_insider_flow.py::test_local_form4_command_persists_and_annotates
  Differing items:
  {'rows_inserted': 0} != {'rows_inserted': 3}
  {'fetch_errors': 0} != {'fetch_errors': 1}
```

The break left `insider_transactions` empty and dropped the MSFT fetch error
from the summary, showing the named test detects absent ingestion and lost
per-issuer error accounting.

## Revert and restoration

The ingestion call was restored before this evidence file was committed. The
named gate and full `test_insider_flow.py` module pass on the restored tree (see
outputs above).

## Acceptance criteria reconciliation

| Criterion | Evidence |
| --- | --- |
| Named local CLI gate | `test_local_form4_command_persists_and_annotates` |
| Deliberate break → revert | Local break recorded in this doc; tree restored |
| Full module | `tests/test_insider_flow.py` |
| Deliberate-break unit gate | `test_local_form4_skip_ingest_documents_deliberate_break_regression` |
| README local invocation | `README.md` → **Local Form 4 ingestion** |