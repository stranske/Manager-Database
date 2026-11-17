# Form 4 CLI deliberate-break evidence

On November 16, 2026, the named acceptance test ran on exact commit snapshots
from PR #1922 with the repository's Anaconda Python interpreter.

```sh
/opt/anaconda3/bin/python3 -m pytest tests/test_insider_flow.py::test_local_form4_command_persists_and_annotates -q
/opt/anaconda3/bin/python3 -m pytest tests/test_insider_flow.py -q
```

| Commit | Named test | Focused test file |
| --- | --- | --- |
| `27fe8b0f5b278ea84dc21fefe88624c5e98f17f4` (skip ingestion) | Exit 1, 1 failed | |
| `dc648814b2d1fff556655f1fff1716146924fa69` (restore ingestion) | Exit 0, 1 passed | Exit 0, 9 passed |
| `51fcd593a5bddf1b7d96754ea1b7e93107aca7c4` (original PR head) | Exit 0, 1 passed | Exit 0, 10 passed |

The break replaced the production call to `ingest_insider_for_issuers` in
`run_form4_ingest` with an empty result. The named test failed at the CLI JSON
assertion. It reported `rows_inserted=0` where it expected 3 and
`fetch_errors=0` where it expected 1. The next commit restored the call and
the named test passed against the seeded SQLite database.
