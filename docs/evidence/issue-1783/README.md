# Campaign identifier stability and rollback

Existing timeline/document filing ownership is resolved before target grouping. Identifier changes update metadata on that campaign ID; new filings for the corrected target join the same owner. Related filings keep their shared identity even when only the latest filing is corrected. An identifier already owned by a distinct campaign fails closed for explicit reconciliation, rather than merging or renumbering unrelated identities.

A savepoint covers the metadata update and derived timeline/document replacement. Forced failure after metadata/timeline changes restores all prior materialized rows. The original missing-source-table no-op remains outside that transaction.

`1783-red.txt`: reverting only `etl/activism_campaign_flow.py` to the original production version produces **6 actual semantic failures** on the unchanged identifier tests. `1783-green.txt`: byte-identical restoration returns **6 passed**. The mutation receipt records the source hashes and command.

`1783-validation.txt`: campaign materialization, actual API detail/timeline routes, and chain PostgreSQL integration modules yield **23 passed, 6 skipped**. The PostgreSQL identifier-update/forced-rollback case is supplied but **not run locally** because `MGRDB_PG_TEST_URL` is unset. No PostgreSQL acceptance claim is made. The existing uniqueness constraints and API schemas are unchanged.
