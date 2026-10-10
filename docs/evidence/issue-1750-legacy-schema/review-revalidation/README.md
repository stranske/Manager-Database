# Replay review revalidation

The replay now accepts RED only when the selected named node fails with its exact regression assertion message. An unrelated call-time RuntimeError is not proof. Explicit assertion messages bind the four schema mutations, and CIK uniqueness is checked directly before the duplicate insert.

Ten driver controls plus four schema tests pass. Against the previous driver, the four unrelated-exception controls fail; against a driver with both production restoration writes removed, the timeout and malformed-JUnit controls fail. Restoring the driver bytes makes both pass. Failure controls use a temporary source copy and exercise actual main() cleanup, receipt emission and absence of the success message.

The actual schema replay repeats all four real SQLite production mutations: eight phases, intended assertion RED exit1, exact source restoration GREEN exit0, source and callers unchanged. Complete logs/JUnit, commands and hashes are losslessly archived in raw-proof.tar.gz; manifest.json binds every member and current source/test/driver. Command: python3 docs/evidence/issue-1750-legacy-schema/replay_schema_boundaries.py --output /tmp/new-schema-proof. Focused command: python3 -m pytest tests/test_manager_legacy_schema.py tests/test_manager_schema_replay.py -q -o addopts=.

Original full-suite and coverage receipts remain historical evidence for 183f9719. This review changes assertion messages, adds one uniqueness assertion and ten driver controls, without changing api/managers.py, coverage configuration or floors. No new full-suite/coverage/provider/hosted parity claim is made for this review head. The broad source1750 remains open. Python3.12/pytest9.1.1/coverage7.16.0 local environment remains different from the pinned hosted coverage version.
