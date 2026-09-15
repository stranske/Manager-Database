# Alert delivery retry semantics

The dispatcher commits a history record before delivery, then commits a unique
`alert_delivery_attempts` claim for each history ID/channel before calling the
channel. Replayed filing events reuse the history record using rule ID, event
type, manager ID and filing ID; a newly generated event timestamp does not cause
a resend. Other event producers must preserve the event timestamp and payload
when retrying. Distinct rules and distinct filings remain independent.

Delivery uses **at-most-once attempts**, not exactly-once delivery. An existing
claim suppresses automatic resends even if the channel raised, timed out, was
unconfigured, or the delivery-result commit failed. A crash between claiming and
sending can lose a notification. A claim with neither a delivered channel nor a
recorded error means the outcome is unknown; reconcile with the provider before
any manual action. Claims and history must be retained for deduplication to hold.
Previously stored history without an event key is not retroactively deduplicated.

A retry can attempt channels that have no claim. Database uniqueness also
prevents two dispatchers from claiming the same rule/event/channel. Delivery
outcomes are committed after each attempt. If the outcome commit is rolled back,
the earlier claim survives; if the database committed but its response was lost,
the same claim still suppresses replay.

The dispatcher owns commits on its supplied connection. EDGAR commits documents,
filings and holdings before invoking alerts. Callback failure or cleanup cannot
erase those filings or previously committed claims. This does not add a worker
that recovers a crash between the filing commit and alert creation; replaying the
filing is still required to recover that gap.

## Verification in this run

- SQLite regression: four cases passed, covering a failed claim commit (no send),
  failed outcome commit, accepted outcome commit with a lost response, and a
  channel exception. Each case rebuilds the filing event and retries on a new
  connection; total external calls remain one.
- Alert engine and focused dispatcher tests: 133 passed.
- Alert integration and EDGAR tests: 27 passed, including all 22 EDGAR tests
  with `--run-nightly`, on Python 3.14.7.
- PostgreSQL suite: eight skipped. The added real-database regression injects
  both outcome-commit failure modes through scheduled EDGAR ingestion, observes
  durable filings and claims from a separate connection before mock email/Slack
  delivery, and retries ingestion to assert one send per channel.
- Live PostgreSQL execution remains required. PostgreSQL 16 startup was attempted
  with TCP and Unix sockets; both were denied by the sandbox (`Operation not
  permitted`). Run the PostgreSQL suite against a disposable database using
  `MGRDB_PG_TEST_URL` (the existing fixture resets its public schema).
- The full channel suite stalled during asyncio teardown after its existing SMTP
  test; the new retry cases and existing dispatcher test passed independently.
- Test-dependency verification reports missing `hypothesis` and optional `uv`.

The task remains unchecked pending successful live PostgreSQL verification.
Independent review-thread disposition remains with the closer.
