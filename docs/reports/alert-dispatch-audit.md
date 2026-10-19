# Alert dispatch audit — issue #1791

Reviewed base: `0a244845e908d20d770ce09a44938393de92f271`. Eight event declarations and four standing checks are inventoried below. Function-level source/call edges, replay contracts, named tests and known issue ownership live in [the machine-readable inventory](alert-dispatch-inventory.json). The gate checks current declarations, actual authored calls and addressable test names; it does not claim that a listed test ran or an external provider delivered. The source hashes describe the reviewed base; the gate intentionally inspects live source calls rather than requiring unrelated source bytes to remain frozen.

## Event coverage

| Event | Producer | Disposition | Existing scope |
|---|---|---|---|
| `new_filing` | `etl/edgar_flow.py:fetch_and_store` | connected | [#1786](https://github.com/stranske/Manager-Database/issues/1786) |
| `large_delta` | `etl/daily_diff_flow.py:_large_delta_event_from_diff` | connected | [#1785](https://github.com/stranske/Manager-Database/issues/1785) |
| `news_spike` | `etl/news_flow.py:emit_news_spike_alerts` | connected | No additional owned repair created |
| `crowded_trade_change` | `etl/conviction_flow.py:dispatch_conviction_alerts` | connected | No additional owned repair created |
| `contrarian_signal` | `etl/conviction_flow.py:dispatch_conviction_alerts` | connected | No additional owned repair created |
| `etl_failure` | `etl/evaluation_flow.py:fire_quality_alerts; etl/data_quality_flow.py:data_quality_flow` | connected | [#1789](https://github.com/stranske/Manager-Database/issues/1789), [#1790](https://github.com/stranske/Manager-Database/issues/1790) |
| `activism_event` | `etl/activism_flow.py:fetch_activism_filings` | connected | No additional owned repair created |
| `missing_filing` | `None` | intentional-exception | [#1792](https://github.com/stranske/Manager-Database/issues/1792) |

All connected rows reach `alerts.integration.fire_alerts_for_event` and the ordinary dispatcher. `missing_filing` is an intentional compatibility exception: new rules are rejected while legacy histories remain readable. No missing-filing detector or deadline assumption was introduced. The older large-delta, evaluation and standing-quality record-only gaps now have production call edges and remain linked to #1785, #1789 and #1790; missing-filing retirement belongs to #1792. Original issue acceptance/verification is not inferred from this audit.

## Standing checks

`check_harvest_freshness` (for configured EDGAR/news harvests), `check_news_freshness`, `check_row_count_drop` (for configured filings/holdings/news tables), and `check_unmapped_cusip_rate` run through `evaluate_daily_quality`, `_daily_snapshot`, `data_quality_flow`, `fire_data_quality_alerts` and the existing `etl_failure` channel. The date-keyed snapshot and UTC check date preserve the daily replay payload. The gate compares this inventory to all current `check_*` declarations, so a new disconnected standing check fails coverage.

## Returned findings — review only

- **F22-R1 — confirmed producer retry identity gap:** the real news emitter, given identical retained items at two controlled clock times, produces identical payloads but different actual `_delivery_event_key` values. The focused runtime test retains that evidence. Ordinary `news_flow` filters previously inserted item identities; this finding concerns re-entering the emitter after delivery failure, and is not a claim of observed live duplicate notifications. A proposed bounded follow-up would persist the delivery event timestamp/identity before dispatch and prove crash/retry behavior. No production fix or follow-up issue was filed.
- **F22-R2 — source-confirmed conviction replay risk:** both conviction alert constructors use a generated `occurred_at`; the durable key includes that timestamp even though payloads retain `report_date`. Existing tests observe both real emitter branches. A proposed separate scope would derive/persist a replay-stable event identity and validate rerun behavior. No channel, rule, or event semantics were changed.
- **F22-U1 — UNKNOWN activism delivery crash window:** domain event deduplication and emitter connectivity have executable coverage. The listed evidence does not establish stable external-delivery retry identity after a crash. This is an evidence gap, not a newly proven production delivery failure.

## Validation boundary

The exact issue command is `python -m pytest tests/test_daily_diff.py -q` (43 passed in the latest run). Additional focused alert/news/conviction/quality suites exercise the existing owned delivery behavior. The inventory gate rejects a new unconnected event, a new unconnected standing check, an unresolvable producer, an `unknown` disposition returned for human review, and a disconnected real large-delta dispatch call while preserving the justified missing-filing exception. Deliberate-break evidence: replacing the `fire_alerts_for_event_sync(conn, _large_delta_event_from_diff(row))` call in `etl/daily_diff_flow.py` made the inventory and large-delta tests fail (exit 1); restoring the file byte-for-byte (SHA-256 verified) returned the full file to green (exit 0). The actual large-delta call is temporarily disconnected for a RED run and restored byte-for-byte for GREEN; command transcripts and restoration hashes are retained with the opener run-20261019T0701 evidence. PostgreSQL/live-provider execution is not inferred from static test references.
