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

## Known-case trace (record-only / missing-caller at the reviewed base)

| Case at reviewed base | Live trace now | Disposition | Owned work |
|---|---|---|---|
| `large_delta` record-only in `etl/daily_diff_flow.py` | `_large_delta_event_from_diff` → `_record_large_delta_alerts` → `fire_alerts_for_event_sync`, invoked by `daily_diff_flow` after the diff commit | confirmed connected (former missing path, closed by owned work; not repaired in this PR) | [#1785](https://github.com/stranske/Manager-Database/issues/1785) |
| `etl_failure` record-only in `etl/evaluation_flow.py` | `fire_quality_alerts` → `fire_alerts_for_event_sync`, invoked by both `evaluation_flow` and `live_evaluation_flow` | confirmed connected | [#1789](https://github.com/stranske/Manager-Database/issues/1789) |
| `build_data_quality_alert_event` had no caller | `alerts/data_quality.py:build_data_quality_alert_event` ← `alerts/integration.py:fire_data_quality_alerts` (which also calls `fire_alerts_for_event_sync`) ← `etl/data_quality_flow.py:data_quality_flow`. The record-only sibling `evaluate_and_record_data_quality_alerts` is not the dispatch path and is not relied on here | confirmed connected | [#1790](https://github.com/stranske/Manager-Database/issues/1790) |
| `missing_filing` had no emitter | No production emitter exists; the name is retained only for stored-rule/history compatibility and new rule creation rejects it | intentional exception, not a missing path | [#1792](https://github.com/stranske/Manager-Database/issues/1792) |

`tests/test_daily_diff.py:test_known_record_only_cases_reach_dispatch_or_are_retired` asserts each live call edge above plus the inventory disposition/owners, and `test_known_cases_gate_fails_if_evaluation_dispatch_is_disconnected` proves the gate rejects a re-disconnected evaluation dispatch.

## Standing checks

`check_harvest_freshness` (for configured EDGAR/news harvests), `check_news_freshness`, `check_row_count_drop` (for configured filings/holdings/news tables), and `check_unmapped_cusip_rate` run through `evaluate_daily_quality`, `_daily_snapshot`, `data_quality_flow`, `fire_data_quality_alerts` and the existing `etl_failure` channel. The date-keyed snapshot and UTC check date preserve the daily replay payload. The gate compares this inventory to all current `check_*` declarations, so a new disconnected standing check fails coverage.

The news, conviction and activism rows name #1791 as the owner of this audit inventory and returned evidence. That ownership does not assign or authorize production repairs; the findings below remain review-only. Rows with existing delivery scopes retain their original owners.

## Returned findings — review only

- **F22-R1 — confirmed producer retry identity gap:** the real news emitter, given identical retained items at two controlled clock times, produces identical payloads but different actual `_delivery_event_key` values. The focused runtime test retains that evidence. Ordinary `news_flow` filters previously inserted item identities; this finding concerns re-entering the emitter after delivery failure, and is not a claim of observed live duplicate notifications. A proposed bounded follow-up would persist the delivery event timestamp/identity before dispatch and prove crash/retry behavior. No production fix or follow-up issue was filed.
- **F22-R2 — source-confirmed conviction replay risk:** both conviction alert constructors use a generated `occurred_at`; the durable key includes that timestamp even though payloads retain `report_date`. Existing tests observe both real emitter branches. A proposed separate scope would derive/persist a replay-stable event identity and validate rerun behavior. No channel, rule, or event semantics were changed.
- **F22-U1 — UNKNOWN activism delivery crash window:** domain event deduplication and emitter connectivity have executable coverage. The listed evidence does not establish stable external-delivery retry identity after a crash. This is an evidence gap, not a newly proven production delivery failure.

## Validation boundary

The exact issue command is `python -m pytest tests/test_daily_diff.py -q` (48 passed in an earlier native run, including the known-case trace and terminal-dispatch gate tests). Every connected event row must now carry a `fire_alerts_for_event(_sync)` edge and the `alerts/integration.py` dispatcher-terminus edge, every connected standing-check row must carry the `fire_data_quality_alerts` → `fire_alerts_for_event_sync` terminal edge, and any row claiming a channel outside `ALERT_CHANNELS` is rejected; no new event or channel was added. Additional focused alert/news/conviction/quality suites exercise the existing owned delivery behavior. The inventory gate rejects a new unconnected event, a new unconnected standing check, an unresolvable producer, an `unknown` disposition returned for human review, and a disconnected real large-delta dispatch call while preserving the justified missing-filing exception. Deliberate-break evidence: replacing the `fire_alerts_for_event_sync(conn, _large_delta_event_from_diff(row))` call in `etl/daily_diff_flow.py` made the inventory, large-delta and known-case trace tests fail (exit 1, 10 failures); restoring the file byte-for-byte (SHA-256 verified) returned the full file to green (exit 0, 48 passed). The actual large-delta call is temporarily disconnected for a RED run and restored byte-for-byte for GREEN; command transcripts and restoration hashes are retained with the opener run-20261019T0701 evidence. PostgreSQL/live-provider execution is not inferred from static test references.

### Review recovery (2026-10-19)

The inventory gate now rejects empty owners, reports missing producer files as findings, binds each event producer to its declared emission/call-path evidence, and scans production constructors for retired events independently of the exception rows. A separate regression removes only the dispatcher-terminus edge. Four new cases fail against the preceding gate; the existing terminus check already passes its new isolation test. The real large-delta dispatch was then disconnected: the inventory assertion failed, and byte-identical restoration returned the exact daily-diff suite (53 tests) plus focused alert/news/conviction/activism suites to green (109 total, no skips). Audit ownership on four previously ownerless rows belongs to #1791 only; no production repair, event or channel was added.

The follow-up review extends retirement detection to constant assignments and aliases in the enclosing function/class scope and rejects blank owner strings while retaining positive issue IDs as owners. This is static constant resolution, not proof about dynamically computed event types.

Latest follow-up verification: 57 daily-diff tests and 113 focused tests pass with no skips. Removing local-constant resolution and nonblank-owner validation produces three assertion failures in the new controls; restoring the test gate byte-for-byte returns all four targeted controls to green.
