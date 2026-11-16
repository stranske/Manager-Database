# Issue #1916 conviction replay deliberate-break evidence

This records the falsification proof for durable delivery identity of
`crowded_trade_change` and `contrarian_signal` events in `alerts/db.py`
(`_delivery_event_key`). The named gate is the replay test that dispatches
conviction alerts twice for the same `report_date` at different clock times and
expects a single `alert_history` row per event type.

## Named gate on restored code

Command:

```console
$ python3 -m pytest tests/test_crowded_contrarian.py::test_conviction_dispatch_replay_reuses_delivery_claim -q
.                                                                        [100%]
1 passed
```

Supporting gates for distinct payload, distinct report date, and
`time_window_hours` freshness:

```console
$ python3 -m pytest tests/test_crowded_contrarian.py::test_conviction_dispatch_changed_payload_creates_new_alert tests/test_crowded_contrarian.py::test_conviction_dispatch_different_report_date_creates_new_alert tests/test_crowded_contrarian.py::test_conviction_dispatch_time_window_rule_fires_on_fresh_run -q
...                                                                      [100%]
3 passed

$ python3 -m pytest tests/test_crowded_contrarian.py -q
.....................                                                    [100%]
21 passed
```

## Deliberate break

In `alerts/db.py`, `_delivery_event_key` temporarily included `occurred_at` in
the identity dict for `crowded_trade_change` and `contrarian_signal` (restoring
pre-fix behavior). A second dispatch at a different clock time therefore minted
new `event_key` values and duplicated `alert_history` rows.

Running the named gate failed for the expected reason:

```console
$ python3 -m pytest tests/test_crowded_contrarian.py::test_conviction_dispatch_replay_reuses_delivery_claim -q
F                                                                        [100%]
=================================== FAILURES ===================================
____________ test_conviction_dispatch_replay_reuses_delivery_claim _____________
...
>       assert history_count == 2
E       assert 4 == 2

tests/test_crowded_contrarian.py:511: AssertionError
=========================== short test summary info ============================
FAILED tests/test_crowded_contrarian.py::test_conviction_dispatch_replay_reuses_delivery_claim - assert 4 == 2
```

The second replay inserted two additional history rows (four total instead of
two), demonstrating that the named test detects loss of payload-only identity.

## Revert and restoration

The `occurred_at` inclusion was removed before this evidence file was merged.
The named gate and full `test_crowded_contrarian.py` module pass on the restored
tree (see outputs above).

Branch history retains the disposable break commit immediately before the
restoration commit so the PR diff shows the mutation and its revert.

## Acceptance criteria reconciliation (2026-11-16)

All acceptance criteria are satisfied on the current branch:

| Criterion | Evidence |
| --- | --- |
| Named replay gate | `test_conviction_dispatch_replay_reuses_delivery_claim` |
| Deliberate break → revert | Commits `2a763d8` (break) and `a5dac24` (revert + this doc) |
| Full module | `tests/test_crowded_contrarian.py` (21 tests) |
| Deliberate-break unit gate | `test_conviction_delivery_event_key_documents_deliberate_break_regression` |
| Payload / report_date / time_window | `test_conviction_dispatch_changed_payload_creates_new_alert`, `test_conviction_dispatch_different_report_date_creates_new_alert`, `test_conviction_dispatch_time_window_rule_fires_on_fresh_run` |
| Unit identity gate | `test_conviction_delivery_event_key_ignores_occurred_at` |

```console
$ python3 -m pytest tests/test_crowded_contrarian.py -q
.....................                                                    [100%]
21 passed
```