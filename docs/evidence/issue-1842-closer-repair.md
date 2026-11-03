# Issue #1842: closer acceptance evidence (repair of #1876)

Checked 2026-11-03. Refs [#1842](https://github.com/stranske/Manager-Database/issues/1842).
Repair of merged [#1876](https://github.com/stranske/Manager-Database/pull/1876); production
code unchanged.

## Change

`tests/test_coverage_round_1842_followup.py::test_load_unacknowledged_alert_count_offline_mode`
uses `unittest.mock.MagicMock` for `httpx.Client`, clears the Streamlit cache before/after,
and asserts `call_count == 0` after `load_unacknowledged_alert_count()` returns (production
`except Exception: return 0` made the prior raise-on-`Client` guard ineffective).

## Focused pytest

```text
$ PYTHONPATH=. /opt/anaconda3/bin/python3 -m pytest tests/test_coverage_round_1842_followup.py -m 'not slow' -q
.....                                                                    [100%]
5 passed
```

## Gate listed-target coverage (branch-enabled, Python 3.13)

Artifacts: Gate run [41248974079](https://github.com/stranske/Manager-Database/actions/runs/41248974079)
(`gate-coverage-3.13-1` / `coverage.json`). Baseline row from hosted Gate on PR #1843 run
39362579319 (commit `3631f0ff`), recorded in [#1843](https://github.com/stranske/Manager-Database/pull/1843).

| Target | Baseline U (ml+mb) | Head U (ml+mb) | Δ U |
| --- | ---: | ---: | ---: |
| `etl/daily_diff_flow.py` | 2 (1+1) | 2 (1+1) | 0 |
| `api/managers.py` | 16 (1+15) | 13 (0+13) | 3 |
| `api/chat.py` | 14 (0+14) | 4 (0+4) | 10 |
| `ui/dashboard.py` | 67 (40+27) | 31 (19+12) | 36 |
| **Sum (listed targets)** | **99** | **50** | **49** |

Issue dispatch baseline (CI run 39357422686, [#1843](https://github.com/stranske/Manager-Database/pull/1843)):
159 U on the same four files (5+4, 1+15, 0+14, 73+47). Total measured reduction #1843+#1876:
109 U. **Shortfall vs ~180 goal:** max(0, 180 − 109) = **71**.

Gate index on run 41248974079: overall **92.77%** (3.13), **2614** collected cases including
`tests/test_coverage_round_1842_followup.py` (5 tests).

## Ranking and deferrals (#1842 scope)

Issue order (unchanged): `etl/daily_diff_flow.py` → `api/managers.py` → `api/chat.py` →
`ui/dashboard.py`.

- **`etl/daily_diff_flow.py`:** Primary #1843 tests cover Decimal alert preservation and UTC
  fallback; Gate still shows 1+1 U on the Prefect `__main__` launch path (intentional /
  not meaningfully unit-tested). No additional follow-up tests in #1876.
- **`api/managers.py`:** Follow-up hits partial legacy SQLite `quality_flags` migration; remaining
  U is API/Postgres finally-branches and bulk paths not forced by this chunk.
- **`api/chat.py`:** Follow-up covers normalizers and manager-name resolution; remaining U is
  formatter/timeout/endpoint branches already partially exercised in #1843.
- **`ui/dashboard.py`:** Largest deferral mass — Streamlit renderers, DB loaders, and auth-gated
  sections; #1843+#1876 cut dashboard U from 120 (dispatch) → 31 (Gate head) but not to zero.
