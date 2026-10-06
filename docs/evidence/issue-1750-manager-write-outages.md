# Manager write outage regression evidence

Source initiative #1750; previous bulk-CIK chunk #1751 is merged and dispositioned.
Baseline main: `afe8fe54f0e382590acaef42de254bfbc6db27e3`.
This chunk adds tests only; production bytes are unchanged. It neither completes
the 90% initiative nor repairs the existing UI authentication failures.

## Selection and scope

Ranked the last 500 main commits by a clearly named repair-history proxy (commit
subjects containing fix, regression or repair), churn and measured uncovered
statements. This is a proxy, not an escaped-incident count. Current ranking:

| Module | Repair proxy | Churn | Missing statements |
| --- | ---: | ---: | ---: |
| api/managers.py | 9 | 16 | 70 |
| ui/dashboard.py | 5 | 8 | 104 |
| etl/manager_similarity_flow.py | 5 | 7 | 3 |
| etl/edgar_flow.py | 4 | 10 | 25 |

The selected three write endpoints had unexecuted database-exception branches.
Nine real ASGI requests cover connection, schema and write failures for manager
update, tag patch and deletion. The SQLite connection double proves acquisition
and release control; injected failures are not a PostgreSQL/live-service trial.
Existing behavior correctly returns 503 without revealing the exception's private
database path, closes acquired connections and leaves cached state intact. The
follow-up below strengthens the original invalidation-spy check with stored cache
contents and verifies that each request reaches its specified failure stage.
No newly reproduced production defect needed a source change.

## Commands and observed results

```sh
/opt/anaconda3/bin/python3 -m pytest tests/test_manager_write_failures.py -q
# exit 0, 9 passed
/opt/anaconda3/bin/python3 -m pytest tests/test_manager_api.py tests/test_manager_bulk_api.py tests/test_manager_cache.py tests/test_manager_write_failures.py -q
# exit 0, 128 passed
/opt/anaconda3/bin/python3 -m pytest tests -q --cov --cov-report=json:coverage.json --cov-report=term --junitxml=full-suite.xml
# baseline/candidate exit 1, identical 24 failing nodes in tests/test_ui_auth.py
/opt/anaconda3/bin/python3 -m black --check .
# exit 0, 374 files unchanged
/opt/anaconda3/bin/python3 -m ruff check tests/test_manager_write_failures.py
# exit 0
```

The full-suite command used distinct absolute report destinations in the
automation evidence directory for baseline/candidate. All other options,
interpreter and config bytes were identical. Default nightly skips were retained;
no exclusion, threshold or assertion was changed. Current coverage is the configured
statement-only metric, not a fabricated combined line/branch metric.

| Measurement | Baseline | Candidate |
| --- | ---: | ---: |
| Passed | 1787 | 1796 |
| Failed | 24 | 24 |
| Skipped | 21 | 21 |
| Total nodes | 1832 | 1841 |
| All measured statements | 14110 | 14110 |
| Covered statements | 12224 | 12227 |
| Coverage | 86.6335931963% | 86.6548547130% |
| api/managers.py covered / total | 751 / 821 | 754 / 821 |
| api/managers.py missing | 70 | 67 |

## Actual deliberate break and restoration

Each mutation edited actual `api/managers.py`, then executed the complete new
suite using `/opt/anaconda3/bin/python3 -X pycache_prefix=<fresh temporary cache>
-m pytest tests/test_manager_write_failures.py -q --junitxml=<receipt>`. Fresh
bytecode caches prevent timestamp/size collisions from reusing unmutated source.
The source was restored in finally, compared byte-for-byte and hashed before the
restored GREEN. Original and restored SHA256:
`401a4591cd8965308f5ccffaf2d95144f2fd190a114e223cf3bccd77c0ff8245`.

| Actual source mutation | Exit | Failing new cases |
| --- | ---: | ---: |
| _raise_db_unavailable: HTTP status 503 -> 500 | 1 | 9 / 9 |
| acquired conn.close() -> pass | 1 | 6 / 9 |
| invalidate manager cache inside _raise_db_unavailable | 1 | 9 / 9 |
| byte-identical restored source | 0 | 0 / 9 |

The three connection-acquisition failures cannot close an unacquired connection,
so those cases correctly keep passing the cleanup mutation. Each of the nine
new nodes fails for two real mutations and passes after restoration.
The companion JSON contains every named node and observed disposition.

### Captured named-node output


mutation-outage-status

```text
FAIL tests.test_manager_write_failures::test_write_outage_returns_503_and_preserves_cache[connect-update]
FAIL tests.test_manager_write_failures::test_write_outage_returns_503_and_preserves_cache[connect-tags]
FAIL tests.test_manager_write_failures::test_write_outage_returns_503_and_preserves_cache[connect-delete]
FAIL tests.test_manager_write_failures::test_write_outage_returns_503_and_preserves_cache[schema-update]
FAIL tests.test_manager_write_failures::test_write_outage_returns_503_and_preserves_cache[schema-tags]
FAIL tests.test_manager_write_failures::test_write_outage_returns_503_and_preserves_cache[schema-delete]
FAIL tests.test_manager_write_failures::test_write_outage_returns_503_and_preserves_cache[write-update]
FAIL tests.test_manager_write_failures::test_write_outage_returns_503_and_preserves_cache[write-tags]
FAIL tests.test_manager_write_failures::test_write_outage_returns_503_and_preserves_cache[write-delete]
```

mutation-close-acquired-connection

```text
PASS tests.test_manager_write_failures::test_write_outage_returns_503_and_preserves_cache[connect-update]
PASS tests.test_manager_write_failures::test_write_outage_returns_503_and_preserves_cache[connect-tags]
PASS tests.test_manager_write_failures::test_write_outage_returns_503_and_preserves_cache[connect-delete]
FAIL tests.test_manager_write_failures::test_write_outage_returns_503_and_preserves_cache[schema-update]
FAIL tests.test_manager_write_failures::test_write_outage_returns_503_and_preserves_cache[schema-tags]
FAIL tests.test_manager_write_failures::test_write_outage_returns_503_and_preserves_cache[schema-delete]
FAIL tests.test_manager_write_failures::test_write_outage_returns_503_and_preserves_cache[write-update]
FAIL tests.test_manager_write_failures::test_write_outage_returns_503_and_preserves_cache[write-tags]
FAIL tests.test_manager_write_failures::test_write_outage_returns_503_and_preserves_cache[write-delete]
```

mutation-preserve-cache-on-outage

```text
FAIL tests.test_manager_write_failures::test_write_outage_returns_503_and_preserves_cache[connect-update]
FAIL tests.test_manager_write_failures::test_write_outage_returns_503_and_preserves_cache[connect-tags]
FAIL tests.test_manager_write_failures::test_write_outage_returns_503_and_preserves_cache[connect-delete]
FAIL tests.test_manager_write_failures::test_write_outage_returns_503_and_preserves_cache[schema-update]
FAIL tests.test_manager_write_failures::test_write_outage_returns_503_and_preserves_cache[schema-tags]
FAIL tests.test_manager_write_failures::test_write_outage_returns_503_and_preserves_cache[schema-delete]
FAIL tests.test_manager_write_failures::test_write_outage_returns_503_and_preserves_cache[write-update]
FAIL tests.test_manager_write_failures::test_write_outage_returns_503_and_preserves_cache[write-tags]
FAIL tests.test_manager_write_failures::test_write_outage_returns_503_and_preserves_cache[write-delete]
```

mutation-restored-green

```text
PASS tests.test_manager_write_failures::test_write_outage_returns_503_and_preserves_cache[connect-update]
PASS tests.test_manager_write_failures::test_write_outage_returns_503_and_preserves_cache[connect-tags]
PASS tests.test_manager_write_failures::test_write_outage_returns_503_and_preserves_cache[connect-delete]
PASS tests.test_manager_write_failures::test_write_outage_returns_503_and_preserves_cache[schema-update]
PASS tests.test_manager_write_failures::test_write_outage_returns_503_and_preserves_cache[schema-tags]
PASS tests.test_manager_write_failures::test_write_outage_returns_503_and_preserves_cache[schema-delete]
PASS tests.test_manager_write_failures::test_write_outage_returns_503_and_preserves_cache[write-update]
PASS tests.test_manager_write_failures::test_write_outage_returns_503_and_preserves_cache[write-tags]
PASS tests.test_manager_write_failures::test_write_outage_returns_503_and_preserves_cache[write-delete]
```

### Existing full-suite failures (unchanged exact node set)

```text
tests.test_ui_auth::test_configured_login_renders_with_real_authenticator
tests.test_ui_auth::test_configured_login_rejects_invalid_credentials[analyst-wrong-password]
tests.test_ui_auth::test_configured_login_rejects_invalid_credentials[unknown-synthetic-test-password]
tests.test_ui_auth::test_valid_login_rerun_and_logout
tests.test_ui_auth::test_configured_login_does_not_trust_cached_dev_auth[None]
tests.test_ui_auth::test_configured_login_does_not_trust_cached_dev_auth[False]
tests.test_ui_auth::test_require_login_fails_closed_without_dependency
tests.test_ui_auth::test_dev_mode_discards_previous_authenticated_session
tests.test_ui_auth::test_configured_login_requires_cookie_secret[None-False]
tests.test_ui_auth::test_configured_login_requires_cookie_secret[None-True]
tests.test_ui_auth::test_configured_login_requires_cookie_secret[-False]
tests.test_ui_auth::test_configured_login_requires_cookie_secret[-True]
tests.test_ui_auth::test_configured_login_requires_cookie_secret[                                -False]
tests.test_ui_auth::test_configured_login_requires_cookie_secret[                                -True]
tests.test_ui_auth::test_configured_login_requires_cookie_secret[short-key-False]
tests.test_ui_auth::test_configured_login_requires_cookie_secret[short-key-True]
tests.test_ui_auth::test_authenticator_receives_independent_secret_and_cached_hash
tests.test_ui_auth::test_cookie_reauthentication_rejects_rotated_key
tests.test_ui_auth::test_require_login_preserves_blank_credential_dev_mode[None-UI_USERNAME]
tests.test_ui_auth::test_require_login_preserves_blank_credential_dev_mode[None-UI_PASSWORD]
tests.test_ui_auth::test_require_login_preserves_blank_credential_dev_mode[-UI_USERNAME]
tests.test_ui_auth::test_require_login_preserves_blank_credential_dev_mode[-UI_PASSWORD]
tests.test_ui_auth::test_require_login_preserves_blank_credential_dev_mode[   -UI_USERNAME]
tests.test_ui_auth::test_require_login_preserves_blank_credential_dev_mode[   -UI_PASSWORD]
```

Full console logs, JUnit XML, source hashes, before/after coverage JSON and
ranking are retained at `/Users/teacher/.codex/automations/pd-workloop-resume/evidence/20261006T1402Z`.
After opening, matching Codex keepalive owns CI/review. Merge Verify Closer owns
current-head expected checks, full review findings, seven-minute floor, guarded
merge and compare/chunk disposition. Issue1750 remains open for the broader initiative.

## Cache preservation follow-up (Python 3.14.7 runner)

The same nine cases now seed the real, isolated in-memory backend with manager
item, list and count entries using production cache keys. They compare all three
serialized values before and after each failed HTTP request. The invalidation spy
wraps the real function, so accidental invalidation actually removes these entries.
The tag-write case reads its current manager from the warmed production item
cache. Connection, schema and write spies verify that each designated failure
stage is reached exactly once and that earlier failures never attempt a write.
Acquired connections still close exactly once; acquisition failures never close
the unused connection double. Backend, metrics and environment patches restore
the prior state at teardown.

```sh
pytest tests/test_manager_api.py tests/test_manager_bulk_api.py tests/test_manager_cache.py tests/test_manager_write_failures.py -q -m "not slow" --cov=api.managers --cov-report=term-missing --cov-report=json:<report> --junitxml=<receipt>
# exit 0, 128 passed; api/managers.py: 821 statements, 147 missing, Cover 82%
```

This focused run covers 674/821 manager statements (82.0950060901%). It is a
targeted measurement, not a replacement for the historical full-suite coverage
comparison above. Coverage configuration, exclusions and the 75% floor remain
unchanged. The full suite was not rerun in this follow-up; its recorded result
remains NON_PASS with 24 existing UI authentication failures.

The changed test was formatted with Black at line length 100. The required
whole-repository check passed with exit 0 and 374 files unchanged:

```sh
PYTHONPATH=/tmp/manager-black-runtime black --check --line-length 100 --exclude '(\.workflows-lib|node_modules)' .
ruff check tests/test_manager_write_failures.py
git diff --check
```

Ordinary Black runs stalled in the sandbox's worker event loop with both Python
3.14 and 3.12. The temporary `sitecustomize.py` under that `PYTHONPATH` wraps
`asyncio.new_event_loop` to schedule an empty callback every 50 ms. This lets
Black's existing workers complete without changing formatter logic, rules or
checked files. Ruff and the diff check also passed with exit 0.

Each follow-up mutation changed actual production source and used a fresh
bytecode cache with the nine-case HTTP suite and `-m "not slow"`. The named
results and test-file digest are recorded in the companion JSON under
`cache_preservation_followup`.

| Actual source mutation | Failing cases |
| --- | ---: |
| Outage HTTP status 503 -> 500 | 9 / 9 |
| Remove acquired-connection cleanup | 6 / 9 |
| Invalidate manager cache during outage handling | 9 / 9 |
| Return private exception text as HTTP detail | 9 / 9 |
| Close each acquired connection twice | 6 / 9 |
| Byte-identical restored source | 0 / 9 |

Both cleanup mutations leave only the three connection-acquisition cases passing.
Restoration preserved the production SHA256 documented above. Raw follow-up
logs and XML receipts were generated under
`/tmp/manager-outage-evidence-ob5iage6`; the committed JSON preserves every node's
result independently of those temporary files.

### Verified acceptance criteria

- [x] Tests
  - [x] Added regression coverage for manager updates, tag changes, and deletions
    during database outages, including safe error responses and cache and
    connection handling (9 passing cases; all five source mutations detected).
- [x] Documentation
  - [x] Added test and mutation-test evidence documenting outage coverage,
    results, and coverage measurements (historical full-suite comparison plus
    follow-up named-node results and targeted coverage).

The PR checkboxes and open/ready state could not be updated or verified from this
runner: `gh pr view --json number,url,state,isDraft,body` failed to connect to
`api.github.com`. The `needs-human` label could not be applied for the same reason.
The checklist above records verified local acceptance only. Committing the test
and evidence changes was also blocked: `git add` could not create
`.git/index.lock` because this workspace mounts `.git` read-only. The requested
source/test commit remains required when Git metadata is writable.
