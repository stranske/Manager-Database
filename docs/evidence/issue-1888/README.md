# Issue 1888 coverage progress

The runtime diff is test-only. Seven EDGAR test functions (14 parametrized cases)
exercise missing/deleted managers, unavailable watermarks, identifier-resolution
failure with raw persistence, empty holdings, bitemporal idempotency, deletion
between downloads, and binary unowned filings. Five additional functions (six
cases) exercise legacy holdings search, vector/shared-manager document results,
PostgreSQL activism context, conviction annotations without holdings, and report
query failure cleanup. A separate search guard function adds one case. Two existing
functions now assert exact digest configuration errors and escaped headlines.

## Selection and CI baseline blocker

Current main: `a7f4e626bb4b8359a64c56a56211218e41b6020a`. The working branch starts
at `e873f629381aaa37f019c52c496cce8a75403040`; its only difference from main is the
agent bootstrap markdown. Runtime and existing test bytes are identical.

Main push CI [41702885246](https://github.com/stranske/Manager-Database/actions/runs/41702885246)
succeeded. Its Python 3.13 artifact is `gate-coverage-3.13-1`, ID `13265760076`,
SHA-256 `0ad415416368e7718da8435c0c541d7ea66aa8e02f70812701b83032b193475a`.
The GitHub connector successfully returned a download reference, but the shell
could not transfer the ZIP (`URLError`, network-restricted sandbox). Neither its
JSON contents nor an exact CI uncovered-unit base has been verified. The historical
issue counts (26 EDGAR, 52 search, 35 report, 27 signals) are not relabeled as current
main measurements. The artifact-baseline checkbox remains open.

`ranking.json` records a provisional ranking using the repository's documented
500-source-commit repair-subject proxy (`fix|bug|repair|regress|recover`), then source
touches, then local uncovered units. The proxy is not a verified escaped-defect
count. Within the scoped targets the resulting order is EDGAR (19/41), daily report
(4/14), signals (3/10), search (2/9), digest (0/4), ASIC (0/2), where each pair is
repair-subject matches / source touches. Artifact-based ranking remains incomplete.

## Local whole-suite measurement, not CI artifact credit

Python 3.14.9, branch coverage, supported runtime source scope. Commands, from the
original archived tree and from the working tree respectively:

```sh
PYTHONPATH=/tmp/issue1888-runtime timeout 300 pytest tests/ --cov=etl.edgar_flow --cov=api.search --cov=api.signals --cov=ui.daily_report --cov-branch --cov-config=pyproject.toml --cov-report=json:/tmp/issue1888-main-local-full.json --cov-report=term-missing --cov-fail-under=0 -m 'not slow and not nightly'
PYTHONPATH=/tmp/issue1888-runtime timeout 300 pytest tests/ --cov=etl.edgar_flow --cov=api.search --cov=api.signals --cov=ui.daily_report --cov-branch --cov-config=pyproject.toml --cov-report=json:/tmp/issue1888-candidate-local.json --cov-report=term-missing --cov-fail-under=0 -m 'not slow and not nightly'
```

The original checkout was created with `git archive HEAD | tar -x -C
/tmp/issue1888-baseline`. The archived tree lacks Git metadata, causing four extra
baseline failures in replay/hygiene tests. Both runs also fail the same existing
chat dependency timing test, EDGAR parser home-directory write, and WASM server
socket binding. Consequently these are local progress measurements, not matched
successful Gate runs. Full outcomes are in `pytest-summary.txt`:

- Original: **7 failed, 2581 passed, 26 skipped, 2 deselected**.
- Candidate: **3 failed, 2606 passed, 26 skipped, 2 deselected**. The 25-pass increase is 21 new cases plus four Git-dependent cases that pass in the real checkout.
- Focused changed modules: **58 passed, 1 existing nightly skip**.
- Final affected suites: **130 passed, 1 existing nightly skip**; targeted branch coverage is search 95%, signals 94%, EDGAR 94%, daily report 92% (see `targeted-pytest.txt`).

`local-main-coverage.json` and `local-candidate-coverage.json` retain the measured
missing lines and arcs. Unit = missing statements + missing branch arcs:

| Target | Main local | Candidate local | Reduction |
| --- | ---: | ---: | ---: |
| EDGAR | 16+16 = 32 | 9+7 = 16 | 16 |
| Daily report | 20+15 = 35 | 16+15 = 31 | 4 |
| Signals | 9+4 = 13 | 4+2 = 6 | 7 |
| Search | 25+27 = 52 | 10+12 = 22 | 30 |
| Total | 132 | 75 | 57 |

No ~180-unit reduction is claimed. Across these four targets the measured local
base contains only 132 uncovered units, so a reduction of 180 from current main is
not attainable within them. The requested CI comparison remains open; adding
unrelated targets would exceed this round's scope.

Remaining target shortfalls (line numbers refer to unchanged runtime files):

- EDGAR: SQLite schema-without-raw-key guard (198); insert-returning fallback
  (217, 220); PostgreSQL empty returning row (227); holdings schema without accepted
  columns (288); reverse-order lower-authority history (371–372); absent-source
  watermark filtering (391→394); script entry point (534–536). Some are testable
  compatibility cases still pending; running the actual scheduled entry point
  requires external dependencies. None is falsely declared impossible.
- Search: optional offline imports (28–31), zero-rank and blank-score guards (67,
  93), PostgreSQL vector hits without IDs (506), SQLite database-list and vector
  failure recovery (660–661, 668–669), plus remaining optional-schema/query branches
  listed in the JSON. PostgreSQL SQL execution is modeled, not validated against a
  live server. Legacy search and vector deduplication account for the major gain;
  complete branch elimination is not claimed.
- Signals: native-date conversion (92), non-list JSON manager IDs (158), optional
  FastAPI-import fallback (398–399) and associated branch arcs remain. Annotation
  behavior is verified without changing scores or requiring external data.
- Daily report: unexpected diff-view failure (42–43), PostgreSQL news query (66),
  whitespace URL (196), missing report source iteration (295), login stop (349),
  empty-date rendering (364–368), total-value sort (489), and news filtering
  (534, 539–540), plus entry point (569). This round verifies both signal-query
  failure paths close their connection; broader page interaction remains pending.

## Mutation proof

`mutations/receipts.json` lists all 13 new functions and both strengthened functions.
Every function was run against a real source edit (exit 1) and then the same command
against restored source (exit 0), including all parametrized cases. The exact source
diffs and complete command/output captures are adjacent `.diff`, `-red.txt`, and
`-green.txt` files. Original/restored source SHA-256 values are recorded. The runner
uses `finally` to restore source and only permits a disposable checkout under `/tmp`.

```sh
mkdir -p /tmp/issue1888-mutations
git archive HEAD | tar -x -C /tmp/issue1888-mutations
cp tests/test_edgar_flow.py tests/test_issue_1806_coverage.py tests/test_issue_1888_coverage.py /tmp/issue1888-mutations/tests/
PYTHONPATH=/tmp/issue1888-runtime python docs/evidence/issue-1888/mutations/replay.py --checkout /tmp/issue1888-mutations --output /tmp/issue1888-proof
```

The original run resumed at case 12 after correcting a mutation selector's expected
occurrence count (the activism snippet occurs in both database implementations).
The first 12 successful pairs were retained; the final three were verified with
`--start-at 12`. All 15 receipts are successful. The two existing strengthened
functions are included beyond the strict new-function requirement.

## Environment, formatting, and cache inspection

The sandbox prevents selector event loops from being awakened by worker threads.
A standalone `asyncio.run(asyncio.to_thread(print, 'thread callback works'))`
reproduction hangs without the temporary runtime shim. The following temporary
`/tmp/issue1888-runtime/sitecustomize.py` schedules a periodic wakeup; no repository
runtime code, validation, networking restriction, or security control is changed:

```python
import asyncio

_original_run_forever = asyncio.BaseEventLoop.run_forever


def _run_forever_with_poll(self):
    def poll():
        self.call_later(0.01, poll)

    handle = self.call_later(0.01, poll)
    try:
        return _original_run_forever(self)
    finally:
        handle.cancel()


asyncio.BaseEventLoop.run_forever = _run_forever_with_poll
```

Using that environment, `black --check --line-length 100 --exclude
'(\.workflows-lib|node_modules)' .` passes (451 files unchanged). Ruff passes on
all changed Python files. Focused coverage explicitly uses the requested `not slow`
marker; the existing `nightly` skip is retained. No test is disabled or modified to
turn an existing failure into a pass.

The touched runtime surfaces were inspected for `cache.get(key) is None` negative
caching. None occurs. Search's `deduped.get(key)` only stores concrete `SearchResult`
objects, so `None` correctly means absent. No production bug or production edit is
claimed.

Local `git add` fails because `.git/index.lock` cannot be created on the read-only
Git metadata mount. The GitHub connection also rejects tree creation, PR updates, labels, and comments
because each requires approval while approval policy is `never`. No commit or PR
update was created. `pr-body.md` preserves the prepared replacement body, including
all mutation outputs and only verified task checkmarks. `source.patch` preserves
the three test-file changes for application in a writable checkout. No protected
workflow is changed.

## EDGAR follow-up verification (2026-11-04)

Six further test functions (10 cases) cover reverse-order original/amendment
history, watermarks on schemas without `source`, schemas without supported insert
columns, SQLite triggers that suppress `RETURNING` rows, and PostgreSQL storage-key
SQL generation including empty returned rows. The PostgreSQL check uses a mock;
it does not claim live PostgreSQL validation. There is no production change.

```sh
pytest tests/test_edgar_flow.py --cov=etl.edgar_flow --cov-branch --cov-config=pyproject.toml --cov-report=term-missing --cov-report=json:/tmp/issue1888-edgar-after.json --cov-fail-under=0 -m 'not slow' --tb=short --disable-warnings
pytest tests/test_edgar_flow.py tests/test_ingest_flow.py tests/test_issue_1806_coverage.py tests/test_issue_1888_coverage.py -m 'not slow' --tb=short --disable-warnings
pytest tests/test_bitemporal_holdings.py -m 'not slow' --tb=short --disable-warnings
```

Results: **47 passed, 1 skipped**; **99 passed, 1 skipped**; and **20 passed,
1 skipped**, respectively. All skips are existing nightly tests. Python 3.14.8.
The targeted EDGAR coverage table reports **98%** (236 statements, 5 missing;
72 branches, 1 missing). The identical EDGAR-only command before these additions
reported **94%** (12 missing statements and 7 missing branches). The measured local
uncovered-unit reduction is **19 to 6 = 13**, with raw JSON in
`edgar-followup-before-coverage.json` and `edgar-followup-after-coverage.json`.
These are targeted local measurements, not whole-suite or CI artifact credit.
The previously listed schema, fallback, reverse-order and watermark gaps are now
covered. Remaining misses in this focused run are log proxy forwarding (74, 77–78)
and the scheduled CLI entry point (534, 536, arc 533→534). The latter is integration
wiring; these tests exercise ingestion directly without starting a live scheduler.

All six functions failed against an actual source edit and passed after restoring
the original bytes. Receipts 16–21, their exact diffs, and complete output are in
`mutations/`. To replay only these additions after replaying cases 1–15:

```sh
python docs/evidence/issue-1888/mutations/replay.py --checkout /tmp/issue1888-edgar-mutations --output /tmp/issue1888-edgar-proof --start-at 15
```

The disposable checkout was produced by extracting `git archive HEAD`, then copying
in the updated `tests/test_edgar_flow.py`. All original/restored EDGAR source hashes
match. No `cache.get(key) is None` negative-cache pattern occurs in the tested
EDGAR module. Ruff passes for both changed Python files. Black formatting and the
required repository check pass (452 unchanged), using the previously documented
`/tmp/issue1888-runtime` event-loop wakeup shim. No tests were disabled or skipped
beyond the repository's existing nightly configuration. The CI baseline/ranking,
~180-unit CI comparison, and CI-pytest acceptance checkboxes remain open.

### Follow-up publication blocker

The follow-up `git add tests/test_edgar_flow.py docs/evidence/issue-1888` failed:
`Unable to create .git/index.lock: Read-only file system`. No commit can be created
in this checkout. Updating PR #1889 with this prepared body was rejected with:
`MCP tool call requires approval, but approval policy is never`. The live PR was
verified open with draft=false; its body has not been updated. The baseline,
CI-artifact reduction and CI pytest acceptance checks remain open. The local
verified checkmarks in this prepared body are not a claim of live PR publication.

## Search, signal, and report follow-up (2026-11-04)

Six new test functions (11 cases) extend `tests/test_issue_1888_coverage.py`.
The report page uses the existing SQLite seed and real loaders to verify total-value
sorting, topic filtering, the no-matching-news message, and displayed/exported rows.
Additional checks cover unexpected diff-query errors with connection cleanup, missing
report sources, native PostgreSQL dates with malformed manager IDs, SQLite vector or
database-list failures preserving text results, and PostgreSQL vector hits without
an identifier. PostgreSQL is modeled; there is no live-server validation or
production-code change. The inspected search, signals, report and EDGAR files have
no `cache.get(key) is None` negative-cache pattern. Search's `deduped.get(key)` stores
only concrete `SearchResult` instances.

Matched targeted comparison at `9c4c8ac` before and after these additions, Python
3.14.10, using the temporary event-loop shim documented above:

```sh
PYTHONPATH=/tmp/issue1888-runtime pytest tests/test_issue_1888_coverage.py tests/test_daily_report.py tests/test_daily_report_default_date.py tests/test_daily_report_views.py tests/test_search.py tests/test_signals_api.py --cov=api.search --cov=api.signals --cov=ui.daily_report --cov-branch --cov-config=pyproject.toml --cov-report=term-missing --cov-report=json:/tmp/issue1888-boundaries-before.json --cov-fail-under=0 -m 'not slow' --tb=short --disable-warnings
PYTHONPATH=/tmp/issue1888-runtime pytest tests/test_issue_1888_coverage.py tests/test_daily_report.py tests/test_daily_report_default_date.py tests/test_daily_report_views.py tests/test_search.py tests/test_signals_api.py --cov=api.search --cov=api.signals --cov=ui.daily_report --cov-branch --cov-config=pyproject.toml --cov-report=term-missing --cov-report=json:/tmp/issue1888-boundaries-after.json --cov-fail-under=0 -m 'not slow' --tb=short --disable-warnings
```

The first command ran in an archived checkout of `9c4c8ac`; the second ran in the
working checkout. Results: **64 passed** before and **75 passed** after. Raw JSON
and terminal output are `boundaries-{before,after}-coverage.json` and
`boundaries-{before,after}-pytest.txt`.

| Target | Before missing statements+branches | After | Reduction | After coverage |
| --- | ---: | ---: | ---: | ---: |
| Search | 13+15=28 | 8+12=20 | 8 | 95% |
| Signals | 38+21=59 | 33+18=51 | 8 | 84% |
| Daily report | 65+31=96 | 14+15=29 | 67 | 92% |
| Total | 183 | 100 | 83 | 91% |

These targeted numbers are local, incremental measurements, not CI artifact or
whole-suite credit. They must not be added to earlier deltas. In particular, the
signals module has additional tests outside this targeted selection.

Every new function has a real source mutation and restoration recorded in receipts
22–27: all red exits are 1 and all restored exits are 0. Exact source diffs, full
commands/output, and original/restored hashes are in `mutations/`. The replay command
ran after copying the changed test module into the disposable archived checkout:

```sh
PYTHONPATH=/tmp/issue1888-runtime python docs/evidence/issue-1888/mutations/replay.py --checkout /tmp/issue1888-boundaries --output docs/evidence/issue-1888/mutations --start-at 21
```

The CI-artifact baseline remains blocked: `gh run download 41702885246 -n
gate-coverage-3.13-1 -D /tmp/issue1888-main-gate` returned `error connecting to
api.github.com`. The GitHub connector verified that the artifact is unexpired and
belongs to main SHA `a7f4e626bb4b8359a64c56a56211218e41b6020a`, but its content
remains unverified. No CI baseline/ranking, ~180-unit reduction, or CI-pytest
checkbox is marked complete.

### Matched full-suite regression and coverage check

The complete suite was run on `9c4c8ac` in a writable local clone and on the working
candidate, with Python 3.14.10 and the same temporary event-loop shim:

```sh
PYTHONPATH=/tmp/issue1888-runtime pytest tests/ --cov=etl.edgar_flow --cov=api.search --cov=api.signals --cov=ui.daily_report --cov-branch --cov-config=pyproject.toml --cov-report=term-missing --cov-report=json:/tmp/issue1888-boundaries-full-before.json --cov-fail-under=0 -m 'not slow' --tb=short --disable-warnings
PYTHONPATH=/tmp/issue1888-runtime pytest tests/ --cov=etl.edgar_flow --cov=api.search --cov=api.signals --cov=ui.daily_report --cov-branch --cov-config=pyproject.toml --cov-report=term-missing --cov-report=json:/tmp/issue1888-boundaries-full.json --cov-fail-under=0 -m 'not slow' --tb=short --disable-warnings
```

Before: **3 failed, 2616 passed, 28 skipped**. After: **3 failed, 2627 passed,
28 skipped**. Both commands exit 1. The failures are identical: the existing chat
dependency test gets `timeout` instead of `dependency down`; edgartools cannot
create `/home/runner/.edgar` on the read-only filesystem; the WASM static-route test
cannot create a server socket. Existing tests were not disabled or changed.
Complete terminal output and raw JSON are in `boundaries-full-{before,after}-pytest.txt`
and `boundaries-full-{before,after}-coverage.json`.

| Target | Before missing statements+branches | After | Reduction | After coverage |
| --- | ---: | ---: | ---: | ---: |
| Search | 10+12=22 | 5+9=14 | 8 | 97% |
| Signals | 4+2=6 | 2+0=2 | 4 | 99% |
| EDGAR | 2+1=3 | 2+1=3 | 0 | 99% |
| Daily report | 16+15=31 | 6+8=14 | 17 | 96% |
| Total | 62 | 33 | 29 | 98% |

This is an incremental local comparison against the previous PR head, not current
main's CI artifact baseline. It does not establish the requested ~180-unit CI
reduction. All new tests pass; the broader search/signals/report implementation task
is now checked in the prepared body. The artifact baseline/ranking, CI reduction,
and CI pytest acceptance items remain open.

Remaining local misses: search retains optional-import handling, blank/zero-score
helper guards and schema/null-result branches; signals retains only the optional
FastAPI-import error fallback; EDGAR retains scheduled CLI entry wiring; daily report
retains PostgreSQL news-query construction, whitespace URLs, latest-date query-error
propagation, login-stop wiring, manager-count sorting, and entry-point/iteration
arcs. These are residual compatibility/wiring paths, not claims of untestability.
Report value-sort/topic-filter behavior accounts for the largest gain in this round.

Black's required repository check passes (452 files unchanged); Ruff passes for
both changed Python files. PR #1889 is open and `draft=false`. The PR-body update,
`needs-human` label, and blocker comment were all rejected by the tool approval check:
`MCP tool call requires approval, but approval policy is never`. The main checkout
also rejects staging because `.git/index.lock` is on a read-only filesystem. The
prepared PR body and source changes remain in this workspace for publication.


The source commit is preserved in the writable isolated checkout
`/tmp/issue1888-comparison` (commit subject: `test: cover report exports and search
fallback boundaries`). Its 75 affected tests and the required repository-wide
Black check pass before committing. The primary checkout retains the same file
changes, but its branch cannot be advanced while Git metadata is read-only. Exact
terminal captures and mutation diffs retain their original output whitespace.
