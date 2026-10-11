# Manager ingress acceptance revalidation

The original [coverage notes](README.md), archive and four ingress regressions
remain the evidence for this bounded chunk related to #1750. The follow-up adds
four automated evidence checks in `tests/test_manager_ingress_evidence.py`.
They verify raw archive contents rather than relying on the comparison summary.

Verified acceptance checklist:

- [x] **Bug Fixes**
  - [x] Oversized bulk imports stop receiving at the first chunk that crosses
    the configured byte limit, before storage access.
- [x] **Tests**
  - [x] Regression coverage protects invalid bulk-size configuration, empty CSV,
    invalid patch requests and oversized bodies with malformed Content-Length.
  - [x] The archived run records four new passing cases and six additional
    covered manager lines, with every prior test outcome unchanged.
- [x] **Documentation**
  - [x] Coverage notes include reproduction commands, archived test evidence,
    mutation replay results and limitations.
- [x] **Developer Setup**
  - [x] Starlette is present in the optional `dev` dependencies in
    `pyproject.toml`; installed Starlette 1.7.0 successfully runs the regressions.

The evidence checks validate the archive SHA256, all member hashes, reviewed
source/test/replay snapshots, the four focused JUnit outcomes, every baseline
and candidate JUnit outcome, unchanged coverage denominators and exclusions,
the six newly covered lines, and all eight named mutation phases. Each RED
phase must contain its exact intended assertion; GREEN must record the original
source hash. The three current checkout bindings were also separately verified.
The automated checks bind the historical snapshots, allowing future production
changes without requiring the historical archive to be rewritten.

## Reproduce the focused verification

```bash
python -m pytest tests/test_manager_ingress_boundaries.py tests/test_manager_ingress_evidence.py \
  --cov=api.managers --cov-report=term-missing --cov-fail-under=0 \
  -m "not slow" -o addopts= -q
black --check --line-length 100 --exclude '(\.workflows-lib|node_modules)' .
python -m ruff check tests/test_manager_ingress_boundaries.py \
  tests/test_manager_ingress_evidence.py \
  docs/evidence/issue-1750-ingress/replay_ingress_boundaries.py
git diff --check
```

On Python 3.14.8, pytest 9.1.1, pytest-cov 7.1.0 and coverage 7.16.2, the focused
command passes all eight cases. Its manager-only coverage table shows 21%
(175/821 statements); this intentionally narrow run does not measure the
repository-wide coverage target. The command-level `--cov-fail-under=0` permits
that focused measurement without changing repository coverage configuration.

The historical full-suite result remains 1,827 -> 1,831 passing cases, 24
unchanged UI-auth failures and 21 skips; both archived commands exited 1.
Historical coverage remains 86.7818513836213% -> 86.82399213372665%. The four
new evidence checks are separate from those historical counts. No fresh
full-suite pass or repository-wide 90% completion is claimed.

Black 26.10.0's threaded check stalled in this sandbox. Its single-file
`reformat_one` check verified all 391 discovered Python files and populated a
cache in `/tmp/manager-ingress-black-cache`. The required full CLI command then
passed with exit 0 using `BLACK_CACHE_DIR=/tmp/manager-ingress-black-cache`.
Focused Ruff 0.16.10 and whitespace checks also passed.

GitHub API access failed from this runner, so the PR body checkboxes and current
ready-for-review state could not be inspected or updated. This local checklist
records verified implementation criteria, without claiming hosted checks passed.

The workspace's `.git` directory is read-only, so committing on its branch is
blocked by creation of `.git/index.lock`. The follow-up commit is prepared in
an isolated checkout under `/tmp`, with an applyable patch exported separately.

## Streaming follow-up (2026-10-11)

Reconciled recent commits `0bb9fcf`, `c6ad920`, `87af211` and `2aa02eb` against
the acceptance criteria. The production streaming fix already exists. New
`tests/test_manager_ingress_streaming.py` adds 22 app-level cases covering JSON
and CSV, absent/malformed/underdeclared Content-Length, first-chunk oversize,
cumulative oversize, and a byte exactly at the limit followed by one extra byte.
It verifies that remaining chunks are unread and storage is never opened for
oversized requests. Exact-limit UTF-8 payloads successfully import into SQLite;
declared oversize is rejected without receiving any request body.

```bash
python -m pytest tests/test_manager_ingress_streaming.py \
  tests/test_manager_ingress_boundaries.py tests/test_manager_ingress_evidence.py \
  tests/test_manager_ingress_review.py --cov=api.managers \
  --cov-report=term-missing --cov-fail-under=0 -m "not slow" -o addopts= -q
python -m pytest tests/test_manager_bulk_api.py -m "not slow" -o addopts= -q
python -m ruff check tests/test_manager_ingress_streaming.py
git diff --check
```

The focused ingress/evidence command passes **34 cases**. The current
`api/managers.py` coverage table shows **36%** (294/826 statements); this narrow
measurement does not replace historical repository-wide coverage. Existing
bulk API tests pass **56 cases**. A broader bulk/OpenAPI/rate-contract command
stalled after 60 cases and was interrupted; no full-run pass is claimed.
Focused Ruff and whitespace checks pass. Black passes the required full CLI
check across **393 Python files**. As in the earlier revalidation, sequential
`reformat_one` checks populated `/tmp/manager-ingress-streaming-black-cache`
before the full command passed with that `BLACK_CACHE_DIR`; no source files
were changed by this check.

The GitHub connector successfully confirmed PR #1762 is open and ready for
review (`draft=false`), but rejected the reconciled PR-body update with
`MCP tool call requires approval, but approval policy is never`. The checked
acceptance criteria above therefore remain a local reconciliation pending a
permitted PR-body update. No protected workflow or repository configuration
was edited. The original archive and review-repair manifest remain unchanged;
their bindings identify their historical snapshots.

## Final-chunk follow-up (2026-10-11)

Reviewed the six recent commits through `6bd4bac` and reverified all 34 existing
focused ingress/evidence cases. The review-repair archive SHA256, every member
hash and all three current source/test/replay bindings also match. All seven
acceptance checkboxes in the local checklist above are verified. The PR-body
reconciliation was attempted before further edits, but the connector returned
`MCP tool call requires approval, but approval policy is never`; the remote
checkbox update remains blocked. PR #1762 was confirmed open with `draft=false`.

Six additional cases in `tests/test_manager_ingress_streaming.py` cover a final
ASGI body chunk that exceeds the byte limit by exactly one byte, while the
UTF-8 character count fits. JSON and CSV bodies split within a UTF-8 character
are rejected before storage for absent, malformed and character-count
Content-Length values. These tests also verify the cumulative-byte warning.
In an isolated checkout, allowing one extra byte makes all six tests fail
with the storage sentinel (exit 1); byte-identical source restoration makes
all six pass (exit 0).

```bash
python -m pytest tests/test_manager_ingress_streaming.py \
  tests/test_manager_ingress_boundaries.py tests/test_manager_ingress_evidence.py \
  tests/test_manager_ingress_review.py tests/test_manager_bulk_api.py \
  --cov=api.managers --cov-report=term-missing --cov-fail-under=0 -m "not slow"
```

This command passes **96 cases**. The targeted coverage table reports
`api/managers.py`: **826 statements, 424 missed, 49% covered**. It is a focused
measurement; the historical matched-suite counts and repository-wide coverage
remain unchanged. Focused Ruff and whitespace checks pass. Sequential Black
checks verified all 393 Python files and populated a fresh cache before the
required full Black CLI check passed; no formatting exclusions were changed.

The working checkout still cannot create `.git/index.lock` because its `.git`
directory is read-only. The source/test commit is prepared in a writable
checkout under `/tmp`, with an exported patch. Neither the working branch nor
the PR head is claimed to contain that commit. Protected files and historical
evidence archives remain unchanged.
