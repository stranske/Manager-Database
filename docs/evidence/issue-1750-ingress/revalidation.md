# Manager ingress acceptance revalidation

The original [coverage notes](README.md), archive and four ingress regressions
remain the evidence for this bounded chunk related to #1750. The follow-up adds
four automated evidence checks in `tests/test_manager_ingress_evidence.py`.
They verify raw archive contents rather than relying on the comparison summary.

Verified acceptance checklist:

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
