# Issue 1888 evidence index

Checked **2026-11-10**. PR #1889 merged at `2026-11-10T15:50:24Z` as
`48ef1e750fbbf6fafe9938b094a3f1ceed579ae8`. All merged tests, runtime files, pins and archived evidence remain unchanged.

The single current evidence account is [pr-body.md](pr-body.md): per-function commands and red/green
excerpts for **37 added functions / 39 receipts**, six-target artifact accounting, exact-head CI,
residual statement/branch lists, and disposition of all six review findings. Tables and acceptance
claims are maintained there once, so this index cannot diverge by duplicating them.

The reproduced four-target CI comparison is **129 → 13**, reduction **116**; including digest and ASIC
makes **133 → 17**, reduction **116**. The approximately-180 criterion is **accepted at 116 by owner
disposition** ([2026-11-11](https://github.com/stranske/Manager-Database/issues/1910#issuecomment-6802405647)). `ranking.json` retains historical nulls for digest/ASIC;
the current evidence account fills them from downloaded CI JSON and corrects its erroneous 130/117 totals to 129/116 without editing historical data.
No local incremental deltas are credited. The historical candidate is distinct from the final merged test head.

This replaces accumulated attempt-by-attempt status, obsolete open-PR/publication-blocker statements,
duplicate measurements, and partial replay instructions. The previous documents remain available at
[the merged README](https://github.com/stranske/Manager-Database/blob/48ef1e750fbbf6fafe9938b094a3f1ceed579ae8/docs/evidence/issue-1888/README.md) and [the merged prepared body](https://github.com/stranske/Manager-Database/blob/48ef1e750fbbf6fafe9938b094a3f1ceed579ae8/docs/evidence/issue-1888/pr-body.md).
Their historical local failures do not invalidate the successful final-head Gate run.

Raw receipts, source diffs, mutation logs, local measurements and replay script are immutable records here.
Use the current account's pinned links to inspect them. The documentation check does not replay mutations;
archived hosted-runtime commands require their original environment and must not be run as macOS commands.

Publication is controller-owned: prepared evidence is complete only after replacing PR #1889's body and
reading it back. The evidence-link finding is obsolete; the coverage-criterion finding remains substantive.
The body records these states without treating an outdated flag as resolution.

Branch audit: the only matching local `closer*/issue-1888` branch was the provisioned
`closer-r1/issue-1888`, initially identical to current `origin/main` at `48ef1e750fbbf6fafe9938b094a3f1ceed579ae8`.
There were no unmerged prior repair commits to integrate. Kept all #1889 tests/evidence; discarded only
superseded/duplicated prose and partial replay instructions in these two Markdown files.
