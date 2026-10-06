# Manager write outage evidence index

This is the bounded evidence follow-up to merged #1753, originating in #1750.
The original provider verdicts remain **CONCERNS**. Production and regression
test source are unchanged; the added Python verifier only reads retained receipts.

## Verified task records

- [x] Tests: both verification records below are complete.
- [x] Added verification records for manager updates, tag changes and deletions
  during connection, schema and write outages. All nine cases expect HTTP 503
  with `{"detail": "Database unavailable"}` and preserve the warmed item, list
  and count cache entries. The tests also check failure-stage execution and
  exactly-once release of acquired connections.
- [x] Recorded privacy and restored-source checks. The private-detail mutation
  fails all nine cases; the byte-identical restored source passes all nine.
  The current checkout replay also passes all nine using unchanged test bytes.
- [x] Documentation: the evidence index below is complete.
- [x] Added an evidence index with results, provenance and scope notes.

## Artifact index

| Artifacts | Provenance and observed result |
| --- | --- |
| [Baseline console](manager-baseline.log), [JUnit](manager-baseline.xml), [coverage](manager-baseline-coverage.json) | Original opener full-suite capture: 1787 passed, 24 failed, 21 skipped; 86.633593% statement coverage. |
| [Candidate console](manager-candidate.log), [JUnit](manager-candidate.xml), [coverage](manager-candidate-coverage.json) | Original opener full-suite capture: 1796 passed, 24 failed, 21 skipped; 86.654855% statement coverage. The same failing nodes and 114-file scope are retained. |
| `1753-<control>-RED.txt` and `.xml` | Independent closer semantic controls: outage-status 9 failures, no-close 6, cache-invalidation 9, private-detail 9, double-close 6, early-close 6, drop-retained-tags 1. Each ran all nine cases with exit 1 and no collection errors. |
| [Restored console](1753-restored-GREEN.txt), [JUnit](1753-restored-GREEN.xml), [control record](1753-controls.json) | Independent closer restored-source capture: nine passed. Original and restored source SHA256: `401a4591cd8965308f5ccffaf2d95144f2fd190a114e223cf3bccd77c0ff8245`. |
| [Current replay console](current-restored-GREEN.txt), [JUnit](current-restored-GREEN.xml) | Current checkout replay: nine passed, production bytes unchanged. Exact command, interpreter version, source and test hashes are in `current_verification` in the manifest. |
| [Verifier rejection records](verifier-checks.json) | Six CLI checks against temporary copies reject changed raw captures, conflicting provenance hashes, an unhashed privacy capture, changed test bytes, a collection-error RED and an incomplete passing case matrix. |
| [Manifest](manifest.json) | Artifact SHA256 values, capture provenance, historical counts and scopes, mutation dispositions, source restoration and current replay command. The two stale provenance hashes for historical restored GREEN were corrected to match the retained bytes. |

## Verification

From the repository root, use the local Python interpreter:

```sh
python docs/evidence/issue-1750-1753-raw-proof/verify.py
python -m pytest tests/test_manager_write_failures.py -q -m "not slow"
```

The read-only verifier checks every indexed artifact hash, consistency between
the two hash indexes, current source and test hashes, exact JUnit case identities
and outcomes, all seven semantic RED controls, restored GREEN, historical failure
sets and coverage scopes and denominators. It exits nonzero when evidence is
missing or inconsistent. Hash checks establish consistency with the manifest;
they do not independently authenticate who originally produced a capture.

The historical mutation scripts are retained for inspection. They edit source
temporarily and overwrite receipts if executed in place; use an isolated checkout
and retain its outputs separately when replaying them. The initial double-close
collection error was rejected; only the corrected six-assertion semantic failure
counts as RED. The read-only verifier does not execute these scripts.

## Scope limits

The nine HTTP cases use SQLite connection doubles and injected failures. They
establish response, cleanup and cache behavior, without a live PostgreSQL trial
or deployment claim. Historical full-suite failures remain 24 existing failures
in UI authentication. The broader 90% coverage initiative in #1750 remains open;
this follow-up neither raises that measurement nor claims a passing full suite.
