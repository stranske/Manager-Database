# Parser Regression Snapshots

Manager-Database keeps retained filing snapshots under
`tests/fixtures/filing_snapshots/` so parser changes can be checked without
live EDGAR, Companies House, SEDAR, or other network calls.

## Current Coverage

- `tests/fixtures/filing_snapshots/edgar_13f_prior_snapshot.xml` is a redacted
  EDGAR 13F XML snapshot used by
  `tests/test_parser_snapshot_regression.py`.
- `.github/workflows/nightly.yml` runs the parser snapshot regression test path
  explicitly in the nightly lane.

## Adding Snapshots

1. Save snapshots in `tests/fixtures/filing_snapshots/` with a source and form
   type in the filename, for example `edgar_13f_<case>.xml`.
2. Keep the fixture small and deterministic. Include only the fields needed to
   prove the parser contract.
3. Redact manager names, issuer names, account identifiers, URLs, signatures,
   emails, phone numbers, and any non-public notes before committing.
4. Add or extend a parser regression test that reads the fixture from disk and
   asserts the parsed structured rows.
5. Mark the regression with `@pytest.mark.nightly` when it should run in the
   nightly lane.

The parser snapshot tests must not perform live network requests. Network-backed
adapter coverage belongs in separate integration tests with explicit credentials
or service fixtures.
