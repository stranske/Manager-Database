"""Replay individual source mutations in a disposable checkout, never the live tree."""

import argparse
import difflib
import hashlib
import json
import os
import shlex
import subprocess
import sys
from pathlib import Path

CASES = [
    (
        "tests/test_edgar_flow.py",
        "test_manager_lookup_rejects_missing_or_deleted_manager",
        "etl/edgar_flow.py",
        "if not manager_cols:\n        return None",
        "if not manager_cols:\n        return 99",
        1,
    ),
    (
        "tests/test_edgar_flow.py",
        "test_watermark_unavailable_without_filing_history",
        "etl/edgar_flow.py",
        'if "filed_date" not in filing_columns or "manager_id" not in filing_columns:\n            return None',
        'if "filed_date" not in filing_columns or "manager_id" not in filing_columns:\n            return "2000-01-01"',
        1,
    ),
    (
        "tests/test_edgar_flow.py",
        "test_holdings_resolution_failure_preserves_raw_rows",
        "etl/edgar_flow.py",
        'except Exception:\n            logger.warning("Identifier resolution failed; inserting raw holdings", exc_info=True)',
        "except Exception:\n            raise",
        1,
    ),
    (
        "tests/test_edgar_flow.py",
        "test_empty_holdings_do_not_delete_existing_rows",
        "etl/edgar_flow.py",
        "if not rows:\n        return",
        "if rows is None:\n        return",
        1,
    ),
    (
        "tests/test_edgar_flow.py",
        "test_bitemporal_unowned_filing_rerun_preserves_version",
        "etl/edgar_flow.py",
        "if existing is not None and existing == digest:",
        'if existing == "never-matches":',
        1,
    ),
    (
        "tests/test_edgar_flow.py",
        "test_deletion_during_second_download_keeps_first_filing",
        "etl/edgar_flow.py",
        "return all_rows\n                manager_lock_id",
        "return []\n                manager_lock_id",
        1,
    ),
    (
        "tests/test_edgar_flow.py",
        "test_binary_filing_without_manager_skips_text_index",
        "etl/edgar_flow.py",
        'str(filing.get("accession") or "unknown")',
        'str(filing.get("accession") or "incorrect")',
        1,
    ),
    (
        "tests/test_issue_1806_coverage.py",
        "test_search_guards_return_before_accessing_database",
        "api/search.py",
        "if not query.strip() or limit <= 0:\n        return []",
        "if not query.strip() or limit <= 0:\n        return [None]",
        1,
    ),
    (
        "tests/test_issue_1806_coverage.py",
        "test_digest_delivery_configuration_guards",
        "etl/digest_flow.py",
        'error_message="DIGEST_EMAIL_FROM is not configured"',
        'error_message="DIGEST_EMAIL_FROM is not configured!"',
        1,
    ),
    (
        "tests/test_issue_1806_coverage.py",
        "test_daily_report_formatters_cover_empty_and_directional_values",
        "ui/daily_report.py",
        "if not url:\n        return text",
        'if not url:\n        return text + "!"',
        1,
    ),
    (
        "tests/test_issue_1888_coverage.py",
        "test_search_legacy_holdings_return_filing_groups_and_holdings",
        "api/search.py",
        'f"{row_count} holdings; filed',
        'f"{row_count + 1} holdings; filed',
        1,
    ),
    (
        "tests/test_issue_1888_coverage.py",
        "test_search_vector_documents_keep_shared_managers_and_deduplicate",
        "api/search.py",
        '", ".join(names)',
        '" / ".join(names)',
        1,
    ),
    (
        "tests/test_issue_1888_coverage.py",
        "test_search_postgres_activism_preserves_filing_context",
        "api/search.py",
        "f\"{filing_type or 'Activism filing'} | CUSIP:",
        "f\"{filing_type or 'Activism filing'} | Wrong:",
        2,
    ),
    (
        "tests/test_issue_1888_coverage.py",
        "test_conviction_annotation_survives_unavailable_holdings",
        "api/signals.py",
        "insider_net_direction=direction,",
        "insider_net_direction=None,",
        1,
    ),
    (
        "tests/test_issue_1888_coverage.py",
        "test_report_signal_query_failure_returns_empty_and_closes",
        "ui/daily_report.py",
        "except Exception:\n        rows = []",
        "except Exception:\n        raise",
        2,
    ),
    (
        "tests/test_edgar_flow.py",
        "test_watermark_without_source_column_is_scoped_to_manager",
        "etl/edgar_flow.py",
        'where = f"manager_id = {marker}"',
        'where = f"manager_id <> {marker}"',
        1,
    ),
    (
        "tests/test_edgar_flow.py",
        "test_upsert_without_raw_key_leaves_legacy_table_unchanged",
        "etl/edgar_flow.py",
        'if not insert_columns or "raw_key" not in insert_columns:\n            return 0',
        'if not insert_columns or "raw_key" not in insert_columns:\n            return 99',
        1,
    ),
    (
        "tests/test_edgar_flow.py",
        "test_upsert_ignored_by_trigger_returns_existing_id_or_zero",
        "etl/edgar_flow.py",
        "return int(existing[0]) if existing and existing[0] is not None else 0",
        "return 99",
        1,
    ),
    (
        "tests/test_edgar_flow.py",
        "test_holding_insert_without_recognized_columns_preserves_legacy_rows",
        "etl/edgar_flow.py",
        "if not insert_columns:\n        return",
        'if not insert_columns:\n        raise RuntimeError("schema rejected")',
        1,
    ),
    (
        "tests/test_edgar_flow.py",
        "test_postgres_upsert_preserves_storage_key",
        "etl/edgar_flow.py",
        "storage_key = EXCLUDED.storage_key",
        "storage_key = filings.storage_key",
        1,
    ),
    (
        "tests/test_edgar_flow.py",
        "test_reverse_order_original_keeps_amendment_current",
        "etl/edgar_flow.py",
        "if bitemporal and not is_authority:",
        "if bitemporal and is_authority:",
        1,
    ),
    (
        "tests/test_issue_1888_coverage.py",
        "test_report_page_sorts_value_and_filters_news_exports",
        "ui/daily_report.py",
        'if selected_topic != "All topics":',
        'if selected_topic == "All topics":',
        1,
    ),
    (
        "tests/test_issue_1888_coverage.py",
        "test_report_diff_unexpected_error_propagates_and_closes",
        "ui/daily_report.py",
        "conn.close()\n            raise",
        "raise",
        1,
    ),
    (
        "tests/test_issue_1888_coverage.py",
        "test_report_latest_date_ignores_missing_sources",
        "ui/daily_report.py",
        "continue\n            parsed = pd.to_datetime(value",
        "raise\n            parsed = pd.to_datetime(value",
        1,
    ),
    (
        "tests/test_issue_1888_coverage.py",
        "test_crowded_query_preserves_native_dates_and_tolerates_invalid_ids",
        "api/signals.py",
        "return []\n\n\ndef _manager_id_column",
        "return [1]\n\n\ndef _manager_id_column",
        1,
    ),
    (
        "tests/test_issue_1888_coverage.py",
        "test_search_sqlite_vector_failure_preserves_text_results",
        "api/search.py",
        "except Exception:\n                    vector_hits = []",
        "except Exception:\n                    raise",
        1,
    ),
    (
        "tests/test_issue_1888_coverage.py",
        "test_search_postgres_ignores_vector_hits_without_document_id",
        "api/search.py",
        "if doc_id is None:\n                continue",
        'if doc_id is None:\n                raise ValueError("missing document ID")',
        1,
    ),
]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--checkout", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--start-at", type=int, default=0)
    args = parser.parse_args()
    root = args.checkout.resolve()
    if not root.is_relative_to(Path("/tmp")):
        parser.error("mutation checkout must be disposable and under /tmp")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    env = dict(os.environ, PYTHONDONTWRITEBYTECODE="1")
    receipts = json.loads((output / "receipts.json").read_text()) if args.start_at else []
    assert len(receipts) == args.start_at
    for test_file, name, source, before, after, count in CASES[args.start_at :]:
        path = root / source
        original = path.read_text()
        assert original.count(before) == count, (name, original.count(before), count)
        mutant = original.replace(before, after)
        patch = "".join(
            difflib.unified_diff(
                original.splitlines(True),
                mutant.splitlines(True),
                fromfile="a/" + source,
                tofile="b/" + source,
            )
        )
        (output / f"{name}.diff").write_text(patch)
        command = [
            sys.executable,
            "-m",
            "pytest",
            f"{test_file}::{name}",
            "-m",
            "not slow",
            "--tb=short",
            "--disable-warnings",
        ]
        statuses = {}
        try:
            for phase in ("red", "green"):
                path.write_text(mutant if phase == "red" else original)
                result = subprocess.run(
                    command,
                    cwd=root,
                    env=env,
                    text=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    timeout=60,
                )
                log = (
                    f"cwd: {root}\nPYTHONPATH={env.get('PYTHONPATH', '')} "
                    f"PYTHONDONTWRITEBYTECODE=1 {shlex.join(command)}\n"
                    f"{result.stdout}\nexit: {result.returncode}\n"
                )
                (output / f"{name}-{phase}.txt").write_text(log)
                statuses[phase] = result.returncode
                if phase == "red":
                    assert result.returncode == 1 and f"::{name}" in result.stdout, log
                else:
                    assert result.returncode == 0, log
        finally:
            path.write_text(original)
        digest = hashlib.sha256(original.encode()).hexdigest()
        assert hashlib.sha256(path.read_bytes()).hexdigest() == digest
        receipts.append(dict(test=f"{test_file}::{name}", source=source, sha256=digest, **statuses))
        (output / "receipts.json").write_text(json.dumps(receipts, indent=2) + "\n")
        print(f"{name}: red={statuses['red']} green={statuses['green']}", flush=True)


if __name__ == "__main__":
    main()
