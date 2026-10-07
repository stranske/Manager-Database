"""Replay stats mutations using a portable checkout and fresh evidence directory."""

import argparse
import gzip
import hashlib
import json
import subprocess
import sys
import tempfile
from pathlib import Path

cases = [
    (
        "missing-column",
        b"if not missing_jurisdiction:",
        b"if True:",
        ["test_stats_current_schema_retries_missing_legacy_column"],
    ),
    (
        "legacy-fallback",
        b"if not jurisdiction_values and jurisdiction_raw is not None:",
        b"if False:",
        [
            "test_stats_legacy_jurisdiction_only_fills_empty_array[empty-array]",
            "test_stats_legacy_jurisdiction_only_fills_empty_array[empty-text]",
        ],
    ),
    (
        "unrelated-error",
        b"if not missing_jurisdiction:",
        b"if False:",
        ["test_stats_unrelated_database_error_is_not_retried"],
    ),
]


def main(repo, out):
    # Reserve before reading or mutating source; never replace historical evidence.
    out.mkdir(parents=True, exist_ok=False)
    source = repo / "api/managers.py"
    original = source.read_bytes()
    test = "tests/test_manager_stats_schema_fallback.py"
    receipts = []
    for name, old, new, nodes in cases:
        assert original.count(old) == 1
        changed = original.replace(old, new)
        for node in nodes:
            runs = {}
            for phase in ["red", "green"]:
                with tempfile.TemporaryDirectory(prefix="mdb-proof-") as cache:
                    argv = [
                        sys.executable,
                        "-X",
                        "pycache_prefix=" + cache,
                        "-m",
                        "pytest",
                        test + "::" + node,
                        "-q",
                        "--junitxml=" + str(out / (node + "-" + phase + ".xml")),
                    ]
                    try:
                        if phase == "red":
                            source.write_bytes(changed)
                        result = subprocess.run(
                            argv, cwd=repo, capture_output=True, text=True, timeout=120
                        )
                        (out / (node + "-" + phase + ".log.gz")).write_bytes(
                            gzip.compress((result.stdout + result.stderr).encode(), mtime=0)
                        )
                        assert result.returncode == (1 if phase == "red" else 0), (
                            node,
                            phase,
                            result.stdout,
                            result.stderr,
                        )
                        runs[phase] = {"argv": argv, "exit": result.returncode}
                    finally:
                        if phase == "red":
                            source.write_bytes(original)
                    assert source.read_bytes() == original
            receipts.append(
                {
                    "case": node,
                    "mutation": name,
                    "old": old.decode(),
                    "new": new.decode(),
                    "runs": runs,
                    "source_sha256": hashlib.sha256(original).hexdigest(),
                    "mutant_sha256": hashlib.sha256(changed).hexdigest(),
                    "restored_byte_identical": True,
                }
            )
            print(node, "RED", runs["red"]["exit"], "GREEN", runs["green"]["exit"], flush=True)
    (out / "manager-mutations.json").write_text(json.dumps(receipts, indent=2) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output", type=Path, required=True, help="New directory; existing paths are refused"
    )
    parser.add_argument("--repo", type=Path, default=Path(__file__).resolve().parents[3])
    args = parser.parse_args()
    main(args.repo.resolve(), args.output.resolve())
