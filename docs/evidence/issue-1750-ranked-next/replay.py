"""Replay actual source mutations; reserve fresh output and always restore bytes."""

import argparse
import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    root = Path(__file__).resolve().parents[3]
    source = root / "api/managers.py"
    original = source.read_bytes()
    start = original.index(b"async def get_similar_managers(")
    end = original.index(b"@router.patch(", start)
    function = original[start:end].decode("utf-8")
    cases = []
    for invalid in ["jaccard", "cosine"]:
        for basis in ["jaccard", "cosine"]:
            cases.append(
                (
                    "test_similarity_discards_nonfinite_rows_before_limit["
                    + invalid
                    + "-"
                    + basis
                    + "]",
                    "value is None or math.isfinite(float(value))",
                    "True",
                )
            )
    cases.extend(
        [
            (
                "test_similarity_database_outage_is_sanitized_and_connection_closed[connect]",
                "_raise_db_unavailable(exc)",
                "raise HTTPException(status_code=500, detail=str(exc))",
            ),
            (
                "test_similarity_database_outage_is_sanitized_and_connection_closed[query]",
                "conn.close()",
                "pass  # deliberately omit acquired connection cleanup",
            ),
        ]
    )
    receipt = {"source_sha256": hashlib.sha256(original).hexdigest(), "records": []}
    env = {**os.environ, "PYTHONDONTWRITEBYTECODE": "1"}
    try:
        for index, (node, before, after) in enumerate(cases):
            if function.count(before) != 1:
                raise RuntimeError("Mutation anchor must be unique: " + node)
            mutated = (
                original[:start] + function.replace(before, after).encode("utf-8") + original[end:]
            )
            for phase, data, expected in [("red", mutated, 1), ("green", original, 0)]:
                source.write_bytes(data)
                # Invalidate cached bytecode even on filesystems with coarse mtimes.
                for cached in (root / "api/__pycache__").glob("managers.*.pyc"):
                    cached.unlink()
                stem = str(index) + "-" + phase
                argv = [
                    sys.executable,
                    "-m",
                    "pytest",
                    "tests/test_manager_similarity_boundaries.py::" + node,
                    "-q",
                    "-o",
                    "addopts=",
                    "--junitxml=" + str(output / (stem + ".xml")),
                ]
                record = {
                    "node": node,
                    "phase": phase,
                    "argv": argv,
                    "cwd": str(root),
                    "mutation_before": before,
                    "mutation_after": after,
                    "phase_source_sha256": hashlib.sha256(data).hexdigest(),
                }
                receipt["records"].append(record)
                with (output / (stem + ".txt")).open("wb") as stream:
                    result = subprocess.run(
                        argv,
                        cwd=root,
                        env=env,
                        stdout=stream,
                        stderr=subprocess.STDOUT,
                        timeout=180,
                        check=False,
                    )
                record["exit_code"] = result.returncode
                if result.returncode != expected:
                    raise RuntimeError("Unexpected phase exit: " + node + " " + phase)
    finally:
        source.write_bytes(original)
        receipt["restored_sha256"] = hashlib.sha256(source.read_bytes()).hexdigest()
        (output / "controls.json").write_text(
            json.dumps(receipt, indent=2) + "\n", encoding="utf-8"
        )
    if len(receipt["records"]) != 12:
        raise RuntimeError("Incomplete mutation replay")


if __name__ == "__main__":
    main()
