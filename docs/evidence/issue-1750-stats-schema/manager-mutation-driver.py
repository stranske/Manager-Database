import gzip
import hashlib
import json
import pathlib
import subprocess
import tempfile

repo = pathlib.Path(
    "/Users/teacher/.codex/automations/pd-workloop-resume/worktrees/manager-1750-next-write-boundaries"
)
out = pathlib.Path("/Users/teacher/.codex/automations/pd-workloop-resume/evidence/20261007T0501Z")
source = repo / "api/managers.py"
original = source.read_bytes()
test = "tests/test_manager_stats_schema_fallback.py"
receipts = []
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
for name, old, new, nodes in cases:
    assert original.count(old) == 1
    changed = original.replace(old, new)
    for node in nodes:
        argv = [
            "/opt/anaconda3/bin/python3",
            "-X",
            "pycache_prefix=" + tempfile.mkdtemp(prefix="mdb-proof-"),
            "-m",
            "pytest",
            test + "::" + node,
            "-q",
            "--junitxml="
            + str(out / ("manager-" + name + "-" + node.split("[")[-1].rstrip("]") + ".xml")),
        ]
        try:
            source.write_bytes(changed)
            red = subprocess.run(argv, cwd=repo, capture_output=True, text=True, timeout=120)
            (out / ("manager-" + node + "-red.log.gz")).write_bytes(
                gzip.compress((red.stdout + red.stderr).encode(), mtime=0)
            )
            assert red.returncode == 1, (node, red.stdout, red.stderr)
        finally:
            source.write_bytes(original)
        assert source.read_bytes() == original
        argv[2] = "pycache_prefix=" + tempfile.mkdtemp(prefix="mdb-proof-")
        green = subprocess.run(argv, cwd=repo, capture_output=True, text=True, timeout=120)
        (out / ("manager-" + node + "-green.log.gz")).write_bytes(
            gzip.compress((green.stdout + green.stderr).encode(), mtime=0)
        )
        assert green.returncode == 0, (node, green.stdout, green.stderr)
        receipts.append(
            {
                "case": node,
                "mutation": name,
                "old": old.decode(),
                "new": new.decode(),
                "argv": argv,
                "red_exit": red.returncode,
                "green_exit": green.returncode,
                "source_sha256": hashlib.sha256(original).hexdigest(),
                "mutant_sha256": hashlib.sha256(changed).hexdigest(),
                "restored_byte_identical": True,
            }
        )
        print(node, "RED", red.returncode, "GREEN", green.returncode, flush=True)
(out / "manager-mutations.json").write_text(json.dumps(receipts, indent=2) + "\n")
