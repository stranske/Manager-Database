import hashlib
import json
import pathlib
import re
import subprocess
import tempfile

p = pathlib.Path(tempfile.mkdtemp(prefix="1753-replay-"))
print("Replay output:", p)
w = pathlib.Path(__file__).resolve().parents[3]
s = w / "api/managers.py"
original = s.read_bytes()
raw = original.decode()
out = []
start = raw.index("async def patch_manager(")
prefix = raw[:start]
tail = raw[start:]


def replace_exact(text, old, new, expected):
    actual = text.count(old)
    if actual != expected:
        raise ValueError(f"mutation anchor expected {expected} matches, found {actual}")
    return text.replace(old, new)


def double_close(text):
    changed, count = re.subn(r"(?m)^( +)conn.close\(\)$", r"\1conn.close()\n\1conn.close()", text)
    if count != 10:
        raise ValueError(f"double-close expected 10 matches, found {count}")
    return changed


mutations = {
    "outage-status": replace_exact(
        raw,
        'status_code=503, detail="Database unavailable"',
        'status_code=500, detail="Database unavailable"',
        1,
    ),
    "no-close": replace_exact(raw, "            conn.close()", "            pass", 10),
    "cache-invalidation": replace_exact(
        raw,
        '    logger.exception("Database error in managers API.", exc_info=exc)',
        '    invalidate_cache_prefix("managers")\n    logger.exception("Database error in managers API.", exc_info=exc)',
        1,
    ),
    "private-detail": replace_exact(
        raw, 'status_code=503, detail="Database unavailable"', "status_code=503, detail=str(exc)", 1
    ),
    "double-close": double_close(raw),
}
try:
    for name, text in mutations.items():
        assert text != raw
        s.write_text(text)
        with tempfile.TemporaryDirectory(prefix="1753-cache-") as cache:
            r = subprocess.run(
                [
                    "/opt/anaconda3/bin/python3",
                    "-X",
                    "pycache_prefix=" + cache,
                    "-m",
                    "pytest",
                    "tests/test_manager_write_failures.py",
                    "-q",
                    "-o",
                    "addopts=",
                    "--junitxml=" + str(p / f"1753-{name}-RED.xml"),
                ],
                cwd=w,
                text=True,
                capture_output=True,
            )
        (p / f"1753-{name}-RED.txt").write_text(r.stdout + r.stderr)
        out.append({"mutation": name, "returncode": r.returncode})
        assert r.returncode == 1
finally:
    s.write_bytes(original)
assert s.read_bytes() == original
r = subprocess.run(
    [
        "/opt/anaconda3/bin/python3",
        "-m",
        "pytest",
        "tests/test_manager_write_failures.py",
        "-q",
        "-o",
        "addopts=",
        "--junitxml=" + str(p / "1753-restored-GREEN.xml"),
    ],
    cwd=w,
    text=True,
    capture_output=True,
)
(p / "1753-restored-GREEN.txt").write_text(r.stdout + r.stderr)
out.append(
    {
        "restored": True,
        "sha256": hashlib.sha256(s.read_bytes()).hexdigest(),
        "returncode": r.returncode,
    }
)
(p / "1753-original-five-controls.json").write_text(json.dumps(out, indent=2))
print(json.dumps(out))
assert r.returncode == 0
