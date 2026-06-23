"""Download the vendored Pyodide wheel closure used by the offline stlite demo.

Wheels under ``web/vendor/pyodide/v0.27.3/full/`` are committed so the browser
demo boots offline with zero network access. This script regenerates or updates
those wheels reproducibly from the Pyodide lock file.
"""

from __future__ import annotations

import json
import sys
import urllib.error
import urllib.request
from collections import deque
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
VENDOR_DIR = REPO_ROOT / "web" / "vendor" / "pyodide" / "v0.27.3" / "full"
LOCK_PATH = VENDOR_DIR / "pyodide-lock.json"
CDN_BASE = "https://cdn.jsdelivr.net/pyodide/v0.27.3/full"
SEED_PACKAGES = ("micropip", "packaging", "sqlite3")


def _load_packages(lock_path: Path) -> dict[str, dict[str, object]]:
    lock = json.loads(lock_path.read_text(encoding="utf-8"))
    packages = lock.get("packages")
    if not isinstance(packages, dict):
        raise SystemExit(f"invalid pyodide lock file: missing packages map in {lock_path}")
    return packages


def _package_names_for_vendored_wheels(
    packages: dict[str, dict[str, object]],
    vendor_dir: Path,
) -> set[str]:
    file_to_package = {
        str(meta["file_name"]): name
        for name, meta in packages.items()
        if isinstance(meta, dict) and meta.get("file_name")
    }
    seeded = set(SEED_PACKAGES)
    for wheel_path in vendor_dir.glob("*.whl"):
        package_name = file_to_package.get(wheel_path.name)
        if package_name:
            seeded.add(package_name)
    return seeded


def _package_aliases(packages: dict[str, dict[str, object]]) -> dict[str, str]:
    aliases: dict[str, str] = {}
    for key, meta in packages.items():
        aliases[key] = key
        aliases[key.replace("-", "_")] = key
        package_name = meta.get("name")
        if isinstance(package_name, str):
            aliases[package_name] = key
            aliases[package_name.replace("-", "_")] = key
    return aliases


def _resolve_package_name(name: str, aliases: dict[str, str]) -> str:
    resolved = aliases.get(name) or aliases.get(name.replace("_", "-"))
    if resolved is None:
        raise SystemExit(f"package {name!r} missing from pyodide lock file")
    return resolved


def _transitive_closure(packages: dict[str, dict[str, object]], seed: set[str]) -> set[str]:
    aliases = _package_aliases(packages)
    needed: set[str] = set()
    queue: deque[str] = deque()
    for package_name in seed:
        resolved = _resolve_package_name(package_name, aliases)
        if resolved not in needed:
            needed.add(resolved)
            queue.append(resolved)
    while queue:
        package_name = queue.popleft()
        meta = packages[package_name]
        if not isinstance(meta, dict):
            raise SystemExit(f"package {package_name!r} has invalid metadata in lock file")
        depends = meta.get("depends", [])
        if not isinstance(depends, list):
            raise SystemExit(f"package {package_name!r} has invalid depends entry in lock file")
        for dependency in depends:
            resolved = _resolve_package_name(str(dependency), aliases)
            if resolved not in needed:
                needed.add(resolved)
                queue.append(resolved)
    return needed


def _download(url: str, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    request = urllib.request.Request(url, headers={"User-Agent": "md-stlite-fetch-offline-runtime/1.0"})
    with urllib.request.urlopen(request) as response:
        destination.write_bytes(response.read())


def fetch_offline_runtime(*, vendor_dir: Path = VENDOR_DIR) -> tuple[list[str], list[str]]:
    """Download missing Pyodide lock artifacts into ``vendor_dir``."""
    lock_path = vendor_dir / "pyodide-lock.json"
    if not lock_path.is_file():
        raise SystemExit(f"missing pyodide lock file: {lock_path}")

    packages = _load_packages(lock_path)
    seed = _package_names_for_vendored_wheels(packages, vendor_dir)
    needed = _transitive_closure(packages, seed)

    downloaded: list[str] = []
    skipped: list[str] = []
    for package_name in sorted(needed):
        meta = packages[package_name]
        file_name = str(meta["file_name"])
        destination = vendor_dir / file_name
        if destination.exists() and destination.stat().st_size > 0:
            skipped.append(file_name)
            continue
        url = f"{CDN_BASE}/{file_name}"
        try:
            _download(url, destination)
        except urllib.error.URLError as exc:
            raise SystemExit(f"failed to download {url}: {exc}") from exc
        downloaded.append(file_name)
    return downloaded, skipped


def main(argv: list[str] | None = None) -> int:
    argv = argv or sys.argv[1:]
    vendor_dir = Path(argv[0]).resolve() if argv else VENDOR_DIR
    downloaded, skipped = fetch_offline_runtime(vendor_dir=vendor_dir)
    print(f"Pyodide vendor dir: {vendor_dir}")
    print(f"Downloaded {len(downloaded)} file(s)")
    for file_name in downloaded:
        print(f"  + {file_name}")
    print(f"Skipped {len(skipped)} existing file(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
