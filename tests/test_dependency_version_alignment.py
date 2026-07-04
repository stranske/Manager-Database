"""Ensure the lock file captures every dependency declared in pyproject.toml."""

from __future__ import annotations

import tomllib
from pathlib import Path

_OPERATORS = ("==", ">=", "<=", "~=", "!=", ">", "<", "===")
_ACTIVE_INSTALL_SOURCE_GLOBS = (
    ".github/workflows/*.yml",
    "README*.md",
    "docs/**/*.md",
)
_STALE_REQUIREMENTS_INSTALL = "pip install -r requirements.txt"


def _split_spec(raw: str) -> str:
    entry = raw.strip().strip(",").strip('"')
    # Direct references ("name @ git+https://...") declare the package name before
    # the " @ "; there is no version operator to split on.
    if " @ " in entry:
        name, _ = entry.split(" @ ", 1)
        return name.strip().split("[")[0]
    for operator in _OPERATORS:
        if operator in entry:
            name, _ = entry.split(operator, 1)
            return name.strip().split("[")[0]
    return entry.strip().split("[")[0]


def _load_lock_versions(path: Path) -> dict[str, str]:
    versions: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if stripped.startswith("--"):
            continue
        # Direct references in the lock ("name @ git+https://...") have no pinned
        # "==" version; record them so the presence check still passes.
        if " @ " in stripped:
            name, _ = stripped.split(" @ ", 1)
            versions[name.strip().lower()] = "<direct-reference>"
            continue
        if "==" not in stripped:
            continue
        name, version = stripped.split("==", 1)
        versions[name.lower()] = version
    return versions


def test_all_pyproject_dependencies_are_in_lock() -> None:
    pyproject = tomllib.loads(Path("pyproject.toml").read_text(encoding="utf-8"))
    project = pyproject.get("project", {})

    declared = set()
    for entry in project.get("dependencies", []):
        declared.add(_split_spec(entry).lower())

    for group in project.get("optional-dependencies", {}).values():
        for entry in group:
            declared.add(_split_spec(entry).lower())

    # Packages intentionally excluded from the lock via uv's no-emit-package
    # (monorepo deps consumed from an unpinned @main git URL, e.g.
    # app-baseline-kit) are not expected to be pinned in requirements.lock.
    no_emit = {
        _split_spec(name).lower()
        for name in pyproject.get("tool", {})
        .get("uv", {})
        .get("pip", {})
        .get("no-emit-package", [])
    }
    declared -= no_emit

    lock_versions = _load_lock_versions(Path("requirements.lock"))

    missing = []
    for dependency in sorted(declared):
        normalised = dependency.replace("-", "_")
        if dependency not in lock_versions and normalised not in lock_versions:
            missing.append(dependency)

    assert not missing, "requirements.lock is missing pinned versions for: " + ", ".join(missing)


def test_active_install_instructions_do_not_use_legacy_requirements_txt() -> None:
    offenders: list[str] = []

    for pattern in _ACTIVE_INSTALL_SOURCE_GLOBS:
        for path in Path(".").glob(pattern):
            if not path.is_file():
                continue
            for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
                if _STALE_REQUIREMENTS_INSTALL in line:
                    offenders.append(f"{path}:{line_number}")

    assert not offenders, (
        "Active workflows/docs must install from pyproject.toml, not legacy "
        f"requirements.txt: {', '.join(offenders)}"
    )
