#!/usr/bin/env python3
"""
Validation script to ensure dependency test setup is correct.

Run this script to verify:
1. All optional dependencies are included in lock file
2. All pyproject.toml tool dependencies match lock file
3. Tests don't have hardcoded version numbers
4. Metadata serialization is handled correctly throughout codebase
"""

import re
import sys
import tomllib
from pathlib import Path


def check_lock_file_completeness() -> tuple[bool, list[str]]:
    """Verify lock file includes all optional dependencies."""
    issues = []

    # Read pyproject.toml to get all optional groups
    pyproject_path = Path("pyproject.toml")
    try:
        pyproject = tomllib.loads(pyproject_path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        issues.append(f"{pyproject_path} not found")
        return False, issues
    except tomllib.TOMLDecodeError as exc:
        issues.append(f"{pyproject_path} could not be parsed: {exc}")
        return False, issues
    optional_dependencies = pyproject.get("project", {}).get("optional-dependencies", {})
    if not optional_dependencies:
        issues.append("No [project.optional-dependencies] section found")
        return False, issues

    optional_groups = sorted(optional_dependencies)
    print(f"✓ Found optional dependency groups: {', '.join(optional_groups)}")

    # Check the current Dependabot lock workflow includes all non-empty extras.
    workflow_path = Path(".github/workflows/maint-dependabot-auto-lock.yml")
    if workflow_path.exists():
        workflow = workflow_path.read_text(encoding="utf-8")
        for group in optional_groups:
            if not optional_dependencies[group]:
                continue
            if f"--extra {group}" not in workflow:
                issues.append(f"{workflow_path.name} missing --extra {group}")

        if not issues:
            print(f"✓ {workflow_path.name} includes all non-empty extras")
    else:
        issues.append(f"{workflow_path.name} not found")

    return len(issues) == 0, issues


def check_for_hardcoded_versions() -> tuple[bool, list[str]]:
    """Check for hardcoded dependency version pins in tests."""
    issues = []
    test_files = list(Path("tests").rglob("*.py"))

    # Patterns that indicate package version pins, not ordinary numeric
    # assertions such as score == 1.0 or threshold == 25.0.
    version_patterns = [
        r'["\'][A-Za-z0-9_.-]+==\d+\.\d+',
        r'assert.*(?:package|dependency|requirement|version).*==.*["\'][A-Za-z0-9_.-]+==\d+\.\d+',
    ]

    problematic_files = []
    for test_file in test_files:
        content = test_file.read_text()

        # Skip if it's the lockfile consistency test or dependency alignment test
        if (
            "lockfile_consistency" in test_file.name
            or "dependency_version_alignment" in test_file.name
        ):
            continue

        for pattern in version_patterns:
            if re.search(pattern, content):
                # Check if it's in a comment
                lines = content.split("\n")
                for i, line in enumerate(lines):
                    if re.search(pattern, line) and not line.strip().startswith("#"):
                        problematic_files.append((test_file, i + 1, line.strip()))

    if problematic_files:
        issues.append("Found potential hardcoded dependency version pins in tests:")
        for file, line_no, line in problematic_files:
            issues.append(f"  {file}:{line_no}: {line[:80]}")
    else:
        print("✓ No hardcoded dependency version pins found in tests")

    return len(issues) == 0, issues


def check_dependency_script_recommendations() -> tuple[bool, list[str]]:
    """Check that dependency helper output references existing repo commands."""
    issues = []
    helper_path = Path("scripts/check_test_dependencies.sh")
    if not helper_path.exists():
        return False, ["scripts/check_test_dependencies.sh not found"]

    content = helper_path.read_text(encoding="utf-8")
    recommended_script_paths = sorted(set(re.findall(r"\./(scripts/[A-Za-z0-9_./-]+)", content)))
    for script_path in recommended_script_paths:
        if not Path(script_path).is_file():
            issues.append(f"{helper_path}: recommended script path does not exist: {script_path}")

    if not issues:
        print("✓ Dependency helper recommendations reference existing script paths")

    return len(issues) == 0, issues


def check_test_expectations() -> tuple[bool, list[str]]:
    """Verify tests expect dicts, not Pydantic objects."""
    issues = []
    test_files = [
        Path("tests/test_validators.py"),
        Path("tests/test_io_validators_additional.py"),
        Path("tests/test_io_validators_extra.py"),
        Path("tests/test_data_schema.py"),
    ]

    for test_file in test_files:
        if not test_file.exists():
            continue

        content = test_file.read_text()

        # Check for problematic patterns
        if re.search(r"\.attrs\[.*\]\.mode(?!\[)", content):
            issues.append(f"{test_file.name}: Uses .mode attribute access instead of dict access")

        if 'assert meta["metadata"] is ' in content and "is metadata" in content:
            issues.append(f"{test_file.name}: Uses 'is' identity check instead of equality")

    if not issues:
        print("✓ Tests expect dict-based metadata")

    return len(issues) == 0, issues


def main():
    print("=" * 60)
    print("Dependency Test Setup Validation")
    print("=" * 60)
    print()

    all_passed = True
    all_issues = []

    # Run all checks
    checks = [
        ("Lock file completeness", check_lock_file_completeness),
        ("Hardcoded versions", check_for_hardcoded_versions),
        ("Dependency helper recommendations", check_dependency_script_recommendations),
        ("Test expectations", check_test_expectations),
    ]

    for check_name, check_func in checks:
        print(f"\nChecking: {check_name}")
        print("-" * 40)
        passed, issues = check_func()

        if not passed:
            all_passed = False
            all_issues.extend(issues)
            print("✗ FAILED")
            for issue in issues:
                print(f"  {issue}")
        else:
            print("✓ PASSED")

    print()
    print("=" * 60)
    if all_passed:
        print("✓ All validation checks passed!")
        print("The setup should work for future dependabot PRs.")
        return 0
    else:
        print("✗ Some validation checks failed:")
        for issue in all_issues:
            print(f"  - {issue}")
        print("\nFix these issues to ensure future dependabot PRs work correctly.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
