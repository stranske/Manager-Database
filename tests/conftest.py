"""Pytest configuration for Manager-Database tests.

This conftest.py automatically skips tests marked as `nightly` unless
explicitly requested via `-m nightly` or `--run-nightly`.
"""

import pytest


def pytest_configure(config):
    """Register the nightly marker and configure auto-skip."""
    config.addinivalue_line(
        "markers", "nightly: mark test as nightly regression test (skipped by default)"
    )


def pytest_collection_modifyitems(config, items):
    """Skip nightly tests unless explicitly requested."""
    # Check if nightly tests are explicitly requested
    if config.getoption("-m") and "nightly" in config.getoption("-m"):
        return  # User explicitly asked for nightly tests

    # Check for custom flag
    if hasattr(config.option, "run_nightly") and config.option.run_nightly:
        return

    skip_nightly = pytest.mark.skip(
        reason="Nightly test skipped (use -m nightly or --run-nightly to run)"
    )
    for item in items:
        if "nightly" in item.keywords:
            item.add_marker(skip_nightly)


def pytest_addoption(parser):
    """Add custom command line option for running nightly tests."""
    parser.addoption(
        "--run-nightly",
        action="store_true",
        default=False,
        help="Run nightly tests",
    )
