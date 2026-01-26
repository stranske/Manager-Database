import datetime as dt
import importlib.util
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "scripts" / "prepare_memory_review.py"


def _load_prepare_memory_review():
    spec = importlib.util.spec_from_file_location("prepare_memory_review", SCRIPT_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError("Failed to load prepare_memory_review module")
    module = importlib.util.module_from_spec(spec)
    import sys

    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


prepare_memory_review = _load_prepare_memory_review()


def test_build_review_includes_stability_and_checklist():
    base_time = dt.datetime(2024, 1, 2, tzinfo=dt.UTC)
    # Build a flat RSS series after warmup to confirm stability output.
    samples = [
        prepare_memory_review.analyze_memory.MemorySample(
            timestamp=base_time + dt.timedelta(hours=hour),
            rss_kb=200,
            vms_kb=400,
            pid=11,
        )
        for hour in range(4)
    ]

    report = prepare_memory_review.build_review(
        samples,
        min_hours=2.0,
        warmup_hours=1.0,
        max_slope_kb_per_hour=1.0,
        pid=11,
    )

    assert "# Memory Leak Fix Review" in report
    assert "stable_after_warmup: true" in report
    assert "## Review Checklist" in report


def test_build_review_requires_minimum_duration():
    base_time = dt.datetime(2024, 1, 2, tzinfo=dt.UTC)
    samples = [
        prepare_memory_review.analyze_memory.MemorySample(
            timestamp=base_time + dt.timedelta(hours=hour),
            rss_kb=100 + hour,
            vms_kb=200,
            pid=3,
        )
        for hour in range(2)
    ]

    with pytest.raises(ValueError, match="Insufficient duration"):
        prepare_memory_review.build_review(
            samples,
            min_hours=4.0,
            warmup_hours=1.0,
            max_slope_kb_per_hour=5.0,
            pid=3,
        )


# Commit-message checklist:
# - [ ] type is accurate (feat, fix, test)
# - [ ] scope is clear (memory)
# - [ ] summary is concise and imperative
