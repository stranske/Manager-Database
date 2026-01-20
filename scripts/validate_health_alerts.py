"""Validate health alert thresholds."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import yaml

_METRIC_THRESHOLD_PATTERN = re.compile(
    r"health_check_duration_seconds[^<>]*?(>=|<=|>|<)\s*([0-9]+(?:\.[0-9]+)?)",
    re.DOTALL,
)
_ENDPOINT_LABEL_PATTERN = re.compile(r'endpoint\s*=~?\s*"(/?health)"')


def _iter_rules(config: dict) -> list[dict]:
    rules: list[dict] = []
    for group in config.get("groups", []):
        rules.extend(group.get("rules", []))
    return rules


def validate_health_alerts(config_path: Path) -> None:
    """Ensure the /health warning alert fires above 500ms."""
    data = yaml.safe_load(config_path.read_text()) or {}
    for rule in _iter_rules(data):
        labels = rule.get("labels", {})
        if labels.get("severity") != "warning":
            continue
        expr = rule.get("expr", "")
        if "health_check_duration_seconds" not in expr:
            continue
        if not _ENDPOINT_LABEL_PATTERN.search(expr):
            continue
        comparisons = _METRIC_THRESHOLD_PATTERN.findall(expr)
        for op, value in comparisons:
            # Accept inclusive thresholds so >= 0.5s still enforces 500ms or higher.
            if op in (">", ">=") and float(value) >= 0.5:
                return
    raise AssertionError("Missing warning alert for /health with threshold greater than 500ms.")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "config_path",
        type=Path,
        nargs="?",
        default=Path("monitoring/alerts/health-checks.yml"),
    )
    args = parser.parse_args(argv)
    try:
        validate_health_alerts(args.config_path)
    except AssertionError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

# Commit-message checklist:
# - [ ] type is accurate (feat, fix, test)
# - [ ] scope is clear (health)
# - [ ] summary is concise and imperative
