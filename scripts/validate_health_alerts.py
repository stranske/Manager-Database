"""Validate health alert thresholds."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import yaml

_THRESHOLD_PATTERN = re.compile(r">\s*([0-9]+(?:\.[0-9]+)?)")


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
        if 'endpoint="health"' not in expr:
            continue
        thresholds = [float(match) for match in _THRESHOLD_PATTERN.findall(expr)]
        if any(value >= 0.5 for value in thresholds):
            return
    raise AssertionError(
        "Missing warning alert for /health with threshold greater than 500ms."
    )


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
