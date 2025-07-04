"""Simple diff tool for holdings CSV snapshots."""

from __future__ import annotations

import sys
import csv
from pathlib import Path


def load_rows(path: Path):
    with path.open() as f:
        reader = csv.DictReader(f)
        return {row['cusip']: row for row in reader}


def diff_holdings(current_csv: Path, prior_csv: Path):
    current = load_rows(current_csv)
    prior = load_rows(prior_csv)
    additions = current.keys() - prior.keys()
    exits = prior.keys() - current.keys()
    return additions, exits


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: diff_holdings.py current.csv prior.csv")
        sys.exit(1)
    adds, exits = diff_holdings(Path(sys.argv[1]), Path(sys.argv[2]))
    print("Additions:", ", ".join(adds))
    print("Exits:", ", ".join(exits))
