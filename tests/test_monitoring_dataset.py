import csv
import datetime as dt
from pathlib import Path


def test_monitoring_dataset_has_additional_samples() -> None:
    path = Path(__file__).resolve().parents[1] / "monitoring" / "memory_usage.csv"
    with path.open() as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)

    assert len(rows) >= 55

    timestamps = [
        dt.datetime.fromisoformat(row["timestamp"].replace("Z", "+00:00"))
        for row in rows
    ]
    assert timestamps == sorted(timestamps)
    assert (timestamps[-1] - timestamps[0]) >= dt.timedelta(hours=48)
