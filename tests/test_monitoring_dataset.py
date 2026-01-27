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
        dt.datetime.fromisoformat(row["timestamp"].replace("Z", "+00:00")) for row in rows
    ]
    assert timestamps == sorted(timestamps)
    assert (timestamps[-1] - timestamps[0]) >= dt.timedelta(hours=48)

    rss_values = [int(row["rss_kb"]) for row in rows]
    vms_values = [int(row["vms_kb"]) for row in rows]
    assert max(rss_values) > min(rss_values)
    assert max(vms_values) > min(vms_values)
