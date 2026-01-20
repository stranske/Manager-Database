import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from api.chat import metrics


def test_metrics_exposes_health_check_histogram():
    # Ensure Prometheus can scrape the health check duration histogram.
    response = metrics()
    assert response.status_code == 200
    assert "health_check_duration_seconds" in response.body.decode("utf-8")


# Commit-message checklist:
# - [ ] type is accurate (feat, fix, test)
# - [ ] scope is clear (health)
# - [ ] summary is concise and imperative
