from __future__ import annotations

import sqlite3
from pathlib import Path

from etl.activism_detection import (
    ALERT_EVENT_TYPE,
    AlertEvent,
    detect_events,
    ensure_activism_events_table,
    ensure_alert_tables,
    event_payload,
    fire_alerts_for_event,
    insert_activism_events,
)


def _setup_db(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE TABLE managers (manager_id INTEGER PRIMARY KEY, name TEXT NOT NULL, cik TEXT)"
    )
    conn.execute("INSERT INTO managers(manager_id, name, cik) VALUES (1, 'Elliott', '0001791786')")
    conn.execute("""CREATE TABLE activism_filings (
            filing_id INTEGER PRIMARY KEY AUTOINCREMENT,
            manager_id INTEGER NOT NULL,
            filing_type TEXT NOT NULL,
            subject_company TEXT NOT NULL,
            subject_cusip TEXT,
            ownership_pct REAL,
            shares INTEGER,
            group_members TEXT,
            purpose_snippet TEXT,
            filed_date TEXT NOT NULL,
            url TEXT NOT NULL,
            raw_key TEXT
        )""")
    ensure_activism_events_table(conn)
    ensure_alert_tables(conn)
    conn.commit()
    return conn


def _insert_filing(
    conn: sqlite3.Connection,
    *,
    filing_type: str,
    filed_date: str,
    ownership_pct: float,
    subject_cusip: str = "037833100",
    group_members: str | None = None,
    subject_company: str = "Apple Inc.",
) -> dict[str, object]:
    cursor = conn.execute(
        """INSERT INTO activism_filings(
            manager_id, filing_type, subject_company, subject_cusip, ownership_pct, shares,
            group_members, purpose_snippet, filed_date, url, raw_key
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            1,
            filing_type,
            subject_company,
            subject_cusip,
            ownership_pct,
            1000,
            group_members,
            "Purpose text",
            filed_date,
            "https://www.sec.gov/Archives/test.txt",
            f"raw/{filed_date}-{filing_type}.txt",
        ),
    )
    assert cursor.lastrowid is not None
    filing_id = int(cursor.lastrowid)
    conn.commit()
    return {
        "filing_id": filing_id,
        "manager_id": 1,
        "filing_type": filing_type,
        "subject_company": subject_company,
        "subject_cusip": subject_cusip,
        "ownership_pct": ownership_pct,
        "group_members": group_members,
        "filed_date": filed_date,
    }


def _event_types(events: list) -> list[str]:
    return [event.event_type for event in events]


def test_detect_events_initial_stake(tmp_path):
    conn = _setup_db(tmp_path / "activism.db")
    try:
        filing = _insert_filing(
            conn, filing_type="SC 13D", filed_date="2024-05-01", ownership_pct=4.0
        )

        events = detect_events(conn, filing)

        assert _event_types(events) == ["initial_stake"]
    finally:
        conn.close()


def test_detect_events_ignores_non_finite_ownership(tmp_path):
    conn = _setup_db(tmp_path / "activism.db")
    try:
        filing = _insert_filing(
            conn,
            filing_type="SC 13D",
            filed_date="2024-05-01",
            ownership_pct=float("inf"),
        )

        events = detect_events(conn, filing)

        assert "initial_stake" in _event_types(events)
        assert "threshold_crossing" not in _event_types(events)
        assert all(event.ownership_pct is None for event in events)
    finally:
        conn.close()


def test_detect_events_rejects_invalid_threshold_env(tmp_path, monkeypatch):
    monkeypatch.setenv("ACTIVISM_THRESHOLDS", "5,inf,25")
    conn = _setup_db(tmp_path / "activism.db")
    try:
        filing = _insert_filing(
            conn,
            filing_type="SC 13D",
            filed_date="2024-05-01",
            ownership_pct=6.0,
        )

        try:
            detect_events(conn, filing)
        except ValueError as exc:
            assert "non-finite numeric value" in str(exc)
        else:
            raise AssertionError("invalid ACTIVISM_THRESHOLDS should fail fast")
    finally:
        conn.close()


def test_detect_events_stake_increase(tmp_path):
    conn = _setup_db(tmp_path / "activism.db")
    try:
        _insert_filing(conn, filing_type="SC 13D", filed_date="2024-05-01", ownership_pct=5.1)
        filing = _insert_filing(
            conn, filing_type="SC 13D", filed_date="2024-05-03", ownership_pct=7.3
        )

        events = detect_events(conn, filing)

        assert "stake_increase" in _event_types(events)
        increase = next(event for event in events if event.event_type == "stake_increase")
        assert increase.previous_pct == 5.1
        assert increase.delta_pct == 2.2
    finally:
        conn.close()


def test_detect_events_stake_decrease(tmp_path):
    conn = _setup_db(tmp_path / "activism.db")
    try:
        _insert_filing(conn, filing_type="SC 13D", filed_date="2024-05-01", ownership_pct=12.0)
        filing = _insert_filing(
            conn, filing_type="SC 13D", filed_date="2024-05-03", ownership_pct=8.5
        )

        events = detect_events(conn, filing)

        assert "stake_decrease" in _event_types(events)
        decrease = next(event for event in events if event.event_type == "stake_decrease")
        assert decrease.previous_pct == 12.0
        assert decrease.delta_pct == -3.5
    finally:
        conn.close()


def test_detect_events_threshold_crossings_in_both_directions(tmp_path):
    conn = _setup_db(tmp_path / "activism.db")
    try:
        _insert_filing(conn, filing_type="SC 13D", filed_date="2024-05-01", ownership_pct=4.9)
        upward = _insert_filing(
            conn, filing_type="SC 13D", filed_date="2024-05-02", ownership_pct=5.2
        )
        upward_events = detect_events(conn, upward)
        upward_thresholds = [
            event.threshold_crossed
            for event in upward_events
            if event.event_type == "threshold_crossing"
        ]
        assert upward_thresholds == [5.0]

        _insert_filing(conn, filing_type="SC 13D", filed_date="2024-05-03", ownership_pct=10.5)
        downward = _insert_filing(
            conn, filing_type="SC 13D", filed_date="2024-05-04", ownership_pct=9.8
        )
        downward_events = detect_events(conn, downward)
        downward_thresholds = [
            event.threshold_crossed
            for event in downward_events
            if event.event_type == "threshold_crossing"
        ]
        assert downward_thresholds == [10.0]
    finally:
        conn.close()


def test_detect_events_multiple_threshold_crossings(tmp_path):
    conn = _setup_db(tmp_path / "activism.db")
    try:
        _insert_filing(conn, filing_type="SC 13D", filed_date="2024-05-01", ownership_pct=4.0)
        filing = _insert_filing(
            conn, filing_type="SC 13D", filed_date="2024-05-02", ownership_pct=11.0
        )

        events = detect_events(conn, filing)
        thresholds = sorted(
            event.threshold_crossed for event in events if event.event_type == "threshold_crossing"
        )

        assert thresholds == [5.0, 10.0]
    finally:
        conn.close()


def test_detect_events_form_upgrade_group_and_amendment(tmp_path):
    conn = _setup_db(tmp_path / "activism.db")
    try:
        _insert_filing(conn, filing_type="SC 13G", filed_date="2024-05-01", ownership_pct=5.2)
        filing = _insert_filing(
            conn,
            filing_type="SC 13D/A",
            filed_date="2024-05-03",
            ownership_pct=6.1,
            group_members="Elliott|Blue Pool",
        )

        events = detect_events(conn, filing)

        assert {"form_upgrade", "group_formation", "amendment"}.issubset(set(_event_types(events)))
    finally:
        conn.close()


def test_detect_events_multiple_events_from_single_initial_filing(tmp_path):
    conn = _setup_db(tmp_path / "activism.db")
    try:
        filing = _insert_filing(
            conn,
            filing_type="SC 13D",
            filed_date="2024-05-01",
            ownership_pct=11.0,
            group_members="Elliott|Blue Pool",
        )

        events = detect_events(conn, filing)

        assert "initial_stake" in _event_types(events)
        assert "group_formation" in _event_types(events)
        thresholds = sorted(
            event.threshold_crossed for event in events if event.event_type == "threshold_crossing"
        )
        assert thresholds == [5.0, 10.0]
    finally:
        conn.close()


# -------------------------------------------------------------------------------------------
# group_formation must fire on the TRANSITION into a group, not on group presence alone.
#
# `detect_events` judges every other event type against the prior filing: stake_increase and
# stake_decrease compare ownership deltas, form_upgrade/form_downgrade compare filing types.
# group_formation was the odd one out - it fired whenever the CURRENT filing merely listed group
# members, with no comparison to what the prior filing looked like. An activist campaign group
# routinely stays intact across many amendments over months or years, and every one of those
# amendments carries the same non-empty group_members field. Each one re-reported "a group has
# formed" for a group that has existed since the very first filing, which would fire a spurious
# "new group formed" alert (see ui/alerts.py's group_formation alert subtype) on every routine
# amendment of an ongoing campaign.
# -------------------------------------------------------------------------------------------


def test_group_formation_does_not_refire_for_an_already_known_group(tmp_path):
    """THE BUG: an unchanged, pre-existing group was re-reported as newly formed.

    Before the fix, `group_formation` fired whenever the current filing's `group_members` was
    non-empty, with no check on the prior filing. A years-long activist campaign amends its
    original 13D repeatedly without ever changing its group membership, so this fired on every
    single amendment - misreporting a stale, already-known group as brand new each time.
    """
    conn = _setup_db(tmp_path / "activism.db")
    try:
        initial = _insert_filing(
            conn,
            filing_type="SC 13D",
            filed_date="2024-05-01",
            ownership_pct=11.0,
            group_members="Elliott|Blue Pool",
        )
        initial_events = detect_events(conn, initial)
        # Sanity check: the group is genuinely new here, so formation SHOULD fire once.
        assert "group_formation" in _event_types(initial_events)

        amendment = _insert_filing(
            conn,
            filing_type="SC 13D/A",
            filed_date="2024-11-01",
            ownership_pct=11.0,
            group_members="Elliott|Blue Pool",
        )
        amendment_events = detect_events(conn, amendment)

        assert "group_formation" not in _event_types(amendment_events)
        # The amendment is still correctly detected as an event - just not as a new group.
        assert "amendment" in _event_types(amendment_events)
    finally:
        conn.close()


def test_group_formation_fires_when_a_solo_filer_first_discloses_a_group(tmp_path):
    """The transition this event type exists to catch, isolated from amendment/form churn.

    Two ordinary (non-amendment) 13D filings for the same manager and company: the first files
    alone, the second discloses a group for the first time. That is a genuine formation and must
    still be reported.
    """
    conn = _setup_db(tmp_path / "activism.db")
    try:
        solo = _insert_filing(
            conn, filing_type="SC 13D", filed_date="2024-05-01", ownership_pct=6.0
        )
        solo_events = detect_events(conn, solo)
        assert "group_formation" not in _event_types(solo_events)

        joined = _insert_filing(
            conn,
            filing_type="SC 13D",
            filed_date="2024-06-01",
            ownership_pct=6.0,
            group_members="Elliott|Blue Pool",
        )
        joined_events = detect_events(conn, joined)

        assert "group_formation" in _event_types(joined_events)
    finally:
        conn.close()


def test_group_formation_does_not_fire_when_an_existing_groups_membership_changes(tmp_path):
    """A deliberate scope boundary, recorded rather than left to accident.

    `ACTIVISM_EVENT_TYPES` has no event for "group composition changed" - only for a group
    coming into being. A new co-filer joining an ALREADY-formed group is therefore not (and
    cannot cheaply be, without a new event type + schema/UI changes) distinguished from no
    change at all. This pins that current, narrower behavior so a future reader sees it was a
    choice, not a gap nobody noticed.
    """
    conn = _setup_db(tmp_path / "activism.db")
    try:
        _insert_filing(
            conn,
            filing_type="SC 13D",
            filed_date="2024-05-01",
            ownership_pct=6.0,
            group_members="Elliott",
        )
        expanded = _insert_filing(
            conn,
            filing_type="SC 13D/A",
            filed_date="2024-08-01",
            ownership_pct=6.0,
            group_members="Elliott|Icahn Capital",
        )

        events = detect_events(conn, expanded)

        assert "group_formation" not in _event_types(events)
    finally:
        conn.close()


def test_insert_activism_events_deduplicates_reruns(tmp_path):
    conn = _setup_db(tmp_path / "activism.db")
    try:
        _insert_filing(conn, filing_type="SC 13D", filed_date="2024-05-01", ownership_pct=4.0)
        filing = _insert_filing(
            conn, filing_type="SC 13D", filed_date="2024-05-02", ownership_pct=11.0
        )
        events = detect_events(conn, filing)

        first_insert = insert_activism_events(conn, events)
        second_insert = insert_activism_events(conn, events)
        conn.commit()

        assert len(first_insert) == len(events)
        assert second_insert == []
        stored = conn.execute("SELECT COUNT(*) FROM activism_events").fetchone()
        assert stored is not None
        assert stored[0] == len(events)
    finally:
        conn.close()


def test_fire_alerts_for_matching_activism_event(tmp_path):
    conn = _setup_db(tmp_path / "activism.db")
    try:
        conn.execute(
            """INSERT INTO alert_rules(name, event_type, condition_json, channels, enabled, manager_id)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (
                "Threshold Rule",
                ALERT_EVENT_TYPE,
                '{"event_type":"threshold_crossing","min_ownership_pct":5.0}',
                '["streamlit"]',
                1,
                None,
            ),
        )
        filing = _insert_filing(
            conn, filing_type="SC 13D", filed_date="2024-05-01", ownership_pct=5.2
        )
        events = detect_events(conn, filing)
        persisted = insert_activism_events(conn, events)

        alerts = 0
        for event in persisted:
            alerts += fire_alerts_for_event(
                conn,
                AlertEvent(
                    event_type=ALERT_EVENT_TYPE,
                    manager_id=event.manager_id,
                    payload=event_payload(event),
                ),
            )
        conn.commit()

        assert alerts == 1
        history = conn.execute(
            """SELECT alert_rules.name, alert_history.event_type, alert_history.payload_json,
                      alert_history.delivered_channels
               FROM alert_history
               JOIN alert_rules ON alert_rules.rule_id = alert_history.rule_id"""
        ).fetchone()
        assert history is not None
        assert history[0] == "Threshold Rule"
        assert history[1] == ALERT_EVENT_TYPE
        assert '"threshold_crossing"' in str(history[2])
        assert history[3] == '["streamlit"]'
    finally:
        conn.close()
