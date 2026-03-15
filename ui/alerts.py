"""Streamlit alerts page for rule management and alert inbox workflows."""

from __future__ import annotations

import json
import os
from datetime import date, datetime, timedelta
from typing import Any, cast

import altair as alt
import httpx
import pandas as pd
import streamlit as st

from alerts import ALERT_CHANNELS, ALERT_EVENT_TYPES

from . import require_login


def _api_base_url() -> str:
    return (
        os.getenv("ALERTS_API_BASE_URL") or os.getenv("API_BASE_URL") or "http://localhost:8000"
    ).rstrip("/")


def _api_request(
    method: str,
    path: str,
    *,
    params: dict[str, Any] | None = None,
    json_body: dict[str, Any] | None = None,
) -> tuple[bool, Any]:
    url = f"{_api_base_url()}{path}"
    try:
        with httpx.Client(timeout=10.0) as client:
            response = client.request(method, url, params=params, json=json_body)
        if response.status_code >= 400:
            detail: Any
            try:
                detail = response.json()
            except ValueError:
                detail = response.text
            return False, detail
        if response.content:
            return True, response.json()
        return True, None
    except httpx.HTTPError as exc:
        return False, str(exc)


@st.cache_data(show_spinner=False)
def _load_managers() -> list[tuple[int, str]]:
    items: list[dict[str, Any]] = []
    offset = 0
    limit = 100
    while True:
        ok, payload = _api_request("GET", "/managers", params={"limit": limit, "offset": offset})
        if not ok:
            break
        if isinstance(payload, dict):
            page_items = payload.get("items") or []
            total = int(payload.get("total") or 0)
        elif isinstance(payload, list):
            page_items = payload
            total = len(page_items)
        else:
            break
        if not isinstance(page_items, list):
            break
        items.extend(item for item in page_items if isinstance(item, dict))
        offset += len(page_items)
        if not page_items or offset >= total:
            break

    managers = [
        (int(item["id"]), str(item["name"]))
        for item in items
        if item.get("id") is not None and item.get("name")
    ]
    return sorted(set(managers), key=lambda manager: manager[1].lower())


@st.cache_data(show_spinner=False)
def _load_rules(event_type: str | None = None, enabled: bool | None = None) -> list[dict[str, Any]]:
    params: dict[str, Any] = {}
    if event_type:
        params["event_type"] = event_type
    if enabled is not None:
        params["enabled"] = enabled
    ok, payload = _api_request("GET", "/api/alerts/rules", params=params)
    if not ok:
        return []
    return payload or []


@st.cache_data(show_spinner=False)
def _load_alerts(
    since_iso: str | None,
    acknowledged: bool | None,
    event_type: str | None,
    limit: int,
) -> list[dict[str, Any]]:
    params: dict[str, Any] = {"limit": limit}
    if since_iso is not None:
        params["since"] = since_iso
    if acknowledged is not None:
        params["acknowledged"] = acknowledged
    if event_type:
        params["event_type"] = event_type
    ok, payload = _api_request("GET", "/api/alerts/history", params=params)
    if not ok:
        return []
    return payload or []


def _clear_alert_caches() -> None:
    cast(Any, _load_managers).clear()
    cast(Any, _load_rules).clear()
    cast(Any, _load_alerts).clear()


def _condition_inputs(event_type: str, defaults: dict[str, Any] | None = None) -> dict[str, Any]:
    defaults = defaults or {}
    if event_type == "activism_event":
        subtype_options = [
            "any",
            "initial_stake",
            "threshold_crossing",
            "stake_increase",
            "stake_decrease",
            "group_formation",
            "amendment",
            "form_upgrade",
            "form_downgrade",
        ]
        default_subtype = str(defaults.get("event_type") or "any")
        subtype_index = (
            subtype_options.index(default_subtype) if default_subtype in subtype_options else 0
        )
        event_subtype = st.selectbox("event subtype", subtype_options, index=subtype_index)
        subject_cusip = st.text_input(
            "subject_cusip", value=str(defaults.get("subject_cusip") or "")
        )
        min_ownership_pct = st.number_input(
            "min_ownership_pct",
            min_value=0.0,
            value=float(defaults.get("min_ownership_pct") or 0.0),
            step=0.1,
        )
        min_delta_pct = st.number_input(
            "min_delta_pct",
            min_value=0.0,
            value=float(defaults.get("min_delta_pct") or 0.0),
            step=0.1,
        )
        condition: dict[str, Any] = {}
        if event_subtype != "any":
            condition["event_type"] = event_subtype
        if subject_cusip.strip():
            condition["subject_cusip"] = subject_cusip.strip()
        if min_ownership_pct > 0:
            condition["min_ownership_pct"] = min_ownership_pct
        if min_delta_pct > 0:
            condition["min_delta_pct"] = min_delta_pct
        return condition
    if event_type == "large_delta":
        delta_type_options = ["buy", "sell", "net"]
        default_delta_type = str(defaults.get("delta_type") or "buy")
        delta_index = (
            delta_type_options.index(default_delta_type)
            if default_delta_type in delta_type_options
            else 0
        )
        delta_type = st.selectbox("delta_type", delta_type_options, index=delta_index)
        value_usd_gt = st.number_input(
            "value_usd_gt",
            min_value=0.0,
            value=float(defaults.get("value_usd_gt") or 100000.0),
            step=10000.0,
        )
        return {"delta_type": delta_type, "value_usd_gt": value_usd_gt}
    if event_type == "new_filing":
        filing_options = ["13F-HR", "13D", "13G"]
        default_filing = str(defaults.get("filing_type") or "13F-HR")
        filing_index = (
            filing_options.index(default_filing) if default_filing in filing_options else 0
        )
        filing_type = st.selectbox("filing_type", filing_options, index=filing_index)
        source_options = ["sec", "manual"]
        default_source = str(defaults.get("source") or "sec")
        source_index = (
            source_options.index(default_source) if default_source in source_options else 0
        )
        source = st.selectbox("source", source_options, index=source_index)
        return {"filing_type": filing_type, "source": source}
    field_options = ["role", "department", "name"]
    default_field = str(defaults.get("field") or "role")
    field_index = field_options.index(default_field) if default_field in field_options else 0
    field = st.selectbox("field", field_options, index=field_index)
    changed_to = st.text_input("changed_to", value=str(defaults.get("changed_to") or ""))
    return {"field": field, "changed_to": changed_to}


def _payload_summary(payload: dict[str, Any], max_len: int = 90) -> str:
    text = ", ".join(f"{k}={v}" for k, v in payload.items())
    if len(text) <= max_len:
        return text
    return f"{text[: max_len - 3]}..."


def _render_rule_builder() -> None:
    st.subheader("Rule Builder")
    managers = _load_managers()
    manager_options = ["All managers"] + [
        f"{name} (#{manager_id})" for manager_id, name in managers
    ]

    with st.form("create_alert_rule"):
        name = st.text_input("name", placeholder="Large Buy Delta")
        event_type = str(st.selectbox("event_type", ALERT_EVENT_TYPES, index=0))
        st.caption("Condition")
        condition_json = _condition_inputs(event_type)
        channels = st.multiselect("channels", ALERT_CHANNELS, default=["email"])
        manager_choice = str(st.selectbox("manager filter", manager_options, index=0))
        enabled = st.checkbox("enabled", value=True)
        submitted = st.form_submit_button("Create Rule")

    if submitted:
        manager_id: int | None = None
        if manager_choice != "All managers":
            manager_id = int(manager_choice.rsplit("#", 1)[-1].rstrip(")"))
        body = {
            "name": name,
            "event_type": event_type,
            "condition_json": condition_json,
            "channels": channels,
            "enabled": enabled,
            "manager_id": manager_id,
        }
        ok, payload = _api_request("POST", "/api/alerts/rules", json_body=body)
        if ok:
            st.success(f"Created rule #{payload['rule_id']}")
            _clear_alert_caches()
            st.rerun()
        else:
            st.error(f"Failed to create rule: {payload}")

    rules = _load_rules()
    if not rules:
        st.info("No alert rules found.")
        return

    st.markdown("#### Existing Rules")
    for rule in rules:
        cols = st.columns([4, 2, 2, 2])
        cols[0].markdown(
            f"**{rule['name']}**  \n`{rule['event_type']}`  \nchannels: `{', '.join(rule['channels'])}`"
        )

        enabled_target = cols[1].toggle(
            "enabled",
            value=bool(rule["enabled"]),
            key=f"rule_enabled_{rule['rule_id']}",
            label_visibility="collapsed",
        )
        if enabled_target != bool(rule["enabled"]):
            ok, payload = _api_request(
                "PUT",
                f"/api/alerts/rules/{rule['rule_id']}",
                json_body={"enabled": enabled_target},
            )
            if ok:
                _clear_alert_caches()
                st.rerun()
            else:
                st.error(f"Unable to update rule #{rule['rule_id']}: {payload}")

        if cols[2].button("Delete", key=f"rule_delete_{rule['rule_id']}"):
            ok, payload = _api_request("DELETE", f"/api/alerts/rules/{rule['rule_id']}")
            if ok:
                _clear_alert_caches()
                st.rerun()
            else:
                st.error(f"Unable to delete rule #{rule['rule_id']}: {payload}")

        with cols[3].expander("Edit", expanded=False):
            st.caption(f"event_type: `{rule['event_type']}`")
            with st.form(f"edit_rule_{rule['rule_id']}"):
                edit_name = st.text_input(
                    "name", value=rule["name"], key=f"edit_name_{rule['rule_id']}"
                )
                st.caption("Condition")
                edit_condition = _condition_inputs(
                    rule["event_type"], defaults=rule.get("condition_json") or {}
                )
                edit_channels = st.multiselect(
                    "channels",
                    ALERT_CHANNELS,
                    default=rule.get("channels") or [],
                    key=f"edit_channels_{rule['rule_id']}",
                )
                edit_enabled = st.checkbox(
                    "enabled",
                    value=bool(rule["enabled"]),
                    key=f"edit_enabled_{rule['rule_id']}",
                )
                save = st.form_submit_button("Save Rule")
            st.code(json.dumps(rule["condition_json"], separators=(",", ":")))

            if save:
                ok, payload = _api_request(
                    "PUT",
                    f"/api/alerts/rules/{rule['rule_id']}",
                    json_body={
                        "name": edit_name,
                        "condition_json": edit_condition,
                        "channels": edit_channels,
                        "enabled": edit_enabled,
                    },
                )
                if ok:
                    st.success(f"Updated rule #{rule['rule_id']}")
                    _clear_alert_caches()
                    st.rerun()
                else:
                    st.error(f"Unable to update rule #{rule['rule_id']}: {payload}")


def _render_alert_inbox() -> None:
    st.subheader("Alert Inbox")
    filter_cols = st.columns(3)
    selected_type = filter_cols[0].selectbox("event_type", ["all", *ALERT_EVENT_TYPES], index=0)
    status = filter_cols[1].selectbox(
        "acknowledged", ["all", "unacknowledged", "acknowledged"], index=0
    )
    date_range = filter_cols[2].date_input(
        "date range",
        value=(date.today() - timedelta(days=30), date.today()),
    )

    selected_ack: bool | None
    if status == "unacknowledged":
        selected_ack = False
    elif status == "acknowledged":
        selected_ack = True
    else:
        selected_ack = None

    start_date: date
    end_date: date
    if isinstance(date_range, tuple) and len(date_range) == 2:
        start_date, end_date = date_range
    else:
        start_date = date.today() - timedelta(days=30)
        end_date = date.today()

    alerts = _load_alerts(
        f"{start_date.isoformat()}T00:00:00",
        selected_ack,
        None if selected_type == "all" else selected_type,
        500,
    )

    filtered: list[dict[str, Any]] = []
    for alert in alerts:
        fired_at_raw = alert.get("fired_at")
        fired_at = datetime.fromisoformat(str(fired_at_raw).replace("Z", "+00:00")).date()
        if fired_at < start_date or fired_at > end_date:
            continue
        filtered.append(alert)

    if st.button("Acknowledge All"):
        ok, payload = _api_request(
            "POST", "/api/alerts/history/acknowledge-all", params={"by": "ui"}
        )
        if ok:
            st.success(f"Acknowledged {payload['acknowledged']} alerts")
            _clear_alert_caches()
            st.rerun()
        else:
            st.error(f"Unable to acknowledge alerts: {payload}")

    if not filtered:
        st.info("No alerts for the selected filters.")
        return

    table_rows = [
        {
            "timestamp": alert["fired_at"],
            "rule": alert["rule_name"],
            "event_type": alert["event_type"],
            "payload": _payload_summary(alert.get("payload_json") or {}),
            "status": "acknowledged" if alert["acknowledged"] else "unacknowledged",
        }
        for alert in filtered
    ]
    st.dataframe(pd.DataFrame(table_rows), use_container_width=True)

    st.markdown("#### Actions")
    for alert in filtered:
        if alert["acknowledged"]:
            continue
        cols = st.columns([5, 2])
        cols[0].write(
            f"{alert['fired_at']} | {alert['rule_name']} | {_payload_summary(alert.get('payload_json') or {}, 120)}"
        )
        if cols[1].button("Acknowledge", key=f"ack_{alert['alert_id']}"):
            ok, payload = _api_request(
                "POST",
                f"/api/alerts/history/{alert['alert_id']}/acknowledge",
                params={"by": "ui"},
            )
            if ok:
                _clear_alert_caches()
                st.rerun()
            else:
                st.error(f"Unable to acknowledge alert #{alert['alert_id']}: {payload}")


def _render_alert_stats() -> None:
    st.subheader("Alert Stats")
    since = datetime.combine(date.today() - timedelta(days=30), datetime.min.time())
    alerts = _load_alerts(since.isoformat(), None, None, 1000)
    if not alerts:
        st.info("No alerts in the last 30 days.")
        return

    stats_df = pd.DataFrame(alerts)
    stats_df["fired_at"] = pd.to_datetime(stats_df["fired_at"], errors="coerce")
    stats_df = stats_df.dropna(subset=["fired_at"])
    if stats_df.empty:
        st.info("No alerts in the last 30 days.")
        return

    counts = stats_df.groupby("event_type", as_index=False).size().rename(columns={"size": "count"})
    bar_chart = (
        alt.Chart(counts)
        .mark_bar()
        .encode(x="event_type:N", y="count:Q", tooltip=["event_type", "count"])
    )
    st.altair_chart(bar_chart, use_container_width=True)

    timeline = (
        stats_df.assign(day=stats_df["fired_at"].dt.date)
        .groupby("day", as_index=False)
        .size()
        .rename(columns={"size": "count"})
    )
    line_chart = alt.Chart(timeline).mark_line(point=True).encode(x="day:T", y="count:Q")
    st.altair_chart(line_chart, use_container_width=True)


def main() -> None:
    if not require_login():
        st.stop()
    st.title("Alerts")
    st.caption(f"API: {_api_base_url()}")

    _render_rule_builder()
    st.divider()
    _render_alert_inbox()
    st.divider()
    _render_alert_stats()


if __name__ == "__main__":
    main()
