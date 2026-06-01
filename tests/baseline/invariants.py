"""Structural + economic invariants on the /managers HTTP responses.

These are properties that must hold for the seeded, deterministic universe --
grounded in the endpoint contract (``api/managers.py`` + the response models in
``api/models.py``), NOT generic placeholders:

  * status_code is 200 for valid list/stats requests; detail is 200 when the id
    exists and 404 when it does not.
  * a list body is a ``{items, total, limit, offset}`` envelope; ``items`` is a
    list; the per-scenario seeded ``count``/``total`` expectation holds.
  * every returned manager has a non-empty ``name`` and an integer
    ``manager_id``; ids on a page are unique and sorted ascending (the SELECT is
    ``ORDER BY id``) -- ordering is therefore stable/deterministic.
  * ``count`` never exceeds the requested ``limit`` nor the reported ``total``;
    ``total`` is non-negative.
  * detail: a found manager echoes the requested id; a miss returns
    ``{"detail": ...}`` and no body fields.
  * stats: ``total_managers`` matches the seeded expectation and equals the sum
    over no single breakdown (a manager may carry several tags/jurisdictions, so
    the breakdown sums are bounded BELOW by total, not equal); counts are
    non-negative; ``with_cik``/``with_lei`` never exceed ``total_managers``;
    every breakdown key is lowercased (the aggregator lowercases).

The result type and assertion helper are shared
(``baseline_kit.InvariantResult`` / ``assert_invariants``).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from baseline_kit import InvariantResult

from . import adapter
from .conftest import seed_rows_for


def _check_list(
    scenario: dict[str, Any], status_code: int, body: dict[str, Any]
) -> list[InvariantResult]:
    results: list[InvariantResult] = []

    def add(name: str, ok: bool, detail: str, severity: str = "error") -> None:
        results.append(InvariantResult(name, bool(ok), severity, detail))

    add("list.status_200", status_code == 200, f"status={status_code}")

    is_envelope = all(k in body for k in ("items", "total", "limit", "offset"))
    add("list.envelope_shape", is_envelope, f"keys={sorted(body)}")
    if not is_envelope:
        return results

    items = body["items"]
    total = body["total"]
    limit = body["limit"]
    offset = body["offset"]

    add("list.items_is_list", isinstance(items, list), f"type={type(items).__name__}")
    add("list.total_nonneg", isinstance(total, int) and total >= 0, f"total={total}")
    add("list.limit_positive", isinstance(limit, int) and limit >= 1, f"limit={limit}")
    add("list.offset_nonneg", isinstance(offset, int) and offset >= 0, f"offset={offset}")

    ids = [it.get("manager_id") for it in items]
    names = [it.get("name") for it in items]

    add("list.count_le_limit", len(items) <= limit, f"count={len(items)} limit={limit}")
    add("list.count_le_total", len(items) <= total, f"count={len(items)} total={total}")
    add("list.ids_all_int", all(isinstance(i, int) for i in ids), f"ids={ids}")
    add("list.ids_unique", len(set(ids)) == len(ids), f"ids={ids}")
    add("list.ids_sorted_ascending", ids == sorted(ids), f"ids={ids}")
    add("list.names_nonempty", all(bool(n) for n in names), f"names={names}")
    add(
        "list.quality_flags_is_list",
        all(isinstance(it.get("quality_flags"), list) for it in items),
        "quality_flags must be a list on every item",
    )

    expect = scenario.get("expect", {})
    if "count" in expect:
        add(
            "list.count_matches_seed_expectation",
            len(items) == expect["count"],
            f"count={len(items)} expected={expect['count']}",
        )
    if "total" in expect:
        add(
            "list.total_matches_seed_expectation",
            total == expect["total"],
            f"total={total} expected={expect['total']}",
        )
    return results


def _check_detail(
    scenario: dict[str, Any], status_code: int, body: dict[str, Any]
) -> list[InvariantResult]:
    results: list[InvariantResult] = []

    def add(name: str, ok: bool, detail: str, severity: str = "error") -> None:
        results.append(InvariantResult(name, bool(ok), severity, detail))

    expect_found = bool(scenario.get("expect", {}).get("found", 0))
    if expect_found:
        add("detail.status_200", status_code == 200, f"status={status_code}")
        add("detail.echoes_id", body.get("manager_id") == scenario["manager_id"], f"body={body}")
        add("detail.has_name", bool(body.get("name")), f"name={body.get('name')!r}")
        add(
            "detail.required_fields_present",
            all(
                k in body
                for k in ("manager_id", "name", "aliases", "jurisdictions", "tags", "quality_flags")
            ),
            f"keys={sorted(body)}",
        )
        add(
            "detail.quality_flags_is_list",
            isinstance(body.get("quality_flags"), list),
            f"quality_flags={body.get('quality_flags')!r}",
        )
    else:
        add("detail.status_404", status_code == 404, f"status={status_code}")
        add("detail.has_error_detail", "detail" in body, f"body={body}")
        add("detail.no_manager_id", "manager_id" not in body, f"body={body}")
    return results


def _check_stats(
    scenario: dict[str, Any],
    status_code: int,
    body: dict[str, Any],
    n_seeded: int,
) -> list[InvariantResult]:
    results: list[InvariantResult] = []

    def add(name: str, ok: bool, detail: str, severity: str = "error") -> None:
        results.append(InvariantResult(name, bool(ok), severity, detail))

    add("stats.status_200", status_code == 200, f"status={status_code}")

    total = body.get("total_managers")
    by_jur = body.get("by_jurisdiction", {}) or {}
    by_tag = body.get("by_tag", {}) or {}
    with_cik = body.get("with_cik", 0)
    with_lei = body.get("with_lei", 0)

    add("stats.total_is_int", isinstance(total, int) and total >= 0, f"total={total}")
    add("stats.total_equals_seeded", total == n_seeded, f"total={total} seeded={n_seeded}")

    expect = scenario.get("expect", {})
    if "total_managers" in expect:
        add(
            "stats.total_matches_expectation",
            total == expect["total_managers"],
            f"total={total} expected={expect['total_managers']}",
        )

    add(
        "stats.with_cik_nonneg", isinstance(with_cik, int) and with_cik >= 0, f"with_cik={with_cik}"
    )
    add(
        "stats.with_lei_nonneg", isinstance(with_lei, int) and with_lei >= 0, f"with_lei={with_lei}"
    )
    add("stats.with_cik_le_total", with_cik <= (total or 0), f"with_cik={with_cik} total={total}")
    add("stats.with_lei_le_total", with_lei <= (total or 0), f"with_lei={with_lei} total={total}")

    # Each breakdown sum is >= total only if every manager has exactly one
    # key; managers may carry several tags/jurisdictions, so the sum is bounded
    # below by 0 and each per-key count is in [1, total]. A manager appears in a
    # jurisdiction bucket at most once, so no single count can exceed total.
    for label, breakdown in (("jurisdiction", by_jur), ("tag", by_tag)):
        for key, cnt in breakdown.items():
            add(
                f"stats.{label}_key_lowercased",
                key == key.lower(),
                f"key={key!r}",
            )
            add(
                f"stats.{label}_count_in_range[{key}]",
                isinstance(cnt, int) and 1 <= cnt <= (total or 0),
                f"{key}={cnt} total={total}",
            )
    return results


def check_scenario(scenario: dict[str, Any], db_path: Path) -> list[InvariantResult]:
    """Run every invariant against one scenario's response."""
    seed_rows = seed_rows_for(scenario["seed"])
    payload, _metrics = adapter.run_scenario(scenario, seed_rows, db_path)
    status_code = int(payload["status_code"])
    body = payload["json"] if isinstance(payload["json"], dict) else {}
    endpoint = scenario["endpoint"]

    if endpoint == "list":
        return _check_list(scenario, status_code, body)
    if endpoint == "detail":
        return _check_detail(scenario, status_code, body)
    if endpoint == "stats":
        return _check_stats(scenario, status_code, body, n_seeded=len(seed_rows))
    raise ValueError(f"unknown endpoint {endpoint!r}")  # pragma: no cover - catalog typo guard
