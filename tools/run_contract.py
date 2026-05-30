"""Uniform ``RunResult`` envelope shared across Manager-Database tools.

Backplane interoperability requires every tool run to be representable as one
JSON object so an orchestrator can capture, replay, and diff it. The design
intent already existed for the chat surface — ``api.chat.ChatResponse`` carries a
stable ``response_id``/``trace_url``/``latency_ms`` and the per-turn
``langsmith-fleet/v1`` record standardizes ``run_id``/``request_id``
(``llm.langsmith_fleet.ChatFleetContext``). The deterministic tools, by
contrast, returned bare data with no run metadata. ``RunResult`` gives them the
same JSON-representable envelope.

The ``run_id`` field name is deliberately the same one used by
:class:`llm.langsmith_fleet.ChatFleetContext` so chat-api turns and tool runs
share a single correlation vocabulary (see
``tests/test_run_contract.py::test_run_id_field_name_matches_chat_context``).
"""

from __future__ import annotations

import hashlib
import json
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field


def new_run_id() -> str:
    """Return a fresh run identifier (uuid4 hex), matching ``ChatFleetContext.run_id``."""
    return uuid.uuid4().hex


def write_artifact_bundle(
    run_id: str,
    tool: str,
    files: dict[str, bytes | str],
    *,
    inputs: dict[str, Any] | None = None,
    root: str | Path = "artifacts",
) -> list[dict[str, Any]]:
    """Write named local artifacts plus a re-hashable manifest."""
    bundle_dir = Path(root) / tool / run_id
    bundle_dir.mkdir(parents=True, exist_ok=True)

    refs: list[dict[str, Any]] = []
    for name in sorted(files):
        content = files[name]
        if Path(name).name != name:
            raise ValueError(f"artifact name must be a plain file name: {name!r}")
        payload = content.encode("utf-8") if isinstance(content, str) else content
        path = bundle_dir / name
        path.write_bytes(payload)
        refs.append(
            {
                "name": name,
                "path": str(path),
                "sha256": hashlib.sha256(payload).hexdigest(),
                "bytes": len(payload),
            }
        )

    manifest = {
        "run_id": run_id,
        "tool": tool,
        "inputs": inputs or {},
        "created_at": datetime.now(UTC).isoformat(),
        "files": refs,
    }
    (bundle_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return refs


class RunCost(BaseModel):
    """Cost accounting for a single tool run."""

    usd: float = 0.0
    tokens: int = 0


class RunResult(BaseModel):
    """One JSON-representable record of a single tool run.

    A populated ``RunResult`` is enough to replay, audit, and diff a tool run.
    ``outputs`` holds the tool's named result payload (a list for
    ``diff_holdings``, a summary dict for ``daily_diff_flow``); ``inputs`` echoes
    the validated invocation arguments; ``provenance`` records the source IDs the
    run was derived from.
    """

    run_id: str = Field(default_factory=new_run_id)
    tool: str
    requested_by: str | None = None
    reason: str | None = None
    inputs: dict[str, Any] = Field(default_factory=dict)
    outputs: Any = None
    artifacts: list[Any] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    cost: RunCost = Field(default_factory=RunCost)
    latency_ms: int | None = None
    provenance: dict[str, Any] = Field(default_factory=dict)
    status: str = "success"

    @property
    def deltas(self) -> Any:
        """Back-compat accessor: the holdings-delta list lives in ``outputs``.

        ``diff_holdings`` historically returned the delta list directly; callers
        that still want just the deltas can read ``result.deltas`` instead of
        ``result.outputs``.
        """
        return self.outputs
