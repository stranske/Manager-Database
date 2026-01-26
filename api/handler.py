"""Handle /api/data upstream responses."""

from __future__ import annotations

from fastapi.responses import JSONResponse

from api.parser import ParseResult, parseResponse


def _error_payload(message: str) -> dict[str, str]:
    return {"error": message}


def handleRequest(raw: str | bytes | None) -> JSONResponse:  # noqa: N802
    """Return an API response for parsed upstream content."""
    try:
        result: ParseResult = parseResponse(raw)
    except Exception:
        # Guard against unexpected parser failures.
        return JSONResponse(
            status_code=400,
            content=_error_payload("Malformed JSON response from upstream."),
        )
    if not result.ok:
        return JSONResponse(status_code=400, content=_error_payload(result.error or "Error"))
    return JSONResponse(status_code=200, content={"data": result.data})

