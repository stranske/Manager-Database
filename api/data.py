"""Endpoint for proxying data from an external API."""

from __future__ import annotations

import os

import httpx
from fastapi import APIRouter
from fastapi.responses import JSONResponse

from api.handler import handleRequest

router = APIRouter()


def _upstream_url() -> str:
    return os.getenv("DATA_API_URL", "")


async def _fetch_upstream_payload(url: str) -> str:
    async with httpx.AsyncClient(timeout=5.0) as client:
        response = await client.get(url)
        response.raise_for_status()
        return response.text


@router.get(
    "/api/data",
    summary="Fetch and normalize upstream data",
    description="Retrieve data from an external API and return a normalized response.",
)
async def data_endpoint() -> JSONResponse:
    url = _upstream_url()
    if not url:
        return JSONResponse(
            status_code=500,
            content={"error": "DATA_API_URL is not configured."},
        )
    try:
        raw = await _fetch_upstream_payload(url)
    except (httpx.RequestError, httpx.HTTPStatusError):
        return JSONResponse(
            status_code=502,
            content={"error": "Upstream API request failed."},
        )
    return handleRequest(raw)
