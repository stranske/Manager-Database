import asyncio
import sys
from pathlib import Path

import httpx

sys.path.append(str(Path(__file__).resolve().parents[1]))

from api.chat import app


def _load_openapi_schema() -> dict:
    # Use ASGITransport so the HTTP call avoids socket permissions.
    async def _fetch() -> dict:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.get("/openapi.json")
            assert response.status_code == 200
            return response.json()

    return asyncio.run(_fetch())


def test_openapi_chat_schema():
    schema = _load_openapi_schema()
    chat_schema = schema["paths"]["/chat"]["get"]

    assert chat_schema["summary"] == "Answer a chat query"
    assert "Search stored documents" in chat_schema["description"]

    response_schema = chat_schema["responses"]["200"]["content"]["application/json"]["schema"]
    assert response_schema["$ref"] == "#/components/schemas/ChatResponse"

    parameters = {param["name"]: param for param in chat_schema["parameters"]}
    query_param = parameters["q"]
    assert query_param["description"] == "User question"
    assert (
        query_param["schema"]["examples"]["basic"]["value"] == "What is the latest holdings update?"
    )

    chat_component = schema["components"]["schemas"]["ChatResponse"]
    assert chat_component["examples"][0]["answer"].startswith("Context:")


def test_openapi_managers_schema():
    schema = _load_openapi_schema()
    manager_schema = schema["paths"]["/managers"]["post"]

    assert manager_schema["summary"] == "Create a manager record"
    assert "Validate the incoming manager details" in manager_schema["description"]

    request_schema = manager_schema["requestBody"]["content"]["application/json"]["schema"]
    assert request_schema["$ref"] == "#/components/schemas/ManagerCreate"
    manager_create = schema["components"]["schemas"]["ManagerCreate"]
    assert manager_create["examples"][0]["name"] == "Grace Hopper"

    response_schema = manager_schema["responses"]["201"]["content"]["application/json"]["schema"]
    assert response_schema["$ref"] == "#/components/schemas/ManagerResponse"
    manager_component = schema["components"]["schemas"]["ManagerResponse"]
    assert manager_component["examples"][0]["role"] == "Engineering Director"

    error_examples = manager_schema["responses"]["400"]["content"]["application/json"]["examples"]
    assert error_examples["missing-role"]["value"]["errors"][0]["field"] == "role"

    manager_list_schema = schema["paths"]["/managers"]["get"]
    assert manager_list_schema["summary"] == "List managers"
    list_response_schema = manager_list_schema["responses"]["200"]["content"]["application/json"][
        "schema"
    ]
    assert list_response_schema["$ref"] == "#/components/schemas/ManagerListResponse"
    # Validate list endpoint documents request validation errors.
    list_error_schema = manager_list_schema["responses"]["400"]["content"]["application/json"][
        "schema"
    ]
    assert list_error_schema["$ref"] == "#/components/schemas/ErrorResponse"
    list_parameters = {param["name"]: param for param in manager_list_schema["parameters"]}
    assert list_parameters["limit"]["schema"].get("default") is None
    assert list_parameters["offset"]["schema"]["default"] == 0
    assert "department" in list_parameters

    manager_detail_schema = schema["paths"]["/managers/{id}"]["get"]
    assert manager_detail_schema["summary"] == "Retrieve a manager"
    detail_response_schema = manager_detail_schema["responses"]["200"]["content"][
        "application/json"
    ]["schema"]
    assert detail_response_schema["$ref"] == "#/components/schemas/ManagerResponse"
    # Validate detail endpoint exposes path validation errors.
    detail_error_schema = manager_detail_schema["responses"]["400"]["content"]["application/json"][
        "schema"
    ]
    assert detail_error_schema["$ref"] == "#/components/schemas/ErrorResponse"
    not_found_schema = manager_detail_schema["responses"]["404"]["content"]["application/json"][
        "schema"
    ]
    assert not_found_schema["$ref"] == "#/components/schemas/NotFoundResponse"

    bulk_schema = schema["paths"]["/api/managers/bulk"]["post"]
    assert bulk_schema["summary"] == "Bulk import managers"
    bulk_response_schema = bulk_schema["responses"]["200"]["content"]["application/json"][
        "schema"
    ]
    assert bulk_response_schema["$ref"] == "#/components/schemas/BulkImportResponse"
    bulk_error_schema = bulk_schema["responses"]["400"]["content"]["application/json"]["schema"]
    assert bulk_error_schema["$ref"] == "#/components/schemas/ErrorResponse"
    request_body = bulk_schema["requestBody"]["content"]
    assert "application/json" in request_body
    assert "text/csv" in request_body


def test_openapi_health_db_schema():
    schema = _load_openapi_schema()
    health_schema = schema["paths"]["/health/db"]["get"]

    assert health_schema["summary"] == "Check database connectivity"
    assert "lightweight database ping" in health_schema["description"]

    response_schema = health_schema["responses"]["200"]["content"]["application/json"]["schema"]
    assert response_schema["$ref"] == "#/components/schemas/HealthDbResponse"
    health_component = schema["components"]["schemas"]["HealthDbResponse"]
    assert health_component["examples"][0]["healthy"] is True

    timeout_example = health_schema["responses"]["503"]["content"]["application/json"]["examples"][
        "timeout"
    ]
    assert timeout_example["value"]["healthy"] is False


def test_docs_route_exposes_openapi():
    # Ensure Swagger UI is available for the documented manager endpoints.
    async def _fetch() -> str:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.get("/docs")
            assert response.status_code == 200
            return response.text

    docs_html = asyncio.run(_fetch())
    assert "Swagger UI" in docs_html
    assert "/openapi.json" in docs_html
