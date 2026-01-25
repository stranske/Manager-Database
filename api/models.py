"""Shared API response models."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class ManagerResponse(BaseModel):
    """Response payload for manager records."""

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "id": 101,
                    "name": "Grace Hopper",
                    "role": "Engineering Director",
                }
            ]
        }
    )
    id: int = Field(..., description="Manager identifier")
    name: str = Field(..., description="Manager name")
    role: str = Field(..., description="Manager role")
    # Optional to preserve legacy manager payloads without departments.
    department: str | None = Field(None, description="Manager department")


class ManagerListResponse(BaseModel):
    """Response payload for manager list requests."""

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "items": [
                        {
                            "id": 101,
                            "name": "Grace Hopper",
                            "role": "Engineering Director",
                            "department": "Engineering",
                        }
                    ],
                    "total": 1,
                    "limit": 1,
                    "offset": 0,
                }
            ]
        }
    )
    items: list[ManagerResponse] = Field(..., description="Managers in the requested page")
    total: int = Field(..., description="Total number of managers available")
    limit: int = Field(..., description="Maximum managers returned per page")
    offset: int = Field(..., description="Offset into the manager list")


class BulkImportItemError(BaseModel):
    """Validation error for a single bulk import field."""

    field: str = Field(..., description="Field that failed validation")
    message: str = Field(..., description="Validation error message")


class BulkImportFailure(BaseModel):
    """Bulk import failure detail for a single record."""

    index: int = Field(..., description="Record index in the incoming payload")
    errors: list[BulkImportItemError] = Field(
        ..., description="Field-level validation errors for the record"
    )


class BulkImportSuccess(BaseModel):
    """Bulk import success detail for a single record."""

    index: int = Field(..., description="Record index in the incoming payload")
    manager: ManagerResponse = Field(..., description="Created manager payload")


class BulkImportResponse(BaseModel):
    """Response payload for bulk manager imports."""

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "total": 3,
                    "succeeded": 2,
                    "failed": 1,
                    "successes": [
                        {
                            "index": 0,
                            "manager": {
                                "id": 101,
                                "name": "Grace Hopper",
                                "role": "Engineering Director",
                                "department": "Engineering",
                            },
                        }
                    ],
                    "failures": [
                        {
                            "index": 2,
                            "errors": [{"field": "role", "message": "Role is required."}],
                        }
                    ],
                }
            ]
        }
    )
    total: int = Field(..., description="Total number of records processed")
    succeeded: int = Field(..., description="Number of records imported successfully")
    failed: int = Field(..., description="Number of records that failed validation")
    successes: list[BulkImportSuccess] = Field(
        ..., description="Details for successfully imported records"
    )
    failures: list[BulkImportFailure] = Field(
        ..., description="Details for records that failed validation"
    )
