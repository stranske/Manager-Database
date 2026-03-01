"""Shared API response models."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class ManagerResponse(BaseModel):
    """Response payload for manager records."""

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "manager_id": 101,
                    "name": "Elliott Investment Management L.P.",
                    "cik": "0001791786",
                    "lei": "549300U3N12T57QLOU60",
                    "aliases": ["Elliott Management"],
                    "jurisdictions": ["us"],
                    "tags": ["activist"],
                    "registry_ids": {"fca_frn": "122927"},
                    "created_at": "2026-02-01T10:00:00Z",
                    "updated_at": "2026-02-01T10:00:00Z",
                }
            ]
        }
    )
    manager_id: int = Field(..., description="Manager identifier")
    name: str = Field(..., description="Legal manager name")
    cik: str | None = Field(None, description="SEC Central Index Key")
    lei: str | None = Field(None, description="Legal Entity Identifier")
    aliases: list[str] = Field(default_factory=list, description="Alternative names")
    jurisdictions: list[str] = Field(default_factory=list, description="Filing jurisdictions")
    tags: list[str] = Field(default_factory=list, description="Classification tags")
    registry_ids: dict[str, str] = Field(default_factory=dict, description="External registry IDs")
    created_at: str | None = Field(None, description="Creation timestamp")
    updated_at: str | None = Field(None, description="Last update timestamp")


class ManagerListResponse(BaseModel):
    """Response payload for manager list requests."""

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "items": [
                        {
                            "manager_id": 101,
                            "name": "Elliott Investment Management L.P.",
                            "cik": "0001791786",
                            "lei": "549300U3N12T57QLOU60",
                            "aliases": ["Elliott Management"],
                            "jurisdictions": ["us"],
                            "tags": ["activist"],
                            "registry_ids": {"fca_frn": "122927"},
                            "created_at": "2026-02-01T10:00:00Z",
                            "updated_at": "2026-02-01T10:00:00Z",
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
                                "manager_id": 101,
                                "name": "Elliott Investment Management L.P.",
                                "cik": "0001791786",
                                "lei": "549300U3N12T57QLOU60",
                                "aliases": ["Elliott Management"],
                                "jurisdictions": ["us"],
                                "tags": ["activist"],
                                "registry_ids": {"fca_frn": "122927"},
                                "created_at": "2026-02-01T10:00:00Z",
                                "updated_at": "2026-02-01T10:00:00Z",
                            },
                        }
                    ],
                    "failures": [
                        {
                            "index": 2,
                            "errors": [{"field": "name", "message": "Name is required."}],
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


class UniverseImportResponse(BaseModel):
    """Response payload for manager universe imports."""

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "created": 8,
                    "updated": 2,
                    "skipped": 1,
                }
            ]
        }
    )
    created: int = Field(..., description="Number of new manager records created")
    updated: int = Field(..., description="Number of existing manager records updated")
    skipped: int = Field(..., description="Number of records skipped due to invalid inputs")
