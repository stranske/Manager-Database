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
                        }
                    ],
                    "total": 1,
                    "limit": 25,
                    "offset": 0,
                }
            ]
        }
    )
    items: list[ManagerResponse] = Field(..., description="Managers in the requested page")
    total: int = Field(..., description="Total number of managers available")
    limit: int = Field(..., description="Maximum managers returned per page")
    offset: int = Field(..., description="Offset into the manager list")
