from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field


class Incident(BaseModel):
    id: int | None = None
    pipeline_name: str
    task_id: str | None = None
    error_signature: str
    error_summary: str | None = None
    root_cause: str | None = None
    severity: str | None = None
    suggested_actions: list[str] = Field(default_factory=list)
    resolved: bool = False
    resolution_notes: str | None = None
    created_at: datetime | None = None
