from __future__ import annotations

import asyncpg

from memory.schema import Incident

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS incidents (
    id SERIAL PRIMARY KEY,
    pipeline_name TEXT NOT NULL,
    task_id TEXT,
    error_signature TEXT NOT NULL,
    error_summary TEXT,
    root_cause TEXT,
    severity TEXT,
    suggested_actions JSONB DEFAULT '[]'::jsonb,
    resolved BOOLEAN DEFAULT FALSE,
    resolution_notes TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
"""

CREATE_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_incidents_signature
    ON incidents USING gin(to_tsvector('english', error_signature));
"""

CREATE_PIPELINE_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_incidents_pipeline
    ON incidents(pipeline_name);
"""


class IncidentStore:
    def __init__(self, dsn: str) -> None:
        # asyncpg expects plain postgresql://, not postgresql+asyncpg:// (SQLAlchemy dialect)
        self._dsn = dsn.replace("+asyncpg", "")
        self._pool: asyncpg.Pool | None = None

    async def _get_pool(self) -> asyncpg.Pool:
        if self._pool is None:
            self._pool = await asyncpg.create_pool(self._dsn, min_size=2, max_size=5)
        return self._pool

    async def init(self) -> None:
        pool = await self._get_pool()
        async with pool.acquire() as conn:
            await conn.execute(CREATE_TABLE_SQL)
            await conn.execute(CREATE_INDEX_SQL)
            await conn.execute(CREATE_PIPELINE_INDEX_SQL)

    async def save(self, incident: Incident) -> int:
        pool = await self._get_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO incidents
                    (pipeline_name, task_id, error_signature, error_summary,
                     root_cause, severity, suggested_actions, resolved, resolution_notes)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                RETURNING id
                """,
                incident.pipeline_name,
                incident.task_id,
                incident.error_signature,
                incident.error_summary,
                incident.root_cause,
                incident.severity,
                incident.suggested_actions,
                incident.resolved,
                incident.resolution_notes,
            )
            return row["id"]

    async def find_similar(
        self,
        error_signature: str,
        pipeline_name: str,
        limit: int = 3,
    ) -> list[Incident]:
        pool = await self._get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, pipeline_name, task_id, error_signature, error_summary,
                       root_cause, severity, suggested_actions, resolved,
                       resolution_notes, created_at
                FROM incidents
                WHERE pipeline_name = $1
                  AND to_tsvector('english', error_signature) @@
                      plainto_tsquery('english', $2)
                ORDER BY created_at DESC
                LIMIT $3
                """,
                pipeline_name,
                error_signature,
                limit,
            )

        return [
            Incident(
                id=r["id"],
                pipeline_name=r["pipeline_name"],
                task_id=r["task_id"],
                error_signature=r["error_signature"],
                error_summary=r["error_summary"],
                root_cause=r["root_cause"],
                severity=r["severity"],
                suggested_actions=r["suggested_actions"],
                resolved=r["resolved"],
                resolution_notes=r["resolution_notes"],
                created_at=r["created_at"],
            )
            for r in rows
        ]

    async def close(self) -> None:
        if self._pool:
            await self._pool.close()
