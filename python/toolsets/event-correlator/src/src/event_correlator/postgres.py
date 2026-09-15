from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from importlib.resources import files
from typing import Any

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from .models import PendingTrade

AuditBuilder = Callable[[list[PendingTrade]], dict[str, Any]]
BreakAuditBuilder = Callable[[str, list[PendingTrade]], dict[str, Any]]


class PostgresStore:
    def __init__(self, database_url: str) -> None:
        self._connection = psycopg.connect(
            database_url,
            autocommit=True,
            row_factory=dict_row,
            connect_timeout=5,
        )

    def close(self) -> None:
        self._connection.close()

    def migrate(self) -> None:
        migration = (
            files("event_correlator.migrations")
            .joinpath("00001_initial_schema.sql")
            .read_text(encoding="utf-8")
        )
        self._connection.execute(migration)

    def ingest(
        self,
        pending: PendingTrade,
        expected_sources: list[str],
        build_audit: AuditBuilder,
    ) -> tuple[bool, list[PendingTrade]]:
        with self._connection.transaction():
            self._connection.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                (pending.trade_id,),
            )
            finalized = self._connection.execute(
                "SELECT EXISTS (SELECT 1 FROM audit_log WHERE trade_id = %s) AS finalized",
                (pending.trade_id,),
            ).fetchone()
            if finalized and finalized["finalized"]:
                return False, []

            existing = self._get_events(pending.trade_id)
            if existing:
                pending = PendingTrade(
                    trade_id=pending.trade_id,
                    source=pending.source,
                    payload=pending.payload,
                    first_seen=min(event.first_seen for event in existing),
                    deadline=min(event.deadline for event in existing),
                )

            inserted = self._connection.execute(
                """
                INSERT INTO pending_events
                    (trade_id, source, payload, first_seen, deadline)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (trade_id, source) DO NOTHING
                RETURNING trade_id
                """,
                (
                    pending.trade_id,
                    pending.source,
                    Jsonb(pending.payload),
                    pending.first_seen,
                    pending.deadline,
                ),
            ).fetchone()
            if inserted is None:
                return False, []

            events = self._get_events(pending.trade_id)
            received_sources = {event.source for event in events}
            if not set(expected_sources).issubset(received_sources):
                return True, []

            expected_events = [
                event for event in events if event.source in set(expected_sources)
            ]
            self._connection.execute(
                "DELETE FROM pending_events WHERE trade_id = %s",
                (pending.trade_id,),
            )
            self._record_audit(build_audit(expected_events))
            return True, expected_events

    def sweep_expired(
        self,
        now: datetime,
        build_audit: BreakAuditBuilder,
    ) -> dict[str, list[PendingTrade]]:
        rows = self._connection.execute(
            """
            SELECT DISTINCT trade_id
            FROM pending_events
            WHERE deadline <= %s
            ORDER BY trade_id
            """,
            (now,),
        ).fetchall()

        expired: dict[str, list[PendingTrade]] = {}
        for row in rows:
            trade_id = row["trade_id"]
            with self._connection.transaction():
                lock = self._connection.execute(
                    "SELECT pg_try_advisory_xact_lock(hashtextextended(%s, 0)) AS locked",
                    (trade_id,),
                ).fetchone()
                if not lock or not lock["locked"]:
                    continue

                events = self._get_events(trade_id)
                if not events or min(event.deadline for event in events) > now:
                    continue

                self._connection.execute(
                    "DELETE FROM pending_events WHERE trade_id = %s",
                    (trade_id,),
                )
                self._record_audit(build_audit(trade_id, events))
                expired[trade_id] = events
        return expired

    def get_audit(
        self,
        trade_id: str | None,
        outcome: str | None,
        limit: int,
    ) -> list[dict[str, Any]]:
        rows = self._connection.execute(
            """
            SELECT id::text, trade_id, outcome, sources, detail,
                   occurred_at, recorded_at
            FROM audit_log
            WHERE (%s::text IS NULL OR trade_id = %s::text)
              AND (%s::text IS NULL OR outcome = %s::text)
            ORDER BY recorded_at DESC, id DESC
            LIMIT %s
            """,
            (trade_id, trade_id, outcome, outcome, min(max(limit, 1), 500)),
        ).fetchall()
        return [_json_ready(dict(row)) for row in rows]

    def _get_events(self, trade_id: str) -> list[PendingTrade]:
        rows = self._connection.execute(
            """
            SELECT trade_id, source, payload, first_seen, deadline
            FROM pending_events
            WHERE trade_id = %s
            ORDER BY source
            FOR UPDATE
            """,
            (trade_id,),
        ).fetchall()
        return [
            PendingTrade(
                trade_id=row["trade_id"],
                source=row["source"],
                payload=row["payload"],
                first_seen=row["first_seen"],
                deadline=row["deadline"],
            )
            for row in rows
        ]

    def _record_audit(self, entry: dict[str, Any]) -> None:
        self._connection.execute(
            """
            INSERT INTO audit_log (trade_id, outcome, sources, detail, occurred_at)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (
                entry["trade_id"],
                entry["outcome"],
                Jsonb(entry["sources"]),
                Jsonb(entry["detail"]),
                entry["occurred_at"],
            ),
        )


def _json_ready(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat().replace("+00:00", "Z")
    if isinstance(value, dict):
        return {key: _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    return value
