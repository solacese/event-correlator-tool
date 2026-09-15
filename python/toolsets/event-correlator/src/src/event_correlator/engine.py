from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Protocol

from .models import BreakEvent, PendingTrade, ReconciledEvent, TradeEvent


class CorrelationStore(Protocol):
    def ingest(
        self,
        pending: PendingTrade,
        expected_sources: list[str],
        build_audit: Any,
    ) -> tuple[bool, list[PendingTrade]]: ...

    def sweep_expired(self, now: datetime, build_audit: Any) -> dict[str, list[PendingTrade]]: ...

    def get_audit(self, trade_id: str | None, outcome: str | None, limit: int) -> list[dict[str, Any]]: ...


class Correlator:
    def __init__(
        self,
        store: CorrelationStore,
        expected_sources: list[str],
        correlation_window: timedelta,
    ) -> None:
        self._store = store
        self._expected_sources = set(expected_sources)
        self._correlation_window = correlation_window

    def ingest(self, event: TradeEvent, now: datetime) -> dict[str, Any]:
        if not event.trade_id:
            raise ValueError("trade_id is required")
        if event.source not in self._expected_sources:
            return {"status": "ignored_unknown_source", "trade_id": event.trade_id}

        pending = PendingTrade(
            trade_id=event.trade_id,
            source=event.source,
            payload=event.to_dict(),
            first_seen=now,
            deadline=now + self._correlation_window,
        )
        reconciled: ReconciledEvent | None = None

        def build_audit(events: list[PendingTrade]) -> dict[str, Any]:
            nonlocal reconciled
            reconciled = self._build_reconciled(events, now)
            return {
                "trade_id": event.trade_id,
                "outcome": "reconciled",
                "sources": reconciled.sources,
                "detail": reconciled.to_dict(),
                "occurred_at": now,
            }

        inserted, events = self._store.ingest(
            pending, sorted(self._expected_sources), build_audit
        )
        if not inserted:
            return {"status": "duplicate", "trade_id": event.trade_id}
        if not events:
            return {"status": "pending", "trade_id": event.trade_id}
        return {
            "status": "reconciled",
            "trade_id": event.trade_id,
            "reconciled": reconciled.to_dict(),
        }

    def sweep(self, now: datetime) -> list[dict[str, Any]]:
        built: dict[str, BreakEvent] = {}

        def build_audit(trade_id: str, events: list[PendingTrade]) -> dict[str, Any]:
            break_event = self._build_break(trade_id, events, now)
            built[trade_id] = break_event
            return {
                "trade_id": trade_id,
                "outcome": "break",
                "sources": break_event.received_sources,
                "detail": break_event.to_dict(),
                "occurred_at": now,
            }

        expired = self._store.sweep_expired(now, build_audit)
        return [built[trade_id].to_dict() for trade_id in sorted(expired)]

    def audit_history(
        self,
        trade_id: str | None = None,
        outcome: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        return self._store.get_audit(trade_id, outcome, limit)

    @staticmethod
    def _build_reconciled(events: list[PendingTrade], now: datetime) -> ReconciledEvent:
        ordered = sorted(events, key=lambda event: event.source)
        first_seen = min(event.first_seen for event in ordered)
        return ReconciledEvent(
            trade_id=ordered[0].trade_id,
            sources=[event.source for event in ordered],
            reconciled_at=now,
            match_duration_ms=int((now - first_seen).total_seconds() * 1000),
            events=[event.payload for event in ordered],
        )

    def _build_break(
        self, trade_id: str, events: list[PendingTrade], now: datetime
    ) -> BreakEvent:
        ordered = sorted(events, key=lambda event: event.source)
        received = [event.source for event in ordered]
        return BreakEvent(
            trade_id=trade_id,
            missing_sources=sorted(self._expected_sources.difference(received)),
            received_sources=received,
            detected_at=now,
            window_expiry=max(event.deadline for event in ordered),
            events=[event.payload for event in ordered],
        )
