from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from event_correlator.engine import Correlator
from event_correlator.models import PendingTrade, TradeEvent


class MemoryStore:
    def __init__(self) -> None:
        self.events: dict[str, list[PendingTrade]] = {}
        self.audit: list[dict[str, Any]] = []

    def ingest(self, pending, expected_sources, build_audit):
        if any(entry["trade_id"] == pending.trade_id for entry in self.audit):
            return False, []
        events = self.events.setdefault(pending.trade_id, [])
        if any(event.source == pending.source for event in events):
            return False, []
        if events:
            first_deadline = min(event.deadline for event in events)
            pending = PendingTrade(
                trade_id=pending.trade_id,
                source=pending.source,
                payload=pending.payload,
                first_seen=min(event.first_seen for event in events),
                deadline=first_deadline,
            )
        events.append(pending)
        if not set(expected_sources).issubset({event.source for event in events}):
            return True, []
        completed = [event for event in events if event.source in set(expected_sources)]
        del self.events[pending.trade_id]
        self.audit.append(build_audit(completed))
        return True, completed

    def sweep_expired(self, now, build_audit):
        expired = {}
        for trade_id in sorted(list(self.events)):
            events = self.events[trade_id]
            if events and min(event.deadline for event in events) <= now:
                expired[trade_id] = list(events)
                del self.events[trade_id]
                self.audit.append(build_audit(trade_id, expired[trade_id]))
        return expired

    def get_audit(self, trade_id, outcome, limit):
        entries = [
            entry
            for entry in reversed(self.audit)
            if (trade_id is None or entry["trade_id"] == trade_id)
            and (outcome is None or entry["outcome"] == outcome)
        ]
        return entries[:limit]


def event(trade_id: str, source: str) -> TradeEvent:
    return TradeEvent(
        trade_id=trade_id,
        source=source,
        timestamp=datetime(2026, 7, 3, 14, 30, tzinfo=timezone.utc),
        instrument="ISIN-001",
        quantity=1000,
        price=12.5,
        currency="EUR",
        counterparty="CounterpartyX",
    )


def test_ingest_lifecycle_and_audit():
    store = MemoryStore()
    correlator = Correlator(
        store, ["source_a", "source_b", "source_c"], timedelta(minutes=5)
    )
    now = datetime(2026, 7, 3, 14, 30, tzinfo=timezone.utc)

    assert correlator.ingest(event("TRD-001", "source_a"), now)["status"] == "pending"
    assert correlator.ingest(event("TRD-001", "source_a"), now)["status"] == "duplicate"
    assert correlator.ingest(event("TRD-001", "source_b"), now)["status"] == "pending"

    result = correlator.ingest(
        event("TRD-001", "source_c"), now + timedelta(seconds=2)
    )
    assert result["status"] == "reconciled"
    assert result["reconciled"]["sources"] == ["source_a", "source_b", "source_c"]
    assert result["reconciled"]["match_duration_ms"] == 2000
    assert "TRD-001" not in store.events
    assert correlator.audit_history(trade_id="TRD-001")[0]["outcome"] == "reconciled"
    assert correlator.ingest(
        event("TRD-001", "source_a"), now + timedelta(seconds=3)
    )["status"] == "duplicate"


def test_unknown_source_is_ignored():
    store = MemoryStore()
    correlator = Correlator(store, ["source_a"], timedelta(minutes=5))

    result = correlator.ingest(
        event("TRD-001", "unknown"), datetime.now(timezone.utc)
    )

    assert result == {"status": "ignored_unknown_source", "trade_id": "TRD-001"}
    assert store.events == {}


def test_sweep_creates_break_and_audit():
    store = MemoryStore()
    correlator = Correlator(
        store, ["source_a", "source_b", "source_c"], timedelta(milliseconds=100)
    )
    now = datetime(2026, 7, 3, 14, 30, tzinfo=timezone.utc)

    correlator.ingest(event("TRD-002", "source_a"), now)
    correlator.ingest(event("TRD-002", "source_b"), now)

    assert correlator.sweep(now + timedelta(milliseconds=50)) == []
    breaks = correlator.sweep(now + timedelta(milliseconds=200))

    assert len(breaks) == 1
    assert breaks[0]["missing_sources"] == ["source_c"]
    assert correlator.audit_history(outcome="break")[0]["trade_id"] == "TRD-002"
