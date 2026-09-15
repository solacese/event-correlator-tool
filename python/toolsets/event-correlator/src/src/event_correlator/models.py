from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any


@dataclass(frozen=True)
class TradeEvent:
    trade_id: str
    source: str
    timestamp: datetime
    instrument: str | None = None
    quantity: float | None = None
    price: float | None = None
    currency: str | None = None
    counterparty: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return _json_ready(asdict(self))


@dataclass(frozen=True)
class PendingTrade:
    trade_id: str
    source: str
    payload: dict[str, Any]
    first_seen: datetime
    deadline: datetime


@dataclass(frozen=True)
class ReconciledEvent:
    trade_id: str
    sources: list[str]
    reconciled_at: datetime
    match_duration_ms: int
    events: list[dict[str, Any]]

    def to_dict(self) -> dict[str, Any]:
        return _json_ready(asdict(self))


@dataclass(frozen=True)
class BreakEvent:
    trade_id: str
    missing_sources: list[str]
    received_sources: list[str]
    detected_at: datetime
    window_expiry: datetime
    events: list[dict[str, Any]]

    def to_dict(self) -> dict[str, Any]:
        return _json_ready(asdict(self))


def _json_ready(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat().replace("+00:00", "Z")
    if isinstance(value, dict):
        return {key: _json_ready(item) for key, item in value.items() if item is not None}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    return value
