from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from sam_tool_sdk import (
    ConfigSchemaField,
    DynamicToolProvider,
    SandboxToolContextFacade,
    ToolResult,
    provider_cli,
)

from .engine import Correlator
from .models import TradeEvent
from .postgres import PostgresStore


class EventCorrelatorTools(DynamicToolProvider):
    @property
    def config_schema(self) -> list[dict[str, Any]]:
        return [
            ConfigSchemaField(
                key="database_url",
                type="string",
                description="PostgreSQL connection string used for durable pending events and audit history.",
                required=True,
                secret=True,
            ).to_dict(),
            ConfigSchemaField(
                key="expected_sources",
                type="string",
                description="Comma-separated source names required to reconcile a trade.",
                required=True,
                default="solar,murex,client_reporting",
            ).to_dict(),
            ConfigSchemaField(
                key="correlation_window_seconds",
                type="integer",
                description="Seconds to wait after the first event before a trade becomes a break.",
                required=True,
                default=300,
            ).to_dict(),
        ]

    def create_tools(self, tool_config=None):
        return []

    async def ingest_trade_event(
        self,
        trade_id: str,
        source: str,
        timestamp: str,
        instrument: str = "",
        quantity: float = 0,
        price: float = 0,
        currency: str = "",
        counterparty: str = "",
        tool_context: Optional[SandboxToolContextFacade] = None,
    ) -> ToolResult:
        """Persist one trade event and reconcile it when every configured source has reported.

        Args:
            trade_id: Unique trade identifier shared by all source events.
            source: Source system that emitted this event.
            timestamp: Event timestamp in RFC 3339 format.
            instrument: Optional traded instrument identifier.
            quantity: Optional traded quantity.
            price: Optional traded price.
            currency: Optional ISO currency code.
            counterparty: Optional counterparty name or identifier.
        """
        try:
            event_timestamp = _parse_timestamp(timestamp)
        except ValueError:
            return ToolResult.error(
                "timestamp must be RFC 3339", code="INVALID_TIMESTAMP"
            )

        try:
            correlator, store = _new_correlator(self._tool_config)
        except Exception as exc:
            return ToolResult.error(
                str(exc), code="CONFIGURATION_OR_DATABASE_ERROR"
            )

        try:
            if tool_context is not None:
                tool_context.send_status(f"Correlating trade {trade_id}")
            result = correlator.ingest(
                TradeEvent(
                    trade_id=trade_id,
                    source=source,
                    timestamp=event_timestamp,
                    instrument=instrument or None,
                    quantity=quantity or None,
                    price=price or None,
                    currency=currency or None,
                    counterparty=counterparty or None,
                ),
                datetime.now(timezone.utc),
            )
            return ToolResult.ok(
                f"Trade event processed: {result['status']}", data=result
            )
        except Exception as exc:
            return ToolResult.error(str(exc), code="INGEST_FAILED")
        finally:
            store.close()

    async def sweep_expired_trades(
        self,
        tool_context: Optional[SandboxToolContextFacade] = None,
    ) -> ToolResult:
        """Finalize expired incomplete trade correlations as breaks and return their details."""
        try:
            correlator, store = _new_correlator(self._tool_config)
        except Exception as exc:
            return ToolResult.error(
                str(exc), code="CONFIGURATION_OR_DATABASE_ERROR"
            )

        try:
            if tool_context is not None:
                tool_context.send_status("Checking for expired trade correlations")
            breaks = correlator.sweep(datetime.now(timezone.utc))
            return ToolResult.ok(
                f"Detected {len(breaks)} expired trade correlation(s)",
                data={"count": len(breaks), "breaks": breaks},
            )
        except Exception as exc:
            return ToolResult.error(str(exc), code="SWEEP_FAILED")
        finally:
            store.close()

    async def get_audit_history(
        self,
        trade_id: str = "",
        outcome: str = "",
        limit: int = 100,
    ) -> ToolResult:
        """Read the immutable audit history of reconciled trades and correlation breaks.

        Args:
            trade_id: Optional trade identifier filter.
            outcome: Optional outcome filter: reconciled or break.
            limit: Maximum records to return, from 1 to 500.
        """
        normalized_outcome = outcome.strip()
        if normalized_outcome not in ("", "reconciled", "break"):
            return ToolResult.error(
                "outcome must be reconciled or break", code="INVALID_OUTCOME"
            )
        if limit < 1 or limit > 500:
            return ToolResult.error(
                "limit must be between 1 and 500", code="INVALID_LIMIT"
            )

        try:
            correlator, store = _new_correlator(self._tool_config)
        except Exception as exc:
            return ToolResult.error(
                str(exc), code="CONFIGURATION_OR_DATABASE_ERROR"
            )

        try:
            entries = correlator.audit_history(
                trade_id=trade_id.strip() or None,
                outcome=normalized_outcome or None,
                limit=limit,
            )
            return ToolResult.ok(
                f"Found {len(entries)} audit entrie(s)",
                data={"count": len(entries), "entries": entries},
            )
        except Exception as exc:
            return ToolResult.error(str(exc), code="AUDIT_QUERY_FAILED")
        finally:
            store.close()


EventCorrelatorTools.register_tool(EventCorrelatorTools.ingest_trade_event)
EventCorrelatorTools.register_tool(EventCorrelatorTools.sweep_expired_trades)
EventCorrelatorTools.register_tool(EventCorrelatorTools.get_audit_history)


def _new_correlator(tool_config: dict[str, Any]) -> tuple[Correlator, PostgresStore]:
    database_url = str(tool_config.get("database_url", "")).strip()
    if not database_url:
        raise ValueError("database_url tool configuration is required")

    expected_sources = _split_sources(
        str(tool_config.get("expected_sources", "solar,murex,client_reporting"))
    )
    if not expected_sources:
        raise ValueError("expected_sources must contain at least one source")

    try:
        window_seconds = int(tool_config.get("correlation_window_seconds", 300))
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "correlation_window_seconds must be a positive integer"
        ) from exc
    if window_seconds <= 0:
        raise ValueError("correlation_window_seconds must be a positive integer")

    store = PostgresStore(database_url)
    try:
        store.migrate()
    except Exception:
        store.close()
        raise
    return Correlator(
        store,
        expected_sources,
        timedelta(seconds=window_seconds),
    ), store


def _split_sources(value: str) -> list[str]:
    return list(dict.fromkeys(source.strip() for source in value.split(",") if source.strip()))


def _parse_timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("timestamp must include a timezone")
    return parsed


cli = provider_cli(EventCorrelatorTools)


if __name__ == "__main__":
    cli()
