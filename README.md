# Event Correlator for Solace Agent Mesh

A production-ready, Postgres-backed event correlation engine that runs as a native SAM Go AWE instance kind. It subscribes to multiple Solace topics, correlates trade events by trade ID with durable state, maintains a full audit trail, and publishes reconciled/break events that trigger downstream SAM workflows.

## Architecture

```
Solace Topics (data plane)         SAM Go AWE Process
─────────────────────────         ──────────────────────────────────
                                  ┌──────────────────────────────┐
 trades/solar/>        ────────►  │                              │
 trades/murex/>        ────────►  │  Correlator Instance         │
 trades/client_rep/>   ────────►  │  (durable queue, Postgres)   │
                                  │                              │
                                  └──────────┬───────────────────┘
                                             │
                              ┌──────────────┼──────────────┐
                              │                             │
                    reconciliation/matched       reconciliation/breaks
                              │                             │
                              │                             ▼
                              │              ┌──────────────────────┐
                              │              │  SAM Workflow         │
                              │              │  "break_investigation"│
                              │              │  (auto-triggered)     │
                              │              └──────────────────────┘
                              ▼
                   (downstream consumers)
```

## Key Features

- **Postgres persistence** — correlation state and audit trail survive restarts
- **Audit log** — every reconciliation and break is recorded with full event detail
- **Durable Solace queue** — competing consumers for horizontal scaling
- **SAM workflow trigger** — break events automatically invoke investigation workflows
- **Graceful lifecycle** — Init/Start/Stop/Remove via AWE control plane
- **Long correlation windows** — hours or days, not just minutes

## Quick Start

**1. Register the kind** (one line in SAM Go's `internal/bootstrap/awe.go`):

```go
exe.RegisterKind("correlator", instance.Factory)
```

**2. Declare in your SAM config:**

```yaml
apps:
  - name: trade_correlator
    app_config:
      kind: correlator
      namespace: solace-agent-mesh
      session_service:
        database_url: ${DATABASE_URL}
      sources:
        - name: solar
          topic: "bbva/trades/solar/>"
        - name: murex
          topic: "bbva/trades/murex/>"
        - name: client_reporting
          topic: "bbva/trades/client_reporting/>"
      expected_sources: [solar, murex, client_reporting]
      correlation_window: 5m
      sweep_interval: 30s
      output:
        reconciled_topic: "bbva/reconciliation/matched"
        break_topic: "bbva/reconciliation/breaks"
        workflow_trigger_topic: "solace-agent-mesh/a2a/v1/break_investigation/request"
```

**3. Deploy.** The correlator starts alongside your agents and workflows in the same AWE process, using SAM's shared Postgres instance.

## How It Works

1. Multiple source systems publish trade events to Solace topics
2. The correlator subscribes via a single durable queue (supports competing consumers)
3. Each event is persisted to Postgres with the trade's correlation deadline
4. When all expected sources confirm: publish a **reconciled** event, write audit entry
5. When the correlation window expires with missing sources: publish a **break** event, trigger a SAM workflow to investigate, write audit entry

## Persistence Model

### `pending_events` table

Holds in-flight correlations. Rows are deleted on reconciliation or break detection.

| Column | Type | Description |
|--------|------|-------------|
| `trade_id` | TEXT | Trade identifier (composite PK with source) |
| `source` | TEXT | Source system name |
| `payload` | JSONB | Full trade event |
| `first_seen` | TIMESTAMPTZ | When this source first reported |
| `deadline` | TIMESTAMPTZ | Correlation window expiry |

### `audit_log` table

Immutable regulatory audit trail. Never deleted.

| Column | Type | Description |
|--------|------|-------------|
| `id` | UUID | Auto-generated |
| `trade_id` | TEXT | Trade identifier |
| `outcome` | TEXT | `reconciled` or `break` |
| `sources` | JSONB | Which sources reported |
| `detail` | JSONB | Full reconciled/break event payload |
| `occurred_at` | TIMESTAMPTZ | When the outcome was determined |
| `recorded_at` | TIMESTAMPTZ | When the audit row was written |

## SAM Go Integration

The correlator implements the `awe.Instance` interface:

| Method | Behavior |
|--------|----------|
| `Name()` | Instance name from config |
| `Kind()` | Returns `"correlator"` |
| `Init(ctx, cfg, svc)` | Opens DB, runs migrations, builds engine |
| `Start(ctx)` | Creates queue, subscribes, launches loops |
| `Stop(ctx)` | Drains work, closes DB, leaves queue for restart |
| `Remove(ctx)` | Stops + deprovisions queue (permanent undeploy) |
| `Health()` | Reports broker connectivity |

See [INTEGRATION.md](INTEGRATION.md) for the full step-by-step guide.

## Configuration Reference

| Parameter | Default | Description |
|-----------|---------|-------------|
| `session_service.database_url` | (required) | Postgres connection string |
| `sources` | (required) | List of `{name, topic}` source systems |
| `expected_sources` | (required) | Sources that must all report for reconciliation |
| `correlation_window` | `5m` | Time to wait for all sources per trade |
| `sweep_interval` | `30s` | How often to check for expired trades |
| `output.reconciled_topic` | (required) | Where matched events are published |
| `output.break_topic` | (required) | Where break events are published |
| `output.workflow_trigger_topic` | (optional) | A2A topic to trigger a workflow on break |
| `queue_prefix` | namespace | Prefix for the durable queue name |

## Event Formats

### Input: Trade Event
```json
{
  "trade_id": "TRD-20260703-000001",
  "source": "solar",
  "timestamp": "2026-07-03T14:30:00Z",
  "instrument": "ES0113211835",
  "quantity": 1000,
  "price": 12.50,
  "currency": "EUR",
  "counterparty": "BBVA-Madrid"
}
```

### Output: Reconciled Event
```json
{
  "trade_id": "TRD-20260703-000001",
  "sources": ["client_reporting", "murex", "solar"],
  "reconciled_at": "2026-07-03T14:30:02Z",
  "match_duration_ms": 2000,
  "events": [...]
}
```

### Output: Break Event (triggers workflow)
```json
{
  "trade_id": "TRD-20260703-000002",
  "missing_sources": ["client_reporting"],
  "received_sources": ["murex", "solar"],
  "detected_at": "2026-07-03T14:35:30Z",
  "window_expiry": "2026-07-03T14:35:00Z",
  "events": [...]
}
```

## Running Tests

The engine tests use an in-memory store (no Postgres required):

```bash
go test -race -count=1 ./internal/...
```

## Design Decisions

- **Postgres over in-memory**: correlation windows can be hours/days; state must survive restarts; regulatory audit trail is mandatory
- **Shared SAM database**: uses the same `DATABASE_URL` as SAM's session service; no separate infrastructure
- **Store interface**: swap Postgres for any backend (the `memory` implementation is used in tests)
- **UPSERT with ON CONFLICT DO NOTHING**: duplicate source events are idempotent at the DB level
- **Periodic sweep (not per-trade timers)**: bounds goroutine count regardless of trade volume
- **Workflow trigger on break**: configurable A2A publish so SAM workflows react autonomously
- **Remover interface**: clean undeploy deprovisions broker resources

## License

Apache 2.0
