# Event Correlator for SAM Go

A real-time event correlation engine that runs as a native SAM Go AWE instance kind. It subscribes to multiple Solace topics, correlates trade events by trade ID, and publishes reconciled/break events that trigger downstream SAM workflows.

## Quick Start

**1. Register the kind** (one line in `internal/bootstrap/awe.go`):

```go
exe.RegisterKind("correlator", func(cfg config.Config) (awe.Instance, error) {
    return correlator.InstanceFactory(cfg)
})
```

**2. Declare in your SAM config:**

```yaml
apps:
  - name: trade_correlator
    app_config:
      kind: correlator
      namespace: solace-agent-mesh
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

**3. Deploy.** The correlator starts alongside your agents and workflows in the same AWE process.

## How It Works

```
Solace Topics (data plane)         SAM Go AWE Process
─────────────────────────         ─────────────────────────────────
                                  ┌─────────────────────────────┐
 trades/solar/>        ────────►  │                             │
 trades/murex/>        ────────►  │  Correlator Instance        │
 trades/client_rep/>   ────────►  │  (durable queue, N sources) │
                                  │                             │
                                  └──────────┬──────────────────┘
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

1. Multiple source systems publish trade events to Solace topics
2. The correlator subscribes via a single durable queue (supports competing consumers)
3. Events are correlated by `trade_id` using an in-memory engine
4. When all expected sources confirm: publish a **reconciled** event
5. When the correlation window expires with missing sources: publish a **break** event and trigger a SAM workflow to investigate

## SAM Go Integration

The correlator implements the `awe.Instance` interface:

| Method | Behavior |
|--------|----------|
| `Name()` | Instance name from config |
| `Kind()` | Returns `"correlator"` |
| `Init(ctx, cfg, svc)` | Validates config, builds engine |
| `Start(ctx)` | Creates queue, subscribes, launches loops |
| `Stop(ctx)` | Drains work, leaves queue for restart |
| `Remove(ctx)` | Stops + deprovisions queue (permanent undeploy) |
| `Health()` | Reports broker connectivity |

See [INTEGRATION.md](INTEGRATION.md) for the full step-by-step guide.

## Configuration Reference

| Parameter | Default | Description |
|-----------|---------|-------------|
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

### Output: Break Event (triggers workflow)
```json
{
  "trade_id": "TRD-20260703-000002",
  "missing_sources": ["client_reporting"],
  "received_sources": ["murex", "solar"],
  "detected_at": "2026-07-03T14:35:30Z",
  "window_expiry": "2026-07-03T14:35:00Z"
}
```

## Running Tests

The engine tests are self-contained (no SAM dependency):

```bash
go test -race -count=1 ./internal/correlator/
```

## Design Decisions

- **Durable queue with competing consumers**: horizontal scaling without application-level partitioning
- **Periodic sweep (not per-trade timers)**: bounds goroutine count regardless of trade volume
- **Engine/Instance split**: pure correlation logic is unit-testable without broker or SAM
- **Workflow trigger on break**: configurable A2A publish so SAM workflows react autonomously
- **Remover interface**: clean undeploy deprovisions broker resources

## License

Apache 2.0
