# Event Correlator Toolset for Solace Agent Mesh

PostgreSQL-backed trade-event correlation for Solace Agent Mesh, available as interchangeable Go and Python Secure Tool Runtime toolsets.

## Layout

```text
.
├── go/       # Go toolset project
├── python/   # Python toolset project
└── config.yaml
```

Each language directory is a standalone SAM declarative-config root with an `event-correlator` toolset. Choose one language root for deployment; both implementations expose the same tool names, inputs, results, configuration, and PostgreSQL schema.

## Tools

| Tool | Purpose |
|---|---|
| `ingest_trade_event` | Persist one source event. Returns `pending`, `duplicate`, `ignored_unknown_source`, or a complete `reconciled` event. |
| `sweep_expired_trades` | Finalize incomplete trades whose correlation window has elapsed and return break events. |
| `get_audit_history` | Query newest-first reconciliation and break audit entries. |

The toolset runtime is request/response. Connectors, workflows, or agents should call `ingest_trade_event` for incoming events. Schedule `sweep_expired_trades` at the desired cadence; unlike the previous AWE instance, a toolset does not keep a background broker subscription or timer alive.

## Shared configuration

[config.yaml](config.yaml) documents values shared by both implementations:

- `database_url` — PostgreSQL DSN, stored as a secret toolset configuration value.
- `expected_sources` — comma-separated source names.
- `correlation_window_seconds` — positive correlation-window duration.

Attach exactly one implementation to an agent:

```yaml
spec:
  toolsets:
    - event-correlator
  toolsetConfigs:
    - toolsetName: event-correlator
      configValues:
        database_url: ${DATABASE_URL}
        expected_sources: solar,murex,client_reporting
        correlation_window_seconds: 300
```

Run `sam config apply` from [go/](go/) for the Go option or [python/](python/) for the Python option.

## Go

```bash
go test ./...
go run . --schema
```

Run those commands in [go/toolsets/event-correlator/src/](go/toolsets/event-correlator/src/), or validate from the repository root:

```bash
sam toolset validate event-correlator go
```

## Python

```bash
python3 -m venv .venv
.venv/bin/pip install '.[dev]'
.venv/bin/pytest
.venv/bin/python -m event_correlator --schema
```

Run those commands in [python/toolsets/event-correlator/src/](python/toolsets/event-correlator/src/), or validate from the repository root:

```bash
sam toolset validate event-correlator python
```

## Packaging

Point the SAM CLI at the language directory and the platform URL so dependencies and binaries match the Secure Tool Runtime architecture:

```bash
sam toolset package event-correlator go --url https://platform.example.com \
  --output event-correlator-go.zip
sam toolset package event-correlator python --url https://platform.example.com \
  --output event-correlator-python.zip
```

## Data model and concurrency

Both implementations create and use the same tables:

- `pending_events` holds one row per `(trade_id, source)` until the correlation is finalized.
- `audit_log` stores immutable `reconciled` and `break` outcomes.

Each trade is processed under a PostgreSQL transaction-level advisory lock. This prevents duplicate finalization when multiple tool invocations—or a mixed Go/Python deployment—operate concurrently against the same database.

## License

Apache 2.0
