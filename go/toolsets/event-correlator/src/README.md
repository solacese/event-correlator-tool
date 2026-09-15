# Event Correlator — Go

Solace Agent Mesh toolset implemented in Go. It exposes three tools from one Secure Tool Runtime executable:

- `ingest_trade_event` stores one source event and returns `pending`, `duplicate`, `ignored_unknown_source`, or `reconciled`.
- `sweep_expired_trades` finalizes expired incomplete correlations as breaks.
- `get_audit_history` reads immutable reconciliation and break records.

All state is stored in PostgreSQL. The tool automatically creates the shared `pending_events` and `audit_log` tables. PostgreSQL advisory locks make calls safe when multiple Go or Python tool instances share the database.

## Develop

```bash
go test ./...
go run . --schema
```

The SAM CLI-generated SDK is vendored under `_sdk/samtoolsdk`. Refresh it after a CLI upgrade with `sam toolset sync event-correlator go`.

## Validate and package

From the repository root:

```bash
sam toolset validate event-correlator go
sam toolset package event-correlator go --url https://platform.example.com
```

For a manual build, set `SAM_TOOL_TARGET_OS` and `SAM_TOOL_TARGET_ARCH` before running `./build.sh`.
