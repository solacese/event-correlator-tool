# Event Correlator — Python

Solace Agent Mesh toolset implemented in Python. It exposes the same three tools and PostgreSQL schema as the Go option:

- `ingest_trade_event`
- `sweep_expired_trades`
- `get_audit_history`

## Develop

```bash
python3 -m venv .venv
.venv/bin/pip install '.[dev]'
.venv/bin/pytest
.venv/bin/python -m event_correlator --schema
```

## Validate and package

From the repository root:

```bash
sam toolset validate event-correlator python
sam toolset package event-correlator python --url https://platform.example.com
```

The deployment bundle vendors `sam-tool-sdk`, Psycopg, and their dependencies under the Python runtime tree. The SAM CLI selects wheels for the Secure Tool Runtime target architecture.
