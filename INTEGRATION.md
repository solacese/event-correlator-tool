# Integrating the Event Correlator into SAM Go

The correlator runs as a first-class AWE instance kind inside SAM Go. Once registered, it appears in the control plane alongside agents and workflows with full lifecycle management.

## Why It Lives Inside SAM Go

SAM Go's broker, runtime, and config packages are `internal/` (Go visibility rules). The correlator engine and store packages are self-contained, but the **integration shim** that wires them to the SAM AWE must live inside the SAM Go module tree.

Three pieces:
1. **Store interface + Postgres implementation** (this repo) — persistence layer with migrations
2. **Engine** (this repo) — correlation logic operating on the store
3. **Integration shim** (`sam-integration/instance.go`) — reference code to copy into SAM Go

## Step 1: Copy the Shim Into SAM Go

Copy `sam-integration/instance.go` into the SAM Go repo:

```
solace-agent-mesh-go/
  examples/correlator/
    internal/instance/instance.go   <- the shim
```

## Step 2: Register the Kind (one line)

In `internal/bootstrap/awe.go`:

```go
import "github.com/SolaceDev/solace-agent-mesh-go/examples/correlator/internal/instance"

exe.RegisterKind("correlator", instance.Factory)
```

## Step 3: Declare in SAM Config

```yaml
apps:
  - name: trade_correlator
    app_config:
      kind: correlator
      namespace: solace-agent-mesh

      # Uses the shared SAM Postgres (same as session_service).
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
      queue_prefix: bbva-recon
      output:
        reconciled_topic: "bbva/reconciliation/matched"
        break_topic: "bbva/reconciliation/breaks"
        workflow_trigger_topic: "solace-agent-mesh/a2a/v1/break_investigation/request"
```

The correlator runs its own goose migrations on startup (table `correlator_goose_version`), creating `pending_events` and `audit_log` tables. Safe to share the database with SAM's session store.

## Step 4: Create the Downstream Workflow

The correlator publishes break events to `workflow_trigger_topic`. A SAM workflow subscribes and acts:

```yaml
apps:
  - name: break_investigation
    app_config:
      kind: workflow
      namespace: solace-agent-mesh
      nodes:
        - name: analyze
          type: agent
          agent_name: trade_analyst
          input: "${message}"
        - name: notify
          type: tool
          tool_name: send_notification
          input:
            channel: "#trade-ops"
            message: "${analyze.output}"
          depends_on: [analyze]
```

## Architecture

```
Solace Broker (data plane)
    |
    |  bbva/trades/solar/>
    |  bbva/trades/murex/>
    |  bbva/trades/client_reporting/>
    |
    v
+-----------------------------------------------+
|  AWE Process                                   |
|                                                |
|  +-------------------------+                   |
|  |  Correlator Instance    |  kind: correlator |
|  |  - Durable queue        |                   |
|  |  - Postgres store       |                   |
|  |  - Sweep timer          |                   |
|  +------------+------------+                   |
|               | break event                    |
|               v                                |
|  +-------------------------+                   |
|  |  Workflow Instance       |  kind: workflow   |
|  |  "break_investigation"  |                   |
|  +------------+------------+                   |
|               |                                |
|               v                                |
|  +-------------------------+                   |
|  |  Agent Instance          |  kind: agent     |
|  |  "trade_analyst"        |                   |
|  +-------------------------+                   |
+-----------------------------------------------+
            |
            v
+-------------------+
|  PostgreSQL       |
|  - pending_events |
|  - audit_log      |
+-------------------+
```

## What You Get

| Capability | Mechanism |
|------------|-----------|
| Crash recovery | Pending trades restored from Postgres on restart |
| Audit trail | Every outcome (reconciled/break) permanently logged |
| Health checks | `Health()` reports broker connectivity |
| Graceful shutdown | `Stop()` drains work, closes DB, leaves queue for restart |
| Clean undeploy | `Remove()` deprovisions the durable queue |
| Horizontal scaling | Competing consumers on the named queue |
| Dynamic deploy/undeploy | AWE control plane manages lifecycle |
| Workflow trigger | Break events auto-trigger investigation workflows |

## Database

The correlator uses the shared SAM Postgres connection. On first start, it auto-migrates two tables:

- `pending_events` — in-flight correlations (cleaned up after reconciliation/break)
- `audit_log` — immutable regulatory trail (never deleted)

Migration versioning is independent (table: `correlator_goose_version`) so it cannot conflict with SAM's own migrations.

## Broker Config

Uses the shared SAM Solace broker connection. No separate config needed:

```yaml
broker:
  type: solace
  url: tcps://broker.example.com:55443
  vpn: sam
  username: ${SOLACE_USERNAME}
  password: ${SOLACE_PASSWORD}
```
