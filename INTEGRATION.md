# Integrating the Event Correlator into SAM Go

The correlator runs as a first-class AWE instance kind inside SAM Go. Once registered, it appears in the control plane alongside agents and workflows with full lifecycle management.

## Why It Lives Inside SAM Go

SAM Go's broker, runtime, and config packages are `internal/` (Go visibility rules). The correlator engine (`internal/correlator/`) is self-contained and has zero dependencies, but the **integration shim** that wires it to the SAM AWE must live inside the SAM Go module tree.

Two pieces:
1. **Engine** (this repo) - pure correlation logic, no SAM dependency, fully unit-tested
2. **Integration shim** (`sam-integration/instance.go`) - reference code to copy into SAM Go

## Step 1: Copy the Shim Into SAM Go

Copy `sam-integration/instance.go` into the SAM Go repo, e.g.:

```
solace-agent-mesh-go/
  examples/correlator/
    internal/instance/instance.go   ← the shim
```

Or inline it directly into `internal/bootstrap/awe.go` if you prefer fewer packages.

## Step 2: Register the Kind (one line)

In `internal/bootstrap/awe.go`:

```go
import "github.com/SolaceDev/solace-agent-mesh-go/examples/correlator/internal/instance"

// After existing RegisterKind calls:
exe.RegisterKind("correlator", instance.Factory)
```

## Step 3: Declare in SAM Config

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
      queue_prefix: bbva-recon
      output:
        reconciled_topic: "bbva/reconciliation/matched"
        break_topic: "bbva/reconciliation/breaks"
        workflow_trigger_topic: "solace-agent-mesh/a2a/v1/break_investigation/request"
```

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

## Architecture in SAM Go

```
Solace Broker (data plane)
    │
    │  bbva/trades/solar/>
    │  bbva/trades/murex/>
    │  bbva/trades/client_reporting/>
    │
    ▼
┌───────────────────────────────────────────────┐
│  AWE Process                                   │
│                                                │
│  ┌─────────────────────────┐                  │
│  │  Correlator Instance    │  kind: correlator│
│  │  • Durable queue        │                  │
│  │  • Correlation engine   │                  │
│  │  • Sweep timer          │                  │
│  └───────────┬─────────────┘                  │
│              │ break event                     │
│              ▼                                 │
│  ┌─────────────────────────┐                  │
│  │  Workflow Instance       │  kind: workflow  │
│  │  "break_investigation"  │                  │
│  └───────────┬─────────────┘                  │
│              │                                 │
│              ▼                                 │
│  ┌─────────────────────────┐                  │
│  │  Agent Instance          │  kind: agent    │
│  │  "trade_analyst"        │                  │
│  └─────────────────────────┘                  │
└───────────────────────────────────────────────┘
```

## What You Get

| Capability | Mechanism |
|------------|-----------|
| Health checks | `Health()` reports broker connectivity |
| Graceful shutdown | `Stop()` drains work, leaves queue for restart |
| Clean undeploy | `Remove()` deprovisions the durable queue |
| Horizontal scaling | Competing consumers on the named queue |
| Dynamic deploy/undeploy | AWE control plane manages lifecycle |
| Workflow trigger | Break events auto-trigger investigation workflows |
| Monitoring | `Stats()` exposes received/reconciled/breaks/pending |

## Broker Config

The correlator uses the shared SAM Solace broker connection. No separate config needed:

```yaml
broker:
  type: solace
  url: tcps://broker.example.com:55443
  vpn: sam
  username: ${SOLACE_USERNAME}
  password: ${SOLACE_PASSWORD}
```
