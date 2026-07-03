# Event Correlator for Post-Trade Reconciliation

A real-time event correlation engine in Go that reconciles trade events from multiple source systems. It correlates events by trade ID, detects successful matches when all sources report, and raises break alerts when the correlation window expires with missing sources.

Built for high-throughput financial post-trade environments where multiple independent systems (order management, risk, client reporting, settlement) must confirm the same trade within a time window.

## How It Works

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  Source A   │     │  Source B   │     │  Source C   │
│  (e.g. OMS) │     │  (e.g. Risk)│     │ (e.g. Settl)│
└──────┬──────┘     └──────┬──────┘     └──────┬──────┘
       │                   │                   │
  trades/source_a/>   trades/source_b/>   trades/source_c/>
       │                   │                   │
       └───────────────────┼───────────────────┘
                           │
              ┌────────────▼────────────┐
              │   Correlation Engine    │
              │   • Index by trade ID   │
              │   • Match N sources     │
              │   • Sweep for expiries  │
              └────────────┬────────────┘
                           │
             ┌─────────────┼─────────────┐
             │                           │
  reconciliation/matched       reconciliation/breaks
             │                           │
  ┌──────────▼──────────┐    ┌──────────▼──────────┐
  │  All sources matched │    │  Window expired     │
  │  within time window  │    │  (missing sources)  │
  └─────────────────────┘    └─────────────────────┘
```

### Key Design Decisions

- **Single durable queue**: All source topics fan into one queue — supports competing consumers for horizontal scaling
- **Periodic sweep** (not per-trade timers): Bounds goroutine count regardless of trade volume
- **Engine/Component split**: Pure correlation logic (`Engine`) is fully unit-testable without any messaging dependency
- **Lock-free metrics**: Stats use `sync/atomic` for hot-path counters, `sync.Mutex` only for the pending-trade map
- **Source inference**: If `source` isn't in the JSON payload, it's inferred from the topic path

## Usage

### As a Library

```go
import "github.com/solacecommunity/event-correlator-go/internal/correlator"

// Create engine expecting 3 sources within 5 minutes
engine := correlator.NewEngine(
    []string{"oms", "risk", "settlement"},
    5 * time.Minute,
)

// Feed events — returns non-nil when all sources match
result := engine.Ingest(tradeEvent, time.Now())
if result != nil {
    // All sources confirmed this trade
    log.Printf("Reconciled %s in %dms", result.TradeID, result.MatchDuration)
}

// Periodically sweep for expired correlations
breaks := engine.Sweep(time.Now())
for _, b := range breaks {
    log.Printf("BREAK: %s missing %v", b.TradeID, b.MissingSources)
}
```

### As a Component (with message broker)

The `Component` wires the engine to any message broker implementing the `Broker` interface:

```go
comp := correlator.NewComponent(myBroker, &correlator.Config{
    Sources: []correlator.SourceConfig{
        {Name: "oms", Topic: "trades/oms/>"},
        {Name: "risk", Topic: "trades/risk/>"},
        {Name: "settlement", Topic: "trades/settlement/>"},
    },
    ExpectedSources:   []string{"oms", "risk", "settlement"},
    CorrelationWindow:  5 * time.Minute,
    SweepInterval:      30 * time.Second,
    ReconciledTopic:    "reconciliation/matched",
    BreakTopic:         "reconciliation/breaks",
    QueuePrefix:        "recon",
}, logger)

comp.Start(ctx)
defer comp.Stop()
```

## Configuration

See [`config/correlator.yaml`](config/correlator.yaml) for production settings.

| Parameter | Default | Description |
|-----------|---------|-------------|
| `correlator.sources` | — | List of `{name, topic}` source systems |
| `correlator.expected_sources` | — | All sources that must report for reconciliation |
| `correlator.timing.correlation_window` | `5m` | Time to wait for all sources |
| `correlator.timing.sweep_interval` | `30s` | How often to check for expired trades |
| `correlator.output.reconciled_topic` | — | Topic for matched trade events |
| `correlator.output.break_topic` | — | Topic for expired/break events |

## Event Formats

### Input: Trade Event
```json
{
  "trade_id": "TRD-20260703-000001",
  "source": "oms",
  "timestamp": "2026-07-03T14:30:00Z",
  "instrument": "ISIN-XYZ",
  "quantity": 1000,
  "price": 12.50,
  "currency": "EUR",
  "counterparty": "BankX"
}
```

### Output: Reconciled Event
```json
{
  "trade_id": "TRD-20260703-000001",
  "sources": ["oms", "risk", "settlement"],
  "reconciled_at": "2026-07-03T14:30:02Z",
  "match_duration_ms": 2000,
  "events": [...]
}
```

### Output: Break Event
```json
{
  "trade_id": "TRD-20260703-000002",
  "missing_sources": ["settlement"],
  "received_sources": ["oms", "risk"],
  "detected_at": "2026-07-03T14:35:30Z",
  "window_expiry": "2026-07-03T14:35:00Z",
  "events": [...]
}
```

## Running Tests

```bash
go test -race -count=1 ./internal/...
```

## Performance Characteristics

- **Ingest latency**: O(1) per event (map lookup + insert)
- **Sweep latency**: O(n) over pending trades (single pass)
- **Memory**: ~500 bytes per pending trade (configurable via source count)
- **Concurrency**: Full concurrent safety under race detector
- **Scaling**: Horizontal via competing consumers on the durable queue

## Broker Interface

The `Broker` interface is minimal and broker-agnostic:

```go
type Broker interface {
    CreateQueue(name string, opts QueueOptions) (Queue, error)
    Subscribe(queue Queue, topic string) error
    Receive(ctx context.Context, queue Queue) (*Message, error)
    PublishGuaranteed(ctx context.Context, topic string, msg *Message) error
    IsConnected() bool
}
```

Implement this for Solace, Kafka, NATS, RabbitMQ, or any in-memory broker for testing.

## License

Apache 2.0
