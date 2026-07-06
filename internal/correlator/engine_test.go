package correlator

import (
	"sync"
	"testing"
	"time"

	"github.com/solacese/event-correlator-go/internal/model"
	"github.com/solacese/event-correlator-go/internal/store/memory"
)

var testSources = []string{"source_a", "source_b", "source_c"}

func newTestEngine() (*Engine, *memory.Store) {
	s := memory.New()
	e := NewEngine(s, testSources, 5*time.Minute, nil)
	return e, s
}

func baseEvent(tradeID, source string) model.TradeEvent {
	return model.TradeEvent{
		TradeID:      tradeID,
		Source:       source,
		Timestamp:    time.Now(),
		Instrument:   "ISIN-001",
		Quantity:     1000,
		Price:        12.50,
		Currency:     "EUR",
		Counterparty: "CounterpartyX",
	}
}

func TestEngine_IngestSingleSource(t *testing.T) {
	e, s := newTestEngine()
	ctx := t.Context()
	now := time.Now()

	result, err := e.Ingest(ctx, baseEvent("TRD-001", "source_a"), now)
	if err != nil {
		t.Fatal(err)
	}
	if result != nil {
		t.Fatal("expected nil result for single source, got reconciled event")
	}
	events, _ := s.GetEvents(ctx, "TRD-001")
	if len(events) != 1 {
		t.Errorf("pending events: got %d, want 1", len(events))
	}
}

func TestEngine_IngestAllSources_ProducesReconciled(t *testing.T) {
	e, _ := newTestEngine()
	ctx := t.Context()
	now := time.Now()

	e.Ingest(ctx, baseEvent("TRD-001", "source_a"), now)
	e.Ingest(ctx, baseEvent("TRD-001", "source_b"), now.Add(time.Second))
	result, err := e.Ingest(ctx, baseEvent("TRD-001", "source_c"), now.Add(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}

	if result == nil {
		t.Fatal("expected reconciled event, got nil")
	}
	if result.TradeID != "TRD-001" {
		t.Errorf("trade_id: got %q, want TRD-001", result.TradeID)
	}
	if len(result.Sources) != 3 {
		t.Errorf("sources: got %d, want 3", len(result.Sources))
	}
	if result.MatchDuration != 2000 {
		t.Errorf("match_duration_ms: got %d, want 2000", result.MatchDuration)
	}
}

func TestEngine_DuplicateSource_Ignored(t *testing.T) {
	e, _ := newTestEngine()
	ctx := t.Context()
	now := time.Now()

	e.Ingest(ctx, baseEvent("TRD-001", "source_a"), now)
	result, err := e.Ingest(ctx, baseEvent("TRD-001", "source_a"), now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}

	if result != nil {
		t.Fatal("expected nil for duplicate source, got reconciled")
	}

	stats := e.GetStats()
	if stats.Duplicates != 1 {
		t.Errorf("duplicates counter: got %d, want 1", stats.Duplicates)
	}
}

func TestEngine_UnknownSource_Ignored(t *testing.T) {
	e, s := newTestEngine()
	ctx := t.Context()
	now := time.Now()

	result, err := e.Ingest(ctx, baseEvent("TRD-001", "unknown_system"), now)
	if err != nil {
		t.Fatal(err)
	}
	if result != nil {
		t.Fatal("expected nil for unknown source")
	}
	events, _ := s.GetEvents(ctx, "TRD-001")
	if len(events) != 0 {
		t.Errorf("pending events: got %d, want 0", len(events))
	}
}

func TestEngine_Sweep_ExpiresOldTrades(t *testing.T) {
	window := 100 * time.Millisecond
	s := memory.New()
	e := NewEngine(s, testSources, window, nil)
	ctx := t.Context()
	now := time.Now()

	e.Ingest(ctx, baseEvent("TRD-001", "source_a"), now)
	e.Ingest(ctx, baseEvent("TRD-001", "source_b"), now)

	breaks, err := e.Sweep(ctx, now.Add(50*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	if len(breaks) != 0 {
		t.Errorf("expected 0 breaks before expiry, got %d", len(breaks))
	}

	breaks, err = e.Sweep(ctx, now.Add(200*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	if len(breaks) != 1 {
		t.Fatalf("expected 1 break after expiry, got %d", len(breaks))
	}

	brk := breaks[0]
	if brk.TradeID != "TRD-001" {
		t.Errorf("break trade_id: got %q, want TRD-001", brk.TradeID)
	}
	if len(brk.MissingSources) != 1 || brk.MissingSources[0] != "source_c" {
		t.Errorf("missing sources: got %v, want [source_c]", brk.MissingSources)
	}
}

func TestEngine_AuditTrail(t *testing.T) {
	e, s := newTestEngine()
	ctx := t.Context()
	now := time.Now()

	// Reconcile one trade.
	e.Ingest(ctx, baseEvent("TRD-001", "source_a"), now)
	e.Ingest(ctx, baseEvent("TRD-001", "source_b"), now)
	e.Ingest(ctx, baseEvent("TRD-001", "source_c"), now)

	audit := s.AuditLog()
	if len(audit) != 1 {
		t.Fatalf("audit entries: got %d, want 1", len(audit))
	}
	if audit[0].Outcome != "reconciled" {
		t.Errorf("outcome: got %q, want reconciled", audit[0].Outcome)
	}
	if audit[0].TradeID != "TRD-001" {
		t.Errorf("trade_id: got %q, want TRD-001", audit[0].TradeID)
	}
}

func TestEngine_Sweep_AuditTrail(t *testing.T) {
	window := 100 * time.Millisecond
	s := memory.New()
	e := NewEngine(s, testSources, window, nil)
	ctx := t.Context()
	now := time.Now()

	e.Ingest(ctx, baseEvent("TRD-002", "source_a"), now)
	e.Sweep(ctx, now.Add(200*time.Millisecond))

	audit := s.AuditLog()
	if len(audit) != 1 {
		t.Fatalf("audit entries: got %d, want 1", len(audit))
	}
	if audit[0].Outcome != "break" {
		t.Errorf("outcome: got %q, want break", audit[0].Outcome)
	}
}

func TestEngine_ConcurrentAccess(t *testing.T) {
	e, _ := newTestEngine()
	ctx := t.Context()
	now := time.Now()

	var wg sync.WaitGroup
	for i := range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tradeID := "TRD-" + time.Now().Format("150405.000000000")
			for _, src := range testSources {
				e.Ingest(ctx, baseEvent(tradeID, src), now.Add(time.Duration(i)*time.Millisecond))
			}
		}()
	}
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			e.Sweep(ctx, now.Add(time.Hour))
		}()
	}
	wg.Wait()

	stats := e.GetStats()
	if stats.Received != 300 {
		t.Errorf("received: got %d, want 300", stats.Received)
	}
}

func TestEngine_Stats_Counters(t *testing.T) {
	window := 100 * time.Millisecond
	s := memory.New()
	e := NewEngine(s, testSources, window, nil)
	ctx := t.Context()
	now := time.Now()

	e.Ingest(ctx, baseEvent("TRD-001", "source_a"), now)
	e.Ingest(ctx, baseEvent("TRD-001", "source_b"), now)
	e.Ingest(ctx, baseEvent("TRD-001", "source_c"), now)

	e.Ingest(ctx, baseEvent("TRD-002", "source_a"), now)
	e.Sweep(ctx, now.Add(200*time.Millisecond))

	e.Ingest(ctx, baseEvent("TRD-003", "source_a"), now)
	e.Ingest(ctx, baseEvent("TRD-003", "source_a"), now)

	stats := e.GetStats()
	if stats.Reconciled != 1 {
		t.Errorf("reconciled: got %d, want 1", stats.Reconciled)
	}
	if stats.Breaks != 1 {
		t.Errorf("breaks: got %d, want 1", stats.Breaks)
	}
	if stats.Received != 6 {
		t.Errorf("received: got %d, want 6", stats.Received)
	}
	if stats.Duplicates != 1 {
		t.Errorf("duplicates: got %d, want 1", stats.Duplicates)
	}
}
