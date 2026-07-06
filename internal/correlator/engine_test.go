package correlator

import (
	"sync"
	"testing"
	"time"

	"github.com/solacese/event-correlator-go/internal/model"
)

var testSources = []string{"source_a", "source_b", "source_c"}

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
	e := NewEngine(testSources, 5*time.Minute)
	now := time.Now()

	result := e.Ingest(baseEvent("TRD-001", "source_a"), now)
	if result != nil {
		t.Fatal("expected nil result for single source, got reconciled event")
	}
	if e.PendingCount() != 1 {
		t.Errorf("pending count: got %d, want 1", e.PendingCount())
	}
}

func TestEngine_IngestAllSources_ProducesReconciled(t *testing.T) {
	e := NewEngine(testSources, 5*time.Minute)
	now := time.Now()

	e.Ingest(baseEvent("TRD-001", "source_a"), now)
	e.Ingest(baseEvent("TRD-001", "source_b"), now.Add(time.Second))
	result := e.Ingest(baseEvent("TRD-001", "source_c"), now.Add(2*time.Second))

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
	if e.PendingCount() != 0 {
		t.Errorf("pending count after reconcile: got %d, want 0", e.PendingCount())
	}
}

func TestEngine_DuplicateSource_Ignored(t *testing.T) {
	e := NewEngine(testSources, 5*time.Minute)
	now := time.Now()

	e.Ingest(baseEvent("TRD-001", "source_a"), now)
	result := e.Ingest(baseEvent("TRD-001", "source_a"), now.Add(time.Second))

	if result != nil {
		t.Fatal("expected nil for duplicate source, got reconciled")
	}
	if e.PendingCount() != 1 {
		t.Errorf("pending count: got %d, want 1", e.PendingCount())
	}

	stats := e.GetStats()
	if stats.Duplicates != 1 {
		t.Errorf("duplicates counter: got %d, want 1", stats.Duplicates)
	}
}

func TestEngine_UnknownSource_Ignored(t *testing.T) {
	e := NewEngine(testSources, 5*time.Minute)
	now := time.Now()

	result := e.Ingest(baseEvent("TRD-001", "unknown_system"), now)
	if result != nil {
		t.Fatal("expected nil for unknown source")
	}
	if e.PendingCount() != 0 {
		t.Errorf("pending count: got %d, want 0", e.PendingCount())
	}
}

func TestEngine_Sweep_ExpiresOldTrades(t *testing.T) {
	window := 100 * time.Millisecond
	e := NewEngine(testSources, window)
	now := time.Now()

	e.Ingest(baseEvent("TRD-001", "source_a"), now)
	e.Ingest(baseEvent("TRD-001", "source_b"), now)

	breaks := e.Sweep(now.Add(50 * time.Millisecond))
	if len(breaks) != 0 {
		t.Errorf("expected 0 breaks before expiry, got %d", len(breaks))
	}

	breaks = e.Sweep(now.Add(200 * time.Millisecond))
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
	if e.PendingCount() != 0 {
		t.Errorf("pending after sweep: got %d, want 0", e.PendingCount())
	}
}

func TestEngine_ConcurrentAccess(t *testing.T) {
	e := NewEngine(testSources, time.Minute)
	now := time.Now()

	var wg sync.WaitGroup
	for i := range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tradeID := "TRD-" + time.Now().Format("150405.000000000")
			for _, src := range testSources {
				e.Ingest(baseEvent(tradeID, src), now.Add(time.Duration(i)*time.Millisecond))
			}
		}()
	}
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			e.Sweep(now.Add(time.Hour))
		}()
	}
	wg.Wait()

	stats := e.GetStats()
	if stats.Received != 300 {
		t.Errorf("received: got %d, want 300", stats.Received)
	}
}

func TestEngine_Stats_Counters(t *testing.T) {
	e := NewEngine(testSources, 100*time.Millisecond)
	now := time.Now()

	e.Ingest(baseEvent("TRD-001", "source_a"), now)
	e.Ingest(baseEvent("TRD-001", "source_b"), now)
	e.Ingest(baseEvent("TRD-001", "source_c"), now)

	e.Ingest(baseEvent("TRD-002", "source_a"), now)
	e.Sweep(now.Add(200 * time.Millisecond))

	e.Ingest(baseEvent("TRD-003", "source_a"), now)
	e.Ingest(baseEvent("TRD-003", "source_a"), now)

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
	if stats.Pending != 1 {
		t.Errorf("pending: got %d, want 1", stats.Pending)
	}
}
