package correlator

import (
	"context"
	"testing"
	"time"

	"github.com/solacese/event-correlator-tool/go/toolsets/event-correlator/src/internal/model"
	"github.com/solacese/event-correlator-tool/go/toolsets/event-correlator/src/internal/store"
	"github.com/solacese/event-correlator-tool/go/toolsets/event-correlator/src/internal/store/memory"
)

var testSources = []string{"source_a", "source_b", "source_c"}

func newTestEngine() (*Engine, *memory.Store) {
	s := memory.New()
	return NewEngine(s, testSources, 5*time.Minute), s
}

func baseEvent(tradeID, source string) model.TradeEvent {
	return model.TradeEvent{
		TradeID:      tradeID,
		Source:       source,
		Timestamp:    time.Date(2026, 7, 3, 14, 30, 0, 0, time.UTC),
		Instrument:   "ISIN-001",
		Quantity:     1000,
		Price:        12.50,
		Currency:     "EUR",
		Counterparty: "CounterpartyX",
	}
}

func TestEngineIngestLifecycle(t *testing.T) {
	engine, memoryStore := newTestEngine()
	ctx := context.Background()
	now := time.Date(2026, 7, 3, 14, 30, 0, 0, time.UTC)

	pending, err := engine.Ingest(ctx, baseEvent("TRD-001", "source_a"), now)
	if err != nil || pending.Status != "pending" {
		t.Fatalf("first ingest = %+v, %v; want pending", pending, err)
	}

	duplicate, err := engine.Ingest(ctx, baseEvent("TRD-001", "source_a"), now.Add(time.Second))
	if err != nil || duplicate.Status != "duplicate" {
		t.Fatalf("duplicate ingest = %+v, %v; want duplicate", duplicate, err)
	}

	_, _ = engine.Ingest(ctx, baseEvent("TRD-001", "source_b"), now.Add(time.Second))
	reconciled, err := engine.Ingest(ctx, baseEvent("TRD-001", "source_c"), now.Add(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if reconciled.Status != "reconciled" || reconciled.Reconciled == nil {
		t.Fatalf("final ingest = %+v; want reconciled", reconciled)
	}
	if reconciled.Reconciled.MatchDuration != 2000 {
		t.Errorf("match duration = %d; want 2000", reconciled.Reconciled.MatchDuration)
	}
	if got := memoryStore.PendingEvents("TRD-001"); len(got) != 0 {
		t.Errorf("pending events = %d; want 0", len(got))
	}

	audit, err := engine.AuditHistory(ctx, store.AuditFilter{TradeID: "TRD-001", Limit: 10})
	if err != nil || len(audit) != 1 || audit[0].Outcome != "reconciled" {
		t.Fatalf("audit = %+v, %v; want one reconciled entry", audit, err)
	}

	late, err := engine.Ingest(ctx, baseEvent("TRD-001", "source_a"), now.Add(3*time.Second))
	if err != nil || late.Status != "duplicate" {
		t.Fatalf("late ingest = %+v, %v; want duplicate finalized trade", late, err)
	}
}

func TestEngineIgnoresUnknownSource(t *testing.T) {
	engine, memoryStore := newTestEngine()
	result, err := engine.Ingest(
		context.Background(),
		baseEvent("TRD-001", "unknown"),
		time.Now().UTC(),
	)
	if err != nil || result.Status != "ignored_unknown_source" {
		t.Fatalf("ingest = %+v, %v; want ignored_unknown_source", result, err)
	}
	if got := memoryStore.PendingEvents("TRD-001"); len(got) != 0 {
		t.Errorf("pending events = %d; want 0", len(got))
	}
}

func TestEngineSweepCreatesBreak(t *testing.T) {
	memoryStore := memory.New()
	engine := NewEngine(memoryStore, testSources, 100*time.Millisecond)
	ctx := context.Background()
	now := time.Date(2026, 7, 3, 14, 30, 0, 0, time.UTC)

	_, _ = engine.Ingest(ctx, baseEvent("TRD-002", "source_a"), now)
	_, _ = engine.Ingest(ctx, baseEvent("TRD-002", "source_b"), now.Add(10*time.Millisecond))

	before, err := engine.Sweep(ctx, now.Add(50*time.Millisecond))
	if err != nil || len(before) != 0 {
		t.Fatalf("pre-expiry sweep = %+v, %v; want no breaks", before, err)
	}
	after, err := engine.Sweep(ctx, now.Add(200*time.Millisecond))
	if err != nil || len(after) != 1 {
		t.Fatalf("post-expiry sweep = %+v, %v; want one break", after, err)
	}
	if len(after[0].MissingSources) != 1 || after[0].MissingSources[0] != "source_c" {
		t.Errorf("missing sources = %v; want [source_c]", after[0].MissingSources)
	}

	audit, err := engine.AuditHistory(ctx, store.AuditFilter{Outcome: "break", Limit: 10})
	if err != nil || len(audit) != 1 {
		t.Fatalf("break audit = %+v, %v; want one entry", audit, err)
	}
}
