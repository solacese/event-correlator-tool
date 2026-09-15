package correlator

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"time"

	"github.com/solacese/event-correlator-tool/go/toolsets/event-correlator/src/internal/model"
	"github.com/solacese/event-correlator-tool/go/toolsets/event-correlator/src/internal/store"
)

// IngestOutcome describes what happened to an incoming event.
type IngestOutcome struct {
	Status     string                 `json:"status"`
	Reconciled *model.ReconciledEvent `json:"reconciled,omitempty"`
}

// Engine implements correlation rules independently of the SAM tool wrapper.
type Engine struct {
	store              store.Store
	expectedSources    map[string]struct{}
	expectedSourceList []string
	window             time.Duration
}

// NewEngine creates a correlation engine backed by the given store.
func NewEngine(s store.Store, expectedSources []string, window time.Duration) *Engine {
	expected := make(map[string]struct{}, len(expectedSources))
	for _, src := range expectedSources {
		expected[src] = struct{}{}
	}
	return &Engine{
		store:              s,
		expectedSources:    expected,
		expectedSourceList: slices.Clone(expectedSources),
		window:             window,
	}
}

// Ingest processes one trade event.
func (e *Engine) Ingest(ctx context.Context, event model.TradeEvent, now time.Time) (IngestOutcome, error) {
	if event.TradeID == "" {
		return IngestOutcome{}, fmt.Errorf("trade_id is required")
	}
	if _, known := e.expectedSources[event.Source]; !known {
		return IngestOutcome{Status: "ignored_unknown_source"}, nil
	}

	payload, err := json.Marshal(event)
	if err != nil {
		return IngestOutcome{}, fmt.Errorf("marshal event: %w", err)
	}
	pending := store.PendingTrade{
		TradeID:   event.TradeID,
		Source:    event.Source,
		Payload:   payload,
		FirstSeen: now,
		Deadline:  now.Add(e.window),
	}

	var reconciled *model.ReconciledEvent
	result, err := e.store.Ingest(ctx, pending, e.expectedSourceList, func(events []store.PendingTrade) model.AuditEntry {
		reconciled = e.buildReconciled(events, now)
		detail, _ := json.Marshal(reconciled)
		return model.AuditEntry{
			TradeID:    event.TradeID,
			Outcome:    "reconciled",
			Sources:    reconciled.Sources,
			Detail:     detail,
			OccurredAt: now,
			RecordedAt: now,
		}
	})
	if err != nil {
		return IngestOutcome{}, fmt.Errorf("ingest event: %w", err)
	}
	if !result.Inserted {
		return IngestOutcome{Status: "duplicate"}, nil
	}
	if reconciled == nil {
		return IngestOutcome{Status: "pending"}, nil
	}
	return IngestOutcome{Status: "reconciled", Reconciled: reconciled}, nil
}

// Sweep checks for trades whose correlation window has expired.
func (e *Engine) Sweep(ctx context.Context, now time.Time) ([]model.BreakEvent, error) {
	var built = make(map[string]model.BreakEvent)
	expired, err := e.store.SweepExpired(ctx, now, func(tradeID string, events []store.PendingTrade) model.AuditEntry {
		brk := e.buildBreak(tradeID, events, now)
		built[tradeID] = brk
		detail, _ := json.Marshal(brk)
		return model.AuditEntry{
			TradeID:    tradeID,
			Outcome:    "break",
			Sources:    brk.ReceivedSources,
			Detail:     detail,
			OccurredAt: now,
			RecordedAt: now,
		}
	})
	if err != nil {
		return nil, fmt.Errorf("sweep expired trades: %w", err)
	}

	breaks := make([]model.BreakEvent, 0, len(expired))
	for tradeID := range expired {
		breaks = append(breaks, built[tradeID])
	}
	slices.SortFunc(breaks, func(a, b model.BreakEvent) int { return compareStrings(a.TradeID, b.TradeID) })
	return breaks, nil
}

// AuditHistory returns persisted correlation outcomes.
func (e *Engine) AuditHistory(ctx context.Context, filter store.AuditFilter) ([]model.AuditEntry, error) {
	return e.store.GetAudit(ctx, filter)
}

func (e *Engine) buildReconciled(events []store.PendingTrade, now time.Time) *model.ReconciledEvent {
	slices.SortFunc(events, func(a, b store.PendingTrade) int { return compareStrings(a.Source, b.Source) })
	sources := make([]string, 0, len(events))
	tradeEvents := make([]model.TradeEvent, 0, len(events))
	var firstSeen time.Time

	for _, ev := range events {
		sources = append(sources, ev.Source)
		var tradeEvent model.TradeEvent
		_ = json.Unmarshal(ev.Payload, &tradeEvent)
		tradeEvents = append(tradeEvents, tradeEvent)
		if firstSeen.IsZero() || ev.FirstSeen.Before(firstSeen) {
			firstSeen = ev.FirstSeen
		}
	}

	return &model.ReconciledEvent{
		TradeID:       events[0].TradeID,
		Sources:       sources,
		ReconciledAt:  now,
		MatchDuration: now.Sub(firstSeen).Milliseconds(),
		Events:        tradeEvents,
	}
}

func (e *Engine) buildBreak(tradeID string, events []store.PendingTrade, now time.Time) model.BreakEvent {
	slices.SortFunc(events, func(a, b store.PendingTrade) int { return compareStrings(a.Source, b.Source) })
	received := make([]string, 0, len(events))
	tradeEvents := make([]model.TradeEvent, 0, len(events))
	var deadline time.Time

	for _, ev := range events {
		received = append(received, ev.Source)
		var tradeEvent model.TradeEvent
		_ = json.Unmarshal(ev.Payload, &tradeEvent)
		tradeEvents = append(tradeEvents, tradeEvent)
		if deadline.IsZero() || ev.Deadline.After(deadline) {
			deadline = ev.Deadline
		}
	}

	missing := make([]string, 0, len(e.expectedSources)-len(events))
	for src := range e.expectedSources {
		if !slices.Contains(received, src) {
			missing = append(missing, src)
		}
	}
	slices.Sort(missing)

	return model.BreakEvent{
		TradeID:         tradeID,
		MissingSources:  missing,
		ReceivedSources: received,
		DetectedAt:      now,
		WindowExpiry:    deadline,
		Events:          tradeEvents,
	}
}

func compareStrings(a, b string) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}
