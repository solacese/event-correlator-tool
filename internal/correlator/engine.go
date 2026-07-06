package correlator

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"slices"
	"sync/atomic"
	"time"

	"github.com/solacese/event-correlator-go/internal/model"
	"github.com/solacese/event-correlator-go/internal/store"
)

// Engine is the correlation logic backed by a persistent store.
// All state survives restarts.
type Engine struct {
	store           store.Store
	expectedSources map[string]struct{}
	window          time.Duration
	logger          *slog.Logger

	reconciled atomic.Int64
	breaks     atomic.Int64
	received   atomic.Int64
	duplicates atomic.Int64
}

// Stats holds current metric counters.
type Stats struct {
	Reconciled int64 `json:"reconciled"`
	Breaks     int64 `json:"breaks"`
	Received   int64 `json:"received"`
	Duplicates int64 `json:"duplicates"`
}

// NewEngine creates a correlation engine backed by the given store.
func NewEngine(s store.Store, expectedSources []string, window time.Duration, logger *slog.Logger) *Engine {
	expected := make(map[string]struct{}, len(expectedSources))
	for _, src := range expectedSources {
		expected[src] = struct{}{}
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Engine{
		store:           s,
		expectedSources: expected,
		window:          window,
		logger:          logger,
	}
}

// Ingest processes a trade event. Returns a ReconciledEvent if all expected
// sources have now reported, otherwise nil.
func (e *Engine) Ingest(ctx context.Context, event model.TradeEvent, now time.Time) (*model.ReconciledEvent, error) {
	e.received.Add(1)

	if _, known := e.expectedSources[event.Source]; !known {
		return nil, nil
	}

	payload, _ := json.Marshal(event)
	pending := store.PendingTrade{
		TradeID:   event.TradeID,
		Source:    event.Source,
		Payload:   payload,
		FirstSeen: now,
		Deadline:  now.Add(e.window),
	}

	inserted, err := e.store.RecordEvent(ctx, pending)
	if err != nil {
		return nil, fmt.Errorf("record event: %w", err)
	}
	if !inserted {
		e.duplicates.Add(1)
		return nil, nil
	}

	events, err := e.store.GetEvents(ctx, event.TradeID)
	if err != nil {
		return nil, fmt.Errorf("get events: %w", err)
	}

	if len(events) < len(e.expectedSources) {
		return nil, nil
	}

	// All sources arrived — reconcile.
	if err := e.store.DeleteTrade(ctx, event.TradeID); err != nil {
		return nil, fmt.Errorf("delete trade: %w", err)
	}
	e.reconciled.Add(1)

	reconciled := e.buildReconciled(events, now)

	// Audit trail.
	detail, _ := json.Marshal(reconciled)
	_ = e.store.RecordAudit(ctx, model.AuditEntry{
		TradeID:    event.TradeID,
		Outcome:    "reconciled",
		Sources:    string(mustJSON(reconciled.Sources)),
		Detail:     string(detail),
		OccurredAt: now,
		RecordedAt: now,
	})

	return reconciled, nil
}

// Sweep checks for trades whose correlation window has expired.
func (e *Engine) Sweep(ctx context.Context, now time.Time) ([]model.BreakEvent, error) {
	expired, err := e.store.GetExpired(ctx, now)
	if err != nil {
		return nil, fmt.Errorf("get expired: %w", err)
	}

	var breaks []model.BreakEvent
	for _, tradeID := range expired {
		events, err := e.store.GetEvents(ctx, tradeID)
		if err != nil {
			e.logger.Error("sweep: get events", "trade_id", tradeID, "err", err)
			continue
		}

		brk := e.buildBreak(tradeID, events, now)

		if err := e.store.DeleteTrade(ctx, tradeID); err != nil {
			e.logger.Error("sweep: delete trade", "trade_id", tradeID, "err", err)
			continue
		}
		e.breaks.Add(1)

		// Audit trail.
		detail, _ := json.Marshal(brk)
		_ = e.store.RecordAudit(ctx, model.AuditEntry{
			TradeID:    tradeID,
			Outcome:    "break",
			Sources:    string(mustJSON(brk.ReceivedSources)),
			Detail:     string(detail),
			OccurredAt: now,
			RecordedAt: now,
		})

		breaks = append(breaks, brk)
	}
	return breaks, nil
}

// GetStats returns current metric counters (reset on process restart).
func (e *Engine) GetStats() Stats {
	return Stats{
		Reconciled: e.reconciled.Load(),
		Breaks:     e.breaks.Load(),
		Received:   e.received.Load(),
		Duplicates: e.duplicates.Load(),
	}
}

func (e *Engine) buildReconciled(events []store.PendingTrade, now time.Time) *model.ReconciledEvent {
	sources := make([]string, 0, len(events))
	tradeEvents := make([]model.TradeEvent, 0, len(events))
	var firstSeen time.Time

	for _, ev := range events {
		sources = append(sources, ev.Source)
		var te model.TradeEvent
		_ = json.Unmarshal(ev.Payload, &te)
		tradeEvents = append(tradeEvents, te)
		if firstSeen.IsZero() || ev.FirstSeen.Before(firstSeen) {
			firstSeen = ev.FirstSeen
		}
	}
	slices.Sort(sources)

	return &model.ReconciledEvent{
		TradeID:       events[0].TradeID,
		Sources:       sources,
		ReconciledAt:  now,
		MatchDuration: now.Sub(firstSeen).Milliseconds(),
		Events:        tradeEvents,
	}
}

func (e *Engine) buildBreak(tradeID string, events []store.PendingTrade, now time.Time) model.BreakEvent {
	received := make([]string, 0, len(events))
	tradeEvents := make([]model.TradeEvent, 0, len(events))
	var deadline time.Time

	for _, ev := range events {
		received = append(received, ev.Source)
		var te model.TradeEvent
		_ = json.Unmarshal(ev.Payload, &te)
		tradeEvents = append(tradeEvents, te)
		if deadline.IsZero() || ev.Deadline.After(deadline) {
			deadline = ev.Deadline
		}
	}
	slices.Sort(received)

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

func mustJSON(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}
