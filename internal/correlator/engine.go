package correlator

import (
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/solacecommunity/event-correlator-go/internal/model"
)

// pendingTrade tracks which source systems have reported for a single trade ID.
type pendingTrade struct {
	tradeID   string
	events    map[string]model.TradeEvent // source -> event
	firstSeen time.Time
	deadline  time.Time
}

// Engine is the pure correlation logic, decoupled from broker/messaging.
// Thread-safe via internal mutex.
type Engine struct {
	mu              sync.Mutex
	pending         map[string]*pendingTrade
	expectedSources map[string]struct{}
	window          time.Duration

	reconciled atomic.Int64
	breaks     atomic.Int64
	received   atomic.Int64
	duplicates atomic.Int64
}

// Stats holds current metric counters.
type Stats struct {
	Reconciled int64
	Breaks     int64
	Received   int64
	Duplicates int64
	Pending    int
}

// NewEngine creates a correlation engine expecting all listed sources
// within the given time window.
func NewEngine(expectedSources []string, window time.Duration) *Engine {
	expected := make(map[string]struct{}, len(expectedSources))
	for _, s := range expectedSources {
		expected[s] = struct{}{}
	}
	return &Engine{
		pending:         make(map[string]*pendingTrade),
		expectedSources: expected,
		window:          window,
	}
}

// Ingest processes a new trade event. Returns a ReconciledEvent if all expected
// sources have now reported for this trade ID, otherwise returns nil.
func (e *Engine) Ingest(event model.TradeEvent, now time.Time) *model.ReconciledEvent {
	e.received.Add(1)

	// Ignore events from unknown sources.
	if _, known := e.expectedSources[event.Source]; !known {
		return nil
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	pt, exists := e.pending[event.TradeID]
	if !exists {
		pt = &pendingTrade{
			tradeID:   event.TradeID,
			events:    make(map[string]model.TradeEvent, len(e.expectedSources)),
			firstSeen: now,
			deadline:  now.Add(e.window),
		}
		e.pending[event.TradeID] = pt
	}

	// Duplicate source for same trade — skip.
	if _, dup := pt.events[event.Source]; dup {
		e.duplicates.Add(1)
		return nil
	}

	pt.events[event.Source] = event

	// Check if all expected sources are present.
	if len(pt.events) < len(e.expectedSources) {
		return nil
	}

	// All sources arrived — reconcile.
	delete(e.pending, event.TradeID)
	e.reconciled.Add(1)

	sources := make([]string, 0, len(pt.events))
	events := make([]model.TradeEvent, 0, len(pt.events))
	for src, ev := range pt.events {
		sources = append(sources, src)
		events = append(events, ev)
	}
	slices.Sort(sources)

	return &model.ReconciledEvent{
		TradeID:       event.TradeID,
		Sources:       sources,
		ReconciledAt:  now,
		MatchDuration: now.Sub(pt.firstSeen).Milliseconds(),
		Events:        events,
	}
}

// Sweep checks all pending trades for expired correlation windows.
// Returns break events for any expired correlations.
func (e *Engine) Sweep(now time.Time) []model.BreakEvent {
	e.mu.Lock()
	defer e.mu.Unlock()

	var breaks []model.BreakEvent
	for tradeID, pt := range e.pending {
		if now.Before(pt.deadline) {
			continue
		}

		received := make([]string, 0, len(pt.events))
		events := make([]model.TradeEvent, 0, len(pt.events))
		for src, ev := range pt.events {
			received = append(received, src)
			events = append(events, ev)
		}
		slices.Sort(received)

		missing := make([]string, 0, len(e.expectedSources)-len(pt.events))
		for src := range e.expectedSources {
			if _, got := pt.events[src]; !got {
				missing = append(missing, src)
			}
		}
		slices.Sort(missing)

		breaks = append(breaks, model.BreakEvent{
			TradeID:         tradeID,
			MissingSources:  missing,
			ReceivedSources: received,
			DetectedAt:      now,
			WindowExpiry:    pt.deadline,
			Events:          events,
		})

		delete(e.pending, tradeID)
		e.breaks.Add(1)
	}
	return breaks
}

// PendingCount returns the number of in-flight correlations.
func (e *Engine) PendingCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.pending)
}

// GetStats returns current metric counters.
func (e *Engine) GetStats() Stats {
	e.mu.Lock()
	pending := len(e.pending)
	e.mu.Unlock()

	return Stats{
		Reconciled: e.reconciled.Load(),
		Breaks:     e.breaks.Load(),
		Received:   e.received.Load(),
		Duplicates: e.duplicates.Load(),
		Pending:    pending,
	}
}
