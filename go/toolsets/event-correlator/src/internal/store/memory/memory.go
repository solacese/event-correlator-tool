// Package memory provides an in-memory store for tests.
package memory

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/solacese/event-correlator-tool/go/toolsets/event-correlator/src/internal/model"
	"github.com/solacese/event-correlator-tool/go/toolsets/event-correlator/src/internal/store"
)

// Store implements store.Store in memory with no external dependencies.
type Store struct {
	mu     sync.Mutex
	events map[string][]store.PendingTrade
	audit  []model.AuditEntry
}

func New() *Store {
	return &Store{events: make(map[string][]store.PendingTrade)}
}

func (s *Store) Migrate(_ context.Context) error { return nil }
func (s *Store) Close() error                    { return nil }

func (s *Store) Ingest(_ context.Context, trade store.PendingTrade, expectedSources []string, buildAudit func([]store.PendingTrade) model.AuditEntry) (store.IngestResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, entry := range s.audit {
		if entry.TradeID == trade.TradeID {
			return store.IngestResult{}, nil
		}
	}
	for _, event := range s.events[trade.TradeID] {
		if event.Source == trade.Source {
			return store.IngestResult{}, nil
		}
	}
	for _, event := range s.events[trade.TradeID] {
		if event.Deadline.Before(trade.Deadline) {
			trade.Deadline = event.Deadline
		}
	}

	s.events[trade.TradeID] = append(s.events[trade.TradeID], trade)
	result := store.IngestResult{Inserted: true}
	if hasEverySource(s.events[trade.TradeID], expectedSources) {
		result.Events = expectedEvents(s.events[trade.TradeID], expectedSources)
		delete(s.events, trade.TradeID)
		s.audit = append(s.audit, buildAudit(result.Events))
	}
	return result, nil
}

func (s *Store) SweepExpired(_ context.Context, now time.Time, buildAudit func(string, []store.PendingTrade) model.AuditEntry) (map[string][]store.PendingTrade, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	expired := make(map[string][]store.PendingTrade)
	tradeIDs := make([]string, 0, len(s.events))
	for tradeID := range s.events {
		tradeIDs = append(tradeIDs, tradeID)
	}
	sort.Strings(tradeIDs)

	for _, tradeID := range tradeIDs {
		events := s.events[tradeID]
		if len(events) == 0 || now.Before(earliestDeadline(events)) {
			continue
		}
		claimed := append([]store.PendingTrade{}, events...)
		expired[tradeID] = claimed
		delete(s.events, tradeID)
		s.audit = append(s.audit, buildAudit(tradeID, claimed))
	}
	return expired, nil
}

func (s *Store) GetAudit(_ context.Context, filter store.AuditFilter) ([]model.AuditEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	limit := filter.Limit
	if limit <= 0 {
		limit = 100
	}
	result := make([]model.AuditEntry, 0, limit)
	for i := len(s.audit) - 1; i >= 0 && len(result) < limit; i-- {
		entry := s.audit[i]
		if filter.TradeID != "" && entry.TradeID != filter.TradeID {
			continue
		}
		if filter.Outcome != "" && entry.Outcome != filter.Outcome {
			continue
		}
		result = append(result, entry)
	}
	return result, nil
}

func (s *Store) PendingEvents(tradeID string) []store.PendingTrade {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]store.PendingTrade{}, s.events[tradeID]...)
}

func hasEverySource(events []store.PendingTrade, expectedSources []string) bool {
	seen := make(map[string]struct{}, len(events))
	for _, event := range events {
		seen[event.Source] = struct{}{}
	}
	for _, source := range expectedSources {
		if _, ok := seen[source]; !ok {
			return false
		}
	}
	return true
}

func expectedEvents(events []store.PendingTrade, expectedSources []string) []store.PendingTrade {
	expected := make(map[string]struct{}, len(expectedSources))
	for _, source := range expectedSources {
		expected[source] = struct{}{}
	}
	filtered := make([]store.PendingTrade, 0, len(expectedSources))
	for _, event := range events {
		if _, ok := expected[event.Source]; ok {
			filtered = append(filtered, event)
		}
	}
	return filtered
}

func earliestDeadline(events []store.PendingTrade) time.Time {
	earliest := events[0].Deadline
	for _, event := range events[1:] {
		if event.Deadline.Before(earliest) {
			earliest = event.Deadline
		}
	}
	return earliest
}
