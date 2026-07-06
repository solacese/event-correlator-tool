// Package memory provides an in-memory store for tests and development.
package memory

import (
	"context"
	"sync"
	"time"

	"github.com/solacese/event-correlator-go/internal/model"
	"github.com/solacese/event-correlator-go/internal/store"
)

// Store implements store.Store in memory with no external dependencies.
type Store struct {
	mu     sync.Mutex
	events map[string][]store.PendingTrade // trade_id -> events
	audit  []model.AuditEntry
}

func New() *Store {
	return &Store{events: make(map[string][]store.PendingTrade)}
}

func (s *Store) Migrate(_ context.Context) error { return nil }
func (s *Store) Close() error                    { return nil }

func (s *Store) RecordEvent(_ context.Context, trade store.PendingTrade) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, e := range s.events[trade.TradeID] {
		if e.Source == trade.Source {
			return false, nil
		}
	}
	s.events[trade.TradeID] = append(s.events[trade.TradeID], trade)
	return true, nil
}

func (s *Store) GetEvents(_ context.Context, tradeID string) ([]store.PendingTrade, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]store.PendingTrade{}, s.events[tradeID]...), nil
}

func (s *Store) DeleteTrade(_ context.Context, tradeID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.events, tradeID)
	return nil
}

func (s *Store) GetExpired(_ context.Context, now time.Time) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var ids []string
	for tradeID, events := range s.events {
		if len(events) > 0 && !now.Before(events[0].Deadline) {
			ids = append(ids, tradeID)
		}
	}
	return ids, nil
}

func (s *Store) RecordAudit(_ context.Context, entry model.AuditEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.audit = append(s.audit, entry)
	return nil
}

// AuditLog returns all recorded audit entries (test helper).
func (s *Store) AuditLog() []model.AuditEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]model.AuditEntry{}, s.audit...)
}
