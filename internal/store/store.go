// Package store defines the persistence interface for the correlation engine.
package store

import (
	"context"
	"time"

	"github.com/solacese/event-correlator-go/internal/model"
)

// PendingTrade represents a trade awaiting full source confirmation.
type PendingTrade struct {
	TradeID   string    `db:"trade_id"`
	Source    string    `db:"source"`
	Payload   []byte    `db:"payload"` // JSON-encoded TradeEvent
	FirstSeen time.Time `db:"first_seen"`
	Deadline  time.Time `db:"deadline"`
}

// Store is the persistence interface for the correlator.
type Store interface {
	// RecordEvent persists a single source event for a trade.
	// Returns true if this is a new (trade_id, source) pair, false if duplicate.
	RecordEvent(ctx context.Context, trade PendingTrade) (bool, error)

	// GetEvents returns all recorded events for a trade ID.
	GetEvents(ctx context.Context, tradeID string) ([]PendingTrade, error)

	// DeleteTrade removes all events for a trade (after reconciliation or break).
	DeleteTrade(ctx context.Context, tradeID string) error

	// GetExpired returns all trade IDs whose deadline has passed.
	GetExpired(ctx context.Context, now time.Time) ([]string, error)

	// RecordAudit writes an audit trail entry.
	RecordAudit(ctx context.Context, entry model.AuditEntry) error

	// Migrate runs schema migrations.
	Migrate(ctx context.Context) error

	// Close releases database resources.
	Close() error
}
