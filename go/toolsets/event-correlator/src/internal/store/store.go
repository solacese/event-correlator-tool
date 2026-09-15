// Package store defines the atomic persistence operations used by the correlator.
package store

import (
	"context"
	"time"

	"github.com/solacese/event-correlator-tool/go/toolsets/event-correlator/src/internal/model"
)

// PendingTrade represents a trade awaiting full source confirmation.
type PendingTrade struct {
	TradeID   string    `db:"trade_id"`
	Source    string    `db:"source"`
	Payload   []byte    `db:"payload"`
	FirstSeen time.Time `db:"first_seen"`
	Deadline  time.Time `db:"deadline"`
}

// IngestResult is the outcome of atomically recording an event and, when all
// expected sources are present, finalizing its trade.
type IngestResult struct {
	Inserted bool
	Events   []PendingTrade
}

// AuditFilter controls an audit-history query.
type AuditFilter struct {
	TradeID string
	Outcome string
	Limit   int
}

// Store is the persistence interface for the correlator.
type Store interface {
	// Ingest atomically records one event. When all expected sources are present,
	// it deletes the pending trade and writes the supplied reconciled audit entry.
	Ingest(ctx context.Context, trade PendingTrade, expectedSources []string, buildAudit func([]PendingTrade) model.AuditEntry) (IngestResult, error)

	// SweepExpired atomically claims expired trades, deletes their pending rows,
	// and writes one audit entry per trade before returning the claimed events.
	SweepExpired(ctx context.Context, now time.Time, buildAudit func(string, []PendingTrade) model.AuditEntry) (map[string][]PendingTrade, error)

	// GetAudit returns newest-first immutable audit entries matching the filter.
	GetAudit(ctx context.Context, filter AuditFilter) ([]model.AuditEntry, error)

	// Migrate creates or updates the shared database schema.
	Migrate(ctx context.Context) error

	// Close releases database resources.
	Close() error
}
