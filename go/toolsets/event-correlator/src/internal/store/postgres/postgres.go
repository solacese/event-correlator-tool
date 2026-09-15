// Package postgres implements the correlator store backed by PostgreSQL.
package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"

	"github.com/solacese/event-correlator-tool/go/toolsets/event-correlator/src/internal/model"
	"github.com/solacese/event-correlator-tool/go/toolsets/event-correlator/src/internal/store"
)

// Store implements store.Store backed by PostgreSQL.
type Store struct {
	db *sql.DB
}

// New opens a connection pool to the given Postgres DSN.
func New(databaseURL string) (*Store, error) {
	connConfig, err := pgx.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse database URL: %w", err)
	}
	db := stdlib.OpenDB(*connConfig)
	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(2)
	db.SetConnMaxLifetime(5 * time.Minute)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}
	return &Store{db: db}, nil
}

func (s *Store) Migrate(ctx context.Context) error {
	migration, err := migrationSQL()
	if err != nil {
		return fmt.Errorf("read migration: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, migration); err != nil {
		return fmt.Errorf("run migration: %w", err)
	}
	return nil
}

func (s *Store) Ingest(ctx context.Context, trade store.PendingTrade, expectedSources []string, buildAudit func([]store.PendingTrade) model.AuditEntry) (result store.IngestResult, err error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return result, fmt.Errorf("begin transaction: %w", err)
	}
	defer rollback(tx)

	if _, err = tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, trade.TradeID); err != nil {
		return result, fmt.Errorf("lock trade: %w", err)
	}

	var finalized bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM audit_log WHERE trade_id = $1)`, trade.TradeID).Scan(&finalized); err != nil {
		return result, fmt.Errorf("check finalized trade: %w", err)
	}
	if finalized {
		if err := tx.Commit(); err != nil {
			return result, fmt.Errorf("commit finalized check: %w", err)
		}
		return result, nil
	}

	existing, err := getEvents(ctx, tx, trade.TradeID)
	if err != nil {
		return result, err
	}
	for _, event := range existing {
		if event.Deadline.Before(trade.Deadline) {
			trade.Deadline = event.Deadline
		}
	}

	insert, err := tx.ExecContext(ctx, `
		INSERT INTO pending_events (trade_id, source, payload, first_seen, deadline)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (trade_id, source) DO NOTHING`,
		trade.TradeID, trade.Source, trade.Payload, trade.FirstSeen, trade.Deadline,
	)
	if err != nil {
		return result, fmt.Errorf("record event: %w", err)
	}
	rowsAffected, err := insert.RowsAffected()
	if err != nil {
		return result, fmt.Errorf("read insert result: %w", err)
	}
	if rowsAffected == 0 {
		if err := tx.Commit(); err != nil {
			return result, fmt.Errorf("commit duplicate: %w", err)
		}
		return result, nil
	}
	result.Inserted = true

	events, err := getEvents(ctx, tx, trade.TradeID)
	if err != nil {
		return result, err
	}
	if !hasEverySource(events, expectedSources) {
		if err := tx.Commit(); err != nil {
			return result, fmt.Errorf("commit pending event: %w", err)
		}
		return result, nil
	}

	result.Events = expectedEvents(events, expectedSources)
	if _, err := tx.ExecContext(ctx, `DELETE FROM pending_events WHERE trade_id = $1`, trade.TradeID); err != nil {
		return result, fmt.Errorf("delete reconciled trade: %w", err)
	}
	if err := recordAudit(ctx, tx, buildAudit(result.Events)); err != nil {
		return result, err
	}
	if err := tx.Commit(); err != nil {
		return result, fmt.Errorf("commit reconciliation: %w", err)
	}
	return result, nil
}

func (s *Store) SweepExpired(ctx context.Context, now time.Time, buildAudit func(string, []store.PendingTrade) model.AuditEntry) (map[string][]store.PendingTrade, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT DISTINCT trade_id
		FROM pending_events
		WHERE deadline <= $1
		ORDER BY trade_id`, now)
	if err != nil {
		return nil, fmt.Errorf("get expired trades: %w", err)
	}
	var tradeIDs []string
	for rows.Next() {
		var tradeID string
		if err := rows.Scan(&tradeID); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan expired trade: %w", err)
		}
		tradeIDs = append(tradeIDs, tradeID)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close expired rows: %w", err)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate expired trades: %w", err)
	}

	expired := make(map[string][]store.PendingTrade)
	for _, tradeID := range tradeIDs {
		events, claimed, err := s.claimExpired(ctx, tradeID, now, buildAudit)
		if err != nil {
			return nil, err
		}
		if claimed {
			expired[tradeID] = events
		}
	}
	return expired, nil
}

func (s *Store) claimExpired(ctx context.Context, tradeID string, now time.Time, buildAudit func(string, []store.PendingTrade) model.AuditEntry) (events []store.PendingTrade, claimed bool, err error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, false, fmt.Errorf("begin sweep transaction: %w", err)
	}
	defer rollback(tx)

	var locked bool
	if err := tx.QueryRowContext(ctx, `SELECT pg_try_advisory_xact_lock(hashtextextended($1, 0))`, tradeID).Scan(&locked); err != nil {
		return nil, false, fmt.Errorf("lock expired trade %q: %w", tradeID, err)
	}
	if !locked {
		return nil, false, nil
	}

	events, err = getEvents(ctx, tx, tradeID)
	if err != nil {
		return nil, false, err
	}
	if len(events) == 0 || earliestDeadline(events).After(now) {
		if err := tx.Commit(); err != nil {
			return nil, false, fmt.Errorf("commit skipped sweep: %w", err)
		}
		return nil, false, nil
	}

	if _, err := tx.ExecContext(ctx, `DELETE FROM pending_events WHERE trade_id = $1`, tradeID); err != nil {
		return nil, false, fmt.Errorf("delete expired trade: %w", err)
	}
	if err := recordAudit(ctx, tx, buildAudit(tradeID, events)); err != nil {
		return nil, false, err
	}
	if err := tx.Commit(); err != nil {
		return nil, false, fmt.Errorf("commit break: %w", err)
	}
	return events, true, nil
}

func (s *Store) GetAudit(ctx context.Context, filter store.AuditFilter) ([]model.AuditEntry, error) {
	limit := filter.Limit
	if limit <= 0 {
		limit = 100
	}
	if limit > 500 {
		limit = 500
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT id::text, trade_id, outcome, sources, detail, occurred_at, recorded_at
		FROM audit_log
		WHERE ($1 = '' OR trade_id = $1)
		  AND ($2 = '' OR outcome = $2)
		ORDER BY recorded_at DESC, id DESC
		LIMIT $3`, filter.TradeID, filter.Outcome, limit)
	if err != nil {
		return nil, fmt.Errorf("query audit history: %w", err)
	}
	defer rows.Close()

	entries := make([]model.AuditEntry, 0)
	for rows.Next() {
		var entry model.AuditEntry
		var sourcesJSON []byte
		if err := rows.Scan(&entry.ID, &entry.TradeID, &entry.Outcome, &sourcesJSON, &entry.Detail, &entry.OccurredAt, &entry.RecordedAt); err != nil {
			return nil, fmt.Errorf("scan audit history: %w", err)
		}
		if err := json.Unmarshal(sourcesJSON, &entry.Sources); err != nil {
			return nil, fmt.Errorf("decode audit sources: %w", err)
		}
		entries = append(entries, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate audit history: %w", err)
	}
	return entries, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func getEvents(ctx context.Context, tx *sql.Tx, tradeID string) ([]store.PendingTrade, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT trade_id, source, payload, first_seen, deadline
		FROM pending_events
		WHERE trade_id = $1
		ORDER BY source
		FOR UPDATE`, tradeID)
	if err != nil {
		return nil, fmt.Errorf("get events: %w", err)
	}
	defer rows.Close()

	events := make([]store.PendingTrade, 0)
	for rows.Next() {
		var event store.PendingTrade
		if err := rows.Scan(&event.TradeID, &event.Source, &event.Payload, &event.FirstSeen, &event.Deadline); err != nil {
			return nil, fmt.Errorf("scan event: %w", err)
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate events: %w", err)
	}
	return events, nil
}

func recordAudit(ctx context.Context, tx *sql.Tx, entry model.AuditEntry) error {
	sourcesJSON, err := json.Marshal(entry.Sources)
	if err != nil {
		return fmt.Errorf("marshal audit sources: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO audit_log (trade_id, outcome, sources, detail, occurred_at)
		VALUES ($1, $2, $3, $4, $5)`,
		entry.TradeID, entry.Outcome, sourcesJSON, entry.Detail, entry.OccurredAt,
	); err != nil {
		return fmt.Errorf("record audit: %w", err)
	}
	return nil
}

func rollback(tx *sql.Tx) {
	_ = tx.Rollback()
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
