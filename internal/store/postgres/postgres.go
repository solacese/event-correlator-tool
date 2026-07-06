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
	"github.com/pressly/goose/v3"

	"github.com/solacese/event-correlator-go/internal/model"
	"github.com/solacese/event-correlator-go/internal/store"
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
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(30 * time.Minute)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}
	return &Store{db: db}, nil
}

func (s *Store) Migrate(ctx context.Context) error {
	goose.SetTableName("correlator_goose_version")
	provider, err := goose.NewProvider(
		goose.DialectPostgres,
		s.db,
		migrations(),
	)
	if err != nil {
		return fmt.Errorf("create migration provider: %w", err)
	}
	if _, err := provider.Up(ctx); err != nil {
		return fmt.Errorf("run migrations: %w", err)
	}
	return nil
}

func (s *Store) RecordEvent(ctx context.Context, trade store.PendingTrade) (bool, error) {
	result, err := s.db.ExecContext(ctx, `
		INSERT INTO pending_events (trade_id, source, payload, first_seen, deadline)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (trade_id, source) DO NOTHING`,
		trade.TradeID, trade.Source, trade.Payload, trade.FirstSeen, trade.Deadline,
	)
	if err != nil {
		return false, fmt.Errorf("record event: %w", err)
	}
	n, _ := result.RowsAffected()
	return n > 0, nil
}

func (s *Store) GetEvents(ctx context.Context, tradeID string) ([]store.PendingTrade, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT trade_id, source, payload, first_seen, deadline
		FROM pending_events WHERE trade_id = $1`, tradeID)
	if err != nil {
		return nil, fmt.Errorf("get events: %w", err)
	}
	defer rows.Close()

	var events []store.PendingTrade
	for rows.Next() {
		var e store.PendingTrade
		if err := rows.Scan(&e.TradeID, &e.Source, &e.Payload, &e.FirstSeen, &e.Deadline); err != nil {
			return nil, fmt.Errorf("scan event: %w", err)
		}
		events = append(events, e)
	}
	return events, rows.Err()
}

func (s *Store) DeleteTrade(ctx context.Context, tradeID string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM pending_events WHERE trade_id = $1`, tradeID)
	if err != nil {
		return fmt.Errorf("delete trade: %w", err)
	}
	return nil
}

func (s *Store) GetExpired(ctx context.Context, now time.Time) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT DISTINCT trade_id FROM pending_events WHERE deadline <= $1`, now)
	if err != nil {
		return nil, fmt.Errorf("get expired: %w", err)
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan expired: %w", err)
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func (s *Store) RecordAudit(ctx context.Context, entry model.AuditEntry) error {
	sourcesJSON, _ := json.Marshal(entry.Sources)
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO audit_log (trade_id, outcome, sources, detail, occurred_at)
		VALUES ($1, $2, $3, $4, $5)`,
		entry.TradeID, entry.Outcome, sourcesJSON, entry.Detail, entry.OccurredAt,
	)
	if err != nil {
		return fmt.Errorf("record audit: %w", err)
	}
	return nil
}

func (s *Store) Close() error {
	return s.db.Close()
}
