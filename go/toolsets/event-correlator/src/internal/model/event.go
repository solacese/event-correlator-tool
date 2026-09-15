package model

import (
	"encoding/json"
	"time"
)

// TradeEvent is an inbound event from a source system.
type TradeEvent struct {
	TradeID      string    `json:"trade_id"`
	Source       string    `json:"source"`
	Timestamp    time.Time `json:"timestamp"`
	Instrument   string    `json:"instrument,omitempty"`
	Quantity     float64   `json:"quantity,omitempty"`
	Price        float64   `json:"price,omitempty"`
	Currency     string    `json:"currency,omitempty"`
	Counterparty string    `json:"counterparty,omitempty"`
}

// ReconciledEvent is returned when all expected sources have reported.
type ReconciledEvent struct {
	TradeID       string       `json:"trade_id"`
	Sources       []string     `json:"sources"`
	ReconciledAt  time.Time    `json:"reconciled_at"`
	MatchDuration int64        `json:"match_duration_ms"`
	Events        []TradeEvent `json:"events"`
}

// BreakEvent is returned when the correlation window expires without all sources.
type BreakEvent struct {
	TradeID         string       `json:"trade_id"`
	MissingSources  []string     `json:"missing_sources"`
	ReceivedSources []string     `json:"received_sources"`
	DetectedAt      time.Time    `json:"detected_at"`
	WindowExpiry    time.Time    `json:"window_expiry"`
	Events          []TradeEvent `json:"events"`
}

// AuditEntry records a finalized correlation outcome.
type AuditEntry struct {
	ID         string          `json:"id,omitempty"`
	TradeID    string          `json:"trade_id"`
	Outcome    string          `json:"outcome"`
	Sources    []string        `json:"sources"`
	Detail     json.RawMessage `json:"detail"`
	OccurredAt time.Time       `json:"occurred_at"`
	RecordedAt time.Time       `json:"recorded_at"`
}
