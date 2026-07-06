package model

import "time"

// TradeEvent is the inbound event from a source system.
type TradeEvent struct {
	TradeID      string    `json:"trade_id"`
	Source       string    `json:"source"`
	Timestamp    time.Time `json:"timestamp"`
	Instrument   string    `json:"instrument,omitempty"`
	Quantity     float64   `json:"quantity,omitempty"`
	Price        float64   `json:"price,omitempty"`
	Currency     string    `json:"currency,omitempty"`
	Counterparty string    `json:"counterparty,omitempty"`
	RawPayload   []byte    `json:"raw_payload,omitempty"`
}

// ReconciledEvent is published when all expected sources have reported for a trade.
type ReconciledEvent struct {
	TradeID       string       `json:"trade_id"`
	Sources       []string     `json:"sources"`
	ReconciledAt  time.Time    `json:"reconciled_at"`
	MatchDuration int64        `json:"match_duration_ms"`
	Events        []TradeEvent `json:"events"`
}

// BreakEvent is published when the correlation window expires without all sources.
type BreakEvent struct {
	TradeID         string       `json:"trade_id"`
	MissingSources  []string     `json:"missing_sources"`
	ReceivedSources []string     `json:"received_sources"`
	DetectedAt      time.Time    `json:"detected_at"`
	WindowExpiry    time.Time    `json:"window_expiry"`
	Events          []TradeEvent `json:"events"`
}

// AuditEntry records every correlation outcome for regulatory audit trail.
type AuditEntry struct {
	ID         string    `json:"id" db:"id"`
	TradeID    string    `json:"trade_id" db:"trade_id"`
	Outcome    string    `json:"outcome" db:"outcome"` // "reconciled" or "break"
	Sources    string    `json:"sources" db:"sources"` // JSON array
	Detail     string    `json:"detail" db:"detail"`   // full JSON payload
	OccurredAt time.Time `json:"occurred_at" db:"occurred_at"`
	RecordedAt time.Time `json:"recorded_at" db:"recorded_at"`
}
