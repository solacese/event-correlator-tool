package model

import "time"

// TradeEvent is the inbound event from a source system.
type TradeEvent struct {
	TradeID      string    `json:"trade_id"`
	Source       string    `json:"source"`
	Timestamp    time.Time `json:"timestamp"`
	Instrument   string    `json:"instrument"`
	Quantity     float64   `json:"quantity"`
	Price        float64   `json:"price"`
	Currency     string    `json:"currency"`
	Counterparty string    `json:"counterparty"`
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

// BreakEvent is published when the correlation window expires without all sources reporting.
type BreakEvent struct {
	TradeID         string       `json:"trade_id"`
	MissingSources  []string     `json:"missing_sources"`
	ReceivedSources []string     `json:"received_sources"`
	DetectedAt      time.Time    `json:"detected_at"`
	WindowExpiry    time.Time    `json:"window_expiry"`
	Events          []TradeEvent `json:"events"`
}
