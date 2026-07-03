package correlator

import "time"

// Config holds validated correlator configuration.
type Config struct {
	Sources           []SourceConfig
	ExpectedSources   []string
	ReconciledTopic   string
	BreakTopic        string
	CorrelationWindow time.Duration
	SweepInterval     time.Duration
	QueuePrefix       string
}

// SourceConfig describes a source system subscription.
type SourceConfig struct {
	Name  string // e.g. "source_a", "source_b"
	Topic string // e.g. "trades/source_a/>"
}
