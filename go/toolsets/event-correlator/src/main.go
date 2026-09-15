package main

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	sdk "github.com/SolaceDev/solace-agent-mesh-go/pkg/samtoolsdk"

	"github.com/solacese/event-correlator-tool/go/toolsets/event-correlator/src/internal/correlator"
	"github.com/solacese/event-correlator-tool/go/toolsets/event-correlator/src/internal/model"
	"github.com/solacese/event-correlator-tool/go/toolsets/event-correlator/src/internal/store"
	"github.com/solacese/event-correlator-tool/go/toolsets/event-correlator/src/internal/store/postgres"
)

var configSchema = []sdk.ConfigSchemaField{
	{
		Key:         "database_url",
		Type:        "string",
		Description: "PostgreSQL connection string used for durable pending events and audit history.",
		Required:    true,
		Secret:      true,
	},
	{
		Key:         "expected_sources",
		Type:        "string",
		Description: "Comma-separated source names required to reconcile a trade.",
		Required:    true,
		Default:     "solar,murex,client_reporting",
	},
	{
		Key:         "correlation_window_seconds",
		Type:        "integer",
		Description: "Seconds to wait after the first event before a trade becomes a break.",
		Required:    true,
		Default:     300,
	},
}

type IngestTradeEventParams struct {
	TradeID      string   `json:"trade_id" desc:"Unique trade identifier shared by all source events."`
	Source       string   `json:"source" desc:"Source system that emitted this event."`
	Timestamp    string   `json:"timestamp" desc:"Event timestamp in RFC 3339 format."`
	Instrument   *string  `json:"instrument" desc:"Optional traded instrument identifier."`
	Quantity     *float64 `json:"quantity" desc:"Optional traded quantity."`
	Price        *float64 `json:"price" desc:"Optional traded price."`
	Currency     *string  `json:"currency" desc:"Optional ISO currency code."`
	Counterparty *string  `json:"counterparty" desc:"Optional counterparty name or identifier."`
}

type SweepExpiredTradesParams struct{}

type GetAuditHistoryParams struct {
	TradeID *string `json:"trade_id" desc:"Optional trade identifier filter."`
	Outcome *string `json:"outcome" desc:"Optional outcome filter: reconciled or break."`
	Limit   *int    `json:"limit" desc:"Maximum records to return, from 1 to 500."`
}

func ingestTradeEvent(ctx context.Context, p IngestTradeEventParams, tc *sdk.ToolContext) (*sdk.Result, error) {
	timestamp, err := time.Parse(time.RFC3339Nano, p.Timestamp)
	if err != nil {
		return sdk.Error("timestamp must be RFC 3339", sdk.WithErrorCode("INVALID_TIMESTAMP")), nil
	}
	engine, closeStore, err := newEngine(ctx, tc)
	if err != nil {
		return sdk.Error(err.Error(), sdk.WithErrorCode("CONFIGURATION_OR_DATABASE_ERROR")), nil
	}
	defer closeStore()

	_ = tc.SendStatus("Correlating trade " + p.TradeID)
	outcome, err := engine.Ingest(ctx, model.TradeEvent{
		TradeID:      p.TradeID,
		Source:       p.Source,
		Timestamp:    timestamp,
		Instrument:   valueOrZero(p.Instrument),
		Quantity:     valueOrZero(p.Quantity),
		Price:        valueOrZero(p.Price),
		Currency:     valueOrZero(p.Currency),
		Counterparty: valueOrZero(p.Counterparty),
	}, time.Now().UTC())
	if err != nil {
		return sdk.Error(err.Error(), sdk.WithErrorCode("INGEST_FAILED")), nil
	}

	data := map[string]any{"status": outcome.Status, "trade_id": p.TradeID}
	if outcome.Reconciled != nil {
		data["reconciled"] = outcome.Reconciled
	}
	return sdk.OK("Trade event processed: "+outcome.Status, sdk.WithData(data)), nil
}

func sweepExpiredTrades(ctx context.Context, _ SweepExpiredTradesParams, tc *sdk.ToolContext) (*sdk.Result, error) {
	engine, closeStore, err := newEngine(ctx, tc)
	if err != nil {
		return sdk.Error(err.Error(), sdk.WithErrorCode("CONFIGURATION_OR_DATABASE_ERROR")), nil
	}
	defer closeStore()

	_ = tc.SendStatus("Checking for expired trade correlations")
	breaks, err := engine.Sweep(ctx, time.Now().UTC())
	if err != nil {
		return sdk.Error(err.Error(), sdk.WithErrorCode("SWEEP_FAILED")), nil
	}
	return sdk.OK(
		fmt.Sprintf("Detected %d expired trade correlation(s)", len(breaks)),
		sdk.WithData(map[string]any{"count": len(breaks), "breaks": breaks}),
	), nil
}

func getAuditHistory(ctx context.Context, p GetAuditHistoryParams, tc *sdk.ToolContext) (*sdk.Result, error) {
	engine, closeStore, err := newEngine(ctx, tc)
	if err != nil {
		return sdk.Error(err.Error(), sdk.WithErrorCode("CONFIGURATION_OR_DATABASE_ERROR")), nil
	}
	defer closeStore()

	filter := store.AuditFilter{Limit: 100}
	if p.TradeID != nil {
		filter.TradeID = strings.TrimSpace(*p.TradeID)
	}
	if p.Outcome != nil {
		filter.Outcome = strings.TrimSpace(*p.Outcome)
		if filter.Outcome != "" && filter.Outcome != "reconciled" && filter.Outcome != "break" {
			return sdk.Error("outcome must be reconciled or break", sdk.WithErrorCode("INVALID_OUTCOME")), nil
		}
	}
	if p.Limit != nil {
		if *p.Limit < 1 || *p.Limit > 500 {
			return sdk.Error("limit must be between 1 and 500", sdk.WithErrorCode("INVALID_LIMIT")), nil
		}
		filter.Limit = *p.Limit
	}

	entries, err := engine.AuditHistory(ctx, filter)
	if err != nil {
		return sdk.Error(err.Error(), sdk.WithErrorCode("AUDIT_QUERY_FAILED")), nil
	}
	return sdk.OK(
		fmt.Sprintf("Found %d audit entrie(s)", len(entries)),
		sdk.WithData(map[string]any{"count": len(entries), "entries": entries}),
	), nil
}

func newEngine(ctx context.Context, tc *sdk.ToolContext) (*correlator.Engine, func(), error) {
	databaseURL := strings.TrimSpace(tc.GetConfigString("database_url", ""))
	if databaseURL == "" {
		return nil, nil, fmt.Errorf("database_url tool configuration is required")
	}
	expectedSources := splitSources(tc.GetConfigString("expected_sources", "solar,murex,client_reporting"))
	if len(expectedSources) == 0 {
		return nil, nil, fmt.Errorf("expected_sources must contain at least one source")
	}

	windowSeconds, err := configInt(tc, "correlation_window_seconds", 300)
	if err != nil || windowSeconds <= 0 {
		return nil, nil, fmt.Errorf("correlation_window_seconds must be a positive integer")
	}

	postgresStore, err := postgres.New(databaseURL)
	if err != nil {
		return nil, nil, fmt.Errorf("open database: %w", err)
	}
	if err := postgresStore.Migrate(ctx); err != nil {
		_ = postgresStore.Close()
		return nil, nil, fmt.Errorf("migrate database: %w", err)
	}
	return correlator.NewEngine(postgresStore, expectedSources, time.Duration(windowSeconds)*time.Second), func() {
		_ = postgresStore.Close()
	}, nil
}

func configInt(tc *sdk.ToolContext, key string, defaultValue int) (int, error) {
	value, ok := tc.GetConfig(key)
	if !ok || value == nil {
		return defaultValue, nil
	}
	switch typed := value.(type) {
	case float64:
		return int(typed), nil
	case int:
		return typed, nil
	case string:
		return strconv.Atoi(typed)
	default:
		return 0, fmt.Errorf("unsupported value type")
	}
}

func splitSources(value string) []string {
	seen := make(map[string]struct{})
	var sources []string
	for _, source := range strings.Split(value, ",") {
		source = strings.TrimSpace(source)
		if source == "" {
			continue
		}
		if _, exists := seen[source]; exists {
			continue
		}
		seen[source] = struct{}{}
		sources = append(sources, source)
	}
	return sources
}

func valueOrZero[T any](value *T) T {
	if value == nil {
		var zero T
		return zero
	}
	return *value
}

func main() {
	sdk.Run(
		sdk.NewTool(
			"ingest_trade_event",
			"Persist one trade event and reconcile it when every configured source has reported.",
			ingestTradeEvent,
			sdk.WithConfigSchema(configSchema...),
		),
		sdk.NewTool(
			"sweep_expired_trades",
			"Finalize expired incomplete trade correlations as breaks and return their details.",
			sweepExpiredTrades,
			sdk.WithConfigSchema(configSchema...),
		),
		sdk.NewTool(
			"get_audit_history",
			"Read the immutable audit history of reconciled trades and correlation breaks.",
			getAuditHistory,
			sdk.WithConfigSchema(configSchema...),
		),
	)
}
