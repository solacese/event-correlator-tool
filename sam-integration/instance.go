// Package instance provides the SAM Go AWE integration shim for the event
// correlator. This file must live inside the SAM Go module tree where it can
// import internal packages.
//
// Copy this file into your SAM Go tree (e.g. examples/correlator/internal/instance/)
// and register:
//
//	exe.RegisterKind("correlator", instance.Factory)
//
// This is reference code. It cannot compile standalone because it imports
// SAM Go internal packages.

package instance

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	// SAM Go internal imports (require being inside the SAM Go module tree).
	"github.com/SolaceDev/solace-agent-mesh-go/internal/awe"
	"github.com/SolaceDev/solace-agent-mesh-go/internal/broker"
	"github.com/SolaceDev/solace-agent-mesh-go/internal/config"
	"github.com/SolaceDev/solace-agent-mesh-go/internal/runtime"

	// External portable packages from this repo.
	"github.com/solacese/event-correlator-go/internal/correlator"
	"github.com/solacese/event-correlator-go/internal/model"
	"github.com/solacese/event-correlator-go/internal/store"
	"github.com/solacese/event-correlator-go/internal/store/postgres"
)

var (
	_ awe.Instance = (*Instance)(nil)
	_ awe.Remover  = (*Instance)(nil)
)

// Factory is the AWE instance factory for kind "correlator".
func Factory(cfg config.Config) (awe.Instance, error) {
	parsed, err := parseConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("correlator config: %w", err)
	}
	return &Instance{name: parsed.Name, cfg: parsed}, nil
}

// Instance implements awe.Instance for the "correlator" kind.
type Instance struct {
	name   string
	cfg    *instanceConfig
	svc    runtime.Services
	engine *correlator.Engine
	store  store.Store
	logger *slog.Logger

	queue  broker.Queue
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func (inst *Instance) Name() string { return inst.name }
func (inst *Instance) Kind() string { return "correlator" }

func (inst *Instance) Init(ctx context.Context, _ config.Config, svc runtime.Services) error {
	inst.svc = svc
	inst.logger = slog.Default().With("component", "correlator", "instance", inst.name)

	// Open the database (shared with SAM's session_service).
	s, err := postgres.New(inst.cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	if err := s.Migrate(ctx); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	inst.store = s

	inst.engine = correlator.NewEngine(s, inst.cfg.ExpectedSources, inst.cfg.CorrelationWindow, inst.logger)
	inst.logger.Info("correlator initialized",
		"sources", len(inst.cfg.Sources),
		"expected", inst.cfg.ExpectedSources,
		"window", inst.cfg.CorrelationWindow,
	)
	return nil
}

func (inst *Instance) Start(ctx context.Context) error {
	brk := inst.svc.Broker()

	queueName := inst.cfg.QueuePrefix + "-correlator-" + inst.name
	q, err := brk.CreateQueue(queueName, broker.QueueOptions{Named: true})
	if err != nil {
		return fmt.Errorf("create queue %q: %w", queueName, err)
	}
	inst.queue = q

	for _, src := range inst.cfg.Sources {
		if err := brk.Subscribe(q, src.Topic); err != nil {
			return fmt.Errorf("subscribe %s to %q: %w", src.Name, src.Topic, err)
		}
		inst.logger.Info("subscribed", "source", src.Name, "topic", src.Topic)
	}

	loopCtx, cancel := context.WithCancel(ctx)
	inst.cancel = cancel

	inst.wg.Add(2)
	go func() { defer inst.wg.Done(); inst.receiveLoop(loopCtx) }()
	go func() { defer inst.wg.Done(); inst.sweepLoop(loopCtx) }()

	inst.logger.Info("correlator started", "queue", queueName)
	return nil
}

func (inst *Instance) Stop(_ context.Context) error {
	if inst.cancel != nil {
		inst.cancel()
	}
	inst.wg.Wait()
	if inst.store != nil {
		_ = inst.store.Close()
	}
	inst.logger.Info("correlator stopped", "stats", inst.engine.GetStats())
	return nil
}

func (inst *Instance) Remove(ctx context.Context) error {
	if err := inst.Stop(ctx); err != nil {
		return err
	}
	if inst.queue != nil {
		_ = inst.svc.Broker().DeprovisionQueue(inst.queue)
	}
	return nil
}

func (inst *Instance) Health() error {
	if inst.svc.Broker() != nil && !inst.svc.Broker().IsConnected() {
		return fmt.Errorf("broker disconnected")
	}
	return nil
}

// --- loops ---

func (inst *Instance) receiveLoop(ctx context.Context) {
	brk := inst.svc.Broker()
	for {
		msg, err := brk.Receive(ctx, inst.queue)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			inst.logger.Warn("receive error", "err", err)
			continue
		}
		if msg == nil {
			continue
		}

		var event model.TradeEvent
		if err := json.Unmarshal(msg.Payload, &event); err != nil {
			inst.logger.Warn("unmarshal failed", "err", err, "topic", msg.Topic)
			continue
		}
		if event.Source == "" {
			event.Source = inferSource(msg.Topic, inst.cfg.Sources)
		}

		reconciled, err := inst.engine.Ingest(ctx, event, time.Now())
		if err != nil {
			inst.logger.Error("ingest failed", "err", err, "trade_id", event.TradeID)
			continue
		}
		if reconciled != nil {
			inst.publish(ctx, inst.cfg.ReconciledTopic, reconciled)
			inst.logger.Info("trade reconciled",
				"trade_id", reconciled.TradeID,
				"duration_ms", reconciled.MatchDuration,
			)
		}
	}
}

func (inst *Instance) sweepLoop(ctx context.Context) {
	ticker := time.NewTicker(inst.cfg.SweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			breaks, err := inst.engine.Sweep(ctx, time.Now())
			if err != nil {
				inst.logger.Error("sweep failed", "err", err)
				continue
			}
			for i := range breaks {
				inst.publish(ctx, inst.cfg.BreakTopic, &breaks[i])
				inst.logger.Warn("break detected",
					"trade_id", breaks[i].TradeID,
					"missing", breaks[i].MissingSources,
				)
				if inst.cfg.WorkflowTrigger != "" {
					inst.triggerWorkflow(ctx, &breaks[i])
				}
			}
		}
	}
}

func (inst *Instance) publish(ctx context.Context, topic string, v any) {
	data, _ := json.Marshal(v)
	if err := inst.svc.Broker().PublishGuaranteed(ctx, topic, &broker.Message{Payload: data}); err != nil {
		inst.logger.Error("publish failed", "err", err, "topic", topic)
	}
}

func (inst *Instance) triggerWorkflow(ctx context.Context, brk *model.BreakEvent) {
	payload := map[string]any{
		"task_id": fmt.Sprintf("break-%s-%d", brk.TradeID, time.Now().UnixMilli()),
		"message": fmt.Sprintf("Trade %s missing sources %v. Investigate.", brk.TradeID, brk.MissingSources),
		"metadata": map[string]any{
			"trade_id":         brk.TradeID,
			"missing_sources":  brk.MissingSources,
			"received_sources": brk.ReceivedSources,
		},
	}
	data, _ := json.Marshal(payload)
	if err := inst.svc.Broker().PublishGuaranteed(ctx, inst.cfg.WorkflowTrigger, &broker.Message{Payload: data}); err != nil {
		inst.logger.Error("workflow trigger failed", "err", err, "trade_id", brk.TradeID)
	}
}

// --- config ---

type sourceConfig struct {
	Name  string
	Topic string
}

type instanceConfig struct {
	Name              string
	Namespace         string
	DatabaseURL       string
	Sources           []sourceConfig
	ExpectedSources   []string
	CorrelationWindow time.Duration
	SweepInterval     time.Duration
	ReconciledTopic   string
	BreakTopic        string
	WorkflowTrigger   string
	QueuePrefix       string
}

func parseConfig(cfg config.Config) (*instanceConfig, error) {
	appCfg := cfg.GetSection("app_config")
	if appCfg == nil {
		return nil, fmt.Errorf("missing 'app_config'")
	}

	name := cfg.GetString("name")
	if name == "" {
		return nil, fmt.Errorf("missing 'name'")
	}

	ns := appCfg.GetString("namespace")
	if ns == "" {
		ns = "default"
	}
	prefix := appCfg.GetString("queue_prefix")
	if prefix == "" {
		prefix = ns
	}

	// Database URL from session_service block (shared with other SAM components).
	dbURL := ""
	if ss := appCfg.GetSection("session_service"); ss != nil {
		dbURL = ss.GetString("database_url")
	}
	if dbURL == "" {
		dbURL = appCfg.GetString("database_url")
	}
	if dbURL == "" {
		return nil, fmt.Errorf("database_url required (set in app_config.session_service or app_config directly)")
	}

	window := 5 * time.Minute
	if s := appCfg.GetString("correlation_window"); s != "" {
		d, err := time.ParseDuration(s)
		if err != nil {
			return nil, fmt.Errorf("invalid correlation_window: %w", err)
		}
		window = d
	}

	sweep := 30 * time.Second
	if s := appCfg.GetString("sweep_interval"); s != "" {
		d, err := time.ParseDuration(s)
		if err != nil {
			return nil, fmt.Errorf("invalid sweep_interval: %w", err)
		}
		sweep = d
	}

	sourcesList, _ := appCfg.GetRaw("sources").([]any)
	if len(sourcesList) == 0 {
		return nil, fmt.Errorf("sources required")
	}
	var sources []sourceConfig
	for i, raw := range sourcesList {
		m, _ := raw.(map[string]any)
		n, _ := m["name"].(string)
		t, _ := m["topic"].(string)
		if n == "" || t == "" {
			return nil, fmt.Errorf("sources[%d]: name and topic required", i)
		}
		sources = append(sources, sourceConfig{Name: n, Topic: t})
	}

	expected := appCfg.GetStringSlice("expected_sources")
	if len(expected) == 0 {
		return nil, fmt.Errorf("expected_sources required")
	}

	outSec := appCfg.GetSection("output")
	if outSec == nil {
		return nil, fmt.Errorf("output section required")
	}

	return &instanceConfig{
		Name:              name,
		Namespace:         ns,
		DatabaseURL:       dbURL,
		Sources:           sources,
		ExpectedSources:   expected,
		CorrelationWindow: window,
		SweepInterval:     sweep,
		ReconciledTopic:   outSec.GetString("reconciled_topic"),
		BreakTopic:        outSec.GetString("break_topic"),
		WorkflowTrigger:   outSec.GetString("workflow_trigger_topic"),
		QueuePrefix:       prefix,
	}, nil
}

func inferSource(topic string, sources []sourceConfig) string {
	parts := strings.Split(topic, "/")
	for _, src := range sources {
		for _, p := range parts {
			if strings.EqualFold(p, src.Name) {
				return src.Name
			}
		}
	}
	if len(parts) >= 3 {
		return parts[2]
	}
	return "unknown"
}
