package correlator

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/solacecommunity/event-correlator-go/internal/model"
)

// Broker is the minimal interface required by the Component.
// Any Solace, Kafka, NATS, or in-memory broker implementation can satisfy this.
type Broker interface {
	CreateQueue(name string, opts QueueOptions) (Queue, error)
	Subscribe(queue Queue, topic string) error
	Receive(ctx context.Context, queue Queue) (*Message, error)
	PublishGuaranteed(ctx context.Context, topic string, msg *Message) error
	IsConnected() bool
}

// Queue is an opaque queue handle.
type Queue interface{}

// QueueOptions controls queue creation behavior.
type QueueOptions struct {
	Named bool
}

// Message is a broker message with a payload and optional topic.
type Message struct {
	Payload []byte
	Topic   string
}

// Component bridges the broker to the correlation Engine.
// It creates a durable queue, subscribes to all source topics,
// and runs receive + sweep loops as goroutines.
type Component struct {
	broker Broker
	cfg    *Config
	engine *Engine
	logger *slog.Logger

	queue  Queue
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewComponent creates a correlator Component.
func NewComponent(broker Broker, cfg *Config, logger *slog.Logger) *Component {
	return &Component{
		broker: broker,
		cfg:    cfg,
		engine: NewEngine(cfg.ExpectedSources, cfg.CorrelationWindow),
		logger: logger.With("component", "event-correlator"),
	}
}

// Start creates the durable queue, subscribes to all source topics,
// and launches the receive loop and sweep ticker goroutines.
func (c *Component) Start(ctx context.Context) error {
	queueName := c.cfg.QueuePrefix + "-correlator"
	q, err := c.broker.CreateQueue(queueName, QueueOptions{Named: true})
	if err != nil {
		return fmt.Errorf("create queue %q: %w", queueName, err)
	}
	c.queue = q

	for _, src := range c.cfg.Sources {
		if err := c.broker.Subscribe(q, src.Topic); err != nil {
			return fmt.Errorf("subscribe %s to %q: %w", src.Name, src.Topic, err)
		}
		c.logger.Info("subscribed to source", "source", src.Name, "topic", src.Topic)
	}

	loopCtx, cancel := context.WithCancel(ctx)
	c.cancel = cancel

	c.wg.Add(2)
	go func() {
		defer c.wg.Done()
		c.receiveLoop(loopCtx)
	}()
	go func() {
		defer c.wg.Done()
		c.sweepLoop(loopCtx)
	}()

	c.logger.Info("correlator started", "queue", queueName)
	return nil
}

// Stop cancels goroutines and waits for them to drain.
func (c *Component) Stop() {
	if c.cancel != nil {
		c.cancel()
	}
	c.wg.Wait()
	c.logger.Info("correlator stopped", "stats", c.engine.GetStats())
}

// Health reports healthy when the broker is connected.
func (c *Component) Health() error {
	if !c.broker.IsConnected() {
		return fmt.Errorf("broker disconnected")
	}
	return nil
}

// Stats exposes the engine's metrics.
func (c *Component) Stats() Stats {
	return c.engine.GetStats()
}

func (c *Component) receiveLoop(ctx context.Context) {
	for {
		msg, err := c.broker.Receive(ctx, c.queue)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			c.logger.Warn("receive error", "err", err)
			continue
		}
		if msg == nil {
			continue
		}

		var event model.TradeEvent
		if err := json.Unmarshal(msg.Payload, &event); err != nil {
			c.logger.Warn("unmarshal failed, discarding message",
				"err", err, "topic", msg.Topic)
			continue
		}

		// Infer source from topic if not set in the payload.
		if event.Source == "" {
			event.Source = inferSourceFromTopic(msg.Topic, c.cfg.Sources)
		}

		now := time.Now()
		reconciled := c.engine.Ingest(event, now)
		if reconciled != nil {
			c.publishReconciled(ctx, reconciled)
		}
	}
}

func (c *Component) sweepLoop(ctx context.Context) {
	ticker := time.NewTicker(c.cfg.SweepInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := time.Now()
			breaks := c.engine.Sweep(now)
			for i := range breaks {
				c.publishBreak(ctx, &breaks[i])
			}
			if len(breaks) > 0 {
				c.logger.Info("sweep completed",
					"expired", len(breaks),
					"pending", c.engine.PendingCount(),
				)
			}
		}
	}
}

func (c *Component) publishReconciled(ctx context.Context, event *model.ReconciledEvent) {
	data, err := json.Marshal(event)
	if err != nil {
		c.logger.Error("marshal reconciled event", "err", err, "trade_id", event.TradeID)
		return
	}
	if err := c.broker.PublishGuaranteed(ctx, c.cfg.ReconciledTopic, &Message{Payload: data}); err != nil {
		c.logger.Error("publish reconciled event", "err", err, "trade_id", event.TradeID)
		return
	}
	c.logger.Info("trade reconciled",
		"trade_id", event.TradeID,
		"duration_ms", event.MatchDuration,
		"sources", len(event.Sources),
	)
}

func (c *Component) publishBreak(ctx context.Context, event *model.BreakEvent) {
	data, err := json.Marshal(event)
	if err != nil {
		c.logger.Error("marshal break event", "err", err, "trade_id", event.TradeID)
		return
	}
	if err := c.broker.PublishGuaranteed(ctx, c.cfg.BreakTopic, &Message{Payload: data}); err != nil {
		c.logger.Error("publish break event", "err", err, "trade_id", event.TradeID)
		return
	}
	c.logger.Warn("break detected",
		"trade_id", event.TradeID,
		"missing", event.MissingSources,
		"received", event.ReceivedSources,
	)
}

// inferSourceFromTopic extracts the source name from the topic structure.
func inferSourceFromTopic(topic string, sources []SourceConfig) string {
	parts := strings.Split(topic, "/")
	for _, src := range sources {
		for _, part := range parts {
			if strings.EqualFold(part, src.Name) {
				return src.Name
			}
		}
	}
	if len(parts) >= 3 {
		return parts[2]
	}
	return "unknown"
}
