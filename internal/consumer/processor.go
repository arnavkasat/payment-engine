// Package consumer turns at-least-once delivery into exactly-once effect.
//
// The trick is not in the broker. Each message is handled inside one database
// transaction that first writes a (consumer group, message id) marker and
// then runs the handler. If the marker is already there the message is a
// redelivery and the handler is skipped; if the handler fails, the marker
// rolls back with it and the next delivery is treated as the first. Neither
// half can commit without the other.
package consumer

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/arnavkasat/payment-engine/internal/events"
	"github.com/arnavkasat/payment-engine/internal/faults"
	"github.com/arnavkasat/payment-engine/internal/metrics"
	"github.com/arnavkasat/payment-engine/internal/storage"
)

// Metric names exported by the processor.
const (
	MetricHandled    = "consumer_messages_total"
	MetricDuplicates = "consumer_duplicates_total"
	MetricSeconds    = "consumer_handle_seconds"
)

// Handler processes a message inside the transaction that also records the
// deduplication marker. A handler must not perform side effects outside that
// transaction, because it can be rolled back.
type Handler func(ctx context.Context, tx storage.Tx, m events.Message) error

// Config configures a Processor.
type Config struct {
	// Group is the consumer group name. Markers are scoped to it, so two
	// groups each see every message once.
	Group string
	// Faults injects failures. Nil in production.
	Faults *faults.Injector
	// Metrics receives counters and latencies. Nil disables them.
	Metrics *metrics.Registry
	// Logger receives handler failures. Nil discards them.
	Logger *slog.Logger
}

// Processor dispatches messages to handlers with deduplication.
type Processor struct {
	store    storage.Store
	cfg      Config
	handlers map[string]Handler
}

// New builds a Processor.
func New(store storage.Store, cfg Config) (*Processor, error) {
	if store == nil {
		return nil, fmt.Errorf("consumer: store is required")
	}
	if cfg.Group == "" {
		return nil, fmt.Errorf("consumer: group is required")
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	if cfg.Metrics != nil {
		cfg.Metrics.DefineCounter(MetricHandled, "Messages handled, by type and outcome.")
		cfg.Metrics.DefineCounter(MetricDuplicates, "Redelivered messages skipped by deduplication.")
		cfg.Metrics.DefineHistogram(MetricSeconds, "Time to handle one message.", nil)
	}
	return &Processor{store: store, cfg: cfg, handlers: map[string]Handler{}}, nil
}

// Register installs the handler for an event type.
func (p *Processor) Register(eventType string, h Handler) {
	p.handlers[eventType] = h
}

// Handle processes one message. It satisfies events.Handler, so it can be
// passed straight to a Consumer's Run.
func (p *Processor) Handle(ctx context.Context, m events.Message) error {
	handler, ok := p.handlers[m.Type]
	if !ok {
		// An unknown type is not an error: a newer producer may publish
		// events this build does not know about, and failing would block
		// the partition behind a message nobody can handle.
		p.count(MetricHandled, m.Type, "ignored")
		return nil
	}
	if m.ID == "" {
		return fmt.Errorf("consumer: message of type %q has no id to deduplicate on", m.Type)
	}

	start := nowFunc()
	var duplicate bool
	err := p.store.Tx(ctx, func(ctx context.Context, tx storage.Tx) error {
		duplicate = false
		first, err := tx.MarkProcessed(ctx, p.cfg.Group, m.ID)
		if err != nil {
			return err
		}
		if !first {
			duplicate = true
			return nil
		}
		// Failing here proves the marker is not durable on its own: the
		// rollback must take it with the handler's writes.
		if err := p.cfg.Faults.Check(faults.ConsumerAfterDedupe); err != nil {
			return err
		}
		return handler(ctx, tx, m)
	})
	if p.cfg.Metrics != nil {
		p.cfg.Metrics.Observe(MetricSeconds, metrics.Labels{"type": m.Type}, nowFunc().Sub(start).Seconds())
	}
	if err != nil {
		p.count(MetricHandled, m.Type, "failed")
		p.cfg.Logger.Error("handling message", "type", m.Type, "id", m.ID, "error", err)
		return err
	}
	if duplicate {
		p.count(MetricDuplicates, m.Type, "duplicate")
		p.count(MetricHandled, m.Type, "duplicate")
		return nil
	}

	// The effect is committed but the broker has not been told. The next
	// delivery finds the marker and skips the handler, so the redelivery
	// costs a transaction and changes nothing.
	if err := p.cfg.Faults.Check(faults.ConsumerBeforeAck); err != nil {
		p.count(MetricHandled, m.Type, "unacked")
		return err
	}
	p.count(MetricHandled, m.Type, "applied")
	return nil
}

func (p *Processor) count(name, eventType, outcome string) {
	if p.cfg.Metrics != nil {
		p.cfg.Metrics.Inc(name, metrics.Labels{"type": eventType, "outcome": outcome})
	}
}
