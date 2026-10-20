// Package outbox publishes the messages the engine enqueued transactionally.
//
// The guarantee it provides is at-least-once: a message is published one or
// more times, never zero. That is the strongest guarantee available without
// a transaction spanning the database and the broker, and it is enough,
// because consumers deduplicate on the message id and apply their effects in
// the same transaction as the marker. See internal/consumer.
package outbox

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/arnavkasat/payment-engine/internal/events"
	"github.com/arnavkasat/payment-engine/internal/faults"
	"github.com/arnavkasat/payment-engine/internal/metrics"
	"github.com/arnavkasat/payment-engine/internal/storage"
)

// Metric names exported by the publisher.
const (
	MetricPublished    = "outbox_published_total"
	MetricPublishFails = "outbox_publish_failures_total"
	MetricLagSeconds   = "outbox_lag_seconds"
)

// Config tunes the publisher.
type Config struct {
	// Owner identifies this worker in outbox leases. Defaults to a random id.
	Owner string
	// Batch is the maximum number of messages leased per pass.
	Batch int
	// Interval is how long to wait after an empty pass before polling again.
	// A pass that published a full batch polls again immediately.
	Interval time.Duration
	// Lease is how long a claimed message stays claimed. It must comfortably
	// exceed the time a publish takes, or two workers will publish the same
	// message — harmless, but wasteful.
	Lease time.Duration
	// Faults injects failures. Nil in production.
	Faults *faults.Injector
	// Metrics receives counters and lag measurements. Nil disables them.
	Metrics *metrics.Registry
	// Logger receives publish failures. Nil discards them.
	Logger *slog.Logger
}

func (c *Config) applyDefaults() {
	if c.Owner == "" {
		c.Owner = "publisher-" + uuid.NewString()[:8]
	}
	if c.Batch <= 0 {
		c.Batch = 128
	}
	if c.Interval <= 0 {
		c.Interval = 50 * time.Millisecond
	}
	if c.Lease <= 0 {
		c.Lease = 30 * time.Second
	}
	if c.Logger == nil {
		c.Logger = slog.New(slog.DiscardHandler)
	}
}

// Publisher drains the outbox into a broker.
type Publisher struct {
	store storage.Store
	pub   events.Publisher
	cfg   Config
}

// New builds a Publisher.
func New(store storage.Store, pub events.Publisher, cfg Config) (*Publisher, error) {
	if store == nil || pub == nil {
		return nil, fmt.Errorf("outbox: store and publisher are required")
	}
	cfg.applyDefaults()
	if cfg.Metrics != nil {
		cfg.Metrics.DefineCounter(MetricPublished, "Outbox messages handed to the broker.")
		cfg.Metrics.DefineCounter(MetricPublishFails, "Outbox publish attempts that failed.")
		cfg.Metrics.DefineHistogram(MetricLagSeconds, "Age of a message when it was published.", nil)
	}
	return &Publisher{store: store, pub: pub, cfg: cfg}, nil
}

// Run polls until the context is cancelled, returning nil on cancellation.
func (p *Publisher) Run(ctx context.Context) error {
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
		}

		n, err := p.publishBatch(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			p.cfg.Logger.Error("outbox publish failed", "error", err, "owner", p.cfg.Owner)
		}
		// A full batch means there is probably more waiting, so poll again
		// without the idle delay.
		wait := p.cfg.Interval
		if n == p.cfg.Batch {
			wait = 0
		}
		timer.Reset(wait)
	}
}

// Drain publishes everything currently pending and returns how many messages
// were published. It is what tests and the chaos harness use to reach a
// quiescent state without waiting on a ticker.
func (p *Publisher) Drain(ctx context.Context) (int, error) {
	var total int
	for {
		n, err := p.publishBatch(ctx)
		total += n
		if err != nil {
			return total, err
		}
		if n == 0 {
			return total, nil
		}
	}
}

// publishBatch leases a batch, publishes it, and retires it.
func (p *Publisher) publishBatch(ctx context.Context) (int, error) {
	msgs, err := p.store.LeaseOutbox(ctx, p.cfg.Owner, p.cfg.Batch, p.cfg.Lease)
	if err != nil {
		return 0, fmt.Errorf("outbox: leasing: %w", err)
	}
	if len(msgs) == 0 {
		return 0, nil
	}

	// A failure here happens before the broker saw anything, so the messages
	// are released at once instead of waiting out their lease.
	if err := p.cfg.Faults.Check(faults.OutboxBeforePublish); err != nil {
		p.release(ctx, msgs, err)
		return 0, err
	}

	envelopes := make([]events.Message, 0, len(msgs))
	ids := make([]uuid.UUID, 0, len(msgs))
	for _, m := range msgs {
		envelopes = append(envelopes, events.Message{
			ID:        m.ID.String(),
			Key:       m.AggregateID.String(),
			Type:      m.Type,
			Payload:   m.Payload,
			Timestamp: m.CreatedAt,
		})
		ids = append(ids, m.ID)
	}

	if err := p.pub.Publish(ctx, envelopes...); err != nil {
		p.count(MetricPublishFails, len(msgs))
		p.release(ctx, msgs, err)
		return 0, fmt.Errorf("outbox: publishing %d messages: %w", len(msgs), err)
	}

	// The broker has the messages but the rows are not retired. The lease
	// will lapse and another worker will publish them again — which is
	// exactly the duplicate delivery consumers are built to absorb.
	if err := p.cfg.Faults.Check(faults.OutboxAfterPublish); err != nil {
		return 0, err
	}

	if err := p.store.MarkOutboxPublished(ctx, ids); err != nil {
		return 0, fmt.Errorf("outbox: marking %d messages published: %w", len(ids), err)
	}

	p.count(MetricPublished, len(msgs))
	if p.cfg.Metrics != nil {
		now := time.Now()
		for _, m := range msgs {
			p.cfg.Metrics.Observe(MetricLagSeconds, nil, now.Sub(m.CreatedAt).Seconds())
		}
	}
	return len(msgs), nil
}

// release returns leased messages to the queue, recording why.
func (p *Publisher) release(ctx context.Context, msgs []storage.OutboxMessage, cause error) {
	// The context that failed the publish may already be cancelled, and the
	// rows still need releasing, so this uses a fresh short-lived one.
	releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	for _, m := range msgs {
		if err := p.store.ReleaseOutbox(releaseCtx, m.ID, cause.Error()); err != nil {
			p.cfg.Logger.Error("releasing outbox message", "id", m.ID, "error", err)
		}
	}
}

func (p *Publisher) count(name string, n int) {
	if p.cfg.Metrics != nil {
		p.cfg.Metrics.Add(name, nil, int64(n))
	}
}
