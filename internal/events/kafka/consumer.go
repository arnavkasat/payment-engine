package kafka

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/arnavkasat/payment-engine/internal/events"
)

// ConsumerConfig configures the reader.
type ConsumerConfig struct {
	// Brokers is the bootstrap broker list.
	Brokers []string
	// Topic to read.
	Topic string
	// Group is the consumer group id.
	Group string
	// MinBytes and MaxBytes bound a fetch.
	MinBytes, MaxBytes int
	// MaxAttempts is how many times a message is retried in process before
	// the consumer gives up and returns. Offsets are committed only after a
	// successful handle, so giving up means a restart re-reads the message
	// rather than losing it.
	MaxAttempts int
	// RetryBackoff is the base delay between in-process retries.
	RetryBackoff time.Duration
	// CommitInterval is how often offsets are flushed to the broker.
	//
	// Committing synchronously after every message costs a broker round
	// trip per message and caps settlement throughput at the inverse of
	// that latency. It is also unnecessary here: the consumer deduplicates
	// on a stable message id, in the same transaction as its effect, so a
	// crash that replays the last interval's worth of offsets replays
	// messages that are recognised and skipped. The interval therefore
	// trades a bounded amount of redundant work after a crash — which the
	// engine is built to absorb — for an order of magnitude of throughput.
	//
	// Zero selects the default; set it negative to commit synchronously.
	CommitInterval time.Duration
	// Logger receives retry and failure detail. Nil discards them.
	Logger *slog.Logger
}

func (c *ConsumerConfig) applyDefaults() error {
	if len(c.Brokers) == 0 {
		return fmt.Errorf("kafka: at least one broker is required")
	}
	if c.Topic == "" {
		return fmt.Errorf("kafka: topic is required")
	}
	if c.Group == "" {
		return fmt.Errorf("kafka: consumer group is required")
	}
	if c.MinBytes <= 0 {
		c.MinBytes = 1
	}
	if c.MaxBytes <= 0 {
		c.MaxBytes = 10 << 20
	}
	if c.MaxAttempts <= 0 {
		c.MaxAttempts = 5
	}
	if c.RetryBackoff <= 0 {
		c.RetryBackoff = 20 * time.Millisecond
	}
	if c.CommitInterval == 0 {
		c.CommitInterval = 100 * time.Millisecond
	}
	if c.CommitInterval < 0 {
		c.CommitInterval = 0 // synchronous, one round trip per message
	}
	if c.Logger == nil {
		c.Logger = slog.New(slog.DiscardHandler)
	}
	return nil
}

// Consumer reads events from Kafka and hands them to a handler.
type Consumer struct {
	reader *kafka.Reader
	cfg    ConsumerConfig
}

var _ events.Consumer = (*Consumer)(nil)

// NewConsumer builds a Consumer.
func NewConsumer(cfg ConsumerConfig) (*Consumer, error) {
	if err := cfg.applyDefaults(); err != nil {
		return nil, err
	}
	return &Consumer{
		cfg: cfg,
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:  cfg.Brokers,
			Topic:    cfg.Topic,
			GroupID:  cfg.Group,
			MinBytes: cfg.MinBytes,
			MaxBytes: cfg.MaxBytes,
			// CommitMessages is still called explicitly, only ever after
			// the handler's transaction has committed, so an offset can
			// never move past a message that was not applied. The interval
			// only batches the flush of offsets already earned.
			CommitInterval: cfg.CommitInterval,
		}),
	}, nil
}

// Run reads messages until the context is cancelled.
//
// An offset is committed only after its message has been handled, so a crash
// re-delivers whatever was in flight. That is the at-least-once half of the
// contract; the exactly-once half is the consumer's deduplication marker.
func (c *Consumer) Run(ctx context.Context, h events.Handler) error {
	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("kafka: fetching from %s: %w", c.cfg.Topic, err)
		}

		if err := c.handleWithRetries(ctx, h, msg); err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			// Returning without committing leaves the offset where it was,
			// so a restart or a rebalance re-delivers this message.
			return fmt.Errorf("kafka: handling message at %s/%d/%d: %w",
				msg.Topic, msg.Partition, msg.Offset, err)
		}

		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			// The effect is committed but the offset is not: the message
			// will arrive again and be recognised as a duplicate.
			return fmt.Errorf("kafka: committing offset %d: %w", msg.Offset, err)
		}
	}
}

func (c *Consumer) handleWithRetries(ctx context.Context, h events.Handler, msg kafka.Message) error {
	envelope := toEnvelope(msg)
	var last error
	for attempt := 1; attempt <= c.cfg.MaxAttempts; attempt++ {
		if err := h(ctx, envelope); err == nil {
			return nil
		} else {
			last = err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		c.cfg.Logger.Warn("retrying message",
			"topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset,
			"attempt", attempt, "error", last)

		backoff := c.cfg.RetryBackoff << uint(attempt-1)
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return fmt.Errorf("after %d attempts: %w", c.cfg.MaxAttempts, last)
}

// toEnvelope converts a Kafka message into the broker-agnostic form. A
// message with no message-id header falls back to its topic/partition/offset
// coordinates, which are unique but not stable across redeliveries, so such
// a message is deduplicated only against itself.
func toEnvelope(msg kafka.Message) events.Message {
	env := events.Message{
		Key:       string(msg.Key),
		Payload:   msg.Value,
		Timestamp: msg.Time,
	}
	for _, hdr := range msg.Headers {
		switch hdr.Key {
		case HeaderMessageID:
			env.ID = string(hdr.Value)
		case HeaderEventType:
			env.Type = string(hdr.Value)
		}
	}
	if env.ID == "" {
		env.ID = fmt.Sprintf("%s/%d/%d", msg.Topic, msg.Partition, msg.Offset)
	}
	return env
}

// Close shuts down the reader.
func (c *Consumer) Close() error { return c.reader.Close() }
