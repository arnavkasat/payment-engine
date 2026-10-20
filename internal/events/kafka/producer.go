// Package kafka carries engine events over Apache Kafka.
//
// The outbox row's id travels in the message-id header and is what consumers
// deduplicate on. Partitioning is by payment id, so every event about one
// payment lands on one partition and is delivered in the order it was
// written — capture before settlement, always.
package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/arnavkasat/payment-engine/internal/events"
)

// Message headers. They carry the envelope fields that do not belong in the
// payload, so a consumer can deduplicate without parsing the body.
const (
	HeaderMessageID = "message-id"
	HeaderEventType = "event-type"
)

// ProducerConfig configures the writer.
type ProducerConfig struct {
	// Brokers is the bootstrap broker list.
	Brokers []string
	// Topic is where every event is written.
	Topic string
	// BatchTimeout is how long the writer waits to fill a batch.
	BatchTimeout time.Duration
	// WriteTimeout bounds a single write.
	WriteTimeout time.Duration
	// RequiredAcks defaults to all in-sync replicas. Anything weaker trades
	// away the durability the outbox exists to provide: a message the broker
	// acknowledged and then lost would be marked published and never retried.
	RequiredAcks kafka.RequiredAcks
}

func (c *ProducerConfig) applyDefaults() error {
	if len(c.Brokers) == 0 {
		return fmt.Errorf("kafka: at least one broker is required")
	}
	if c.Topic == "" {
		return fmt.Errorf("kafka: topic is required")
	}
	if c.BatchTimeout <= 0 {
		c.BatchTimeout = 5 * time.Millisecond
	}
	if c.WriteTimeout <= 0 {
		c.WriteTimeout = 10 * time.Second
	}
	if c.RequiredAcks == 0 {
		c.RequiredAcks = kafka.RequireAll
	}
	return nil
}

// Producer publishes events to Kafka.
type Producer struct {
	writer *kafka.Writer
	topic  string
}

var _ events.Publisher = (*Producer)(nil)

// NewProducer builds a Producer.
func NewProducer(cfg ProducerConfig) (*Producer, error) {
	if err := cfg.applyDefaults(); err != nil {
		return nil, err
	}
	return &Producer{
		topic: cfg.Topic,
		writer: &kafka.Writer{
			Addr:         kafka.TCP(cfg.Brokers...),
			Topic:        cfg.Topic,
			Balancer:     &kafka.Hash{}, // partition by key: one payment, one partition
			RequiredAcks: cfg.RequiredAcks,
			BatchTimeout: cfg.BatchTimeout,
			WriteTimeout: cfg.WriteTimeout,
			Async:        false, // the caller must learn whether the write landed
			Compression:  kafka.Snappy,
		},
	}, nil
}

// Publish writes messages, returning only once the brokers have acknowledged
// them. A returned error means the outbox rows stay unpublished and will be
// retried, which may deliver a message the broker did in fact accept.
func (p *Producer) Publish(ctx context.Context, msgs ...events.Message) error {
	if len(msgs) == 0 {
		return nil
	}
	out := make([]kafka.Message, 0, len(msgs))
	for _, m := range msgs {
		out = append(out, kafka.Message{
			Key:   []byte(m.Key),
			Value: m.Payload,
			Time:  m.Timestamp,
			Headers: []kafka.Header{
				{Key: HeaderMessageID, Value: []byte(m.ID)},
				{Key: HeaderEventType, Value: []byte(m.Type)},
			},
		})
	}
	if err := p.writer.WriteMessages(ctx, out...); err != nil {
		return fmt.Errorf("kafka: writing %d messages to %s: %w", len(out), p.topic, err)
	}
	return nil
}

// Close flushes and shuts down the writer.
func (p *Producer) Close() error { return p.writer.Close() }
