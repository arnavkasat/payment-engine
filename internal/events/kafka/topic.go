package kafka

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

// TopicConfig describes the topic the engine publishes to.
type TopicConfig struct {
	// Brokers is the bootstrap broker list.
	Brokers []string
	// Topic is the topic to create.
	Topic string
	// Partitions bounds settlement concurrency: events are keyed by payment
	// id, so one partition is consumed by at most one member of the group,
	// and the group can never have more useful members than partitions.
	Partitions int
	// ReplicationFactor must not exceed the number of brokers. One is only
	// appropriate for a single-node development broker.
	ReplicationFactor int
}

func (c *TopicConfig) applyDefaults() error {
	if len(c.Brokers) == 0 {
		return fmt.Errorf("kafka: at least one broker is required")
	}
	if c.Topic == "" {
		return fmt.Errorf("kafka: topic is required")
	}
	if c.Partitions <= 0 {
		c.Partitions = 6
	}
	if c.ReplicationFactor <= 0 {
		c.ReplicationFactor = 1
	}
	return nil
}

// EnsureTopic creates the topic if it does not already exist.
//
// The engine creates its topic explicitly rather than relying on the
// broker's auto-creation. Auto-creation gives whatever partition count the
// broker defaults to, which silently caps how many settlement consumers can
// ever do useful work; it is also disabled on most production clusters; and
// a client only triggers it by asking for it in a metadata request, so a
// publisher that does not ask simply fails against a missing topic.
//
// It is safe to call from every replica on every start: a topic that already
// exists is not an error.
func EnsureTopic(ctx context.Context, cfg TopicConfig) error {
	if err := cfg.applyDefaults(); err != nil {
		return err
	}
	client := &kafka.Client{
		Addr:    kafka.TCP(cfg.Brokers...),
		Timeout: 30 * time.Second,
	}
	resp, err := client.CreateTopics(ctx, &kafka.CreateTopicsRequest{
		Topics: []kafka.TopicConfig{{
			Topic:             cfg.Topic,
			NumPartitions:     cfg.Partitions,
			ReplicationFactor: cfg.ReplicationFactor,
		}},
	})
	if err != nil {
		return fmt.Errorf("kafka: creating topic %q: %w", cfg.Topic, err)
	}
	for topic, topicErr := range resp.Errors {
		if topicErr == nil || errors.Is(topicErr, kafka.TopicAlreadyExists) {
			continue
		}
		return fmt.Errorf("kafka: creating topic %q: %w", topic, topicErr)
	}
	return nil
}
