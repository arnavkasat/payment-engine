// Command worker publishes outbox messages to Kafka and consumes them back
// to settle payments.
//
// Both halves run here because they share a database pool and a lifecycle,
// and because running them together makes the full path — capture, publish,
// consume, settle — observable in one process during development. They scale
// independently in production by running this binary with one half disabled.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/arnavkasat/payment-engine/internal/app"
	"github.com/arnavkasat/payment-engine/internal/config"
	"github.com/arnavkasat/payment-engine/internal/consumer"
	"github.com/arnavkasat/payment-engine/internal/events"
	kafkaevents "github.com/arnavkasat/payment-engine/internal/events/kafka"
	"github.com/arnavkasat/payment-engine/internal/outbox"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "worker: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	publishEnabled := boolEnv("ENABLE_PUBLISHER", true)
	consumeEnabled := boolEnv("ENABLE_CONSUMER", true)
	if !publishEnabled && !consumeEnabled {
		return errors.New("both ENABLE_PUBLISHER and ENABLE_CONSUMER are false; nothing to do")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	startCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	application, err := app.Open(startCtx, cfg)
	if err != nil {
		return err
	}
	defer func() { _ = application.Close() }()
	application.LogBalances(startCtx)

	// Both halves need the topic to exist: a publisher cannot write to a
	// missing topic and a consumer group cannot subscribe to one.
	if cfg.CreateTopic {
		if err := kafkaevents.EnsureTopic(startCtx, kafkaevents.TopicConfig{
			Brokers:           cfg.KafkaBrokers,
			Topic:             cfg.KafkaTopic,
			Partitions:        cfg.KafkaPartitions,
			ReplicationFactor: cfg.KafkaReplicationFactor,
		}); err != nil {
			return err
		}
		application.Log.Info("topic ready",
			"topic", cfg.KafkaTopic, "partitions", cfg.KafkaPartitions)
	}

	var wg sync.WaitGroup
	errc := make(chan error, 2)

	if publishEnabled {
		producer, err := kafkaevents.NewProducer(kafkaevents.ProducerConfig{
			Brokers: cfg.KafkaBrokers,
			Topic:   cfg.KafkaTopic,
		})
		if err != nil {
			return err
		}
		defer func() { _ = producer.Close() }()

		publisher, err := outbox.New(application.Store, producer, outbox.Config{
			Owner:    hostTag("publisher"),
			Batch:    cfg.OutboxBatch,
			Interval: cfg.OutboxInterval,
			Lease:    cfg.OutboxLease,
			Faults:   application.Faults,
			Metrics:  application.Metrics,
			Logger:   application.Log,
		})
		if err != nil {
			return err
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			application.Log.Info("outbox publisher started", "topic", cfg.KafkaTopic, "batch", cfg.OutboxBatch)
			if err := publisher.Run(ctx); err != nil {
				errc <- fmt.Errorf("publisher: %w", err)
			}
		}()
	}

	if consumeEnabled {
		processor, err := consumer.New(application.Store, consumer.Config{
			Group:   cfg.ConsumerGroup,
			Faults:  application.Faults,
			Metrics: application.Metrics,
			Logger:  application.Log,
		})
		if err != nil {
			return err
		}
		processor.Register(events.TypePaymentCaptured, application.Service.HandleCaptured)

		reader, err := kafkaevents.NewConsumer(kafkaevents.ConsumerConfig{
			Brokers: cfg.KafkaBrokers,
			Topic:   cfg.KafkaTopic,
			Group:   cfg.ConsumerGroup,
			Logger:  application.Log,
		})
		if err != nil {
			return err
		}
		defer func() { _ = reader.Close() }()

		wg.Add(1)
		go func() {
			defer wg.Done()
			application.Log.Info("settlement consumer started", "topic", cfg.KafkaTopic, "group", cfg.ConsumerGroup)
			if err := reader.Run(ctx, processor.Handle); err != nil {
				errc <- fmt.Errorf("consumer: %w", err)
			}
		}()
	}

	select {
	case err := <-errc:
		stop()
		wg.Wait()
		return err
	case <-ctx.Done():
		application.Log.Info("shutting down")
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
		application.Log.Info("stopped")
		return nil
	case <-time.After(cfg.ShutdownTimeout):
		return fmt.Errorf("workers did not stop within %s", cfg.ShutdownTimeout)
	}
}

func boolEnv(key string, fallback bool) bool {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return fallback
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return fallback
	}
	return b
}

// hostTag labels outbox leases with the hostname, so an operator looking at
// a stuck lease can tell which replica holds it.
func hostTag(role string) string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		return role
	}
	return role + "-" + host
}
