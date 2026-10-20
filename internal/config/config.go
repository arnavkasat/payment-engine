// Package config loads process configuration from the environment.
//
// Everything has a working default for `docker compose up`, and nothing is
// read anywhere but here, so the full set of knobs is this one file.
package config

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config is the configuration shared by every binary.
type Config struct {
	// HTTPAddr is the address the API listens on.
	HTTPAddr string
	// DatabaseURL is the PostgreSQL connection string.
	DatabaseURL string
	// DatabaseMaxConns caps the connection pool. It should be at least as
	// large as the number of concurrent requests the API will serve, or
	// requests queue for a connection before they queue for a lock.
	DatabaseMaxConns int32

	// KafkaBrokers is the bootstrap broker list.
	KafkaBrokers []string
	// KafkaTopic carries every engine event.
	KafkaTopic string
	// ConsumerGroup names the settlement consumer group. Deduplication
	// markers are scoped to it, so changing it replays history.
	ConsumerGroup string
	// KafkaPartitions is the partition count used when the worker creates
	// the topic. It is the ceiling on settlement concurrency.
	KafkaPartitions int
	// KafkaReplicationFactor is the replication factor used when creating
	// the topic. It must not exceed the broker count.
	KafkaReplicationFactor int
	// CreateTopic makes the worker create the topic at startup.
	CreateTopic bool

	// ClearingAccount and FeeAccount are name prefixes: the engine resolves
	// ClearingShards accounts of each, named "<prefix>-00" upward.
	ClearingAccount string
	FeeAccount      string
	// ClearingShards is how many clearing and fee accounts to spread writes
	// across. Every capture locks one of each, so one account would
	// serialise every write in the engine on a single row.
	ClearingShards int

	// OutboxBatch, OutboxInterval and OutboxLease tune the publisher.
	OutboxBatch    int
	OutboxInterval time.Duration
	OutboxLease    time.Duration

	// Bootstrap creates the standard account set at startup if it is
	// missing. Convenient for demos; harmless when the accounts exist.
	Bootstrap bool

	// EnableFaultInjection exposes /v1/faults, which lets a caller arm the
	// engine's failure-injection points at runtime. It drives the demo
	// dashboard and is off by default: an endpoint that can make a payment
	// API fail on demand has no business being reachable in production.
	EnableFaultInjection bool

	// LogLevel is one of debug, info, warn, error.
	LogLevel string
	// ShutdownTimeout bounds graceful shutdown.
	ShutdownTimeout time.Duration
}

// Default returns the configuration used when nothing is set, which matches
// the docker-compose topology.
func Default() Config {
	return Config{
		HTTPAddr:               ":8080",
		DatabaseURL:            "postgres://payments:payments@localhost:5432/payments?sslmode=disable",
		DatabaseMaxConns:       32,
		KafkaBrokers:           []string{"localhost:9092"},
		KafkaTopic:             "payments.events",
		ConsumerGroup:          "settlement",
		KafkaPartitions:        6,
		KafkaReplicationFactor: 1,
		CreateTopic:            true,
		ClearingAccount:        "clearing",
		FeeAccount:             "fees",
		ClearingShards:         8,
		OutboxBatch:            128,
		OutboxInterval:         50 * time.Millisecond,
		OutboxLease:            30 * time.Second,
		Bootstrap:              true,
		LogLevel:               "info",
		ShutdownTimeout:        15 * time.Second,
	}
}

// Load reads the environment over the defaults.
func Load() (Config, error) {
	cfg := Default()
	var errs []string

	str := func(key string, target *string) {
		if v, ok := os.LookupEnv(key); ok && v != "" {
			*target = v
		}
	}
	str("HTTP_ADDR", &cfg.HTTPAddr)
	str("DATABASE_URL", &cfg.DatabaseURL)
	str("KAFKA_TOPIC", &cfg.KafkaTopic)
	str("CONSUMER_GROUP", &cfg.ConsumerGroup)
	str("CLEARING_ACCOUNT", &cfg.ClearingAccount)
	str("FEE_ACCOUNT", &cfg.FeeAccount)
	str("LOG_LEVEL", &cfg.LogLevel)

	if v, ok := os.LookupEnv("KAFKA_BROKERS"); ok && v != "" {
		var brokers []string
		for _, b := range strings.Split(v, ",") {
			if b = strings.TrimSpace(b); b != "" {
				brokers = append(brokers, b)
			}
		}
		if len(brokers) == 0 {
			errs = append(errs, "KAFKA_BROKERS is set but lists no brokers")
		}
		cfg.KafkaBrokers = brokers
	}

	intVar := func(key string, target *int) {
		v, ok := os.LookupEnv(key)
		if !ok || v == "" {
			return
		}
		n, err := strconv.Atoi(v)
		if err != nil {
			errs = append(errs, fmt.Sprintf("%s=%q is not a number", key, v))
			return
		}
		*target = n
	}
	intVar("OUTBOX_BATCH", &cfg.OutboxBatch)
	intVar("KAFKA_PARTITIONS", &cfg.KafkaPartitions)
	intVar("CLEARING_SHARDS", &cfg.ClearingShards)
	intVar("KAFKA_REPLICATION_FACTOR", &cfg.KafkaReplicationFactor)
	maxConns := int(cfg.DatabaseMaxConns)
	intVar("DATABASE_MAX_CONNS", &maxConns)
	cfg.DatabaseMaxConns = int32(maxConns)

	durVar := func(key string, target *time.Duration) {
		v, ok := os.LookupEnv(key)
		if !ok || v == "" {
			return
		}
		d, err := time.ParseDuration(v)
		if err != nil {
			errs = append(errs, fmt.Sprintf("%s=%q is not a duration", key, v))
			return
		}
		*target = d
	}
	durVar("OUTBOX_INTERVAL", &cfg.OutboxInterval)
	durVar("OUTBOX_LEASE", &cfg.OutboxLease)
	durVar("SHUTDOWN_TIMEOUT", &cfg.ShutdownTimeout)

	boolVar := func(key string, target *bool) {
		v, ok := os.LookupEnv(key)
		if !ok || v == "" {
			return
		}
		b, err := strconv.ParseBool(v)
		if err != nil {
			errs = append(errs, fmt.Sprintf("%s=%q is not a boolean", key, v))
			return
		}
		*target = b
	}
	boolVar("BOOTSTRAP_ACCOUNTS", &cfg.Bootstrap)
	boolVar("ENABLE_FAULT_INJECTION", &cfg.EnableFaultInjection)
	boolVar("KAFKA_CREATE_TOPIC", &cfg.CreateTopic)

	if len(errs) > 0 {
		return Config{}, fmt.Errorf("config: %s", strings.Join(errs, "; "))
	}
	return cfg, cfg.Validate()
}

// Validate checks the configuration is usable.
func (c Config) Validate() error {
	switch {
	case c.HTTPAddr == "":
		return fmt.Errorf("config: HTTP_ADDR is required")
	case c.DatabaseURL == "":
		return fmt.Errorf("config: DATABASE_URL is required")
	case len(c.KafkaBrokers) == 0:
		return fmt.Errorf("config: KAFKA_BROKERS is required")
	case c.KafkaTopic == "":
		return fmt.Errorf("config: KAFKA_TOPIC is required")
	case c.ConsumerGroup == "":
		return fmt.Errorf("config: CONSUMER_GROUP is required")
	case c.ClearingAccount == "":
		return fmt.Errorf("config: CLEARING_ACCOUNT is required")
	case c.KafkaPartitions <= 0:
		return fmt.Errorf("config: KAFKA_PARTITIONS must be positive, got %d", c.KafkaPartitions)
	case c.KafkaReplicationFactor <= 0:
		return fmt.Errorf("config: KAFKA_REPLICATION_FACTOR must be positive, got %d", c.KafkaReplicationFactor)
	case c.ClearingShards <= 0:
		return fmt.Errorf("config: CLEARING_SHARDS must be positive, got %d", c.ClearingShards)
	case c.OutboxBatch <= 0:
		return fmt.Errorf("config: OUTBOX_BATCH must be positive, got %d", c.OutboxBatch)
	case c.DatabaseMaxConns <= 0:
		return fmt.Errorf("config: DATABASE_MAX_CONNS must be positive, got %d", c.DatabaseMaxConns)
	case c.OutboxLease <= c.OutboxInterval:
		return fmt.Errorf("config: OUTBOX_LEASE (%s) must exceed OUTBOX_INTERVAL (%s), or workers will publish each other's messages",
			c.OutboxLease, c.OutboxInterval)
	}
	if _, err := c.SlogLevel(); err != nil {
		return err
	}
	return nil
}

// SlogLevel parses LogLevel.
func (c Config) SlogLevel() (slog.Level, error) {
	switch strings.ToLower(c.LogLevel) {
	case "debug":
		return slog.LevelDebug, nil
	case "info", "":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("config: LOG_LEVEL=%q is not one of debug, info, warn, error", c.LogLevel)
	}
}

// Logger builds the process logger.
func (c Config) Logger() *slog.Logger {
	level, err := c.SlogLevel()
	if err != nil {
		level = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
}
