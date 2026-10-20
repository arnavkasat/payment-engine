package config

import (
	"log/slog"
	"strings"
	"testing"
	"time"
)

func TestDefaultIsValid(t *testing.T) {
	if err := Default().Validate(); err != nil {
		t.Fatalf("Default().Validate() = %v", err)
	}
}

func TestLoadReadsTheEnvironment(t *testing.T) {
	t.Setenv("HTTP_ADDR", ":9999")
	t.Setenv("KAFKA_BROKERS", "a:9092, b:9092 ,")
	t.Setenv("OUTBOX_BATCH", "7")
	t.Setenv("OUTBOX_INTERVAL", "250ms")
	t.Setenv("OUTBOX_LEASE", "1m")
	t.Setenv("BOOTSTRAP_ACCOUNTS", "false")
	t.Setenv("LOG_LEVEL", "debug")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.HTTPAddr != ":9999" {
		t.Errorf("HTTPAddr = %q", cfg.HTTPAddr)
	}
	if len(cfg.KafkaBrokers) != 2 || cfg.KafkaBrokers[0] != "a:9092" || cfg.KafkaBrokers[1] != "b:9092" {
		t.Errorf("KafkaBrokers = %q", cfg.KafkaBrokers)
	}
	if cfg.OutboxBatch != 7 || cfg.OutboxInterval != 250*time.Millisecond || cfg.OutboxLease != time.Minute {
		t.Errorf("outbox settings = %d, %s, %s", cfg.OutboxBatch, cfg.OutboxInterval, cfg.OutboxLease)
	}
	if cfg.Bootstrap {
		t.Error("Bootstrap = true, want false")
	}
	if level, _ := cfg.SlogLevel(); level != slog.LevelDebug {
		t.Errorf("level = %v, want debug", level)
	}
	// Unset variables keep their defaults.
	if cfg.KafkaTopic != Default().KafkaTopic {
		t.Errorf("KafkaTopic = %q, want the default", cfg.KafkaTopic)
	}
}

func TestLoadReportsEveryBadValueAtOnce(t *testing.T) {
	t.Setenv("OUTBOX_BATCH", "lots")
	t.Setenv("OUTBOX_INTERVAL", "soon")
	_, err := Load()
	if err == nil {
		t.Fatal("Load accepted invalid values")
	}
	for _, want := range []string{"OUTBOX_BATCH", "OUTBOX_INTERVAL"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %s", err, want)
		}
	}
}

func TestValidateCatchesUnusableCombinations(t *testing.T) {
	for _, tc := range []struct {
		name string
		mut  func(*Config)
		want string
	}{
		{"no database", func(c *Config) { c.DatabaseURL = "" }, "DATABASE_URL"},
		{"no brokers", func(c *Config) { c.KafkaBrokers = nil }, "KAFKA_BROKERS"},
		{"no topic", func(c *Config) { c.KafkaTopic = "" }, "KAFKA_TOPIC"},
		{"no clearing account", func(c *Config) { c.ClearingAccount = "" }, "CLEARING_ACCOUNT"},
		{"zero batch", func(c *Config) { c.OutboxBatch = 0 }, "OUTBOX_BATCH"},
		{"lease below interval", func(c *Config) { c.OutboxLease = time.Millisecond }, "OUTBOX_LEASE"},
		{"bad log level", func(c *Config) { c.LogLevel = "chatty" }, "LOG_LEVEL"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := Default()
			tc.mut(&cfg)
			err := cfg.Validate()
			if err == nil {
				t.Fatalf("Validate accepted %s", tc.name)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not mention %s", err, tc.want)
			}
		})
	}
}
