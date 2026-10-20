// Package postgres implements storage.Store on PostgreSQL.
//
// Two choices drive the design. First, isolation is READ COMMITTED rather
// than SERIALIZABLE: every balance change takes an explicit row lock, and
// those locks are always acquired in ascending account-id order, which rules
// out the lost update and the deadlock that READ COMMITTED would otherwise
// permit. SERIALIZABLE would buy nothing extra here and would cost
// predictable throughput to false-positive serialisation failures. Second,
// conflicting transactions are retried in the store rather than surfaced to
// callers, because storage.Tx already requires fn to be side-effect free.
package postgres

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"math/rand"
	"path"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/arnavkasat/payment-engine/internal/storage"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

// Config configures the pool and the retry policy.
type Config struct {
	// DSN is a libpq connection string or URL.
	DSN string
	// MaxConns caps the pool. Zero leaves pgx's default.
	MaxConns int32
	// MaxRetries is how many times a transaction is retried after a
	// serialisation failure or deadlock before giving up.
	MaxRetries int
	// RetryBackoff is the base delay between retries; each attempt waits a
	// random duration in [0, backoff*2^attempt).
	RetryBackoff time.Duration
	// Isolation overrides the transaction isolation level.
	Isolation pgx.TxIsoLevel
}

func (c *Config) applyDefaults() {
	if c.MaxRetries <= 0 {
		c.MaxRetries = 5
	}
	if c.RetryBackoff <= 0 {
		c.RetryBackoff = 2 * time.Millisecond
	}
	if c.Isolation == "" {
		c.Isolation = pgx.ReadCommitted
	}
}

// Store is a PostgreSQL-backed storage.Store.
type Store struct {
	pool *pgxpool.Pool
	cfg  Config
	rnd  *rand.Rand
}

var _ storage.Store = (*Store)(nil)

// Open connects to PostgreSQL and verifies the connection.
func Open(ctx context.Context, cfg Config) (*Store, error) {
	cfg.applyDefaults()
	poolCfg, err := pgxpool.ParseConfig(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("postgres: parsing dsn: %w", err)
	}
	if cfg.MaxConns > 0 {
		poolCfg.MaxConns = cfg.MaxConns
	}
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("postgres: connecting: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres: ping: %w", err)
	}
	return &Store{pool: pool, cfg: cfg, rnd: rand.New(rand.NewSource(time.Now().UnixNano()))}, nil
}

// Pool exposes the underlying pool for migrations and diagnostics.
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// Ping reports whether the database is reachable.
func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

// Close releases the pool.
func (s *Store) Close() error {
	s.pool.Close()
	return nil
}

// Migrate applies every embedded migration that has not been applied yet. It
// takes a transaction-scoped advisory lock first, so several instances
// starting at once cannot apply the same migration twice.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	entries, err := migrationFS.ReadDir("migrations")
	if err != nil {
		return fmt.Errorf("postgres: reading migrations: %w", err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() && path.Ext(e.Name()) == ".sql" {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)

	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("postgres: begin migration: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// An arbitrary but stable key, so only one migrator runs at a time.
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(8572331)`); err != nil {
		return fmt.Errorf("postgres: migration lock: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version    TEXT PRIMARY KEY,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`); err != nil {
		return fmt.Errorf("postgres: creating schema_migrations: %w", err)
	}

	applied := map[string]bool{}
	rows, err := tx.Query(ctx, `SELECT version FROM schema_migrations`)
	if err != nil {
		return fmt.Errorf("postgres: reading schema_migrations: %w", err)
	}
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			rows.Close()
			return fmt.Errorf("postgres: scanning schema_migrations: %w", err)
		}
		applied[v] = true
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("postgres: reading schema_migrations: %w", err)
	}

	for _, name := range names {
		if applied[name] {
			continue
		}
		body, err := migrationFS.ReadFile(path.Join("migrations", name))
		if err != nil {
			return fmt.Errorf("postgres: reading %s: %w", name, err)
		}
		if _, err := tx.Exec(ctx, string(body)); err != nil {
			return fmt.Errorf("postgres: applying %s: %w", name, err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations (version) VALUES ($1)`, name); err != nil {
			return fmt.Errorf("postgres: recording %s: %w", name, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("postgres: committing migrations: %w", err)
	}
	return nil
}

// Tx runs fn in a transaction, retrying it when PostgreSQL reports a
// serialisation failure or a deadlock.
func (s *Store) Tx(ctx context.Context, fn func(context.Context, storage.Tx) error) error {
	var last error
	for attempt := 0; attempt <= s.cfg.MaxRetries; attempt++ {
		if attempt > 0 {
			if err := s.sleepBackoff(ctx, attempt); err != nil {
				return err
			}
		}
		err := s.runTx(ctx, fn)
		if err == nil {
			return nil
		}
		if !isRetryable(err) {
			return err
		}
		last = err
	}
	return fmt.Errorf("%w after %d attempts: %v", storage.ErrRetryExhausted, s.cfg.MaxRetries+1, last)
}

func (s *Store) sleepBackoff(ctx context.Context, attempt int) error {
	window := s.cfg.RetryBackoff << uint(attempt-1)
	delay := time.Duration(s.rnd.Int63n(int64(window) + 1))
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (s *Store) runTx(ctx context.Context, fn func(context.Context, storage.Tx) error) error {
	pgTx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: s.cfg.Isolation})
	if err != nil {
		return mapError(err)
	}
	// Rollback is a no-op once the transaction has committed.
	defer func() { _ = pgTx.Rollback(ctx) }()

	if err := fn(ctx, &tx{tx: pgTx}); err != nil {
		return err
	}
	if err := pgTx.Commit(ctx); err != nil {
		return mapError(err)
	}
	return nil
}

// isRetryable reports whether an error is a transient concurrency conflict.
func isRetryable(err error) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}
	switch pgErr.Code {
	case "40001": // serialization_failure
		return true
	case "40P01": // deadlock_detected
		return true
	default:
		return false
	}
}

// mapError translates driver errors into the sentinels in package storage, so
// that callers never need to know pgx exists.
func mapError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("%w: %v", storage.ErrNotFound, err)
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "23505": // unique_violation
			return fmt.Errorf("%w: %s", storage.ErrAlreadyExists, pgErr.ConstraintName)
		case "23503": // foreign_key_violation
			return fmt.Errorf("%w: %s", storage.ErrNotFound, pgErr.ConstraintName)
		case "23514": // check_violation
			if pgErr.ConstraintName == "accounts_balance_floor" {
				return fmt.Errorf("%w: %s", storage.ErrInsufficientFunds, pgErr.Message)
			}
		}
	}
	return err
}
