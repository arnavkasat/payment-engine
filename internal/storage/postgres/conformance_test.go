//go:build integration

// These tests need a real PostgreSQL. They run under `make test-integration`
// and in CI, where a service container supplies TEST_DATABASE_URL; `go test
// ./...` without the integration tag skips the whole file.
package postgres_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/arnavkasat/payment-engine/internal/money"
	"github.com/arnavkasat/payment-engine/internal/storage/postgres"
	"github.com/arnavkasat/payment-engine/internal/storage/storagetest"
)

// balanceFloor is dropped and restored around tests that deliberately corrupt
// balances, because the schema is doing its job and refusing them otherwise.
const balanceFloor = `ALTER TABLE accounts ADD CONSTRAINT accounts_balance_floor CHECK (allow_negative OR balance >= 0)`

func dsn(t *testing.T) string {
	t.Helper()
	v := os.Getenv("TEST_DATABASE_URL")
	if v == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	return v
}

func open(t *testing.T) *postgres.Store {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	store, err := postgres.Open(ctx, postgres.Config{DSN: dsn(t), MaxConns: 8})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := postgres.Migrate(ctx, store.Pool()); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	return store
}

// reset empties every table and puts the schema back the way the migration
// left it, so each subtest starts from nothing.
func reset(t *testing.T, store *postgres.Store) {
	t.Helper()
	ctx := context.Background()
	stmts := []string{
		`ALTER TABLE accounts DROP CONSTRAINT IF EXISTS accounts_balance_floor`,
		`TRUNCATE processed_messages, outbox, idempotency_keys,
		          ledger_postings, ledger_entries, payments, accounts`,
		balanceFloor,
	}
	for _, s := range stmts {
		if _, err := store.Pool().Exec(ctx, s); err != nil {
			t.Fatalf("reset (%s): %v", s, err)
		}
	}
}

func TestConformance(t *testing.T) {
	store := open(t)
	t.Cleanup(func() { _ = store.Close() })

	storagetest.Run(t, func(t *testing.T) storagetest.Harness {
		reset(t, store)
		return storagetest.Harness{
			Store: store,
			Corrupt: func(t *testing.T, id uuid.UUID, delta money.Minor) {
				t.Helper()
				ctx := context.Background()
				// The CHECK constraint exists precisely to stop this, so it
				// has to come off for the invariant checker to have anything
				// to find. reset puts it back before the next subtest.
				if _, err := store.Pool().Exec(ctx, `ALTER TABLE accounts DROP CONSTRAINT IF EXISTS accounts_balance_floor`); err != nil {
					t.Fatalf("dropping balance floor: %v", err)
				}
				if _, err := store.Pool().Exec(ctx,
					`UPDATE accounts SET balance = balance + $2 WHERE id = $1`, id, int64(delta)); err != nil {
					t.Fatalf("corrupting balance: %v", err)
				}
			},
		}
	})
}

// TestDeferredTriggerRejectsUnbalancedEntry proves the database refuses an
// imbalance on its own, with the application code bypassed entirely. This is
// the backstop behind ledger.Entry.Validate.
func TestDeferredTriggerRejectsUnbalancedEntry(t *testing.T) {
	store := open(t)
	t.Cleanup(func() { _ = store.Close() })
	reset(t, store)

	ctx := context.Background()
	pool := store.Pool()

	var payer, payee uuid.UUID
	for name, target := range map[string]*uuid.UUID{"payer": &payer, "payee": &payee} {
		if err := pool.QueryRow(ctx, `
			INSERT INTO accounts (id, name, currency, kind, allow_negative)
			VALUES ($1, $2, 'USD', 'external', true) RETURNING id`,
			uuid.New(), name).Scan(target); err != nil {
			t.Fatalf("creating %s: %v", name, err)
		}
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	entryID := uuid.New()
	if _, err := tx.Exec(ctx, `
		INSERT INTO ledger_entries (id, payment_id, kind, currency, created_at)
		VALUES ($1, $2, 'capture', 'USD', now())`, entryID, uuid.New()); err != nil {
		t.Fatalf("inserting entry: %v", err)
	}
	for _, amount := range []int64{-100, 99} {
		if _, err := tx.Exec(ctx, `
			INSERT INTO ledger_postings (id, entry_id, account_id, amount)
			VALUES ($1, $2, $3, $4)`, uuid.New(), entryID, payer, amount); err != nil {
			// The unique leg-per-account constraint would reject the second
			// posting against the same account, so use distinct accounts.
			t.Fatalf("inserting posting: %v", err)
		}
		payer = payee
	}

	// The trigger is deferred, so the imbalance surfaces at COMMIT, after
	// every posting of the entry has been written.
	if err := tx.Commit(ctx); err == nil {
		t.Fatal("database accepted an entry whose postings sum to -1")
	} else {
		t.Logf("database rejected the imbalance: %v", err)
	}

	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM ledger_entries WHERE id = $1`, entryID).Scan(&n); err != nil {
		t.Fatalf("counting entries: %v", err)
	}
	if n != 0 {
		t.Errorf("entry survived the rejected commit (%d rows)", n)
	}
}

// TestSingleEntryPerKindIndex proves a payment cannot take a second capture
// or settlement even if every layer above the database failed at once.
func TestSingleEntryPerKindIndex(t *testing.T) {
	store := open(t)
	t.Cleanup(func() { _ = store.Close() })
	reset(t, store)

	ctx := context.Background()
	paymentID := uuid.New()
	insert := func() error {
		_, err := store.Pool().Exec(ctx, `
			INSERT INTO ledger_entries (id, payment_id, kind, currency, created_at)
			VALUES ($1, $2, 'settlement', 'USD', now())`, uuid.New(), paymentID)
		return err
	}
	if err := insert(); err != nil {
		t.Fatalf("first settlement: %v", err)
	}
	if err := insert(); err == nil {
		t.Fatal("database accepted a second settlement for one payment")
	}
	// Reversals are exempt: a payment may legitimately be reversed twice.
	for i := 0; i < 2; i++ {
		if _, err := store.Pool().Exec(ctx, `
			INSERT INTO ledger_entries (id, payment_id, kind, currency, created_at)
			VALUES ($1, $2, 'reversal', 'USD', now())`, uuid.New(), paymentID); err != nil {
			t.Fatalf("reversal %d: %v", i, err)
		}
	}
}
