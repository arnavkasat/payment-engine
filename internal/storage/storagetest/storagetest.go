// Package storagetest is a conformance suite for storage.Store. Every
// implementation runs it, which is what lets the in-memory store stand in for
// PostgreSQL in unit tests without the two drifting apart.
package storagetest

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/arnavkasat/payment-engine/internal/ledger"
	"github.com/arnavkasat/payment-engine/internal/money"
	"github.com/arnavkasat/payment-engine/internal/storage"
)

// Harness is one implementation under test, plus the few escape hatches the
// suite needs to provoke conditions a correct store would never produce.
type Harness struct {
	Store storage.Store
	// Corrupt applies a balance change with no matching posting, so the
	// suite can prove VerifyInvariants actually detects drift rather than
	// always returning a clean report.
	Corrupt func(t *testing.T, accountID uuid.UUID, delta money.Minor)
}

// Factory builds a fresh, empty harness for a single subtest.
type Factory func(t *testing.T) Harness

// Run executes the whole suite against the implementation the factory builds.
func Run(t *testing.T, newHarness Factory) {
	t.Helper()
	tests := []struct {
		name string
		fn   func(*testing.T, Harness)
	}{
		{"Accounts", testAccounts},
		{"IdempotencyClaimAndReplay", testIdempotency},
		{"IdempotencyIsExclusiveUnderConcurrency", testIdempotencyConcurrent},
		{"RollbackDiscardsEveryWrite", testRollback},
		{"ApplyEntryMovesBalances", testApplyEntry},
		{"ApplyEntryRefusesOverdraft", testOverdraft},
		{"ApplyEntryRefusesCurrencyMismatch", testCurrencyMismatch},
		{"ApplyEntryRefusesUnbalancedEntry", testUnbalanced},
		{"PaymentTransitions", testTransitions},
		{"OutboxLeasing", testOutboxLeasing},
		{"OutboxLeaseExpires", testOutboxLeaseExpiry},
		{"ProcessedMarkerIsExactlyOnce", testProcessedMarker},
		{"ProcessedMarkerRollsBackWithItsTransaction", testProcessedMarkerRollback},
		{"InvariantsHoldAfterTransfers", testInvariantsClean},
		{"InvariantsCatchUnjournalledBalanceChange", testInvariantsCorrupt},
		{"PaymentCountsByStatus", testPaymentCounts},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) { tc.fn(t, newHarness(t)) })
	}
}

// fixture is the standard set of accounts the suite transacts against.
type fixture struct {
	world, alice, bob, clearing, fees storage.Account
}

func setup(t *testing.T, s storage.Store) fixture {
	t.Helper()
	ctx := context.Background()
	mk := func(name string, kind storage.AccountKind, allowNegative bool) storage.Account {
		a, err := s.CreateAccount(ctx, storage.Account{
			Name: name, Currency: "USD", Kind: kind, AllowNegative: allowNegative,
		})
		if err != nil {
			t.Fatalf("CreateAccount(%q): %v", name, err)
		}
		return a
	}
	f := fixture{
		world:    mk("world", storage.KindExternal, true),
		alice:    mk("alice", storage.KindWallet, false),
		bob:      mk("bob", storage.KindWallet, false),
		clearing: mk("clearing", storage.KindClearing, false),
		fees:     mk("fees", storage.KindRevenue, false),
	}
	// Fund Alice from the external account, which is the only way value
	// enters the ledger and still sums to zero.
	fund := mustEntry(t, ledger.Entry{
		Currency: "USD",
		Kind:     ledger.KindCapture,
		Postings: []ledger.Posting{
			{AccountID: f.world.ID, Amount: -100_000},
			{AccountID: f.alice.ID, Amount: 100_000},
		},
	})
	if err := s.Tx(ctx, func(ctx context.Context, tx storage.Tx) error {
		return tx.ApplyEntry(ctx, fund)
	}); err != nil {
		t.Fatalf("funding alice: %v", err)
	}
	return f
}

// mustEntry stamps ids on a hand-built entry the way ledger's constructors do.
func mustEntry(t *testing.T, e ledger.Entry) ledger.Entry {
	t.Helper()
	e.ID = uuid.New()
	e.PaymentID = uuid.New()
	e.CreatedAt = time.Now().UTC()
	for i := range e.Postings {
		e.Postings[i].ID = uuid.New()
		e.Postings[i].EntryID = e.ID
	}
	if err := e.Validate(); err != nil {
		t.Fatalf("test entry is invalid: %v", err)
	}
	return e
}

func balance(t *testing.T, s storage.Store, id uuid.UUID) money.Minor {
	t.Helper()
	a, err := s.GetAccount(context.Background(), id)
	if err != nil {
		t.Fatalf("GetAccount: %v", err)
	}
	return a.Balance
}

func testAccounts(t *testing.T, h Harness) {
	ctx := context.Background()
	f := setup(t, h.Store)

	if _, err := h.Store.CreateAccount(ctx, storage.Account{Name: "alice", Currency: "USD", Kind: storage.KindWallet}); !errors.Is(err, storage.ErrAlreadyExists) {
		t.Errorf("duplicate name error = %v, want ErrAlreadyExists", err)
	}
	got, err := h.Store.GetAccountByName(ctx, "alice")
	if err != nil || got.ID != f.alice.ID {
		t.Errorf("GetAccountByName = %v, %v, want alice", got.ID, err)
	}
	if _, err := h.Store.GetAccount(ctx, uuid.New()); !errors.Is(err, storage.ErrNotFound) {
		t.Errorf("GetAccount(missing) = %v, want ErrNotFound", err)
	}
	if _, err := h.Store.GetAccountByName(ctx, "nobody"); !errors.Is(err, storage.ErrNotFound) {
		t.Errorf("GetAccountByName(missing) = %v, want ErrNotFound", err)
	}
	list, err := h.Store.ListAccounts(ctx)
	if err != nil {
		t.Fatalf("ListAccounts: %v", err)
	}
	if len(list) != 5 {
		t.Fatalf("ListAccounts returned %d accounts, want 5", len(list))
	}
	for i := 1; i < len(list); i++ {
		if list[i-1].Name > list[i].Name {
			t.Fatalf("ListAccounts not ordered by name: %q before %q", list[i-1].Name, list[i].Name)
		}
	}
	if list[0].Name != "alice" || list[0].Kind != storage.KindWallet || list[0].Currency != "USD" {
		t.Errorf("round-tripped account = %+v", list[0])
	}
	if balance(t, h.Store, f.world.ID) != -100_000 {
		t.Errorf("world balance = %d, want -100000", balance(t, h.Store, f.world.ID))
	}
}

func testIdempotency(t *testing.T, h Harness) {
	ctx := context.Background()
	paymentID := uuid.New()

	var first storage.IdempotencyRecord
	var claimed bool
	if err := h.Store.Tx(ctx, func(ctx context.Context, tx storage.Tx) error {
		var err error
		first, claimed, err = tx.ClaimIdempotencyKey(ctx, "key-1", "fp-a")
		return err
	}); err != nil {
		t.Fatalf("first claim: %v", err)
	}
	if !claimed || first.State != storage.StateInFlight {
		t.Fatalf("first claim = claimed %v, state %q; want true, in_flight", claimed, first.State)
	}

	// A second claim while the first is unfinished reports the in-flight record.
	if err := h.Store.Tx(ctx, func(ctx context.Context, tx storage.Tx) error {
		rec, ok, err := tx.ClaimIdempotencyKey(ctx, "key-1", "fp-a")
		if err != nil {
			return err
		}
		if ok {
			t.Error("second claim won the key")
		}
		if rec.State != storage.StateInFlight || rec.Fingerprint != "fp-a" {
			t.Errorf("second claim record = %+v", rec)
		}
		return nil
	}); err != nil {
		t.Fatalf("second claim: %v", err)
	}

	if err := h.Store.Tx(ctx, func(ctx context.Context, tx storage.Tx) error {
		return tx.CompleteIdempotencyKey(ctx, "key-1", paymentID, 201, []byte(`{"id":"x"}`))
	}); err != nil {
		t.Fatalf("complete: %v", err)
	}

	// After completion the stored response is replayable, and the recorded
	// fingerprint still reflects the original request.
	if err := h.Store.Tx(ctx, func(ctx context.Context, tx storage.Tx) error {
		rec, ok, err := tx.ClaimIdempotencyKey(ctx, "key-1", "fp-b")
		if err != nil {
			return err
		}
		if ok {
			t.Error("claim after completion won the key")
		}
		if rec.State != storage.StateCompleted {
			t.Errorf("state = %q, want completed", rec.State)
		}
		if rec.PaymentID != paymentID {
			t.Errorf("payment id = %s, want %s", rec.PaymentID, paymentID)
		}
		if rec.StatusCode != 201 || string(rec.Response) != `{"id":"x"}` {
			t.Errorf("stored response = %d %q", rec.StatusCode, rec.Response)
		}
		if rec.Fingerprint != "fp-a" {
			t.Errorf("fingerprint = %q, want the original fp-a", rec.Fingerprint)
		}
		if rec.CompletedAt == nil {
			t.Error("CompletedAt not set")
		}
		return nil
	}); err != nil {
		t.Fatalf("replay claim: %v", err)
	}

	if err := h.Store.Tx(ctx, func(ctx context.Context, tx storage.Tx) error {
		return tx.CompleteIdempotencyKey(ctx, "no-such-key", paymentID, 201, nil)
	}); !errors.Is(err, storage.ErrNotFound) {
		t.Errorf("completing an unclaimed key = %v, want ErrNotFound", err)
	}
}

func testIdempotencyConcurrent(t *testing.T, h Harness) {
	ctx := context.Background()
	const racers = 16

	var mu sync.Mutex
	var wins int
	var wg sync.WaitGroup
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = h.Store.Tx(ctx, func(ctx context.Context, tx storage.Tx) error {
				_, claimed, err := tx.ClaimIdempotencyKey(ctx, "hot-key", "fp")
				if err != nil {
					return err
				}
				if claimed {
					mu.Lock()
					wins++
					mu.Unlock()
				}
				return nil
			})
		}()
	}
	wg.Wait()
	if wins != 1 {
		t.Fatalf("%d of %d concurrent claims won the key, want exactly 1", wins, racers)
	}
}

func testRollback(t *testing.T, h Harness) {
	ctx := context.Background()
	f := setup(t, h.Store)
	before := balance(t, h.Store, f.alice.ID)
	paymentID := uuid.New()
	boom := errors.New("boom")

	err := h.Store.Tx(ctx, func(ctx context.Context, tx storage.Tx) error {
		if _, _, err := tx.ClaimIdempotencyKey(ctx, "rolled-back", "fp"); err != nil {
			return err
		}
		if err := tx.InsertPayment(ctx, storage.Payment{
			ID: paymentID, IdempotencyKey: "rolled-back", PayerAccount: f.alice.ID,
			PayeeAccount: f.bob.ID, Amount: 10, Currency: "USD", Status: storage.StatusCaptured,
			CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
		}); err != nil {
			return err
		}
		e := mustEntry(t, ledger.Entry{Currency: "USD", Kind: ledger.KindCapture, Postings: []ledger.Posting{
			{AccountID: f.alice.ID, Amount: -10}, {AccountID: f.clearing.ID, Amount: 10},
		}})
		if err := tx.ApplyEntry(ctx, e); err != nil {
			return err
		}
		if err := tx.Enqueue(ctx, storage.OutboxMessage{
			ID: uuid.New(), AggregateID: paymentID, Type: "payment.captured", Payload: []byte(`{}`),
		}); err != nil {
			return err
		}
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("Tx err = %v, want boom", err)
	}

	if got := balance(t, h.Store, f.alice.ID); got != before {
		t.Errorf("balance after rollback = %d, want %d", got, before)
	}
	if _, err := h.Store.GetPayment(ctx, paymentID); !errors.Is(err, storage.ErrNotFound) {
		t.Errorf("payment survived rollback: %v", err)
	}
	if n, err := h.Store.PendingOutbox(ctx); err != nil || n != 0 {
		t.Errorf("PendingOutbox = %d, %v, want 0", n, err)
	}
	// The idempotency key must be free again, or a retry of a failed request
	// would be rejected forever.
	if err := h.Store.Tx(ctx, func(ctx context.Context, tx storage.Tx) error {
		_, claimed, err := tx.ClaimIdempotencyKey(ctx, "rolled-back", "fp")
		if err != nil {
			return err
		}
		if !claimed {
			t.Error("idempotency key still held after rollback")
		}
		return nil
	}); err != nil {
		t.Fatalf("reclaim: %v", err)
	}
}

func testApplyEntry(t *testing.T, h Harness) {
	ctx := context.Background()
	f := setup(t, h.Store)
	paymentID := uuid.New()

	e, err := ledger.NewCapture(paymentID, f.alice.ID, f.clearing.ID, f.fees.ID, "USD", 2_500, 75, time.Now())
	if err != nil {
		t.Fatalf("NewCapture: %v", err)
	}
	if err := h.Store.Tx(ctx, func(ctx context.Context, tx storage.Tx) error {
		return tx.ApplyEntry(ctx, e)
	}); err != nil {
		t.Fatalf("ApplyEntry: %v", err)
	}

	for _, tc := range []struct {
		id   uuid.UUID
		want money.Minor
		name string
	}{
		{f.alice.ID, 100_000 - 2_575, "alice"},
		{f.clearing.ID, 2_500, "clearing"},
		{f.fees.ID, 75, "fees"},
	} {
		if got := balance(t, h.Store, tc.id); got != tc.want {
			t.Errorf("%s balance = %d, want %d", tc.name, got, tc.want)
		}
	}

	entries, err := h.Store.GetEntries(ctx, paymentID)
	if err != nil {
		t.Fatalf("GetEntries: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("GetEntries returned %d entries, want 1", len(entries))
	}
	got := entries[0]
	if got.ID != e.ID || got.Kind != ledger.KindCapture || got.Currency != "USD" {
		t.Errorf("entry round-trip = %+v", got)
	}
	if len(got.Postings) != 3 {
		t.Fatalf("round-tripped %d postings, want 3", len(got.Postings))
	}
	if err := got.Validate(); err != nil {
		t.Errorf("round-tripped entry does not validate: %v", err)
	}
	if want := e.Deltas(); !sameDeltas(want, got.Deltas()) {
		t.Errorf("round-tripped deltas = %v, want %v", got.Deltas(), want)
	}
}

func sameDeltas(a, b map[uuid.UUID]money.Minor) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

func testOverdraft(t *testing.T, h Harness) {
	ctx := context.Background()
	f := setup(t, h.Store)
	before := balance(t, h.Store, f.bob.ID)

	// Bob has nothing; a wallet may not go negative.
	e := mustEntry(t, ledger.Entry{Currency: "USD", Kind: ledger.KindCapture, Postings: []ledger.Posting{
		{AccountID: f.bob.ID, Amount: -1}, {AccountID: f.clearing.ID, Amount: 1},
	}})
	err := h.Store.Tx(ctx, func(ctx context.Context, tx storage.Tx) error {
		return tx.ApplyEntry(ctx, e)
	})
	if !errors.Is(err, storage.ErrInsufficientFunds) {
		t.Fatalf("ApplyEntry = %v, want ErrInsufficientFunds", err)
	}
	if got := balance(t, h.Store, f.bob.ID); got != before {
		t.Errorf("bob balance = %d, want %d", got, before)
	}
	if got := balance(t, h.Store, f.clearing.ID); got != 0 {
		t.Errorf("clearing balance = %d, want 0", got)
	}

	// The external account is allowed to go negative: that is how money
	// enters the ledger without breaking the zero-sum rule.
	ok := mustEntry(t, ledger.Entry{Currency: "USD", Kind: ledger.KindCapture, Postings: []ledger.Posting{
		{AccountID: f.world.ID, Amount: -5}, {AccountID: f.bob.ID, Amount: 5},
	}})
	if err := h.Store.Tx(ctx, func(ctx context.Context, tx storage.Tx) error {
		return tx.ApplyEntry(ctx, ok)
	}); err != nil {
		t.Fatalf("external account refused to go negative: %v", err)
	}
}

func testCurrencyMismatch(t *testing.T, h Harness) {
	ctx := context.Background()
	f := setup(t, h.Store)
	eur, err := h.Store.CreateAccount(ctx, storage.Account{Name: "eur-wallet", Currency: "EUR", Kind: storage.KindWallet, AllowNegative: true})
	if err != nil {
		t.Fatalf("CreateAccount: %v", err)
	}
	e := mustEntry(t, ledger.Entry{Currency: "USD", Kind: ledger.KindCapture, Postings: []ledger.Posting{
		{AccountID: f.alice.ID, Amount: -10}, {AccountID: eur.ID, Amount: 10},
	}})
	err = h.Store.Tx(ctx, func(ctx context.Context, tx storage.Tx) error {
		return tx.ApplyEntry(ctx, e)
	})
	if !errors.Is(err, storage.ErrCurrencyMismatch) {
		t.Fatalf("ApplyEntry = %v, want ErrCurrencyMismatch", err)
	}
}

func testUnbalanced(t *testing.T, h Harness) {
	ctx := context.Background()
	f := setup(t, h.Store)
	// Built by hand, bypassing the ledger constructors, to prove the store
	// refuses an imbalance rather than trusting its caller.
	e := ledger.Entry{
		ID: uuid.New(), PaymentID: uuid.New(), Kind: ledger.KindCapture, Currency: "USD",
		CreatedAt: time.Now().UTC(),
		Postings: []ledger.Posting{
			{ID: uuid.New(), AccountID: f.alice.ID, Amount: -10},
			{ID: uuid.New(), AccountID: f.clearing.ID, Amount: 9},
		},
	}
	err := h.Store.Tx(ctx, func(ctx context.Context, tx storage.Tx) error {
		return tx.ApplyEntry(ctx, e)
	})
	if !errors.Is(err, ledger.ErrNotBalanced) {
		t.Fatalf("ApplyEntry = %v, want ErrNotBalanced", err)
	}
	if got := balance(t, h.Store, f.alice.ID); got != 100_000 {
		t.Errorf("alice balance = %d, want untouched 100000", got)
	}
}

func testTransitions(t *testing.T, h Harness) {
	ctx := context.Background()
	f := setup(t, h.Store)
	id := uuid.New()
	now := time.Now().UTC()

	if err := h.Store.Tx(ctx, func(ctx context.Context, tx storage.Tx) error {
		return tx.InsertPayment(ctx, storage.Payment{
			ID: id, IdempotencyKey: "k", PayerAccount: f.alice.ID, PayeeAccount: f.bob.ID,
			Amount: 1_000, Fee: 10, Currency: "USD", Status: storage.StatusCaptured,
			Reference: "invoice-7", CreatedAt: now, UpdatedAt: now,
		})
	}); err != nil {
		t.Fatalf("InsertPayment: %v", err)
	}

	got, err := h.Store.GetPayment(ctx, id)
	if err != nil {
		t.Fatalf("GetPayment: %v", err)
	}
	if got.Amount != 1_000 || got.Fee != 10 || got.Reference != "invoice-7" || got.Status != storage.StatusCaptured {
		t.Errorf("payment round-trip = %+v", got)
	}

	if err := h.Store.Tx(ctx, func(ctx context.Context, tx storage.Tx) error {
		return tx.TransitionPayment(ctx, id, storage.StatusCaptured, storage.StatusSettled)
	}); err != nil {
		t.Fatalf("transition to settled: %v", err)
	}
	if got, _ := h.Store.GetPayment(ctx, id); got.Status != storage.StatusSettled {
		t.Errorf("status = %q, want settled", got.Status)
	}

	// Replaying the same transition must be refused, not silently repeated.
	err = h.Store.Tx(ctx, func(ctx context.Context, tx storage.Tx) error {
		return tx.TransitionPayment(ctx, id, storage.StatusCaptured, storage.StatusSettled)
	})
	if !errors.Is(err, storage.ErrStatusConflict) {
		t.Errorf("repeat transition = %v, want ErrStatusConflict", err)
	}
	err = h.Store.Tx(ctx, func(ctx context.Context, tx storage.Tx) error {
		return tx.TransitionPayment(ctx, uuid.New(), storage.StatusCaptured, storage.StatusSettled)
	})
	if !errors.Is(err, storage.ErrNotFound) {
		t.Errorf("transition of missing payment = %v, want ErrNotFound", err)
	}
	if _, err := h.Store.GetPayment(ctx, uuid.New()); !errors.Is(err, storage.ErrNotFound) {
		t.Errorf("GetPayment(missing) = %v, want ErrNotFound", err)
	}
}

func testOutboxLeasing(t *testing.T, h Harness) {
	ctx := context.Background()
	ids := make([]uuid.UUID, 3)
	if err := h.Store.Tx(ctx, func(ctx context.Context, tx storage.Tx) error {
		for i := range ids {
			ids[i] = uuid.New()
			if err := tx.Enqueue(ctx, storage.OutboxMessage{
				ID: ids[i], AggregateID: uuid.New(), Type: "payment.captured",
				Payload: []byte(`{"n":1}`),
			}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if n, err := h.Store.PendingOutbox(ctx); err != nil || n != 3 {
		t.Fatalf("PendingOutbox = %d, %v, want 3", n, err)
	}

	first, err := h.Store.LeaseOutbox(ctx, "worker-a", 2, time.Minute)
	if err != nil {
		t.Fatalf("LeaseOutbox: %v", err)
	}
	if len(first) != 2 {
		t.Fatalf("leased %d messages, want 2", len(first))
	}
	if first[0].Type != "payment.captured" || string(first[0].Payload) != `{"n":1}` {
		t.Errorf("leased message = %+v", first[0])
	}
	if first[0].Attempts != 1 {
		t.Errorf("attempts = %d, want 1", first[0].Attempts)
	}

	// A second worker must not see rows the first still holds.
	second, err := h.Store.LeaseOutbox(ctx, "worker-b", 10, time.Minute)
	if err != nil {
		t.Fatalf("LeaseOutbox: %v", err)
	}
	if len(second) != 1 {
		t.Fatalf("second worker leased %d messages, want 1", len(second))
	}
	if second[0].ID == first[0].ID || second[0].ID == first[1].ID {
		t.Error("second worker leased a message the first worker holds")
	}

	if err := h.Store.MarkOutboxPublished(ctx, []uuid.UUID{first[0].ID, first[1].ID}); err != nil {
		t.Fatalf("MarkOutboxPublished: %v", err)
	}
	if n, err := h.Store.PendingOutbox(ctx); err != nil || n != 1 {
		t.Fatalf("PendingOutbox after publish = %d, %v, want 1", n, err)
	}

	// Releasing returns the row to the queue immediately with its cause kept.
	if err := h.Store.ReleaseOutbox(ctx, second[0].ID, "broker unreachable"); err != nil {
		t.Fatalf("ReleaseOutbox: %v", err)
	}
	again, err := h.Store.LeaseOutbox(ctx, "worker-c", 10, time.Minute)
	if err != nil {
		t.Fatalf("LeaseOutbox: %v", err)
	}
	if len(again) != 1 || again[0].ID != second[0].ID {
		t.Fatalf("released message was not re-leased: %+v", again)
	}
	if again[0].Attempts != 2 {
		t.Errorf("attempts = %d, want 2", again[0].Attempts)
	}
	if again[0].LastError != "broker unreachable" {
		t.Errorf("LastError = %q", again[0].LastError)
	}
	if err := h.Store.MarkOutboxPublished(ctx, []uuid.UUID{again[0].ID}); err != nil {
		t.Fatalf("MarkOutboxPublished: %v", err)
	}
	if n, _ := h.Store.PendingOutbox(ctx); n != 0 {
		t.Errorf("PendingOutbox = %d, want 0", n)
	}
	// Publishing an already-published message is harmless; redeliveries race.
	if err := h.Store.MarkOutboxPublished(ctx, []uuid.UUID{again[0].ID}); err != nil {
		t.Errorf("re-publishing = %v, want nil", err)
	}
	if err := h.Store.MarkOutboxPublished(ctx, nil); err != nil {
		t.Errorf("MarkOutboxPublished(nil) = %v, want nil", err)
	}
}

func testOutboxLeaseExpiry(t *testing.T, h Harness) {
	ctx := context.Background()
	id := uuid.New()
	if err := h.Store.Tx(ctx, func(ctx context.Context, tx storage.Tx) error {
		return tx.Enqueue(ctx, storage.OutboxMessage{ID: id, AggregateID: uuid.New(), Type: "t", Payload: []byte(`{}`)})
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// A worker leases the row and then dies without publishing.
	if got, err := h.Store.LeaseOutbox(ctx, "doomed", 1, 50*time.Millisecond); err != nil || len(got) != 1 {
		t.Fatalf("LeaseOutbox = %d messages, %v", len(got), err)
	}
	if got, err := h.Store.LeaseOutbox(ctx, "other", 1, time.Minute); err != nil || len(got) != 0 {
		t.Fatalf("lease not honoured: %d messages, %v", len(got), err)
	}

	time.Sleep(120 * time.Millisecond)

	got, err := h.Store.LeaseOutbox(ctx, "rescuer", 1, time.Minute)
	if err != nil {
		t.Fatalf("LeaseOutbox: %v", err)
	}
	if len(got) != 1 || got[0].ID != id {
		t.Fatalf("expired lease was not reclaimed: %+v", got)
	}
	if got[0].Attempts != 2 {
		t.Errorf("attempts = %d, want 2", got[0].Attempts)
	}
}

func testProcessedMarker(t *testing.T, h Harness) {
	ctx := context.Background()
	msg := uuid.New().String()

	var results []bool
	for i := 0; i < 3; i++ {
		if err := h.Store.Tx(ctx, func(ctx context.Context, tx storage.Tx) error {
			first, err := tx.MarkProcessed(ctx, "settlement", msg)
			results = append(results, first)
			return err
		}); err != nil {
			t.Fatalf("MarkProcessed: %v", err)
		}
	}
	if want := []bool{true, false, false}; len(results) != 3 || results[0] != want[0] || results[1] != want[1] || results[2] != want[2] {
		t.Fatalf("MarkProcessed results = %v, want %v", results, want)
	}

	// A different consumer group gets its own first delivery.
	if err := h.Store.Tx(ctx, func(ctx context.Context, tx storage.Tx) error {
		first, err := tx.MarkProcessed(ctx, "analytics", msg)
		if err != nil {
			return err
		}
		if !first {
			t.Error("second consumer group was treated as a duplicate")
		}
		return nil
	}); err != nil {
		t.Fatalf("MarkProcessed: %v", err)
	}
}

func testProcessedMarkerRollback(t *testing.T, h Harness) {
	ctx := context.Background()
	msg := uuid.New().String()
	boom := errors.New("handler failed")

	// This is the property that makes at-least-once delivery safe: the
	// dedupe marker and the side effect share a transaction, so a handler
	// that fails after the marker is written leaves no trace of it.
	err := h.Store.Tx(ctx, func(ctx context.Context, tx storage.Tx) error {
		first, err := tx.MarkProcessed(ctx, "settlement", msg)
		if err != nil {
			return err
		}
		if !first {
			t.Error("marker already present")
		}
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("Tx = %v, want boom", err)
	}

	if err := h.Store.Tx(ctx, func(ctx context.Context, tx storage.Tx) error {
		first, err := tx.MarkProcessed(ctx, "settlement", msg)
		if err != nil {
			return err
		}
		if !first {
			t.Error("redelivery after a failed handler was skipped as a duplicate")
		}
		return nil
	}); err != nil {
		t.Fatalf("redelivery: %v", err)
	}
}

func testInvariantsClean(t *testing.T, h Harness) {
	ctx := context.Background()
	f := setup(t, h.Store)

	for i := 0; i < 20; i++ {
		paymentID := uuid.New()
		capture, err := ledger.NewCapture(paymentID, f.alice.ID, f.clearing.ID, f.fees.ID, "USD", 100, 3, time.Now())
		if err != nil {
			t.Fatalf("NewCapture: %v", err)
		}
		settle, err := ledger.NewSettlement(paymentID, f.clearing.ID, f.bob.ID, "USD", 100, time.Now())
		if err != nil {
			t.Fatalf("NewSettlement: %v", err)
		}
		if err := h.Store.Tx(ctx, func(ctx context.Context, tx storage.Tx) error {
			if err := tx.ApplyEntry(ctx, capture); err != nil {
				return err
			}
			return tx.ApplyEntry(ctx, settle)
		}); err != nil {
			t.Fatalf("transfer %d: %v", i, err)
		}
	}

	rep, err := h.Store.VerifyInvariants(ctx)
	if err != nil {
		t.Fatalf("VerifyInvariants: %v", err)
	}
	if !rep.OK() {
		t.Fatalf("invariants violated after clean transfers: %s", rep.Summary())
	}
	if rep.TotalPostingSum != 0 {
		t.Errorf("TotalPostingSum = %d, want 0", rep.TotalPostingSum)
	}
	// One funding entry plus a capture and a settlement per transfer.
	if rep.Entries != 41 {
		t.Errorf("Entries = %d, want 41", rep.Entries)
	}
	if rep.Postings != 2+20*(3+2) {
		t.Errorf("Postings = %d, want %d", rep.Postings, 2+20*(3+2))
	}
	if rep.Accounts != 5 {
		t.Errorf("Accounts = %d, want 5", rep.Accounts)
	}
	if got, want := balance(t, h.Store, f.clearing.ID), money.Minor(0); got != want {
		t.Errorf("clearing balance = %d, want %d: every capture was settled", got, want)
	}
	if got, want := balance(t, h.Store, f.bob.ID), money.Minor(2_000); got != want {
		t.Errorf("bob balance = %d, want %d", got, want)
	}
	if got, want := balance(t, h.Store, f.fees.ID), money.Minor(60); got != want {
		t.Errorf("fees balance = %d, want %d", got, want)
	}
}

func testInvariantsCorrupt(t *testing.T, h Harness) {
	if h.Corrupt == nil {
		t.Skip("harness cannot corrupt state")
	}
	ctx := context.Background()
	f := setup(t, h.Store)

	rep, err := h.Store.VerifyInvariants(ctx)
	if err != nil {
		t.Fatalf("VerifyInvariants: %v", err)
	}
	if !rep.OK() {
		t.Fatalf("clean ledger reported violations: %s", rep.Summary())
	}

	// Credit an account without writing the posting that justifies it, the
	// way a balance update applied outside the journal would.
	h.Corrupt(t, f.bob.ID, 1)

	rep, err = h.Store.VerifyInvariants(ctx)
	if err != nil {
		t.Fatalf("VerifyInvariants: %v", err)
	}
	if rep.OK() {
		t.Fatal("invariant checker missed an unjournalled balance change")
	}
	var found bool
	for _, v := range rep.Violations {
		if v.Rule == ledger.RuleBalanceMatchesPostings {
			found = true
		}
	}
	if !found {
		t.Errorf("violations = %v, want one for %s", rep.Violations, ledger.RuleBalanceMatchesPostings)
	}
	if rep.Err() == nil {
		t.Error("Err() returned nil for a violated report")
	}

	// Debiting an account below zero without a posting must trip both the
	// drift rule and the overdraft rule.
	h.Corrupt(t, f.alice.ID, -200_000)
	rep, err = h.Store.VerifyInvariants(ctx)
	if err != nil {
		t.Fatalf("VerifyInvariants: %v", err)
	}
	var overdraft bool
	for _, v := range rep.Violations {
		if v.Rule == ledger.RuleNoUnauthorisedOverdraft {
			overdraft = true
		}
	}
	if !overdraft {
		t.Errorf("violations = %v, want one for %s", rep.Violations, ledger.RuleNoUnauthorisedOverdraft)
	}
}

func testPaymentCounts(t *testing.T, h Harness) {
	ctx := context.Background()
	f := setup(t, h.Store)

	counts, err := h.Store.CountPaymentsByStatus(ctx)
	if err != nil {
		t.Fatalf("CountPaymentsByStatus: %v", err)
	}
	if len(counts) != 0 {
		t.Errorf("counts on an empty ledger = %v, want empty", counts)
	}

	ids := make([]uuid.UUID, 3)
	for i := range ids {
		ids[i] = uuid.New()
		id := ids[i]
		if err := h.Store.Tx(ctx, func(ctx context.Context, tx storage.Tx) error {
			now := time.Now().UTC()
			return tx.InsertPayment(ctx, storage.Payment{
				ID: id, IdempotencyKey: id.String(), PayerAccount: f.alice.ID,
				PayeeAccount: f.bob.ID, Amount: 100, Currency: "USD",
				Status: storage.StatusCaptured, CreatedAt: now, UpdatedAt: now,
			})
		}); err != nil {
			t.Fatalf("InsertPayment: %v", err)
		}
	}
	if err := h.Store.Tx(ctx, func(ctx context.Context, tx storage.Tx) error {
		return tx.TransitionPayment(ctx, ids[0], storage.StatusCaptured, storage.StatusSettled)
	}); err != nil {
		t.Fatalf("TransitionPayment: %v", err)
	}

	counts, err = h.Store.CountPaymentsByStatus(ctx)
	if err != nil {
		t.Fatalf("CountPaymentsByStatus: %v", err)
	}
	if got := counts[storage.StatusCaptured]; got != 2 {
		t.Errorf("captured = %d, want 2", got)
	}
	if got := counts[storage.StatusSettled]; got != 1 {
		t.Errorf("settled = %d, want 1", got)
	}
	if _, present := counts[storage.StatusFailed]; present {
		t.Errorf("a status with no payments was reported: %v", counts)
	}
}
