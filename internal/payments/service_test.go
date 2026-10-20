package payments_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/arnavkasat/payment-engine/internal/events"
	"github.com/arnavkasat/payment-engine/internal/faults"
	"github.com/arnavkasat/payment-engine/internal/ledger"
	"github.com/arnavkasat/payment-engine/internal/metrics"
	"github.com/arnavkasat/payment-engine/internal/money"
	"github.com/arnavkasat/payment-engine/internal/payments"
	"github.com/arnavkasat/payment-engine/internal/storage"
	"github.com/arnavkasat/payment-engine/internal/storage/memory"
)

type harness struct {
	t        *testing.T
	store    *memory.Store
	svc      *payments.Service
	metrics  *metrics.Registry
	injector *faults.Injector

	world, alice, bob, clearing, fees storage.Account
}

const aliceOpeningBalance = money.Minor(1_000_000)

func newHarness(t *testing.T) *harness {
	t.Helper()
	ctx := context.Background()
	store := memory.New()
	h := &harness{t: t, store: store, metrics: metrics.NewRegistry(), injector: faults.New(1)}

	mk := func(name string, kind storage.AccountKind, allowNegative bool) storage.Account {
		a, err := store.CreateAccount(ctx, storage.Account{Name: name, Currency: "USD", Kind: kind, AllowNegative: allowNegative})
		if err != nil {
			t.Fatalf("CreateAccount(%q): %v", name, err)
		}
		return a
	}
	h.world = mk("world", storage.KindExternal, true)
	h.alice = mk("alice", storage.KindWallet, false)
	h.bob = mk("bob", storage.KindWallet, false)
	h.clearing = mk("clearing", storage.KindClearing, false)
	h.fees = mk("fees", storage.KindRevenue, false)

	fund, err := ledger.NewSettlement(uuid.New(), h.world.ID, h.alice.ID, "USD", aliceOpeningBalance, time.Now())
	if err != nil {
		t.Fatalf("building funding entry: %v", err)
	}
	if err := store.Tx(ctx, func(ctx context.Context, tx storage.Tx) error {
		return tx.ApplyEntry(ctx, fund)
	}); err != nil {
		t.Fatalf("funding alice: %v", err)
	}

	svc, err := payments.New(store, payments.Config{
		Clearing: h.clearing.ID,
		Fees:     h.fees.ID,
		External: h.world.ID,
		Faults:   h.injector,
		Metrics:  h.metrics,
	})
	if err != nil {
		t.Fatalf("payments.New: %v", err)
	}
	h.svc = svc
	return h
}

func (h *harness) request(key string) payments.CreateRequest {
	return payments.CreateRequest{
		IdempotencyKey: key,
		PayerAccount:   h.alice.ID,
		PayeeAccount:   h.bob.ID,
		Amount:         10_000,
		Fee:            250,
		Currency:       "USD",
		Reference:      "invoice-1",
	}
}

func (h *harness) balance(id uuid.UUID) money.Minor {
	h.t.Helper()
	a, err := h.store.GetAccount(context.Background(), id)
	if err != nil {
		h.t.Fatalf("GetAccount: %v", err)
	}
	return a.Balance
}

func (h *harness) pendingOutbox() int {
	h.t.Helper()
	n, err := h.store.PendingOutbox(context.Background())
	if err != nil {
		h.t.Fatalf("PendingOutbox: %v", err)
	}
	return n
}

// assertInvariants fails the test if the ledger is not internally consistent.
// Every test calls it: a test that leaves the books broken has not passed.
func (h *harness) assertInvariants() {
	h.t.Helper()
	rep, err := h.store.VerifyInvariants(context.Background())
	if err != nil {
		h.t.Fatalf("VerifyInvariants: %v", err)
	}
	if !rep.OK() {
		h.t.Fatalf("ledger invariants violated: %s", rep.Summary())
	}
}

func TestCaptureMovesMoneyAndEnqueuesOneEvent(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	res, err := h.svc.CreatePayment(ctx, h.request("key-1"))
	if err != nil {
		t.Fatalf("CreatePayment: %v", err)
	}
	if res.Replayed {
		t.Error("first call reported a replay")
	}
	if res.StatusCode != 201 {
		t.Errorf("status = %d, want 201", res.StatusCode)
	}
	if res.Payment.Status != storage.StatusCaptured {
		t.Errorf("status = %q, want captured", res.Payment.Status)
	}

	if got, want := h.balance(h.alice.ID), aliceOpeningBalance-10_250; got != want {
		t.Errorf("alice = %d, want %d", got, want)
	}
	if got := h.balance(h.clearing.ID); got != 10_000 {
		t.Errorf("clearing = %d, want 10000", got)
	}
	if got := h.balance(h.fees.ID); got != 250 {
		t.Errorf("fees = %d, want 250", got)
	}
	// The payee is not credited until settlement.
	if got := h.balance(h.bob.ID); got != 0 {
		t.Errorf("bob = %d, want 0 before settlement", got)
	}
	if got := h.pendingOutbox(); got != 1 {
		t.Errorf("pending outbox = %d, want 1", got)
	}

	entries, err := h.svc.GetEntries(ctx, res.Payment.ID)
	if err != nil {
		t.Fatalf("GetEntries: %v", err)
	}
	if len(entries) != 1 || entries[0].Kind != ledger.KindCapture {
		t.Fatalf("entries = %+v, want one capture", entries)
	}
	h.assertInvariants()
}

func TestRetryWithSameKeyReplaysByteForByte(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	first, err := h.svc.CreatePayment(ctx, h.request("key-1"))
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	balanceAfterFirst := h.balance(h.alice.ID)

	for i := 0; i < 5; i++ {
		again, err := h.svc.CreatePayment(ctx, h.request("key-1"))
		if err != nil {
			t.Fatalf("retry %d: %v", i, err)
		}
		if !again.Replayed {
			t.Fatalf("retry %d was not reported as a replay", i)
		}
		if again.Payment.ID != first.Payment.ID {
			t.Fatalf("retry %d returned payment %s, want %s", i, again.Payment.ID, first.Payment.ID)
		}
		if string(again.Response) != string(first.Response) {
			t.Fatalf("retry %d returned a different body:\n got %s\nwant %s", i, again.Response, first.Response)
		}
		if again.StatusCode != first.StatusCode {
			t.Fatalf("retry %d status = %d, want %d", i, again.StatusCode, first.StatusCode)
		}
	}

	if got := h.balance(h.alice.ID); got != balanceAfterFirst {
		t.Errorf("alice = %d after five retries, want %d", got, balanceAfterFirst)
	}
	if got := h.pendingOutbox(); got != 1 {
		t.Errorf("pending outbox = %d after five retries, want 1", got)
	}
	if got := h.metrics.CounterValue(payments.MetricPayments, metrics.Labels{"outcome": "replayed"}); got != 5 {
		t.Errorf("replayed counter = %d, want 5", got)
	}
	if got := h.metrics.CounterValue(payments.MetricPayments, metrics.Labels{"outcome": "captured"}); got != 1 {
		t.Errorf("captured counter = %d, want 1", got)
	}
	h.assertInvariants()
}

// TestConcurrentRetriesCaptureExactlyOnce is the property the whole design
// exists for: a client that fires the same request many times at once — a
// retry storm, a double-clicked button, a queue redelivering — must be
// charged once.
func TestConcurrentRetriesCaptureExactlyOnce(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	const racers = 64

	var (
		mu        sync.Mutex
		captured  int
		replayed  int
		conflicts int
		ids       = map[uuid.UUID]struct{}{}
	)
	var wg sync.WaitGroup
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := h.svc.CreatePayment(ctx, h.request("hot-key"))
			mu.Lock()
			defer mu.Unlock()
			switch {
			case errors.Is(err, payments.ErrRequestInFlight):
				conflicts++
			case err != nil:
				t.Errorf("unexpected error: %v", err)
			case res.Replayed:
				replayed++
				ids[res.Payment.ID] = struct{}{}
			default:
				captured++
				ids[res.Payment.ID] = struct{}{}
			}
		}()
	}
	wg.Wait()

	if captured != 1 {
		t.Fatalf("%d of %d concurrent requests captured a payment, want exactly 1", captured, racers)
	}
	if replayed+conflicts != racers-1 {
		t.Fatalf("replayed %d + conflicts %d, want %d", replayed, conflicts, racers-1)
	}
	if len(ids) != 1 {
		t.Fatalf("callers saw %d distinct payment ids, want 1", len(ids))
	}
	if got, want := h.balance(h.alice.ID), aliceOpeningBalance-10_250; got != want {
		t.Errorf("alice = %d, want %d: she was charged more than once", got, want)
	}
	if got := h.pendingOutbox(); got != 1 {
		t.Errorf("pending outbox = %d, want 1", got)
	}
	h.assertInvariants()
}

func TestKeyReuseWithADifferentRequestIsRejected(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	if _, err := h.svc.CreatePayment(ctx, h.request("key-1")); err != nil {
		t.Fatalf("first: %v", err)
	}
	before := h.balance(h.alice.ID)

	changed := h.request("key-1")
	changed.Amount = 99_999
	_, err := h.svc.CreatePayment(ctx, changed)
	if !errors.Is(err, payments.ErrIdempotencyKeyReused) {
		t.Fatalf("CreatePayment = %v, want ErrIdempotencyKeyReused", err)
	}
	if got := h.balance(h.alice.ID); got != before {
		t.Errorf("alice = %d, want %d", got, before)
	}
	if got := h.metrics.CounterValue(payments.MetricPayments, metrics.Labels{"outcome": "conflict"}); got != 1 {
		t.Errorf("conflict counter = %d, want 1", got)
	}
	h.assertInvariants()
}

func TestInsufficientFundsLeavesNoTrace(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	req := h.request("key-1")
	req.Amount = aliceOpeningBalance * 2
	_, err := h.svc.CreatePayment(ctx, req)
	if !errors.Is(err, storage.ErrInsufficientFunds) {
		t.Fatalf("CreatePayment = %v, want ErrInsufficientFunds", err)
	}
	if got := h.balance(h.alice.ID); got != aliceOpeningBalance {
		t.Errorf("alice = %d, want %d", got, aliceOpeningBalance)
	}
	if got := h.pendingOutbox(); got != 0 {
		t.Errorf("pending outbox = %d, want 0", got)
	}

	// The key must be free afterwards: a rejection is not an outcome worth
	// remembering, and the same key may legitimately be retried once the
	// payer has funds.
	ok, err := h.svc.CreatePayment(ctx, h.request("key-1"))
	if err != nil {
		t.Fatalf("retry after rejection: %v", err)
	}
	if ok.Replayed {
		t.Error("retry after a rejection replayed a result that never existed")
	}
	h.assertInvariants()
}

func TestCurrencyMismatchIsRejected(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	req := h.request("key-1")
	req.Currency = "EUR"
	if _, err := h.svc.CreatePayment(ctx, req); !errors.Is(err, storage.ErrCurrencyMismatch) {
		t.Fatalf("CreatePayment = %v, want ErrCurrencyMismatch", err)
	}
	h.assertInvariants()
}

func TestValidationFailsBeforeAnyWrite(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		mut  func(*payments.CreateRequest)
		want error
	}{
		{"no key", func(r *payments.CreateRequest) { r.IdempotencyKey = "" }, payments.ErrIdempotencyKeyRequired},
		{"zero amount", func(r *payments.CreateRequest) { r.Amount = 0 }, payments.ErrInvalidRequest},
		{"self payment", func(r *payments.CreateRequest) { r.PayeeAccount = r.PayerAccount }, payments.ErrInvalidRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := h.request("key-" + tc.name)
			tc.mut(&req)
			if _, err := h.svc.CreatePayment(ctx, req); !errors.Is(err, tc.want) {
				t.Fatalf("CreatePayment = %v, want %v", err, tc.want)
			}
		})
	}
	if got := h.balance(h.alice.ID); got != aliceOpeningBalance {
		t.Errorf("alice = %d, want untouched %d", got, aliceOpeningBalance)
	}
	h.assertInvariants()
}

func TestMissingAccountIsRejected(t *testing.T) {
	h := newHarness(t)
	req := h.request("key-1")
	req.PayeeAccount = uuid.New()
	if _, err := h.svc.CreatePayment(context.Background(), req); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("CreatePayment = %v, want ErrNotFound", err)
	}
	h.assertInvariants()
}

// TestCrashBeforeCommitLosesTheWholeCapture checks the durability boundary:
// a failure before COMMIT must take the payment, the postings, the balance
// changes and the outbox row with it, leaving the key reusable.
func TestCrashBeforeCommitLosesTheWholeCapture(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	h.injector.Set(faults.BeforeCommit, 1.0)
	_, err := h.svc.CreatePayment(ctx, h.request("key-1"))
	if !errors.Is(err, faults.ErrInjected) {
		t.Fatalf("CreatePayment = %v, want an injected failure", err)
	}
	if got := h.balance(h.alice.ID); got != aliceOpeningBalance {
		t.Errorf("alice = %d, want %d", got, aliceOpeningBalance)
	}
	if got := h.pendingOutbox(); got != 0 {
		t.Errorf("pending outbox = %d, want 0", got)
	}
	h.assertInvariants()

	// Recovery: with the fault cleared, the client's retry succeeds once.
	h.injector.Disable()
	res, err := h.svc.CreatePayment(ctx, h.request("key-1"))
	if err != nil {
		t.Fatalf("retry after crash: %v", err)
	}
	if res.Replayed {
		t.Error("retry replayed a capture that never committed")
	}
	if got, want := h.balance(h.alice.ID), aliceOpeningBalance-10_250; got != want {
		t.Errorf("alice = %d, want %d", got, want)
	}
	h.assertInvariants()
}

// TestCrashAfterCommitIsRepairedByTheRetry checks the other side of the same
// boundary: the capture committed but the caller never saw the response, so
// its retry must replay rather than pay twice.
func TestCrashAfterCommitIsRepairedByTheRetry(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	h.injector.Set(faults.AfterCommit, 1.0)
	if _, err := h.svc.CreatePayment(ctx, h.request("key-1")); !errors.Is(err, faults.ErrInjected) {
		t.Fatalf("CreatePayment = %v, want an injected failure", err)
	}
	// The payment is committed even though the caller saw an error.
	charged := aliceOpeningBalance - 10_250
	if got := h.balance(h.alice.ID); got != charged {
		t.Errorf("alice = %d, want %d", got, charged)
	}

	h.injector.Disable()
	res, err := h.svc.CreatePayment(ctx, h.request("key-1"))
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if !res.Replayed {
		t.Fatal("retry after a lost response created a second payment")
	}
	if got := h.balance(h.alice.ID); got != charged {
		t.Errorf("alice = %d after retry, want %d: she was charged twice", got, charged)
	}
	if got := h.pendingOutbox(); got != 1 {
		t.Errorf("pending outbox = %d, want 1", got)
	}
	h.assertInvariants()
}

func TestHandleCapturedSettlesOnceAndIsIdempotent(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	res, err := h.svc.CreatePayment(ctx, h.request("key-1"))
	if err != nil {
		t.Fatalf("CreatePayment: %v", err)
	}
	leased, err := h.store.LeaseOutbox(ctx, "test", 10, time.Minute)
	if err != nil || len(leased) != 1 {
		t.Fatalf("LeaseOutbox = %d messages, %v", len(leased), err)
	}
	msg := events.Message{
		ID:      leased[0].ID.String(),
		Key:     leased[0].AggregateID.String(),
		Type:    leased[0].Type,
		Payload: leased[0].Payload,
	}

	settle := func() error {
		return h.store.Tx(ctx, func(ctx context.Context, tx storage.Tx) error {
			return h.svc.HandleCaptured(ctx, tx, msg)
		})
	}
	if err := settle(); err != nil {
		t.Fatalf("HandleCaptured: %v", err)
	}
	if got := h.balance(h.bob.ID); got != 10_000 {
		t.Errorf("bob = %d, want 10000", got)
	}
	if got := h.balance(h.clearing.ID); got != 0 {
		t.Errorf("clearing = %d, want 0", got)
	}
	payment, err := h.svc.GetPayment(ctx, res.Payment.ID)
	if err != nil {
		t.Fatalf("GetPayment: %v", err)
	}
	if payment.Status != storage.StatusSettled {
		t.Errorf("status = %q, want settled", payment.Status)
	}

	// A redelivery that slipped past the dedupe table must still be a no-op,
	// because the status guard refuses to settle a settled payment.
	if err := settle(); err != nil {
		t.Fatalf("redelivered HandleCaptured: %v", err)
	}
	if got := h.balance(h.bob.ID); got != 10_000 {
		t.Errorf("bob = %d after redelivery, want 10000", got)
	}
	if got := h.metrics.CounterValue(payments.MetricSettlements, metrics.Labels{"outcome": "already_final"}); got != 1 {
		t.Errorf("already_final counter = %d, want 1", got)
	}
	h.assertInvariants()
}

func TestHandleCapturedRejectsForeignEvents(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	err := h.store.Tx(ctx, func(ctx context.Context, tx storage.Tx) error {
		return h.svc.HandleCaptured(ctx, tx, events.Message{Type: "something.else"})
	})
	if !errors.Is(err, payments.ErrUnknownEventType) {
		t.Fatalf("HandleCaptured = %v, want ErrUnknownEventType", err)
	}
}

// TestManyPaymentsKeepTheBooksBalanced runs a storm of concurrent payments
// between several accounts and checks that the ledger still sums to zero.
func TestManyPaymentsKeepTheBooksBalanced(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	const workers, each = 8, 50

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < each; i++ {
				req := h.request(fmt.Sprintf("key-%d-%d", w, i))
				req.Amount = money.Minor(100 + i)
				req.Fee = money.Minor(i % 7)
				if _, err := h.svc.CreatePayment(ctx, req); err != nil {
					t.Errorf("worker %d payment %d: %v", w, i, err)
					return
				}
				// Every other payment is retried, the way a flaky client
				// would retry it.
				if i%2 == 0 {
					if _, err := h.svc.CreatePayment(ctx, req); err != nil {
						t.Errorf("worker %d retry %d: %v", w, i, err)
						return
					}
				}
			}
		}(w)
	}
	wg.Wait()

	if got := h.pendingOutbox(); got != workers*each {
		t.Errorf("pending outbox = %d, want %d: retries enqueued extra events", got, workers*each)
	}
	h.assertInvariants()
}

func TestDepositIsIdempotentAndConservesValue(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	req := payments.DepositRequest{
		IdempotencyKey: "deposit-1",
		Account:        h.bob.ID,
		Amount:         50_000,
		Currency:       "USD",
		Reference:      "card-auth-77",
	}
	first, err := h.svc.Deposit(ctx, req)
	if err != nil {
		t.Fatalf("Deposit: %v", err)
	}
	if first.Replayed || first.StatusCode != 201 {
		t.Errorf("first deposit = replayed %v, status %d", first.Replayed, first.StatusCode)
	}
	if first.Payment.ID != uuid.Nil {
		t.Error("a deposit produced a payment row")
	}
	if got := h.balance(h.bob.ID); got != 50_000 {
		t.Errorf("bob = %d, want 50000", got)
	}
	// Value came from the external account, so the books still balance.
	if got := h.balance(h.world.ID); got != -(aliceOpeningBalance + 50_000) {
		t.Errorf("world = %d, want %d", got, -(aliceOpeningBalance + 50_000))
	}

	for i := 0; i < 3; i++ {
		again, err := h.svc.Deposit(ctx, req)
		if err != nil {
			t.Fatalf("replay %d: %v", i, err)
		}
		if !again.Replayed {
			t.Fatalf("replay %d was not reported as a replay", i)
		}
		if string(again.Response) != string(first.Response) {
			t.Fatalf("replay %d returned different bytes", i)
		}
	}
	if got := h.balance(h.bob.ID); got != 50_000 {
		t.Errorf("bob = %d after replays, want 50000", got)
	}

	// Reusing the key for a different amount is refused rather than
	// silently returning the first receipt.
	changed := req
	changed.Amount = 1
	if _, err := h.svc.Deposit(ctx, changed); !errors.Is(err, payments.ErrIdempotencyKeyReused) {
		t.Fatalf("Deposit = %v, want ErrIdempotencyKeyReused", err)
	}
	h.assertInvariants()
}

func TestDepositValidation(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	base := payments.DepositRequest{IdempotencyKey: "d", Account: h.bob.ID, Amount: 10, Currency: "USD"}
	for _, tc := range []struct {
		name string
		mut  func(*payments.DepositRequest)
		want error
	}{
		{"no key", func(r *payments.DepositRequest) { r.IdempotencyKey = "" }, payments.ErrIdempotencyKeyRequired},
		{"no account", func(r *payments.DepositRequest) { r.Account = uuid.Nil }, payments.ErrInvalidRequest},
		{"zero amount", func(r *payments.DepositRequest) { r.Amount = 0 }, payments.ErrInvalidRequest},
		{"bad currency", func(r *payments.DepositRequest) { r.Currency = "US" }, payments.ErrInvalidRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := base
			req.IdempotencyKey = "d-" + tc.name
			tc.mut(&req)
			if _, err := h.svc.Deposit(ctx, req); !errors.Is(err, tc.want) {
				t.Fatalf("Deposit = %v, want %v", err, tc.want)
			}
		})
	}
	h.assertInvariants()
}
