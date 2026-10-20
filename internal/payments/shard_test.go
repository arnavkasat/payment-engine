package payments_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/arnavkasat/payment-engine/internal/events"
	"github.com/arnavkasat/payment-engine/internal/ledger"
	"github.com/arnavkasat/payment-engine/internal/money"
	"github.com/arnavkasat/payment-engine/internal/payments"
	"github.com/arnavkasat/payment-engine/internal/storage"
	"github.com/arnavkasat/payment-engine/internal/storage/memory"
)

// shardedHarness runs the service with several clearing and fee shards, the
// way a real deployment does.
type shardedHarness struct {
	t        *testing.T
	store    *memory.Store
	svc      *payments.Service
	clearing []uuid.UUID
	fees     []uuid.UUID
	alice    uuid.UUID
	bob      uuid.UUID
}

func newShardedHarness(t *testing.T, shards int) *shardedHarness {
	t.Helper()
	ctx := context.Background()
	store := memory.New()
	h := &shardedHarness{t: t, store: store}

	mk := func(name string, kind storage.AccountKind, allowNegative bool) uuid.UUID {
		a, err := store.CreateAccount(ctx, storage.Account{
			Name: name, Currency: "USD", Kind: kind, AllowNegative: allowNegative,
		})
		if err != nil {
			t.Fatalf("CreateAccount(%q): %v", name, err)
		}
		return a.ID
	}
	world := mk("world", storage.KindExternal, true)
	h.alice = mk("alice", storage.KindWallet, false)
	h.bob = mk("bob", storage.KindWallet, false)
	for i := 0; i < shards; i++ {
		h.clearing = append(h.clearing, mk(fmt.Sprintf("clearing-%02d", i), storage.KindClearing, false))
		h.fees = append(h.fees, mk(fmt.Sprintf("fees-%02d", i), storage.KindRevenue, false))
	}

	svc, err := payments.New(store, payments.Config{
		Clearing: h.clearing, Fees: h.fees, External: world,
	})
	if err != nil {
		t.Fatalf("payments.New: %v", err)
	}
	h.svc = svc

	if _, err := svc.Deposit(ctx, payments.DepositRequest{
		IdempotencyKey: "fund", Account: h.alice, Amount: 100_000_000, Currency: "USD",
	}); err != nil {
		t.Fatalf("funding: %v", err)
	}
	return h
}

func (h *shardedHarness) balance(id uuid.UUID) money.Minor {
	h.t.Helper()
	a, err := h.store.GetAccount(context.Background(), id)
	if err != nil {
		h.t.Fatalf("GetAccount: %v", err)
	}
	return a.Balance
}

func TestNewRejectsAnEmptyClearingSet(t *testing.T) {
	if _, err := payments.New(memory.New(), payments.Config{}); err == nil {
		t.Error("New accepted a config with no clearing account")
	}
	if _, err := payments.New(memory.New(), payments.Config{Clearing: []uuid.UUID{uuid.Nil}}); err == nil {
		t.Error("New accepted the nil uuid as a clearing account")
	}
}

// TestSettlementUsesTheSameShardAsItsCapture is the property sharding must
// not break: funds have to leave the clearing account they went into, and
// the shard is recomputed from the payment id rather than stored, so a bug
// here would silently drain one shard and inflate another.
func TestSettlementUsesTheSameShardAsItsCapture(t *testing.T) {
	h := newShardedHarness(t, 8)
	ctx := context.Background()

	for i := 0; i < 80; i++ {
		res, err := h.svc.CreatePayment(ctx, payments.CreateRequest{
			IdempotencyKey: fmt.Sprintf("key-%d", i),
			PayerAccount:   h.alice, PayeeAccount: h.bob,
			Amount: 1_000, Fee: 10, Currency: "USD",
		})
		if err != nil {
			t.Fatalf("payment %d: %v", i, err)
		}

		// Find the shard the capture actually posted against.
		entries, err := h.svc.GetEntries(ctx, res.Payment.ID)
		if err != nil {
			t.Fatalf("GetEntries: %v", err)
		}
		captured := shardOf(t, entries, ledger.KindCapture, h.clearing)

		leased, err := h.store.LeaseOutbox(ctx, "test", 10, time.Minute)
		if err != nil {
			t.Fatalf("LeaseOutbox: %v", err)
		}
		var msg events.Message
		for _, m := range leased {
			if m.AggregateID == res.Payment.ID && m.Type == events.TypePaymentCaptured {
				msg = events.Message{ID: m.ID.String(), Type: m.Type, Payload: m.Payload}
			}
		}
		if msg.ID == "" {
			t.Fatalf("no capture event for payment %d", i)
		}
		if err := h.store.Tx(ctx, func(ctx context.Context, tx storage.Tx) error {
			return h.svc.HandleCaptured(ctx, tx, msg)
		}); err != nil {
			t.Fatalf("settling payment %d: %v", i, err)
		}

		entries, err = h.svc.GetEntries(ctx, res.Payment.ID)
		if err != nil {
			t.Fatalf("GetEntries: %v", err)
		}
		settled := shardOf(t, entries, ledger.KindSettlement, h.clearing)
		if captured != settled {
			t.Fatalf("payment %d captured into shard %d but settled out of shard %d", i, captured, settled)
		}
	}

	// Every shard nets to zero once everything has settled, which is what
	// proves no shard leaked into another.
	for i, id := range h.clearing {
		if got := h.balance(id); got != 0 {
			t.Errorf("clearing shard %d holds %d after settlement, want 0", i, got)
		}
	}

	rep, err := h.store.VerifyInvariants(ctx)
	if err != nil {
		t.Fatalf("VerifyInvariants: %v", err)
	}
	if !rep.OK() {
		t.Fatalf("invariants violated: %s", rep.Summary())
	}
}

// shardOf returns the index of the clearing account an entry of the given
// kind posted against.
func shardOf(t *testing.T, entries []ledger.Entry, kind ledger.EntryKind, shards []uuid.UUID) int {
	t.Helper()
	index := map[uuid.UUID]int{}
	for i, id := range shards {
		index[id] = i
	}
	for _, e := range entries {
		if e.Kind != kind {
			continue
		}
		for _, p := range e.Postings {
			if i, ok := index[p.AccountID]; ok {
				return i
			}
		}
	}
	t.Fatalf("no %s entry posted against a clearing shard", kind)
	return -1
}

// TestShardingSpreadsTheLoad checks the whole point of sharding: writes must
// not all land on one row.
func TestShardingSpreadsTheLoad(t *testing.T) {
	h := newShardedHarness(t, 8)
	ctx := context.Background()

	for i := 0; i < 400; i++ {
		if _, err := h.svc.CreatePayment(ctx, payments.CreateRequest{
			IdempotencyKey: fmt.Sprintf("key-%d", i),
			PayerAccount:   h.alice, PayeeAccount: h.bob,
			Amount: 100, Fee: 1, Currency: "USD",
		}); err != nil {
			t.Fatalf("payment %d: %v", i, err)
		}
	}

	used := 0
	for i, id := range h.clearing {
		got := h.balance(id)
		if got > 0 {
			used++
		}
		t.Logf("clearing shard %d holds %d", i, got)
	}
	// With 400 payments across 8 shards, every shard should have been hit;
	// anything less means the shard function is degenerate.
	if used != len(h.clearing) {
		t.Errorf("only %d of %d clearing shards were used", used, len(h.clearing))
	}

	rep, err := h.store.VerifyInvariants(ctx)
	if err != nil {
		t.Fatalf("VerifyInvariants: %v", err)
	}
	if !rep.OK() {
		t.Fatalf("invariants violated: %s", rep.Summary())
	}
}

// TestSingleShardStillWorks keeps the simplest configuration honest.
func TestSingleShardStillWorks(t *testing.T) {
	h := newShardedHarness(t, 1)
	ctx := context.Background()
	res, err := h.svc.CreatePayment(ctx, payments.CreateRequest{
		IdempotencyKey: "key-1", PayerAccount: h.alice, PayeeAccount: h.bob,
		Amount: 500, Fee: 5, Currency: "USD",
	})
	if err != nil {
		t.Fatalf("CreatePayment: %v", err)
	}
	if got := h.balance(h.clearing[0]); got != 500 {
		t.Errorf("clearing = %d, want 500", got)
	}
	if got := h.balance(h.fees[0]); got != 5 {
		t.Errorf("fees = %d, want 5", got)
	}
	_ = res
}
