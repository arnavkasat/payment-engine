package memory

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/arnavkasat/payment-engine/internal/ledger"
	"github.com/arnavkasat/payment-engine/internal/money"
	"github.com/arnavkasat/payment-engine/internal/storage"
)

// tx is a transaction against the in-memory state. The enclosing Store holds
// its mutex for the whole life of a tx, so nothing here needs locking.
type tx struct {
	st  *state
	now func() time.Time
}

var _ storage.Tx = (*tx)(nil)

// ClaimIdempotencyKey inserts the key if it is free, and otherwise returns
// the record that already holds it.
func (t *tx) ClaimIdempotencyKey(ctx context.Context, key, fingerprint string) (storage.IdempotencyRecord, bool, error) {
	if existing, ok := t.st.idempotency[key]; ok {
		return existing, false, nil
	}
	rec := storage.IdempotencyRecord{
		Key:         key,
		Fingerprint: fingerprint,
		State:       storage.StateInFlight,
		CreatedAt:   t.now(),
	}
	t.st.idempotency[key] = rec
	return rec, true, nil
}

// CompleteIdempotencyKey stores the response for future replays.
func (t *tx) CompleteIdempotencyKey(ctx context.Context, key string, paymentID uuid.UUID, statusCode int, response []byte) error {
	rec, ok := t.st.idempotency[key]
	if !ok {
		return fmt.Errorf("%w: idempotency key %q", storage.ErrNotFound, key)
	}
	completed := t.now()
	rec.State = storage.StateCompleted
	rec.PaymentID = paymentID
	rec.StatusCode = statusCode
	rec.Response = append([]byte(nil), response...)
	rec.CompletedAt = &completed
	t.st.idempotency[key] = rec
	return nil
}

// GetAccounts reads the named accounts. Locking is implicit here; the sort
// mirrors the deterministic lock order the PostgreSQL store relies on.
func (t *tx) GetAccounts(ctx context.Context, ids []uuid.UUID) (map[uuid.UUID]storage.Account, error) {
	ordered := append([]uuid.UUID(nil), ids...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].String() < ordered[j].String() })
	out := make(map[uuid.UUID]storage.Account, len(ordered))
	for _, id := range ordered {
		a, ok := t.st.accounts[id]
		if !ok {
			return nil, fmt.Errorf("%w: account %s", storage.ErrNotFound, id)
		}
		out[id] = a
	}
	return out, nil
}

// ApplyEntry writes the entry and moves the balances it implies.
func (t *tx) ApplyEntry(ctx context.Context, e ledger.Entry) error {
	if err := e.Validate(); err != nil {
		return err
	}
	if _, exists := t.st.entries[e.ID]; exists {
		return fmt.Errorf("%w: entry %s", storage.ErrAlreadyExists, e.ID)
	}
	accounts, err := t.GetAccounts(ctx, e.Accounts())
	if err != nil {
		return err
	}
	// Apply in a deterministic order so that a rejection mid-way leaves the
	// same partial state every time, which keeps chaos runs reproducible.
	ids := make([]uuid.UUID, 0, len(accounts))
	for id := range accounts {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i].String() < ids[j].String() })

	deltas := e.Deltas()
	for _, id := range ids {
		a := accounts[id]
		if a.Currency != e.Currency {
			return fmt.Errorf("%w: account %s holds %s, entry is %s", storage.ErrCurrencyMismatch, a.Name, a.Currency, e.Currency)
		}
		updated, err := money.Add(a.Balance, deltas[id])
		if err != nil {
			return fmt.Errorf("account %s: %w", a.Name, err)
		}
		if updated < 0 && !a.AllowNegative {
			return fmt.Errorf("%w: account %s has %d, needs %d", storage.ErrInsufficientFunds, a.Name, a.Balance, -deltas[id])
		}
		a.Balance = updated
		t.st.accounts[id] = a
	}

	stored := e
	stored.Postings = append([]ledger.Posting(nil), e.Postings...)
	t.st.entries[e.ID] = stored
	t.st.entryOrder = append(t.st.entryOrder, e.ID)
	return nil
}

// InsertPayment writes a new payment.
func (t *tx) InsertPayment(ctx context.Context, p storage.Payment) error {
	if _, exists := t.st.payments[p.ID]; exists {
		return fmt.Errorf("%w: payment %s", storage.ErrAlreadyExists, p.ID)
	}
	t.st.payments[p.ID] = p
	return nil
}

// GetPayment reads a payment inside the transaction.
func (t *tx) GetPayment(ctx context.Context, id uuid.UUID) (storage.Payment, error) {
	p, ok := t.st.payments[id]
	if !ok {
		return storage.Payment{}, fmt.Errorf("%w: payment %s", storage.ErrNotFound, id)
	}
	return p, nil
}

// TransitionPayment moves a payment between statuses, refusing the move if
// the payment is not in the expected one.
func (t *tx) TransitionPayment(ctx context.Context, id uuid.UUID, from, to storage.PaymentStatus) error {
	p, ok := t.st.payments[id]
	if !ok {
		return fmt.Errorf("%w: payment %s", storage.ErrNotFound, id)
	}
	if p.Status != from {
		return fmt.Errorf("%w: payment %s is %q, expected %q", storage.ErrStatusConflict, id, p.Status, from)
	}
	p.Status = to
	p.UpdatedAt = t.now()
	t.st.payments[id] = p
	return nil
}

// Enqueue appends messages to the outbox.
func (t *tx) Enqueue(ctx context.Context, msgs ...storage.OutboxMessage) error {
	for _, m := range msgs {
		if m.ID == uuid.Nil {
			m.ID = uuid.New()
		}
		if _, exists := t.st.outbox[m.ID]; exists {
			return fmt.Errorf("%w: outbox %s", storage.ErrAlreadyExists, m.ID)
		}
		if m.CreatedAt.IsZero() {
			m.CreatedAt = t.now()
		}
		t.st.outbox[m.ID] = copyOutbox(m)
		t.st.outboxOrder = append(t.st.outboxOrder, m.ID)
	}
	return nil
}

// MarkProcessed records a consumer's handling of a message exactly once.
func (t *tx) MarkProcessed(ctx context.Context, consumer, messageID string) (bool, error) {
	key := consumer + "\x00" + messageID
	if t.st.processed[key] {
		return false, nil
	}
	t.st.processed[key] = true
	return true, nil
}
