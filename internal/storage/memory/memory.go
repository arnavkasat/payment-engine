// Package memory is an in-memory implementation of storage.Store. It exists
// so that unit tests and local chaos runs need no database, and it is held to
// the same behaviour as the PostgreSQL store by the storagetest suite.
//
// Isolation is achieved the blunt way: one mutex serialises every
// transaction, and a transaction that returns an error has its writes undone
// by restoring a snapshot taken at begin. Serial execution is trivially
// serialisable, so the semantics match PostgreSQL's SERIALIZABLE without any
// of the conflict detection.
package memory

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/arnavkasat/payment-engine/internal/ledger"
	"github.com/arnavkasat/payment-engine/internal/money"
	"github.com/arnavkasat/payment-engine/internal/storage"
)

// state is the whole database. It is replaced wholesale on rollback.
type state struct {
	accounts    map[uuid.UUID]storage.Account
	accountName map[string]uuid.UUID
	payments    map[uuid.UUID]storage.Payment
	entries     map[uuid.UUID]ledger.Entry
	entryOrder  []uuid.UUID
	idempotency map[string]storage.IdempotencyRecord
	outbox      map[uuid.UUID]storage.OutboxMessage
	outboxOrder []uuid.UUID
	processed   map[string]bool
}

func newState() *state {
	return &state{
		accounts:    map[uuid.UUID]storage.Account{},
		accountName: map[string]uuid.UUID{},
		payments:    map[uuid.UUID]storage.Payment{},
		entries:     map[uuid.UUID]ledger.Entry{},
		idempotency: map[string]storage.IdempotencyRecord{},
		outbox:      map[uuid.UUID]storage.OutboxMessage{},
		processed:   map[string]bool{},
	}
}

// clone deep-copies the state, including every slice reachable from it, so a
// rollback cannot be defeated by an aliased backing array.
func (s *state) clone() *state {
	c := newState()
	for k, v := range s.accounts {
		c.accounts[k] = v
	}
	for k, v := range s.accountName {
		c.accountName[k] = v
	}
	for k, v := range s.payments {
		c.payments[k] = v
	}
	for k, v := range s.entries {
		e := v
		e.Postings = append([]ledger.Posting(nil), v.Postings...)
		c.entries[k] = e
	}
	c.entryOrder = append([]uuid.UUID(nil), s.entryOrder...)
	for k, v := range s.idempotency {
		r := v
		r.Response = append([]byte(nil), v.Response...)
		if v.CompletedAt != nil {
			t := *v.CompletedAt
			r.CompletedAt = &t
		}
		c.idempotency[k] = r
	}
	for k, v := range s.outbox {
		c.outbox[k] = copyOutbox(v)
	}
	c.outboxOrder = append([]uuid.UUID(nil), s.outboxOrder...)
	for k, v := range s.processed {
		c.processed[k] = v
	}
	return c
}

func copyOutbox(m storage.OutboxMessage) storage.OutboxMessage {
	out := m
	out.Payload = append([]byte(nil), m.Payload...)
	if m.PublishedAt != nil {
		t := *m.PublishedAt
		out.PublishedAt = &t
	}
	if m.LeasedUntil != nil {
		t := *m.LeasedUntil
		out.LeasedUntil = &t
	}
	return out
}

// Store is an in-memory storage.Store.
type Store struct {
	mu  sync.Mutex
	st  *state
	now func() time.Time
}

// New returns an empty in-memory store.
func New() *Store {
	return &Store{st: newState(), now: func() time.Time { return time.Now().UTC() }}
}

// SetClock overrides the clock, which makes lease expiry testable without
// sleeping.
func (s *Store) SetClock(now func() time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.now = now
}

var _ storage.Store = (*Store)(nil)

// Tx runs fn against the store under an exclusive lock, discarding every
// write fn made if it returns an error.
func (s *Store) Tx(ctx context.Context, fn func(context.Context, storage.Tx) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	snapshot := s.st.clone()
	if err := fn(ctx, &tx{st: s.st, now: s.now}); err != nil {
		s.st = snapshot
		return err
	}
	return nil
}

// CreateAccount inserts an account.
func (s *Store) CreateAccount(ctx context.Context, a storage.Account) (storage.Account, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.st.accountName[a.Name]; ok {
		return storage.Account{}, fmt.Errorf("%w: account %q", storage.ErrAlreadyExists, a.Name)
	}
	if a.ID == uuid.Nil {
		a.ID = uuid.New()
	}
	if a.CreatedAt.IsZero() {
		a.CreatedAt = s.now()
	}
	s.st.accounts[a.ID] = a
	s.st.accountName[a.Name] = a.ID
	return a, nil
}

// GetAccount reads an account by id.
func (s *Store) GetAccount(ctx context.Context, id uuid.UUID) (storage.Account, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, ok := s.st.accounts[id]
	if !ok {
		return storage.Account{}, fmt.Errorf("%w: account %s", storage.ErrNotFound, id)
	}
	return a, nil
}

// GetAccountByName reads an account by name.
func (s *Store) GetAccountByName(ctx context.Context, name string) (storage.Account, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id, ok := s.st.accountName[name]
	if !ok {
		return storage.Account{}, fmt.Errorf("%w: account %q", storage.ErrNotFound, name)
	}
	return s.st.accounts[id], nil
}

// ListAccounts returns every account ordered by name.
func (s *Store) ListAccounts(ctx context.Context) ([]storage.Account, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]storage.Account, 0, len(s.st.accounts))
	for _, a := range s.st.accounts {
		out = append(out, a)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// GetPayment reads a payment by id.
func (s *Store) GetPayment(ctx context.Context, id uuid.UUID) (storage.Payment, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.st.payments[id]
	if !ok {
		return storage.Payment{}, fmt.Errorf("%w: payment %s", storage.ErrNotFound, id)
	}
	return p, nil
}

// GetEntries returns a payment's journal entries in insertion order.
func (s *Store) GetEntries(ctx context.Context, paymentID uuid.UUID) ([]ledger.Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []ledger.Entry
	for _, id := range s.st.entryOrder {
		if e := s.st.entries[id]; e.PaymentID == paymentID {
			e.Postings = append([]ledger.Posting(nil), e.Postings...)
			out = append(out, e)
		}
	}
	return out, nil
}

// LeaseOutbox claims unpublished messages whose lease has lapsed.
func (s *Store) LeaseOutbox(ctx context.Context, owner string, limit int, lease time.Duration) ([]storage.OutboxMessage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	var out []storage.OutboxMessage
	for _, id := range s.st.outboxOrder {
		if len(out) >= limit {
			break
		}
		m := s.st.outbox[id]
		if m.PublishedAt != nil {
			continue
		}
		if m.LeasedUntil != nil && m.LeasedUntil.After(now) {
			continue
		}
		until := now.Add(lease)
		m.LeasedUntil = &until
		m.LeaseOwner = owner
		m.Attempts++
		s.st.outbox[id] = m
		out = append(out, copyOutbox(m))
	}
	return out, nil
}

// MarkOutboxPublished retires accepted messages.
func (s *Store) MarkOutboxPublished(ctx context.Context, ids []uuid.UUID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	for _, id := range ids {
		m, ok := s.st.outbox[id]
		if !ok {
			return fmt.Errorf("%w: outbox %s", storage.ErrNotFound, id)
		}
		if m.PublishedAt == nil {
			t := now
			m.PublishedAt = &t
		}
		m.LeasedUntil = nil
		m.LeaseOwner = ""
		s.st.outbox[id] = m
	}
	return nil
}

// ReleaseOutbox returns a message to the queue after a failed publish.
func (s *Store) ReleaseOutbox(ctx context.Context, id uuid.UUID, cause string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.st.outbox[id]
	if !ok {
		return fmt.Errorf("%w: outbox %s", storage.ErrNotFound, id)
	}
	m.LeasedUntil = nil
	m.LeaseOwner = ""
	m.LastError = cause
	s.st.outbox[id] = m
	return nil
}

// PendingOutbox counts messages still awaiting publication.
func (s *Store) PendingOutbox(ctx context.Context) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var n int
	for _, m := range s.st.outbox {
		if m.PublishedAt == nil {
			n++
		}
	}
	return n, nil
}

// VerifyInvariants recomputes every ledger invariant from the journal.
func (s *Store) VerifyInvariants(ctx context.Context) (ledger.Report, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	rep := ledger.Report{
		Accounts: len(s.st.accounts),
		Entries:  len(s.st.entries),
		Payments: len(s.st.payments),
	}
	derived := map[uuid.UUID]money.Minor{}
	var total money.Minor
	kinds := map[string]int{}

	for _, id := range s.st.entryOrder {
		e := s.st.entries[id]
		var entrySum money.Minor
		for _, p := range e.Postings {
			rep.Postings++
			sum, err := money.Add(derived[p.AccountID], p.Amount)
			if err != nil {
				return rep, fmt.Errorf("account %s: %w", p.AccountID, err)
			}
			derived[p.AccountID] = sum
			if entrySum, err = money.Add(entrySum, p.Amount); err != nil {
				return rep, fmt.Errorf("entry %s: %w", e.ID, err)
			}
			if total, err = money.Add(total, p.Amount); err != nil {
				return rep, fmt.Errorf("global total: %w", err)
			}
		}
		if entrySum != 0 {
			rep.Add(ledger.RuleEntryZeroSum, "entry %s sums to %d", e.ID, entrySum)
		}
		if e.Kind != ledger.KindReversal {
			kinds[e.PaymentID.String()+"/"+string(e.Kind)]++
		}
	}
	rep.TotalPostingSum = total
	if total != 0 {
		rep.Add(ledger.RuleGlobalZeroSum, "postings sum to %d, want 0", total)
	}
	for key, n := range kinds {
		if n > 1 {
			rep.Add(ledger.RuleSingleEntryPerKind, "%s written %d times", key, n)
		}
	}
	for id, a := range s.st.accounts {
		if a.Balance != derived[id] {
			rep.Add(ledger.RuleBalanceMatchesPostings, "account %s balance %d, postings sum %d", a.Name, a.Balance, derived[id])
		}
		if !a.AllowNegative && a.Balance < 0 {
			rep.Add(ledger.RuleNoUnauthorisedOverdraft, "account %s balance %d", a.Name, a.Balance)
		}
		delete(derived, id)
	}
	for id, sum := range derived {
		rep.Add(ledger.RuleBalanceMatchesPostings, "postings reference unknown account %s (sum %d)", id, sum)
	}
	return rep, nil
}

// Ping always succeeds: the store is the process.
func (s *Store) Ping(ctx context.Context) error { return ctx.Err() }

// Close is a no-op.
func (s *Store) Close() error { return nil }

// Corrupt mutates an account balance without a matching posting. It exists
// only so tests can prove the invariant checker actually detects drift.
func (s *Store) Corrupt(id uuid.UUID, delta money.Minor) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.st.accounts[id]
	a.Balance += delta
	s.st.accounts[id] = a
}
