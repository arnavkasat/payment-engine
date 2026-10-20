// Package ledger holds the double-entry domain: journal entries, their
// postings, and the conservation rule that every entry sums to zero.
//
// Sign convention: a posting's Amount is the signed delta applied to an
// account's balance. Money leaving an account is negative, money arriving is
// positive, and because an entry's postings must sum to exactly zero, no
// sequence of entries can create or destroy value. In classical terms a
// positive delta is a debit to an asset account and a negative delta is a
// credit to it; the ledger stores the delta rather than a direction flag so
// that the invariant is a plain sum the database can check.
package ledger

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/arnavkasat/payment-engine/internal/money"
)

// EntryKind records why an entry was written. Kinds are not interchangeable:
// a payment has at most one capture and at most one settlement.
type EntryKind string

const (
	// KindCapture moves funds out of the payer and into the clearing account
	// where they are held until settlement.
	KindCapture EntryKind = "capture"
	// KindSettlement moves funds out of clearing and into the payee. It is
	// written by the Kafka consumer, in the same transaction as the dedupe row.
	KindSettlement EntryKind = "settlement"
	// KindReversal unwinds a prior entry, posting its exact negation.
	KindReversal EntryKind = "reversal"
	// KindDeposit moves funds in from an external counterparty — a bank
	// rail or a card network. It is the only way value enters the ledger,
	// and it still sums to zero: the external account goes negative by
	// exactly what the wallet gains.
	KindDeposit EntryKind = "deposit"
)

// Posting is one leg of a journal entry: a signed delta against one account.
type Posting struct {
	ID        uuid.UUID   `json:"id"`
	EntryID   uuid.UUID   `json:"entry_id"`
	AccountID uuid.UUID   `json:"account_id"`
	Amount    money.Minor `json:"amount"`
}

// Entry is a journal entry: an atomic, zero-sum group of postings.
type Entry struct {
	ID        uuid.UUID      `json:"id"`
	PaymentID uuid.UUID      `json:"payment_id"`
	Kind      EntryKind      `json:"kind"`
	Currency  money.Currency `json:"currency"`
	Postings  []Posting      `json:"postings"`
	CreatedAt time.Time      `json:"created_at"`
}

// Validation failures. Each one describes a way an entry could have broken
// conservation, and none of them is recoverable: the caller has a bug.
var (
	ErrNotBalanced     = errors.New("ledger: entry postings do not sum to zero")
	ErrTooFewPostings  = errors.New("ledger: entry needs at least two postings")
	ErrZeroPosting     = errors.New("ledger: posting amount is zero")
	ErrDuplicateLeg    = errors.New("ledger: account appears twice in one entry")
	ErrMissingAccount  = errors.New("ledger: posting has no account")
	ErrMissingCurrency = errors.New("ledger: entry has no currency")
)

// Validate enforces the rules every entry must satisfy before it is written.
// It is called by the service and again by each store implementation, because
// the invariant is cheap to check and expensive to discover violated.
func (e Entry) Validate() error {
	if e.Currency == "" {
		return ErrMissingCurrency
	}
	if len(e.Postings) < 2 {
		return fmt.Errorf("%w: got %d", ErrTooFewPostings, len(e.Postings))
	}
	seen := make(map[uuid.UUID]struct{}, len(e.Postings))
	amounts := make([]money.Minor, 0, len(e.Postings))
	for _, p := range e.Postings {
		if p.AccountID == uuid.Nil {
			return ErrMissingAccount
		}
		if p.Amount == 0 {
			return fmt.Errorf("%w: account %s", ErrZeroPosting, p.AccountID)
		}
		if _, dup := seen[p.AccountID]; dup {
			return fmt.Errorf("%w: account %s", ErrDuplicateLeg, p.AccountID)
		}
		seen[p.AccountID] = struct{}{}
		amounts = append(amounts, p.Amount)
	}
	sum, err := money.SumAll(amounts...)
	if err != nil {
		return fmt.Errorf("ledger: summing postings: %w", err)
	}
	if sum != 0 {
		return fmt.Errorf("%w: sum=%d", ErrNotBalanced, sum)
	}
	return nil
}

// Deltas returns the balance change each account takes from this entry.
// Validate guarantees one posting per account, so the map is lossless.
func (e Entry) Deltas() map[uuid.UUID]money.Minor {
	d := make(map[uuid.UUID]money.Minor, len(e.Postings))
	for _, p := range e.Postings {
		d[p.AccountID] = p.Amount
	}
	return d
}

// Accounts returns every account this entry touches.
func (e Entry) Accounts() []uuid.UUID {
	ids := make([]uuid.UUID, 0, len(e.Postings))
	for _, p := range e.Postings {
		ids = append(ids, p.AccountID)
	}
	return ids
}

// build stamps identifiers onto a freshly constructed entry and validates it,
// so no caller can hand a malformed entry to a store.
func build(e Entry, now time.Time) (Entry, error) {
	e.ID = uuid.New()
	e.CreatedAt = now.UTC()
	for i := range e.Postings {
		e.Postings[i].ID = uuid.New()
		e.Postings[i].EntryID = e.ID
	}
	if err := e.Validate(); err != nil {
		return Entry{}, err
	}
	return e, nil
}

// NewCapture builds the capture entry for a payment: the payer is debited the
// amount plus the fee, the clearing account receives the amount, and the fee
// account receives the fee. A zero fee produces a two-legged entry.
func NewCapture(paymentID, payer, clearing, feeAccount uuid.UUID, cur money.Currency, amount, fee money.Minor, now time.Time) (Entry, error) {
	if amount <= 0 {
		return Entry{}, fmt.Errorf("ledger: capture amount must be positive, got %d", amount)
	}
	if fee < 0 {
		return Entry{}, fmt.Errorf("ledger: capture fee must not be negative, got %d", fee)
	}
	debit, err := money.Add(amount, fee)
	if err != nil {
		return Entry{}, fmt.Errorf("ledger: capture total: %w", err)
	}
	neg, err := money.Neg(debit)
	if err != nil {
		return Entry{}, fmt.Errorf("ledger: capture total: %w", err)
	}
	postings := []Posting{
		{AccountID: payer, Amount: neg},
		{AccountID: clearing, Amount: amount},
	}
	if fee > 0 {
		if feeAccount == uuid.Nil {
			return Entry{}, fmt.Errorf("ledger: fee of %d needs a fee account", fee)
		}
		postings = append(postings, Posting{AccountID: feeAccount, Amount: fee})
	}
	return build(Entry{PaymentID: paymentID, Kind: KindCapture, Currency: cur, Postings: postings}, now)
}

// NewSettlement builds the settlement entry: funds leave clearing and arrive
// at the payee. It is written when the capture event is consumed.
func NewSettlement(paymentID, clearing, payee uuid.UUID, cur money.Currency, amount money.Minor, now time.Time) (Entry, error) {
	if amount <= 0 {
		return Entry{}, fmt.Errorf("ledger: settlement amount must be positive, got %d", amount)
	}
	neg, err := money.Neg(amount)
	if err != nil {
		return Entry{}, fmt.Errorf("ledger: settlement amount: %w", err)
	}
	return build(Entry{
		PaymentID: paymentID,
		Kind:      KindSettlement,
		Currency:  cur,
		Postings: []Posting{
			{AccountID: clearing, Amount: neg},
			{AccountID: payee, Amount: amount},
		},
	}, now)
}

// NewDeposit moves funds from an external account into a wallet.
func NewDeposit(correlationID, external, dest uuid.UUID, cur money.Currency, amount money.Minor, now time.Time) (Entry, error) {
	if amount <= 0 {
		return Entry{}, fmt.Errorf("ledger: deposit amount must be positive, got %d", amount)
	}
	neg, err := money.Neg(amount)
	if err != nil {
		return Entry{}, fmt.Errorf("ledger: deposit amount: %w", err)
	}
	return build(Entry{
		PaymentID: correlationID,
		Kind:      KindDeposit,
		Currency:  cur,
		Postings: []Posting{
			{AccountID: external, Amount: neg},
			{AccountID: dest, Amount: amount},
		},
	}, now)
}

// NewReversal posts the exact negation of src, unwinding it.
func NewReversal(src Entry, now time.Time) (Entry, error) {
	postings := make([]Posting, 0, len(src.Postings))
	for _, p := range src.Postings {
		neg, err := money.Neg(p.Amount)
		if err != nil {
			return Entry{}, fmt.Errorf("ledger: reversing posting %s: %w", p.ID, err)
		}
		postings = append(postings, Posting{AccountID: p.AccountID, Amount: neg})
	}
	return build(Entry{PaymentID: src.PaymentID, Kind: KindReversal, Currency: src.Currency, Postings: postings}, now)
}
