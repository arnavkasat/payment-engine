// Package storage defines the persistence contract the payment engine is
// written against. Two implementations satisfy it — PostgreSQL for real
// deployments and an in-memory store for tests and local chaos runs — and
// both are held to the same behaviour by the suite in storagetest.
//
// The contract is deliberately narrow and transaction-shaped. Everything that
// must happen atomically for a payment (claiming the idempotency key, writing
// the journal entry, moving the balances, enqueuing the outbox message) is
// reachable from a single Tx, because splitting any of it across two
// transactions is exactly how ledgers lose money.
package storage

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"

	"github.com/arnavkasat/payment-engine/internal/ledger"
	"github.com/arnavkasat/payment-engine/internal/money"
)

// Sentinel errors. Callers branch on these, so implementations must map their
// driver-specific errors onto them rather than leaking pgconn types upward.
var (
	// ErrNotFound means the requested row does not exist.
	ErrNotFound = errors.New("storage: not found")
	// ErrAlreadyExists means a uniqueness constraint rejected an insert.
	ErrAlreadyExists = errors.New("storage: already exists")
	// ErrInsufficientFunds means a posting would overdraw an account that
	// does not allow a negative balance. The transaction is rolled back.
	ErrInsufficientFunds = errors.New("storage: insufficient funds")
	// ErrCurrencyMismatch means a posting's currency differs from its
	// account's. Cross-currency movement needs an explicit FX entry.
	ErrCurrencyMismatch = errors.New("storage: currency mismatch")
	// ErrStatusConflict means a payment was not in the expected status, so
	// a state transition was refused.
	ErrStatusConflict = errors.New("storage: payment status conflict")
	// ErrRetryExhausted means a serialisable transaction kept conflicting.
	ErrRetryExhausted = errors.New("storage: serialisation retries exhausted")
)

// AccountKind classifies an account for reporting and overdraft policy.
type AccountKind string

const (
	// KindWallet is customer money. Wallets may not go negative.
	KindWallet AccountKind = "wallet"
	// KindClearing holds captured funds between capture and settlement.
	KindClearing AccountKind = "clearing"
	// KindRevenue accumulates fees.
	KindRevenue AccountKind = "revenue"
	// KindExternal represents the world outside the ledger — a bank rail or
	// a card network. It is the only kind that may be arbitrarily negative,
	// because it is the counterweight that lets money enter the system.
	KindExternal AccountKind = "external"
)

// Account is a ledger account and its materialised balance. The balance is a
// cache of the sum of the account's postings; VerifyInvariants proves it.
type Account struct {
	ID            uuid.UUID      `json:"id"`
	Name          string         `json:"name"`
	Currency      money.Currency `json:"currency"`
	Kind          AccountKind    `json:"kind"`
	AllowNegative bool           `json:"allow_negative"`
	Balance       money.Minor    `json:"balance"`
	CreatedAt     time.Time      `json:"created_at"`
}

// PaymentStatus is the lifecycle of a payment.
type PaymentStatus string

const (
	// StatusCaptured means funds have left the payer and sit in clearing.
	StatusCaptured PaymentStatus = "captured"
	// StatusSettled means the settlement consumer moved them to the payee.
	StatusSettled PaymentStatus = "settled"
	// StatusFailed means the payment was reversed.
	StatusFailed PaymentStatus = "failed"
)

// Payment is a single transfer request and its current state.
type Payment struct {
	ID             uuid.UUID      `json:"id"`
	IdempotencyKey string         `json:"idempotency_key"`
	PayerAccount   uuid.UUID      `json:"payer_account"`
	PayeeAccount   uuid.UUID      `json:"payee_account"`
	Amount         money.Minor    `json:"amount"`
	Fee            money.Minor    `json:"fee"`
	Currency       money.Currency `json:"currency"`
	Status         PaymentStatus  `json:"status"`
	Reference      string         `json:"reference,omitempty"`
	CreatedAt      time.Time      `json:"created_at"`
	UpdatedAt      time.Time      `json:"updated_at"`
}

// IdempotencyState tracks whether a key's request is still running.
type IdempotencyState string

const (
	// StateInFlight means some request holds the key and has not finished.
	StateInFlight IdempotencyState = "in_flight"
	// StateCompleted means the response is stored and replayable forever.
	StateCompleted IdempotencyState = "completed"
)

// IdempotencyRecord is a claimed idempotency key. Fingerprint is a hash of
// the canonical request, so reusing a key with a different body is caught
// instead of silently returning someone else's payment.
type IdempotencyRecord struct {
	Key         string           `json:"key"`
	Fingerprint string           `json:"fingerprint"`
	State       IdempotencyState `json:"state"`
	PaymentID   uuid.UUID        `json:"payment_id"`
	StatusCode  int              `json:"status_code"`
	Response    []byte           `json:"response"`
	CreatedAt   time.Time        `json:"created_at"`
	CompletedAt *time.Time       `json:"completed_at,omitempty"`
}

// OutboxMessage is an event queued for publication in the same transaction
// as the state change that produced it. ID is carried to the broker as the
// message identifier and is what consumers deduplicate on.
type OutboxMessage struct {
	ID          uuid.UUID  `json:"id"`
	AggregateID uuid.UUID  `json:"aggregate_id"`
	Type        string     `json:"type"`
	Payload     []byte     `json:"payload"`
	Attempts    int        `json:"attempts"`
	CreatedAt   time.Time  `json:"created_at"`
	PublishedAt *time.Time `json:"published_at,omitempty"`
	LeasedUntil *time.Time `json:"leased_until,omitempty"`
	LeaseOwner  string     `json:"lease_owner,omitempty"`
	LastError   string     `json:"last_error,omitempty"`
}

// Store is the durable home of accounts, the journal, payments and the outbox.
type Store interface {
	// Tx runs fn in a serialisable transaction, retrying it on serialisation
	// failure. fn must be free of side effects outside the Tx, because it
	// can be called more than once.
	Tx(ctx context.Context, fn func(context.Context, Tx) error) error

	// CreateAccount inserts an account, returning ErrAlreadyExists if the
	// name is taken.
	CreateAccount(ctx context.Context, a Account) (Account, error)
	// GetAccount reads an account by id.
	GetAccount(ctx context.Context, id uuid.UUID) (Account, error)
	// GetAccountByName reads an account by its unique name.
	GetAccountByName(ctx context.Context, name string) (Account, error)
	// ListAccounts returns every account, ordered by name.
	ListAccounts(ctx context.Context) ([]Account, error)

	// GetPayment reads a payment by id.
	GetPayment(ctx context.Context, id uuid.UUID) (Payment, error)
	// GetEntries returns the journal entries written for a payment.
	GetEntries(ctx context.Context, paymentID uuid.UUID) ([]ledger.Entry, error)

	// LeaseOutbox claims up to limit unpublished messages for the duration
	// of lease, skipping rows another worker holds. A worker that dies
	// without publishing loses its claim when the lease expires.
	LeaseOutbox(ctx context.Context, owner string, limit int, lease time.Duration) ([]OutboxMessage, error)
	// MarkOutboxPublished retires messages the broker has accepted.
	MarkOutboxPublished(ctx context.Context, ids []uuid.UUID) error
	// ReleaseOutbox returns a message to the queue after a failed publish,
	// recording the cause.
	ReleaseOutbox(ctx context.Context, id uuid.UUID, cause string) error
	// PendingOutbox counts messages still awaiting publication. Chaos runs
	// use it to decide when the pipeline has drained.
	PendingOutbox(ctx context.Context) (int, error)

	// VerifyInvariants checks every ledger invariant and reports violations.
	VerifyInvariants(ctx context.Context) (ledger.Report, error)

	// Ping reports whether the store is reachable.
	Ping(ctx context.Context) error
	// Close releases resources.
	Close() error
}

// Tx is the transactional surface. Every method participates in the enclosing
// transaction; nothing it writes is visible until that transaction commits.
type Tx interface {
	// ClaimIdempotencyKey atomically claims key for a request with the given
	// fingerprint. claimed is true when this caller won the race and must do
	// the work. When claimed is false the returned record is the existing
	// one: its State says whether another request is still in flight, and
	// its Fingerprint says whether the key is being reused for a different
	// request body.
	ClaimIdempotencyKey(ctx context.Context, key, fingerprint string) (rec IdempotencyRecord, claimed bool, err error)
	// CompleteIdempotencyKey stores the response so later retries replay it.
	CompleteIdempotencyKey(ctx context.Context, key string, paymentID uuid.UUID, statusCode int, response []byte) error

	// GetAccounts reads and locks the named accounts for update, in a
	// deterministic order so that concurrent transfers cannot deadlock.
	GetAccounts(ctx context.Context, ids []uuid.UUID) (map[uuid.UUID]Account, error)

	// ApplyEntry writes a journal entry with its postings and moves the
	// affected balances in the same statement sequence, so a balance can
	// never drift from the journal. It enforces currency agreement and the
	// overdraft policy, returning ErrCurrencyMismatch or
	// ErrInsufficientFunds and leaving the transaction to be rolled back.
	ApplyEntry(ctx context.Context, e ledger.Entry) error

	// InsertPayment writes a new payment row.
	InsertPayment(ctx context.Context, p Payment) error
	// GetPayment reads a payment inside the transaction.
	GetPayment(ctx context.Context, id uuid.UUID) (Payment, error)
	// TransitionPayment moves a payment from one status to another,
	// returning ErrStatusConflict if it was not in the expected status.
	TransitionPayment(ctx context.Context, id uuid.UUID, from, to PaymentStatus) error

	// Enqueue appends messages to the outbox.
	Enqueue(ctx context.Context, msgs ...OutboxMessage) error

	// MarkProcessed records that a consumer group has handled a message.
	// first is true only for the first caller; a redelivery sees false and
	// must skip its side effects. Because the row is written in the same
	// transaction as those side effects, the pair commits or rolls back
	// together, which is what turns at-least-once delivery into
	// exactly-once effect.
	MarkProcessed(ctx context.Context, consumer, messageID string) (first bool, err error)
}
