// Package payments is the engine: it turns a transfer request into a
// zero-sum journal entry, an idempotency record and an outbox message, all
// committed together or not at all.
package payments

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/arnavkasat/payment-engine/internal/events"
	"github.com/arnavkasat/payment-engine/internal/faults"
	"github.com/arnavkasat/payment-engine/internal/ledger"
	"github.com/arnavkasat/payment-engine/internal/metrics"
	"github.com/arnavkasat/payment-engine/internal/money"
	"github.com/arnavkasat/payment-engine/internal/storage"
)

// Metric names exported by the service.
const (
	MetricPayments       = "payments_total"
	MetricCaptureSeconds = "payment_capture_seconds"
	MetricSettlements    = "payment_settlements_total"
)

// Config wires the service to its collaborators.
type Config struct {
	// Clearing is the account captured funds are held in until settlement.
	Clearing uuid.UUID
	// Fees is the revenue account fees are credited to. It may be nil when
	// the deployment never charges fees.
	Fees uuid.UUID
	// External is the counterparty account deposits are drawn from. It may
	// be nil, which disables deposits.
	External uuid.UUID
	// Faults injects failures. Nil in production.
	Faults *faults.Injector
	// Metrics receives counters and latencies. Nil disables them.
	Metrics *metrics.Registry
	// Clock is the source of time. Nil means time.Now.
	Clock func() time.Time
}

// Service captures and settles payments.
type Service struct {
	store   storage.Store
	cfg     Config
	now     func() time.Time
	metrics *metrics.Registry
}

// New builds a Service.
func New(store storage.Store, cfg Config) (*Service, error) {
	if store == nil {
		return nil, fmt.Errorf("payments: store is required")
	}
	if cfg.Clearing == uuid.Nil {
		return nil, fmt.Errorf("payments: clearing account is required")
	}
	now := cfg.Clock
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	s := &Service{store: store, cfg: cfg, now: now, metrics: cfg.Metrics}
	if s.metrics != nil {
		s.metrics.DefineCounter(MetricPayments, "Payment capture attempts by outcome.")
		s.metrics.DefineCounter(MetricSettlements, "Settlement attempts by outcome.")
		s.metrics.DefineHistogram(MetricCaptureSeconds, "Wall-clock time to capture a payment, including retries.", nil)
	}
	return s, nil
}

// Result is the outcome of a capture attempt.
type Result struct {
	// Payment is the captured payment, whether freshly written or replayed.
	Payment storage.Payment
	// Response is the exact response body stored under the idempotency key.
	// Replaying a key returns these bytes verbatim, so a retry is
	// indistinguishable from the original call.
	Response []byte
	// StatusCode is the HTTP status stored alongside the response.
	StatusCode int
	// Replayed reports that this call did no work and returned a stored
	// result.
	Replayed bool
}

// CreatePayment captures a payment exactly once per idempotency key.
//
// Everything the capture touches lives in one transaction: the idempotency
// claim, the journal entry, the balances it moves, the payment row and the
// outbox message. There is no window in which a payment exists without its
// event, or an event without its payment.
func (s *Service) CreatePayment(ctx context.Context, req CreateRequest) (Result, error) {
	started := s.now()
	result, err := s.createPayment(ctx, req)
	if s.metrics != nil {
		s.metrics.Inc(MetricPayments, metrics.Labels{"outcome": outcomeOf(result, err)})
		s.metrics.Observe(MetricCaptureSeconds, nil, s.now().Sub(started).Seconds())
	}
	return result, err
}

func outcomeOf(r Result, err error) string {
	switch {
	case err == nil && r.Replayed:
		return "replayed"
	case err == nil:
		return "captured"
	case isConflict(err):
		return "conflict"
	default:
		return "error"
	}
}

// isConflict reports whether the error is an idempotency conflict rather
// than a failure of the payment itself.
func isConflict(err error) bool {
	return errors.Is(err, ErrRequestInFlight) || errors.Is(err, ErrIdempotencyKeyReused)
}

func (s *Service) createPayment(ctx context.Context, req CreateRequest) (Result, error) {
	if err := req.Validate(); err != nil {
		return Result{}, err
	}
	return s.idempotent(ctx, req.IdempotencyKey, req.Fingerprint(),
		func(ctx context.Context, tx storage.Tx) (uuid.UUID, []byte, int, error) {
			payment, err := s.capture(ctx, tx, req)
			if err != nil {
				return uuid.Nil, nil, 0, err
			}
			body, err := json.Marshal(payment)
			if err != nil {
				return uuid.Nil, nil, 0, fmt.Errorf("payments: encoding response: %w", err)
			}
			return payment.ID, body, 201, nil
		})
}

// work performs the effect behind an idempotency key, returning the payment
// it produced (or uuid.Nil), the response to store, and its status code.
type work func(ctx context.Context, tx storage.Tx) (uuid.UUID, []byte, int, error)

// idempotent runs fn at most once per key.
//
// The claim, the effect and the stored response all commit together. A
// caller that never saw the response retries with the same key and gets the
// stored bytes back; a caller that reuses a key for a different request is
// refused rather than handed someone else's result.
func (s *Service) idempotent(ctx context.Context, key, fingerprint string, fn work) (Result, error) {
	var result Result
	err := s.store.Tx(ctx, func(ctx context.Context, tx storage.Tx) error {
		// The store may run this function again after a serialisation
		// failure, so it starts from a clean slate every time.
		result = Result{}

		rec, claimed, err := tx.ClaimIdempotencyKey(ctx, key, fingerprint)
		if err != nil {
			return err
		}
		if !claimed {
			return s.replay(ctx, tx, key, fingerprint, rec, &result)
		}

		paymentID, body, status, err := fn(ctx, tx)
		if err != nil {
			return err
		}
		if err := tx.CompleteIdempotencyKey(ctx, key, paymentID, status, body); err != nil {
			return err
		}

		// A crash here loses the entire effect, which is the point: the
		// client retries with the same key and finds the slate clean.
		if err := s.cfg.Faults.Check(faults.BeforeCommit); err != nil {
			return err
		}

		if paymentID != uuid.Nil {
			if result.Payment, err = tx.GetPayment(ctx, paymentID); err != nil {
				return err
			}
		}
		result.Response = body
		result.StatusCode = status
		return nil
	})
	if err != nil {
		return Result{}, err
	}

	// A crash here loses only the response. The effect is committed, so the
	// client's retry replays it rather than applying it a second time.
	if err := s.cfg.Faults.Check(faults.AfterCommit); err != nil {
		return Result{}, err
	}
	return result, nil
}

// replay serves a request whose idempotency key is already held.
func (s *Service) replay(ctx context.Context, tx storage.Tx, key, fingerprint string, rec storage.IdempotencyRecord, out *Result) error {
	if rec.Fingerprint != fingerprint {
		return fmt.Errorf("%w: key %q", ErrIdempotencyKeyReused, key)
	}
	if rec.State != storage.StateCompleted {
		return fmt.Errorf("%w: key %q", ErrRequestInFlight, key)
	}
	*out = Result{
		Response:   rec.Response,
		StatusCode: rec.StatusCode,
		Replayed:   true,
	}
	if rec.PaymentID != uuid.Nil {
		payment, err := tx.GetPayment(ctx, rec.PaymentID)
		if err != nil {
			return fmt.Errorf("payments: replaying key %q: %w", key, err)
		}
		out.Payment = payment
	}
	return nil
}

// capture writes the journal entry, the payment and the outbox message.
func (s *Service) capture(ctx context.Context, tx storage.Tx, req CreateRequest) (storage.Payment, error) {
	// The payee is not a leg of the capture entry — it is only credited at
	// settlement — so its existence and currency are checked explicitly
	// here rather than falling out of the posting.
	accounts, err := tx.GetAccounts(ctx, []uuid.UUID{req.PayerAccount, req.PayeeAccount})
	if err != nil {
		return storage.Payment{}, err
	}
	for _, id := range []uuid.UUID{req.PayerAccount, req.PayeeAccount} {
		if got := accounts[id].Currency; got != req.Currency {
			return storage.Payment{}, fmt.Errorf("%w: account %s holds %s, payment is %s",
				storage.ErrCurrencyMismatch, accounts[id].Name, got, req.Currency)
		}
	}

	now := s.now()
	payment := storage.Payment{
		ID:             uuid.New(),
		IdempotencyKey: req.IdempotencyKey,
		PayerAccount:   req.PayerAccount,
		PayeeAccount:   req.PayeeAccount,
		Amount:         req.Amount,
		Fee:            req.Fee,
		Currency:       req.Currency,
		Status:         storage.StatusCaptured,
		Reference:      req.Reference,
		CreatedAt:      now,
		UpdatedAt:      now,
	}

	entry, err := ledger.NewCapture(payment.ID, req.PayerAccount, s.cfg.Clearing, s.cfg.Fees,
		req.Currency, req.Amount, req.Fee, now)
	if err != nil {
		return storage.Payment{}, err
	}
	if err := tx.ApplyEntry(ctx, entry); err != nil {
		return storage.Payment{}, err
	}
	if err := tx.InsertPayment(ctx, payment); err != nil {
		return storage.Payment{}, err
	}

	payload, err := json.Marshal(events.PaymentCaptured{
		PaymentID:    payment.ID,
		EntryID:      entry.ID,
		PayerAccount: payment.PayerAccount,
		PayeeAccount: payment.PayeeAccount,
		Amount:       payment.Amount,
		Fee:          payment.Fee,
		Currency:     payment.Currency,
		Reference:    payment.Reference,
		CapturedAt:   now,
	})
	if err != nil {
		return storage.Payment{}, fmt.Errorf("payments: encoding event: %w", err)
	}
	if err := tx.Enqueue(ctx, storage.OutboxMessage{
		ID:          uuid.New(),
		AggregateID: payment.ID,
		Type:        events.TypePaymentCaptured,
		Payload:     payload,
		CreatedAt:   now,
	}); err != nil {
		return storage.Payment{}, err
	}
	return payment, nil
}

// HandleCaptured settles a captured payment. It is the consumer's handler
// for TypePaymentCaptured and runs inside the consumer's transaction,
// alongside the deduplication marker, so a redelivery can never post a
// second settlement.
func (s *Service) HandleCaptured(ctx context.Context, tx storage.Tx, m events.Message) error {
	if m.Type != events.TypePaymentCaptured {
		return fmt.Errorf("%w: %q", ErrUnknownEventType, m.Type)
	}
	var evt events.PaymentCaptured
	if err := json.Unmarshal(m.Payload, &evt); err != nil {
		return fmt.Errorf("payments: decoding %s: %w", m.Type, err)
	}

	payment, err := tx.GetPayment(ctx, evt.PaymentID)
	if err != nil {
		return err
	}
	if payment.Status != storage.StatusCaptured {
		// Already settled or reversed. The dedupe marker should have caught
		// this, so reaching here means the markers were trimmed or the
		// message came from another source; either way, doing nothing is
		// the correct and safe response.
		s.count(MetricSettlements, "already_final")
		return nil
	}

	now := s.now()
	entry, err := ledger.NewSettlement(payment.ID, s.cfg.Clearing, payment.PayeeAccount, payment.Currency, payment.Amount, now)
	if err != nil {
		return err
	}
	if err := tx.ApplyEntry(ctx, entry); err != nil {
		return err
	}
	if err := tx.TransitionPayment(ctx, payment.ID, storage.StatusCaptured, storage.StatusSettled); err != nil {
		return err
	}

	payload, err := json.Marshal(events.PaymentSettled{
		PaymentID:    payment.ID,
		EntryID:      entry.ID,
		PayeeAccount: payment.PayeeAccount,
		Amount:       payment.Amount,
		Currency:     payment.Currency,
		SettledAt:    now,
	})
	if err != nil {
		return fmt.Errorf("payments: encoding event: %w", err)
	}
	if err := tx.Enqueue(ctx, storage.OutboxMessage{
		ID:          uuid.New(),
		AggregateID: payment.ID,
		Type:        events.TypePaymentSettled,
		Payload:     payload,
		CreatedAt:   now,
	}); err != nil {
		return err
	}
	s.count(MetricSettlements, "settled")
	return nil
}

func (s *Service) count(name, outcome string) {
	if s.metrics != nil {
		s.metrics.Inc(name, metrics.Labels{"outcome": outcome})
	}
}

// GetPayment reads a payment.
func (s *Service) GetPayment(ctx context.Context, id uuid.UUID) (storage.Payment, error) {
	return s.store.GetPayment(ctx, id)
}

// GetEntries reads a payment's journal entries.
func (s *Service) GetEntries(ctx context.Context, id uuid.UUID) ([]ledger.Entry, error) {
	return s.store.GetEntries(ctx, id)
}

// Balance reads an account's balance.
func (s *Service) Balance(ctx context.Context, id uuid.UUID) (money.Minor, error) {
	a, err := s.store.GetAccount(ctx, id)
	if err != nil {
		return 0, err
	}
	return a.Balance, nil
}
