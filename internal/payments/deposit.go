package payments

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/arnavkasat/payment-engine/internal/ledger"
	"github.com/arnavkasat/payment-engine/internal/money"
	"github.com/arnavkasat/payment-engine/internal/storage"
)

// DepositRequest moves funds into a wallet from the external counterparty —
// a bank rail or a card network. It is the only way value enters the ledger.
type DepositRequest struct {
	IdempotencyKey string         `json:"-"`
	Account        uuid.UUID      `json:"account"`
	Amount         money.Minor    `json:"amount"`
	Currency       money.Currency `json:"currency"`
	Reference      string         `json:"reference,omitempty"`
}

// Validate checks the request in isolation.
func (r DepositRequest) Validate() error {
	switch {
	case r.IdempotencyKey == "":
		return ErrIdempotencyKeyRequired
	case len(r.IdempotencyKey) > MaxIdempotencyKeyLen:
		return fmt.Errorf("%w: idempotency key is %d bytes, limit is %d", ErrInvalidRequest, len(r.IdempotencyKey), MaxIdempotencyKeyLen)
	case r.Account == uuid.Nil:
		return fmt.Errorf("%w: account is required", ErrInvalidRequest)
	case r.Amount <= 0:
		return fmt.Errorf("%w: amount must be positive, got %d", ErrInvalidRequest, r.Amount)
	case len(r.Reference) > MaxReferenceLen:
		return fmt.Errorf("%w: reference is %d bytes, limit is %d", ErrInvalidRequest, len(r.Reference), MaxReferenceLen)
	}
	if _, err := money.ParseCurrency(string(r.Currency)); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidRequest, err)
	}
	return nil
}

// Fingerprint hashes the request's meaning. See CreateRequest.Fingerprint.
func (r DepositRequest) Fingerprint() string {
	var b []byte
	for _, field := range []string{
		"payments.deposit/v1",
		r.Account.String(),
		strconv.FormatInt(int64(r.Amount), 10),
		string(r.Currency),
		r.Reference,
	} {
		b = append(b, byte('|'))
		b = append(b, []byte(strconv.Itoa(len(field))+":"+field)...)
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// Deposit is the receipt returned for a deposit.
type Deposit struct {
	EntryID   uuid.UUID      `json:"entry_id"`
	Account   uuid.UUID      `json:"account"`
	Amount    money.Minor    `json:"amount"`
	Currency  money.Currency `json:"currency"`
	Reference string         `json:"reference,omitempty"`
	CreatedAt time.Time      `json:"created_at"`
}

// Deposit credits a wallet from the external account, exactly once per
// idempotency key. Minting money is the most dangerous operation the engine
// offers, so it goes through the same idempotency path as a payment rather
// than being an unguarded admin endpoint.
func (s *Service) Deposit(ctx context.Context, req DepositRequest) (Result, error) {
	if err := req.Validate(); err != nil {
		return Result{}, err
	}
	if s.cfg.External == uuid.Nil {
		return Result{}, fmt.Errorf("payments: no external account is configured, so deposits are disabled")
	}
	return s.idempotent(ctx, req.IdempotencyKey, req.Fingerprint(),
		func(ctx context.Context, tx storage.Tx) (uuid.UUID, []byte, int, error) {
			accounts, err := tx.GetAccounts(ctx, []uuid.UUID{req.Account})
			if err != nil {
				return uuid.Nil, nil, 0, err
			}
			if got := accounts[req.Account].Currency; got != req.Currency {
				return uuid.Nil, nil, 0, fmt.Errorf("%w: account %s holds %s, deposit is %s",
					storage.ErrCurrencyMismatch, accounts[req.Account].Name, got, req.Currency)
			}

			now := s.now()
			entry, err := ledger.NewDeposit(uuid.New(), s.cfg.External, req.Account, req.Currency, req.Amount, now)
			if err != nil {
				return uuid.Nil, nil, 0, err
			}
			if err := tx.ApplyEntry(ctx, entry); err != nil {
				return uuid.Nil, nil, 0, err
			}
			body, err := json.Marshal(Deposit{
				EntryID:   entry.ID,
				Account:   req.Account,
				Amount:    req.Amount,
				Currency:  req.Currency,
				Reference: req.Reference,
				CreatedAt: now,
			})
			if err != nil {
				return uuid.Nil, nil, 0, fmt.Errorf("payments: encoding response: %w", err)
			}
			// A deposit produces no payment row: it is a ledger movement,
			// not a transfer between two of the engine's accounts.
			return uuid.Nil, body, 201, nil
		})
}
