package payments

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	"github.com/google/uuid"

	"github.com/arnavkasat/payment-engine/internal/money"
)

// Limits on caller-supplied strings, enforced so that an idempotency key or
// reference cannot be used to write unbounded data.
const (
	MaxIdempotencyKeyLen = 255
	MaxReferenceLen      = 255
)

// CreateRequest is a request to move money from one account to another.
type CreateRequest struct {
	IdempotencyKey string         `json:"-"`
	PayerAccount   uuid.UUID      `json:"payer_account"`
	PayeeAccount   uuid.UUID      `json:"payee_account"`
	Amount         money.Minor    `json:"amount"`
	Fee            money.Minor    `json:"fee"`
	Currency       money.Currency `json:"currency"`
	Reference      string         `json:"reference,omitempty"`
}

// Validate checks the request in isolation, before any database work.
func (r CreateRequest) Validate() error {
	switch {
	case r.IdempotencyKey == "":
		return ErrIdempotencyKeyRequired
	case len(r.IdempotencyKey) > MaxIdempotencyKeyLen:
		return fmt.Errorf("%w: idempotency key is %d bytes, limit is %d", ErrInvalidRequest, len(r.IdempotencyKey), MaxIdempotencyKeyLen)
	case r.PayerAccount == uuid.Nil:
		return fmt.Errorf("%w: payer_account is required", ErrInvalidRequest)
	case r.PayeeAccount == uuid.Nil:
		return fmt.Errorf("%w: payee_account is required", ErrInvalidRequest)
	case r.PayerAccount == r.PayeeAccount:
		return fmt.Errorf("%w: payer and payee must differ", ErrInvalidRequest)
	case r.Amount <= 0:
		return fmt.Errorf("%w: amount must be positive, got %d", ErrInvalidRequest, r.Amount)
	case r.Fee < 0:
		return fmt.Errorf("%w: fee must not be negative, got %d", ErrInvalidRequest, r.Fee)
	case len(r.Reference) > MaxReferenceLen:
		return fmt.Errorf("%w: reference is %d bytes, limit is %d", ErrInvalidRequest, len(r.Reference), MaxReferenceLen)
	}
	if _, err := money.ParseCurrency(string(r.Currency)); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidRequest, err)
	}
	if _, err := money.Add(r.Amount, r.Fee); err != nil {
		return fmt.Errorf("%w: amount plus fee overflows", ErrInvalidRequest)
	}
	return nil
}

// Fingerprint hashes the request's meaning, so that reusing an idempotency
// key for a different payment is detected instead of silently replaying the
// first one. The key is excluded: it identifies the request rather than
// describing it.
//
// The canonical form is built field by field with an unambiguous separator
// rather than by marshalling, because JSON field order, whitespace and
// optional fields are all free to change without changing the request.
func (r CreateRequest) Fingerprint() string {
	var b strings.Builder
	b.WriteString("payments.create/v1")
	for _, field := range []string{
		r.PayerAccount.String(),
		r.PayeeAccount.String(),
		strconv.FormatInt(int64(r.Amount), 10),
		strconv.FormatInt(int64(r.Fee), 10),
		string(r.Currency),
		r.Reference,
	} {
		// The length prefix keeps fields from running together: without it
		// a reference of "a|b" would collide with two fields "a" and "b".
		b.WriteByte('|')
		b.WriteString(strconv.Itoa(len(field)))
		b.WriteByte(':')
		b.WriteString(field)
	}
	sum := sha256.Sum256([]byte(b.String()))
	return hex.EncodeToString(sum[:])
}
