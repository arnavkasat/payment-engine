package payments

import (
	"errors"
	"math"
	"testing"

	"github.com/google/uuid"
)

func validRequest() CreateRequest {
	return CreateRequest{
		IdempotencyKey: "key-1",
		PayerAccount:   uuid.MustParse("11111111-1111-1111-1111-111111111111"),
		PayeeAccount:   uuid.MustParse("22222222-2222-2222-2222-222222222222"),
		Amount:         1_000,
		Fee:            25,
		Currency:       "USD",
		Reference:      "invoice-9",
	}
}

func TestValidate(t *testing.T) {
	if err := validRequest().Validate(); err != nil {
		t.Fatalf("valid request rejected: %v", err)
	}
	for _, tc := range []struct {
		name string
		mut  func(*CreateRequest)
		want error
	}{
		{"no key", func(r *CreateRequest) { r.IdempotencyKey = "" }, ErrIdempotencyKeyRequired},
		{"long key", func(r *CreateRequest) { r.IdempotencyKey = string(make([]byte, 300)) }, ErrInvalidRequest},
		{"no payer", func(r *CreateRequest) { r.PayerAccount = uuid.Nil }, ErrInvalidRequest},
		{"no payee", func(r *CreateRequest) { r.PayeeAccount = uuid.Nil }, ErrInvalidRequest},
		{"self payment", func(r *CreateRequest) { r.PayeeAccount = r.PayerAccount }, ErrInvalidRequest},
		{"zero amount", func(r *CreateRequest) { r.Amount = 0 }, ErrInvalidRequest},
		{"negative amount", func(r *CreateRequest) { r.Amount = -5 }, ErrInvalidRequest},
		{"negative fee", func(r *CreateRequest) { r.Fee = -1 }, ErrInvalidRequest},
		{"bad currency", func(r *CreateRequest) { r.Currency = "DOLLARS" }, ErrInvalidRequest},
		{"long reference", func(r *CreateRequest) { r.Reference = string(make([]byte, 300)) }, ErrInvalidRequest},
		{"overflow", func(r *CreateRequest) { r.Amount = math.MaxInt64; r.Fee = 1 }, ErrInvalidRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := validRequest()
			tc.mut(&r)
			if err := r.Validate(); !errors.Is(err, tc.want) {
				t.Fatalf("Validate = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestFingerprintIgnoresTheKeyButNotTheRequest(t *testing.T) {
	base := validRequest()
	same := base
	same.IdempotencyKey = "a completely different key"
	if base.Fingerprint() != same.Fingerprint() {
		t.Error("fingerprint changed with the idempotency key")
	}

	for _, tc := range []struct {
		name string
		mut  func(*CreateRequest)
	}{
		{"amount", func(r *CreateRequest) { r.Amount++ }},
		{"fee", func(r *CreateRequest) { r.Fee++ }},
		{"currency", func(r *CreateRequest) { r.Currency = "EUR" }},
		{"payer", func(r *CreateRequest) { r.PayerAccount = uuid.New() }},
		{"payee", func(r *CreateRequest) { r.PayeeAccount = uuid.New() }},
		{"reference", func(r *CreateRequest) { r.Reference += "!" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			changed := base
			tc.mut(&changed)
			if base.Fingerprint() == changed.Fingerprint() {
				t.Fatalf("fingerprint unchanged after %s changed", tc.name)
			}
		})
	}
}

// TestFingerprintFieldsCannotBeConfused guards the length prefixing: without
// it, moving a separator between two adjacent fields would hash the same.
func TestFingerprintFieldsCannotBeConfused(t *testing.T) {
	a := validRequest()
	a.Currency = "USD"
	a.Reference = "x|y"

	b := validRequest()
	b.Currency = "USD"
	b.Reference = "x"
	b.Reference = "x|y" // same so far
	if a.Fingerprint() != b.Fingerprint() {
		t.Fatal("identical requests hashed differently")
	}

	c := validRequest()
	c.Reference = "9:invoice-9"
	if c.Fingerprint() == validRequest().Fingerprint() {
		t.Fatal("a reference that mimics the encoding collided with the real one")
	}
}
