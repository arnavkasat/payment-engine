// Package events defines the messages the engine publishes and the narrow
// broker interfaces the rest of the code depends on. Nothing outside
// events/kafka knows that Kafka is what carries them.
package events

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/arnavkasat/payment-engine/internal/money"
)

// Event types. They are part of the wire contract, so they are strings on
// the message rather than a Go type that would not survive a schema change.
const (
	// TypePaymentCaptured is emitted when funds have left the payer and are
	// held in clearing. The settlement consumer acts on it.
	TypePaymentCaptured = "payment.captured"
	// TypePaymentSettled is emitted when funds reach the payee. It is the
	// terminal event of a successful payment.
	TypePaymentSettled = "payment.settled"
)

// Message is a broker-agnostic envelope.
//
// ID is the outbox row's identifier and is the only thing consumers
// deduplicate on. It is stable across redeliveries, which is what lets a
// consumer recognise a message it has already applied — broker offsets and
// delivery counts are not stable and must never be used for that.
type Message struct {
	ID        string    `json:"id"`
	Key       string    `json:"key"`
	Type      string    `json:"type"`
	Payload   []byte    `json:"payload"`
	Timestamp time.Time `json:"timestamp"`
}

// Publisher writes messages to the broker. Publish is at-least-once: it may
// deliver a message the broker already accepted, and consumers must cope.
type Publisher interface {
	Publish(ctx context.Context, msgs ...Message) error
	Close() error
}

// Handler processes one message. Returning an error means the message was
// not handled and must be redelivered.
type Handler func(ctx context.Context, m Message) error

// Consumer delivers messages to a handler until the context is cancelled.
type Consumer interface {
	Run(ctx context.Context, h Handler) error
	Close() error
}

// PaymentCaptured is the payload of a TypePaymentCaptured message.
type PaymentCaptured struct {
	PaymentID    uuid.UUID      `json:"payment_id"`
	EntryID      uuid.UUID      `json:"entry_id"`
	PayerAccount uuid.UUID      `json:"payer_account"`
	PayeeAccount uuid.UUID      `json:"payee_account"`
	Amount       money.Minor    `json:"amount"`
	Fee          money.Minor    `json:"fee"`
	Currency     money.Currency `json:"currency"`
	Reference    string         `json:"reference,omitempty"`
	CapturedAt   time.Time      `json:"captured_at"`
}

// PaymentSettled is the payload of a TypePaymentSettled message.
type PaymentSettled struct {
	PaymentID    uuid.UUID      `json:"payment_id"`
	EntryID      uuid.UUID      `json:"entry_id"`
	PayeeAccount uuid.UUID      `json:"payee_account"`
	Amount       money.Minor    `json:"amount"`
	Currency     money.Currency `json:"currency"`
	SettledAt    time.Time      `json:"settled_at"`
}
