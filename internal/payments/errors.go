package payments

import "errors"

// Errors the service returns. The HTTP layer maps each to a status code; no
// other package needs to know about them.
var (
	// ErrIdempotencyKeyRequired means the caller omitted the key. Writes are
	// refused without one: a retry that cannot be recognised is a double
	// payment waiting to happen.
	ErrIdempotencyKeyRequired = errors.New("payments: idempotency key required")

	// ErrIdempotencyKeyReused means the key has already been used for a
	// different request body. Returning the original payment would be worse
	// than failing: the caller would believe a payment it never made
	// succeeded.
	ErrIdempotencyKeyReused = errors.New("payments: idempotency key reused with a different request")

	// ErrRequestInFlight means another request holds the key and has not
	// finished. The caller should retry; the outcome is not yet known.
	ErrRequestInFlight = errors.New("payments: a request with this idempotency key is in flight")

	// ErrInvalidRequest means the request failed validation.
	ErrInvalidRequest = errors.New("payments: invalid request")

	// ErrUnknownEventType means the settlement consumer was handed a message
	// it has no handler for.
	ErrUnknownEventType = errors.New("payments: unknown event type")
)
