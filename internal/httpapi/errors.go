package httpapi

import (
	"context"
	"errors"
	"net/http"

	"github.com/arnavkasat/payment-engine/internal/ledger"
	"github.com/arnavkasat/payment-engine/internal/payments"
	"github.com/arnavkasat/payment-engine/internal/storage"
)

// StatusClientClosedRequest is nginx's non-standard code for a client that
// disconnected before the response was written. There is no registered
// status for it, and conflating it with 500 makes a load generator's own
// cancellations look like server failures.
const StatusClientClosedRequest = 499

// errorBody is the single error shape the API returns. Clients branch on
// code, which is stable; message is for humans and may change.
type errorBody struct {
	Error errorDetail `json:"error"`
}

type errorDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// classify maps an error to an HTTP status and a stable machine-readable
// code. Anything unrecognised is a 500 and is logged: a payment API that
// turns an unknown failure into a 4xx is telling the client not to retry
// something that might well have succeeded.
func classify(err error) (status int, code string) {
	switch {
	// A client that hung up mid-request is not a server fault, and must not
	// be counted as one: 499 is the conventional code for it.
	case errors.Is(err, context.Canceled):
		return StatusClientClosedRequest, "client_closed_request"
	case errors.Is(err, context.DeadlineExceeded):
		return http.StatusGatewayTimeout, "timeout"
	case errors.Is(err, payments.ErrIdempotencyKeyRequired):
		return http.StatusBadRequest, "idempotency_key_required"
	case errors.Is(err, payments.ErrInvalidRequest):
		return http.StatusBadRequest, "invalid_request"
	case errors.Is(err, payments.ErrIdempotencyKeyReused):
		// The request is well formed but contradicts a stored one, so it is
		// unprocessable rather than malformed.
		return http.StatusUnprocessableEntity, "idempotency_key_reused"
	case errors.Is(err, payments.ErrRequestInFlight):
		return http.StatusConflict, "request_in_flight"
	case errors.Is(err, payments.ErrUnknownEventType):
		return http.StatusBadRequest, "unknown_event_type"
	case errors.Is(err, storage.ErrInsufficientFunds):
		return http.StatusUnprocessableEntity, "insufficient_funds"
	case errors.Is(err, storage.ErrCurrencyMismatch):
		return http.StatusUnprocessableEntity, "currency_mismatch"
	case errors.Is(err, storage.ErrNotFound):
		return http.StatusNotFound, "not_found"
	case errors.Is(err, storage.ErrAlreadyExists):
		return http.StatusConflict, "already_exists"
	case errors.Is(err, storage.ErrStatusConflict):
		return http.StatusConflict, "status_conflict"
	case errors.Is(err, storage.ErrRetryExhausted):
		// Contention, not a client mistake: the same request may well
		// succeed in a moment.
		return http.StatusServiceUnavailable, "retry_exhausted"
	case errors.Is(err, ledger.ErrNotBalanced):
		return http.StatusInternalServerError, "ledger_unbalanced"
	default:
		return http.StatusInternalServerError, "internal_error"
	}
}
