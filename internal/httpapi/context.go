package httpapi

import (
	"context"
	"net/http"
	"time"
)

// contextWithTimeout bounds a handler's work by both the client's
// disconnection and a deadline of our own.
func contextWithTimeout(r *http.Request, d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(r.Context(), d)
}
