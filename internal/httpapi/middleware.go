package httpapi

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"

	"github.com/arnavkasat/payment-engine/internal/metrics"
)

// Metric and header names used by the HTTP layer.
const (
	MetricRequests = "http_requests_total"
	MetricSeconds  = "http_request_seconds"
	MetricPanics   = "http_panics_total"

	HeaderIdempotencyKey = "Idempotency-Key"
	HeaderRequestID      = "X-Request-Id"
	// HeaderIdempotentReplay tells a client its request was recognised as a
	// retry and no new payment was made. It is informational: the body and
	// status are identical to the original response either way.
	HeaderIdempotentReplay = "Idempotent-Replay"
)

// recorder captures the status code and size for logging and metrics.
type recorder struct {
	http.ResponseWriter
	status int
	bytes  int
}

func (r *recorder) WriteHeader(status int) {
	if r.status == 0 {
		r.status = status
		r.ResponseWriter.WriteHeader(status)
	}
}

func (r *recorder) Write(b []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	n, err := r.ResponseWriter.Write(b)
	r.bytes += n
	return n, err
}

// observe logs and measures every request, and turns a panic into a 500
// rather than a dropped connection.
func (s *Server) observe(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		requestID := r.Header.Get(HeaderRequestID)
		if requestID == "" {
			requestID = uuid.NewString()
		}
		w.Header().Set(HeaderRequestID, requestID)
		rec := &recorder{ResponseWriter: w}

		defer func() {
			if p := recover(); p != nil {
				s.log.Error("panic serving request",
					"panic", p, "method", r.Method, "path", r.URL.Path, "request_id", requestID)
				if s.metrics != nil {
					s.metrics.Inc(MetricPanics, nil)
				}
				// If nothing has been written yet the client still gets a
				// well-formed error; if it has, the connection is all that
				// can be salvaged.
				if rec.status == 0 {
					writeJSON(rec, http.StatusInternalServerError, errorBody{
						Error: errorDetail{Code: "internal_error", Message: "internal error"},
					})
				}
			}

			elapsed := time.Since(start)
			// The route pattern is the label, never the raw path: labelling
			// by path would mint a new time series per payment id.
			route := r.Pattern
			if route == "" {
				route = "unmatched"
			}
			if s.metrics != nil {
				labels := metrics.Labels{"route": route, "method": r.Method, "status": statusClass(rec.status)}
				s.metrics.Inc(MetricRequests, labels)
				s.metrics.Observe(MetricSeconds, metrics.Labels{"route": route, "method": r.Method}, elapsed.Seconds())
			}
			s.log.Log(r.Context(), levelFor(rec.status), "request",
				"method", r.Method, "route", route, "path", r.URL.Path,
				"status", rec.status, "bytes", rec.bytes,
				"duration_ms", float64(elapsed.Microseconds())/1000, "request_id", requestID)
		}()

		next.ServeHTTP(rec, r)
	})
}

func levelFor(status int) slog.Level {
	switch {
	case status >= 500:
		return slog.LevelError
	case status >= 400:
		return slog.LevelWarn
	default:
		return slog.LevelInfo
	}
}

// statusClass buckets statuses so the metric stays low cardinality.
func statusClass(status int) string {
	switch {
	case status >= 500:
		return "5xx"
	case status >= 400:
		return "4xx"
	case status >= 300:
		return "3xx"
	case status >= 200:
		return "2xx"
	default:
		return "unknown"
	}
}
