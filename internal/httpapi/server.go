// Package httpapi exposes the engine over HTTP.
//
// Writes require an Idempotency-Key header. A retry with the same key
// returns the stored response byte for byte, with an Idempotent-Replay
// header to say so, so a client that times out can retry without wondering
// whether it paid twice.
package httpapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"

	"github.com/arnavkasat/payment-engine/internal/metrics"
	"github.com/arnavkasat/payment-engine/internal/payments"
	"github.com/arnavkasat/payment-engine/internal/storage"
)

// MaxBodyBytes caps a request body. Payment requests are small; anything
// larger is a mistake or an attack.
const MaxBodyBytes = 64 << 10

// Config configures the server.
type Config struct {
	// Service performs the payments work.
	Service *payments.Service
	// Store backs the read endpoints and the readiness probe.
	Store storage.Store
	// Metrics is exposed at /metrics. Nil disables the endpoint.
	Metrics *metrics.Registry
	// Logger receives request logs. Nil discards them.
	Logger *slog.Logger
}

// Server is the HTTP API.
type Server struct {
	svc     *payments.Service
	store   storage.Store
	metrics *metrics.Registry
	log     *slog.Logger
	mux     *http.ServeMux
}

// New builds a Server.
func New(cfg Config) (*Server, error) {
	if cfg.Service == nil || cfg.Store == nil {
		return nil, fmt.Errorf("httpapi: service and store are required")
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	s := &Server{svc: cfg.Service, store: cfg.Store, metrics: cfg.Metrics, log: cfg.Logger, mux: http.NewServeMux()}
	if s.metrics != nil {
		s.metrics.DefineCounter(MetricRequests, "HTTP requests by route, method and status class.")
		s.metrics.DefineCounter(MetricPanics, "Handler panics recovered.")
		s.metrics.DefineHistogram(MetricSeconds, "HTTP request duration.", nil)
	}
	s.routes()
	return s, nil
}

func (s *Server) routes() {
	s.mux.HandleFunc("POST /v1/payments", s.createPayment)
	s.mux.HandleFunc("GET /v1/payments/{id}", s.getPayment)
	s.mux.HandleFunc("GET /v1/payments/{id}/entries", s.getPaymentEntries)

	s.mux.HandleFunc("POST /v1/accounts", s.createAccount)
	s.mux.HandleFunc("GET /v1/accounts", s.listAccounts)
	s.mux.HandleFunc("GET /v1/accounts/{id}", s.getAccount)
	s.mux.HandleFunc("POST /v1/accounts/{id}/deposits", s.deposit)

	s.mux.HandleFunc("GET /v1/ledger/invariants", s.verifyInvariants)

	s.mux.HandleFunc("GET /healthz", s.healthz)
	s.mux.HandleFunc("GET /readyz", s.readyz)
	s.mux.HandleFunc("GET /metrics", s.metricsHandler)
}

// ServeHTTP satisfies http.Handler.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.observe(s.mux).ServeHTTP(w, r)
}

// writeJSON writes a JSON body with the given status.
func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if body == nil {
		return
	}
	if err := json.NewEncoder(w).Encode(body); err != nil {
		// The status line is already out; nothing useful is left to do.
		return
	}
}

// writeRaw writes a pre-encoded JSON body, which is how a replayed response
// is returned byte for byte.
func writeRaw(w http.ResponseWriter, status int, body []byte) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

// fail renders an error, logging the ones that are the server's fault.
func (s *Server) fail(w http.ResponseWriter, r *http.Request, err error) {
	status, code := classify(err)
	if status >= 500 {
		s.log.Error("request failed", "method", r.Method, "path", r.URL.Path, "error", err)
	}
	if status == http.StatusConflict && code == "request_in_flight" {
		// The outcome is not yet known; tell the client when to look again.
		w.Header().Set("Retry-After", "1")
	}
	writeJSON(w, status, errorBody{Error: errorDetail{Code: code, Message: err.Error()}})
}

// decode reads a JSON body with a size limit and no unknown fields, so a
// misspelled amount field is an error rather than a silent zero.
func decode(r *http.Request, target any) error {
	body := http.MaxBytesReader(nil, r.Body, MaxBodyBytes)
	dec := json.NewDecoder(body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(target); err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			return fmt.Errorf("%w: body exceeds %d bytes", payments.ErrInvalidRequest, MaxBodyBytes)
		}
		return fmt.Errorf("%w: %v", payments.ErrInvalidRequest, err)
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return fmt.Errorf("%w: body contains more than one JSON value", payments.ErrInvalidRequest)
	}
	return nil
}

// pathUUID reads a UUID path parameter.
func pathUUID(r *http.Request, name string) (uuid.UUID, error) {
	raw := r.PathValue(name)
	id, err := uuid.Parse(raw)
	if err != nil {
		return uuid.Nil, fmt.Errorf("%w: %s %q is not a uuid", payments.ErrInvalidRequest, name, raw)
	}
	return id, nil
}

func (s *Server) healthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// readyz reports whether the store is reachable, which is what decides
// whether this instance should receive traffic.
func (s *Server) readyz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := contextWithTimeout(r, 2*time.Second)
	defer cancel()
	if err := s.store.Ping(ctx); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, errorBody{
			Error: errorDetail{Code: "not_ready", Message: err.Error()},
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func (s *Server) metricsHandler(w http.ResponseWriter, r *http.Request) {
	if s.metrics == nil {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	if err := s.metrics.WriteProm(w); err != nil {
		s.log.Error("writing metrics", "error", err)
	}
}
