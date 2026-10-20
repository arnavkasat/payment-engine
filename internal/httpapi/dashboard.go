package httpapi

import (
	_ "embed"
	"fmt"
	"net/http"
	"time"

	"github.com/arnavkasat/payment-engine/internal/faults"
	"github.com/arnavkasat/payment-engine/internal/ledger"
	"github.com/arnavkasat/payment-engine/internal/money"
	"github.com/arnavkasat/payment-engine/internal/payments"
	"github.com/arnavkasat/payment-engine/internal/storage"
)

// dashboardHTML is the operator dashboard, served at /. It is one embedded
// file with no build step and no JavaScript dependencies, so the engine
// stays a single static binary and the repository stays a Go repository.
//
//go:embed dashboard.html
var dashboardHTML []byte

func (s *Server) dashboard(w http.ResponseWriter, r *http.Request) {
	// ServeMux's "/" pattern matches anything unmatched, so an unknown path
	// must 404 rather than silently rendering the dashboard.
	if r.URL.Path != "/" {
		s.fail(w, r, fmt.Errorf("%w: no route for %s", storage.ErrNotFound, r.URL.Path))
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	// The page is rebuilt into the binary on every build, so it must not be
	// served from a stale cache after a deploy.
	w.Header().Set("Cache-Control", "no-cache")
	_, _ = w.Write(dashboardHTML)
}

// snapshot is everything the dashboard polls for, in one request. Gathering
// it server-side keeps the page to a single round trip per tick instead of
// four, and means the numbers it shows are consistent with each other.
type snapshot struct {
	Accounts        []storage.Account `json:"accounts"`
	NetBalance      money.Minor       `json:"net_balance"`
	ClearingBalance money.Minor       `json:"clearing_balance"`
	InFlight        int               `json:"in_flight"`
	Settled         int               `json:"settled"`
	PendingOutbox   int               `json:"pending_outbox"`
	Invariants      invariantView     `json:"invariants"`
	FaultsEnabled   bool              `json:"faults_enabled"`
	ObservedAt      time.Time         `json:"observed_at"`
}

type invariantView struct {
	OK         bool               `json:"ok"`
	Violations []ledger.Violation `json:"violations"`
	Entries    int                `json:"entries"`
	Postings   int                `json:"postings"`
	Payments   int                `json:"payments"`
	PostingSum money.Minor        `json:"posting_sum"`
}

// getSnapshot handles GET /v1/ledger/snapshot.
func (s *Server) getSnapshot(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := contextWithTimeout(r, requestTimeout)
	defer cancel()

	accounts, err := s.store.ListAccounts(ctx)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	counts, err := s.store.CountPaymentsByStatus(ctx)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	pending, err := s.store.PendingOutbox(ctx)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	report, err := s.store.VerifyInvariants(ctx)
	if err != nil {
		s.fail(w, r, err)
		return
	}

	out := snapshot{
		Accounts:      accounts,
		InFlight:      counts[storage.StatusCaptured],
		Settled:       counts[storage.StatusSettled],
		PendingOutbox: pending,
		FaultsEnabled: s.faults != nil,
		ObservedAt:    time.Now().UTC(),
		Invariants: invariantView{
			OK:         report.OK(),
			Violations: report.Violations,
			Entries:    report.Entries,
			Postings:   report.Postings,
			Payments:   report.Payments,
			PostingSum: report.TotalPostingSum,
		},
	}
	if out.Accounts == nil {
		out.Accounts = []storage.Account{}
	}
	if out.Invariants.Violations == nil {
		out.Invariants.Violations = []ledger.Violation{}
	}
	for _, a := range accounts {
		out.NetBalance += a.Balance
		if a.Kind == storage.KindClearing {
			out.ClearingBalance += a.Balance
		}
	}
	writeJSON(w, http.StatusOK, out)
}

// faultView is one injection point's current state.
type faultView struct {
	Point       faults.Point `json:"point"`
	Description string       `json:"description"`
	Fired       int          `json:"fired"`
}

// faultDescriptions explain what each point breaks, so the dashboard can say
// what is being tested rather than just naming a constant.
var faultDescriptions = map[faults.Point]string{
	faults.BeforeCommit:        "Kill the capture just before COMMIT. Nothing may survive: no payment, no postings, no outbox row — and the idempotency key must be reusable.",
	faults.AfterCommit:         "Commit the capture, then fail before answering the client. The retry must replay, not pay again.",
	faults.OutboxBeforePublish: "Lease an outbox row, then die before publishing. The lease must lapse and another worker must pick it up.",
	faults.OutboxAfterPublish:  "The broker accepted the message, but the row is never retired. It will be published again, and the consumer must absorb the duplicate.",
	faults.ConsumerAfterDedupe: "Write the dedupe marker, then fail before the effect. Both must roll back together.",
	faults.ConsumerBeforeAck:   "Commit the settlement, then fail before acking. The redelivery must change nothing.",
}

// getFaults handles GET /v1/faults.
func (s *Server) getFaults(w http.ResponseWriter, r *http.Request) {
	if s.faults == nil {
		s.failFaultsDisabled(w, r)
		return
	}
	out := make([]faultView, 0, len(faults.AllPoints))
	for _, p := range faults.AllPoints {
		out = append(out, faultView{Point: p, Description: faultDescriptions[p], Fired: s.faults.Fired(p)})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"enabled": true,
		"total":   s.faults.Total(),
		"points":  out,
	})
}

// setFaultsRequest arms or disarms injection points.
type setFaultsRequest struct {
	// Point and Probability arm one point.
	Point       faults.Point `json:"point,omitempty"`
	Probability float64      `json:"probability,omitempty"`
	// Disable clears every armed point and ignores the fields above.
	Disable bool `json:"disable,omitempty"`
}

// setFaults handles POST /v1/faults.
func (s *Server) setFaults(w http.ResponseWriter, r *http.Request) {
	if s.faults == nil {
		s.failFaultsDisabled(w, r)
		return
	}
	var body setFaultsRequest
	if err := decode(r, &body); err != nil {
		s.fail(w, r, err)
		return
	}
	if body.Disable {
		s.faults.Disable()
		s.log.Warn("fault injection disarmed")
		s.getFaults(w, r)
		return
	}
	if _, known := faultDescriptions[body.Point]; !known {
		s.fail(w, r, fmt.Errorf("%w: unknown injection point %q", payments.ErrInvalidRequest, body.Point))
		return
	}
	if body.Probability < 0 || body.Probability > 1 {
		s.fail(w, r, fmt.Errorf("%w: probability must be between 0 and 1, got %v", payments.ErrInvalidRequest, body.Probability))
		return
	}
	s.faults.Set(body.Point, body.Probability)
	s.log.Warn("fault injection armed", "point", body.Point, "probability", body.Probability)
	s.getFaults(w, r)
}

func (s *Server) failFaultsDisabled(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusNotFound, errorBody{Error: errorDetail{
		Code:    "faults_disabled",
		Message: "fault injection is disabled; start the process with ENABLE_FAULT_INJECTION=true",
	}})
}
