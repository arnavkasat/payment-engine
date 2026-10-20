package httpapi

import (
	"fmt"
	"net/http"
	"time"

	"github.com/arnavkasat/payment-engine/internal/ledger"
	"github.com/arnavkasat/payment-engine/internal/money"
	"github.com/arnavkasat/payment-engine/internal/payments"
	"github.com/arnavkasat/payment-engine/internal/storage"
)

// requestTimeout bounds a write. It is generous enough to absorb a lock wait
// and a serialisation retry, and short enough that a stuck request frees its
// connection rather than holding one of a finite pool forever.
const requestTimeout = 10 * time.Second

// createPayment handles POST /v1/payments.
func (s *Server) createPayment(w http.ResponseWriter, r *http.Request) {
	var body payments.CreateRequest
	if err := decode(r, &body); err != nil {
		s.fail(w, r, err)
		return
	}
	body.IdempotencyKey = r.Header.Get(HeaderIdempotencyKey)

	ctx, cancel := contextWithTimeout(r, requestTimeout)
	defer cancel()

	result, err := s.svc.CreatePayment(ctx, body)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	if result.Replayed {
		w.Header().Set(HeaderIdempotentReplay, "true")
	}
	w.Header().Set("Location", "/v1/payments/"+result.Payment.ID.String())
	// The stored bytes are returned verbatim, so a retry is byte-identical
	// to the response the client missed.
	writeRaw(w, result.StatusCode, result.Response)
}

// getPayment handles GET /v1/payments/{id}.
func (s *Server) getPayment(w http.ResponseWriter, r *http.Request) {
	id, err := pathUUID(r, "id")
	if err != nil {
		s.fail(w, r, err)
		return
	}
	ctx, cancel := contextWithTimeout(r, requestTimeout)
	defer cancel()

	payment, err := s.svc.GetPayment(ctx, id)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, payment)
}

// entriesResponse is the journal behind a payment, with each entry's legs.
type entriesResponse struct {
	PaymentID string      `json:"payment_id"`
	Entries   []entryView `json:"entries"`
	Balanced  bool        `json:"balanced"`
}

type entryView struct {
	ID        string           `json:"id"`
	Kind      ledger.EntryKind `json:"kind"`
	Currency  money.Currency   `json:"currency"`
	CreatedAt time.Time        `json:"created_at"`
	Postings  []postingView    `json:"postings"`
	Sum       money.Minor      `json:"sum"`
}

type postingView struct {
	AccountID string      `json:"account_id"`
	Amount    money.Minor `json:"amount"`
}

// getPaymentEntries handles GET /v1/payments/{id}/entries. It exists so that
// the double entry behind a payment can be inspected directly rather than
// taken on trust.
func (s *Server) getPaymentEntries(w http.ResponseWriter, r *http.Request) {
	id, err := pathUUID(r, "id")
	if err != nil {
		s.fail(w, r, err)
		return
	}
	ctx, cancel := contextWithTimeout(r, requestTimeout)
	defer cancel()

	if _, err := s.svc.GetPayment(ctx, id); err != nil {
		s.fail(w, r, err)
		return
	}
	entries, err := s.svc.GetEntries(ctx, id)
	if err != nil {
		s.fail(w, r, err)
		return
	}

	out := entriesResponse{PaymentID: id.String(), Entries: []entryView{}, Balanced: true}
	for _, e := range entries {
		view := entryView{
			ID: e.ID.String(), Kind: e.Kind, Currency: e.Currency,
			CreatedAt: e.CreatedAt, Postings: []postingView{},
		}
		for _, p := range e.Postings {
			view.Postings = append(view.Postings, postingView{AccountID: p.AccountID.String(), Amount: p.Amount})
			view.Sum += p.Amount
		}
		if view.Sum != 0 {
			out.Balanced = false
		}
		out.Entries = append(out.Entries, view)
	}
	writeJSON(w, http.StatusOK, out)
}

// createAccountRequest is the body of POST /v1/accounts.
type createAccountRequest struct {
	Name          string              `json:"name"`
	Currency      money.Currency      `json:"currency"`
	Kind          storage.AccountKind `json:"kind"`
	AllowNegative bool                `json:"allow_negative"`
}

func (s *Server) createAccount(w http.ResponseWriter, r *http.Request) {
	var body createAccountRequest
	if err := decode(r, &body); err != nil {
		s.fail(w, r, err)
		return
	}
	if body.Name == "" {
		s.fail(w, r, fmt.Errorf("%w: name is required", payments.ErrInvalidRequest))
		return
	}
	if _, err := money.ParseCurrency(string(body.Currency)); err != nil {
		s.fail(w, r, fmt.Errorf("%w: %v", payments.ErrInvalidRequest, err))
		return
	}
	switch body.Kind {
	case storage.KindWallet, storage.KindClearing, storage.KindRevenue, storage.KindExternal:
	default:
		s.fail(w, r, fmt.Errorf("%w: kind %q is not one of wallet, clearing, revenue, external",
			payments.ErrInvalidRequest, body.Kind))
		return
	}

	ctx, cancel := contextWithTimeout(r, requestTimeout)
	defer cancel()

	account, err := s.store.CreateAccount(ctx, storage.Account{
		Name:          body.Name,
		Currency:      body.Currency,
		Kind:          body.Kind,
		AllowNegative: body.AllowNegative,
	})
	if err != nil {
		s.fail(w, r, err)
		return
	}
	w.Header().Set("Location", "/v1/accounts/"+account.ID.String())
	writeJSON(w, http.StatusCreated, account)
}

func (s *Server) getAccount(w http.ResponseWriter, r *http.Request) {
	id, err := pathUUID(r, "id")
	if err != nil {
		s.fail(w, r, err)
		return
	}
	ctx, cancel := contextWithTimeout(r, requestTimeout)
	defer cancel()

	account, err := s.store.GetAccount(ctx, id)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, account)
}

func (s *Server) listAccounts(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := contextWithTimeout(r, requestTimeout)
	defer cancel()

	accounts, err := s.store.ListAccounts(ctx)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	if accounts == nil {
		accounts = []storage.Account{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"accounts": accounts})
}

// depositRequest is the body of POST /v1/accounts/{id}/deposits. The account
// comes from the path, so it is not repeated here.
type depositRequest struct {
	Amount    money.Minor    `json:"amount"`
	Currency  money.Currency `json:"currency"`
	Reference string         `json:"reference,omitempty"`
}

func (s *Server) deposit(w http.ResponseWriter, r *http.Request) {
	id, err := pathUUID(r, "id")
	if err != nil {
		s.fail(w, r, err)
		return
	}
	var body depositRequest
	if err := decode(r, &body); err != nil {
		s.fail(w, r, err)
		return
	}

	ctx, cancel := contextWithTimeout(r, requestTimeout)
	defer cancel()

	result, err := s.svc.Deposit(ctx, payments.DepositRequest{
		IdempotencyKey: r.Header.Get(HeaderIdempotencyKey),
		Account:        id,
		Amount:         body.Amount,
		Currency:       body.Currency,
		Reference:      body.Reference,
	})
	if err != nil {
		s.fail(w, r, err)
		return
	}
	if result.Replayed {
		w.Header().Set(HeaderIdempotentReplay, "true")
	}
	writeRaw(w, result.StatusCode, result.Response)
}

// verifyInvariants handles GET /v1/ledger/invariants. It returns 200 when
// the books balance and 500 when they do not, so that a monitoring check can
// treat the ledger's integrity as an ordinary health signal.
func (s *Server) verifyInvariants(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := contextWithTimeout(r, 30*time.Second)
	defer cancel()

	report, err := s.store.VerifyInvariants(ctx)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	status := http.StatusOK
	if !report.OK() {
		status = http.StatusInternalServerError
		s.log.Error("ledger invariants violated", "summary", report.Summary())
	}
	if report.Violations == nil {
		report.Violations = []ledger.Violation{}
	}
	writeJSON(w, status, map[string]any{"ok": report.OK(), "report": report})
}
