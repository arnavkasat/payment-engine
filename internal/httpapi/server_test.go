package httpapi_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/arnavkasat/payment-engine/internal/httpapi"
	"github.com/arnavkasat/payment-engine/internal/metrics"
	"github.com/arnavkasat/payment-engine/internal/money"
	"github.com/arnavkasat/payment-engine/internal/payments"
	"github.com/arnavkasat/payment-engine/internal/storage"
	"github.com/arnavkasat/payment-engine/internal/storage/memory"
)

type apiHarness struct {
	t      *testing.T
	server *httptest.Server
	store  *memory.Store
	reg    *metrics.Registry

	world, alice, bob, clearing, fees uuid.UUID
}

func newAPI(t *testing.T) *apiHarness {
	t.Helper()
	ctx := context.Background()
	store := memory.New()
	reg := metrics.NewRegistry()

	mk := func(name string, kind storage.AccountKind, allowNegative bool) uuid.UUID {
		a, err := store.CreateAccount(ctx, storage.Account{Name: name, Currency: "USD", Kind: kind, AllowNegative: allowNegative})
		if err != nil {
			t.Fatalf("CreateAccount(%q): %v", name, err)
		}
		return a.ID
	}
	h := &apiHarness{t: t, store: store, reg: reg}
	h.world = mk("world", storage.KindExternal, true)
	h.alice = mk("alice", storage.KindWallet, false)
	h.bob = mk("bob", storage.KindWallet, false)
	h.clearing = mk("clearing", storage.KindClearing, false)
	h.fees = mk("fees", storage.KindRevenue, false)

	svc, err := payments.New(store, payments.Config{
		Clearing: []uuid.UUID{h.clearing}, Fees: []uuid.UUID{h.fees}, External: h.world, Metrics: reg,
	})
	if err != nil {
		t.Fatalf("payments.New: %v", err)
	}
	srv, err := httpapi.New(httpapi.Config{Service: svc, Store: store, Metrics: reg})
	if err != nil {
		t.Fatalf("httpapi.New: %v", err)
	}
	h.server = httptest.NewServer(srv)
	t.Cleanup(h.server.Close)

	// Fund Alice so the payment tests have something to move.
	resp := h.do("POST", fmt.Sprintf("/v1/accounts/%s/deposits", h.alice), "fund-alice",
		`{"amount":1000000,"currency":"USD"}`)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("funding alice: status %d", resp.StatusCode)
	}
	resp.Body.Close()
	return h
}

func (h *apiHarness) do(method, path, idempotencyKey, body string) *http.Response {
	h.t.Helper()
	var reader *bytes.Reader
	if body == "" {
		reader = bytes.NewReader(nil)
	} else {
		reader = bytes.NewReader([]byte(body))
	}
	req, err := http.NewRequest(method, h.server.URL+path, reader)
	if err != nil {
		h.t.Fatalf("NewRequest: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if idempotencyKey != "" {
		req.Header.Set(httpapi.HeaderIdempotencyKey, idempotencyKey)
	}
	resp, err := h.server.Client().Do(req)
	if err != nil {
		h.t.Fatalf("%s %s: %v", method, path, err)
	}
	return resp
}

func (h *apiHarness) payment(key string, amount, fee money.Minor) *http.Response {
	h.t.Helper()
	body := fmt.Sprintf(`{"payer_account":%q,"payee_account":%q,"amount":%d,"fee":%d,"currency":"USD","reference":"inv-1"}`,
		h.alice, h.bob, amount, fee)
	return h.do("POST", "/v1/payments", key, body)
}

func decodeBody[T any](t *testing.T, resp *http.Response) T {
	t.Helper()
	defer resp.Body.Close()
	var out T
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decoding body: %v", err)
	}
	return out
}

func TestCreatePaymentReturnsCreated(t *testing.T) {
	h := newAPI(t)
	resp := h.payment("key-1", 10_000, 250)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want 201", resp.StatusCode)
	}
	if got := resp.Header.Get("Content-Type"); !strings.HasPrefix(got, "application/json") {
		t.Errorf("Content-Type = %q", got)
	}
	if resp.Header.Get(httpapi.HeaderIdempotentReplay) != "" {
		t.Error("first request was marked as a replay")
	}
	payment := decodeBody[storage.Payment](t, resp)
	if payment.Amount != 10_000 || payment.Fee != 250 || payment.Status != storage.StatusCaptured {
		t.Errorf("payment = %+v", payment)
	}
	if loc := resp.Header.Get("Location"); loc != "/v1/payments/"+payment.ID.String() {
		t.Errorf("Location = %q", loc)
	}

	got := h.do("GET", "/v1/payments/"+payment.ID.String(), "", "")
	if got.StatusCode != http.StatusOK {
		t.Fatalf("GET status = %d, want 200", got.StatusCode)
	}
	fetched := decodeBody[storage.Payment](t, got)
	if fetched.ID != payment.ID {
		t.Errorf("GET returned %s, want %s", fetched.ID, payment.ID)
	}
}

// TestRetryReturnsTheStoredResponseByteForByte is the API-level statement of
// the idempotency guarantee.
func TestRetryReturnsTheStoredResponseByteForByte(t *testing.T) {
	h := newAPI(t)

	first := h.payment("key-1", 10_000, 250)
	firstBody := readAll(t, first)
	if first.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want 201", first.StatusCode)
	}

	for i := 0; i < 3; i++ {
		again := h.payment("key-1", 10_000, 250)
		againBody := readAll(t, again)
		if again.StatusCode != http.StatusCreated {
			t.Fatalf("retry %d status = %d, want 201", i, again.StatusCode)
		}
		if againBody != firstBody {
			t.Fatalf("retry %d body differs:\n got %s\nwant %s", i, againBody, firstBody)
		}
		if again.Header.Get(httpapi.HeaderIdempotentReplay) != "true" {
			t.Errorf("retry %d missing the replay header", i)
		}
	}

	account := decodeBody[storage.Account](t, h.do("GET", "/v1/accounts/"+h.alice.String(), "", ""))
	if want := money.Minor(1_000_000 - 10_250); account.Balance != want {
		t.Errorf("alice = %d, want %d: she was charged more than once", account.Balance, want)
	}
}

func TestMissingIdempotencyKeyIsRejected(t *testing.T) {
	h := newAPI(t)
	resp := h.payment("", 10_000, 0)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
	body := decodeBody[map[string]map[string]string](t, resp)
	if got := body["error"]["code"]; got != "idempotency_key_required" {
		t.Errorf("code = %q", got)
	}
}

func TestKeyReusedWithADifferentBody(t *testing.T) {
	h := newAPI(t)
	h.payment("key-1", 10_000, 0).Body.Close()
	resp := h.payment("key-1", 20_000, 0)
	if resp.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422", resp.StatusCode)
	}
	body := decodeBody[map[string]map[string]string](t, resp)
	if got := body["error"]["code"]; got != "idempotency_key_reused" {
		t.Errorf("code = %q", got)
	}
}

func TestInsufficientFunds(t *testing.T) {
	h := newAPI(t)
	resp := h.payment("key-1", 99_999_999, 0)
	if resp.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422", resp.StatusCode)
	}
	body := decodeBody[map[string]map[string]string](t, resp)
	if got := body["error"]["code"]; got != "insufficient_funds" {
		t.Errorf("code = %q", got)
	}
}

func TestBadRequests(t *testing.T) {
	h := newAPI(t)
	for _, tc := range []struct {
		name, body string
		status     int
		code       string
	}{
		{"not json", `{`, http.StatusBadRequest, "invalid_request"},
		{"unknown field", fmt.Sprintf(`{"payer_account":%q,"payee_account":%q,"amount":1,"currency":"USD","amonut":2}`, h.alice, h.bob), http.StatusBadRequest, "invalid_request"},
		{"zero amount", fmt.Sprintf(`{"payer_account":%q,"payee_account":%q,"amount":0,"currency":"USD"}`, h.alice, h.bob), http.StatusBadRequest, "invalid_request"},
		{"two json values", `{} {}`, http.StatusBadRequest, "invalid_request"},
		{"unknown payee", fmt.Sprintf(`{"payer_account":%q,"payee_account":"%s","amount":5,"currency":"USD"}`, h.alice, uuid.New()), http.StatusNotFound, "not_found"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp := h.do("POST", "/v1/payments", "key-"+tc.name, tc.body)
			if resp.StatusCode != tc.status {
				t.Fatalf("status = %d, want %d", resp.StatusCode, tc.status)
			}
			body := decodeBody[map[string]map[string]string](t, resp)
			if got := body["error"]["code"]; got != tc.code {
				t.Errorf("code = %q, want %q", got, tc.code)
			}
		})
	}
}

func TestOversizedBodyIsRejected(t *testing.T) {
	h := newAPI(t)
	resp := h.do("POST", "/v1/payments", "key-big",
		fmt.Sprintf(`{"payer_account":%q,"payee_account":%q,"amount":1,"currency":"USD","reference":%q}`,
			h.alice, h.bob, strings.Repeat("x", httpapi.MaxBodyBytes)))
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

func TestMalformedUUIDInPath(t *testing.T) {
	h := newAPI(t)
	resp := h.do("GET", "/v1/payments/not-a-uuid", "", "")
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
	resp.Body.Close()
}

func TestUnknownPaymentIs404(t *testing.T) {
	h := newAPI(t)
	resp := h.do("GET", "/v1/payments/"+uuid.NewString(), "", "")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
	resp.Body.Close()
}

func TestEntriesEndpointShowsBalancedDoubleEntry(t *testing.T) {
	h := newAPI(t)
	payment := decodeBody[storage.Payment](t, h.payment("key-1", 10_000, 250))

	resp := h.do("GET", "/v1/payments/"+payment.ID.String()+"/entries", "", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var body struct {
		Balanced bool `json:"balanced"`
		Entries  []struct {
			Kind     string `json:"kind"`
			Sum      int64  `json:"sum"`
			Postings []struct {
				Amount int64 `json:"amount"`
			} `json:"postings"`
		} `json:"entries"`
	}
	defer resp.Body.Close()
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !body.Balanced {
		t.Error("balanced = false")
	}
	if len(body.Entries) != 1 {
		t.Fatalf("got %d entries, want 1", len(body.Entries))
	}
	if body.Entries[0].Kind != "capture" || body.Entries[0].Sum != 0 {
		t.Errorf("entry = %+v", body.Entries[0])
	}
	if len(body.Entries[0].Postings) != 3 {
		t.Errorf("got %d postings, want 3 (payer, clearing, fees)", len(body.Entries[0].Postings))
	}
}

func TestAccountEndpoints(t *testing.T) {
	h := newAPI(t)

	resp := h.do("POST", "/v1/accounts", "", `{"name":"carol","currency":"USD","kind":"wallet"}`)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want 201", resp.StatusCode)
	}
	carol := decodeBody[storage.Account](t, resp)
	if carol.Name != "carol" || carol.Kind != storage.KindWallet {
		t.Errorf("account = %+v", carol)
	}

	dup := h.do("POST", "/v1/accounts", "", `{"name":"carol","currency":"USD","kind":"wallet"}`)
	if dup.StatusCode != http.StatusConflict {
		t.Errorf("duplicate name status = %d, want 409", dup.StatusCode)
	}
	dup.Body.Close()

	bad := h.do("POST", "/v1/accounts", "", `{"name":"dave","currency":"USD","kind":"slush-fund"}`)
	if bad.StatusCode != http.StatusBadRequest {
		t.Errorf("bad kind status = %d, want 400", bad.StatusCode)
	}
	bad.Body.Close()

	list := decodeBody[map[string][]storage.Account](t, h.do("GET", "/v1/accounts", "", ""))
	if len(list["accounts"]) != 6 {
		t.Errorf("listed %d accounts, want 6", len(list["accounts"]))
	}
}

func TestDepositIsIdempotent(t *testing.T) {
	h := newAPI(t)
	path := "/v1/accounts/" + h.bob.String() + "/deposits"

	first := h.do("POST", path, "dep-1", `{"amount":5000,"currency":"USD"}`)
	firstBody := readAll(t, first)
	if first.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want 201", first.StatusCode)
	}

	again := h.do("POST", path, "dep-1", `{"amount":5000,"currency":"USD"}`)
	againBody := readAll(t, again)
	if againBody != firstBody || again.Header.Get(httpapi.HeaderIdempotentReplay) != "true" {
		t.Errorf("replay body = %s, want %s", againBody, firstBody)
	}

	account := decodeBody[storage.Account](t, h.do("GET", "/v1/accounts/"+h.bob.String(), "", ""))
	if account.Balance != 5000 {
		t.Errorf("bob = %d, want 5000", account.Balance)
	}
}

func TestInvariantsEndpointReportsHealth(t *testing.T) {
	h := newAPI(t)
	h.payment("key-1", 10_000, 250).Body.Close()

	resp := h.do("GET", "/v1/ledger/invariants", "", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	body := decodeBody[map[string]any](t, resp)
	if ok, _ := body["ok"].(bool); !ok {
		t.Errorf("ok = false for a healthy ledger: %v", body)
	}

	// Corrupt a balance and the same endpoint must fail loudly.
	h.store.Corrupt(h.bob, 1)
	broken := h.do("GET", "/v1/ledger/invariants", "", "")
	if broken.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500 for a broken ledger", broken.StatusCode)
	}
	brokenBody := decodeBody[map[string]any](t, broken)
	if ok, _ := brokenBody["ok"].(bool); ok {
		t.Error("ok = true for a corrupted ledger")
	}
}

func TestProbesAndMetrics(t *testing.T) {
	h := newAPI(t)
	for _, path := range []string{"/healthz", "/readyz"} {
		resp := h.do("GET", path, "", "")
		if resp.StatusCode != http.StatusOK {
			t.Errorf("%s status = %d, want 200", path, resp.StatusCode)
		}
		resp.Body.Close()
	}

	h.payment("key-1", 10_000, 0).Body.Close()

	resp := h.do("GET", "/metrics", "", "")
	body := readAll(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/metrics status = %d, want 200", resp.StatusCode)
	}
	for _, want := range []string{
		"http_requests_total",
		"http_request_seconds_bucket",
		"payments_total",
		`route="POST /v1/payments"`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("/metrics missing %q", want)
		}
	}
	// The route label must be the pattern, not the path, or each payment id
	// would mint its own time series.
	if strings.Contains(body, "/v1/payments/"+uuid.Nil.String()) {
		t.Error("/metrics contains a raw path")
	}
}

func readAll(t *testing.T, resp *http.Response) string {
	t.Helper()
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading body: %v", err)
	}
	return string(b)
}
