package httpapi_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/arnavkasat/payment-engine/internal/faults"
	"github.com/arnavkasat/payment-engine/internal/httpapi"
	"github.com/arnavkasat/payment-engine/internal/metrics"
	"github.com/arnavkasat/payment-engine/internal/payments"
	"github.com/arnavkasat/payment-engine/internal/storage"
)

func TestDashboardIsServedAtRootOnly(t *testing.T) {
	h := newAPI(t)

	resp := h.do("GET", "/", "", "")
	body := readAll(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/html") {
		t.Errorf("Content-Type = %q, want text/html", ct)
	}
	for _, want := range []string{"<!doctype html>", "Payment engine", "/v1/ledger/snapshot"} {
		if !strings.Contains(body, want) {
			t.Errorf("dashboard missing %q", want)
		}
	}

	// "GET /" matches every unrouted path, so anything else must still 404
	// rather than quietly rendering the dashboard.
	for _, path := range []string{"/nope", "/v1/", "/v1/nonsense"} {
		missing := h.do("GET", path, "", "")
		if missing.StatusCode != http.StatusNotFound {
			t.Errorf("GET %s = %d, want 404", path, missing.StatusCode)
		}
		missing.Body.Close()
	}
}

func TestSnapshotReportsTheWholeLedger(t *testing.T) {
	h := newAPI(t)
	h.payment("key-1", 10_000, 250).Body.Close()

	resp := h.do("GET", "/v1/ledger/snapshot", "", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var snap struct {
		Accounts        []storage.Account `json:"accounts"`
		NetBalance      int64             `json:"net_balance"`
		ClearingBalance int64             `json:"clearing_balance"`
		InFlight        int               `json:"in_flight"`
		Settled         int               `json:"settled"`
		PendingOutbox   int               `json:"pending_outbox"`
		FaultsEnabled   bool              `json:"faults_enabled"`
		Invariants      struct {
			OK         bool  `json:"ok"`
			Postings   int   `json:"postings"`
			PostingSum int64 `json:"posting_sum"`
			Violations []struct {
				Rule string `json:"rule"`
			} `json:"violations"`
		} `json:"invariants"`
	}
	defer resp.Body.Close()
	if err := json.NewDecoder(resp.Body).Decode(&snap); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if !snap.Invariants.OK || snap.Invariants.PostingSum != 0 {
		t.Errorf("invariants = %+v", snap.Invariants)
	}
	if snap.NetBalance != 0 {
		t.Errorf("net balance = %d, want 0", snap.NetBalance)
	}
	if snap.ClearingBalance != 10_000 {
		t.Errorf("clearing = %d, want 10000: the capture parks funds there", snap.ClearingBalance)
	}
	if snap.InFlight != 1 || snap.Settled != 0 {
		t.Errorf("in flight = %d, settled = %d, want 1 and 0", snap.InFlight, snap.Settled)
	}
	if snap.PendingOutbox != 1 {
		t.Errorf("pending outbox = %d, want 1", snap.PendingOutbox)
	}
	if snap.FaultsEnabled {
		t.Error("faults reported as enabled without an injector")
	}
	if len(snap.Accounts) == 0 {
		t.Error("no accounts returned")
	}
	// Absent collections must marshal as [] so the page can iterate them.
	if snap.Invariants.Violations == nil {
		t.Error("violations marshalled as null, want []")
	}
}

func TestSnapshotSurfacesViolations(t *testing.T) {
	h := newAPI(t)
	h.store.Corrupt(h.bob, 7)

	var snap struct {
		Invariants struct {
			OK         bool `json:"ok"`
			Violations []struct {
				Rule   string `json:"rule"`
				Detail string `json:"detail"`
			} `json:"violations"`
		} `json:"invariants"`
		NetBalance int64 `json:"net_balance"`
	}
	resp := h.do("GET", "/v1/ledger/snapshot", "", "")
	defer resp.Body.Close()
	if err := json.NewDecoder(resp.Body).Decode(&snap); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if snap.Invariants.OK {
		t.Fatal("snapshot reported a corrupted ledger as healthy")
	}
	if len(snap.Invariants.Violations) == 0 {
		t.Fatal("no violations reported")
	}
	if snap.NetBalance == 0 {
		t.Error("net balance still zero after an unjournalled credit")
	}
}

func TestFaultEndpointsAreOffUnlessAnInjectorIsWired(t *testing.T) {
	h := newAPI(t)
	for _, method := range []string{"GET", "POST"} {
		body := ""
		if method == "POST" {
			body = `{"point":"capture.before_commit","probability":1}`
		}
		resp := h.do(method, "/v1/faults", "", body)
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("%s /v1/faults = %d, want 404 when disabled", method, resp.StatusCode)
		}
		got := decodeBody[map[string]map[string]string](t, resp)
		if code := got["error"]["code"]; code != "faults_disabled" {
			t.Errorf("code = %q, want faults_disabled", code)
		}
	}
}

// faultAPI builds a server with fault injection wired, the way the dashboard
// needs it.
func faultAPI(t *testing.T) (*apiHarness, *faults.Injector) {
	t.Helper()
	h := newAPI(t)
	injector := faults.New(1)
	svc, err := payments.New(h.store, payments.Config{
		Clearing: h.clearing, Fees: h.fees, External: h.world,
		Faults: injector, Metrics: metrics.NewRegistry(),
	})
	if err != nil {
		t.Fatalf("payments.New: %v", err)
	}
	srv, err := httpapi.New(httpapi.Config{
		Service: svc, Store: h.store, Metrics: h.reg, Faults: injector,
	})
	if err != nil {
		t.Fatalf("httpapi.New: %v", err)
	}
	h.server.Config.Handler = srv
	return h, injector
}

func TestArmingAFaultMakesPaymentsFail(t *testing.T) {
	h, injector := faultAPI(t)

	listed := decodeBody[map[string]any](t, h.do("GET", "/v1/faults", "", ""))
	points, _ := listed["points"].([]any)
	if len(points) != len(faults.AllPoints) {
		t.Fatalf("listed %d points, want %d", len(points), len(faults.AllPoints))
	}
	if enabled, _ := listed["enabled"].(bool); !enabled {
		t.Error("enabled = false with an injector wired")
	}

	armed := h.do("POST", "/v1/faults", "", `{"point":"capture.before_commit","probability":1}`)
	if armed.StatusCode != http.StatusOK {
		t.Fatalf("arming = %d, want 200", armed.StatusCode)
	}
	armed.Body.Close()

	// With the point armed on every attempt, a capture must fail and leave
	// nothing behind.
	failed := h.do("POST", "/v1/payments", "key-armed",
		`{"payer_account":"`+h.alice.String()+`","payee_account":"`+h.bob.String()+`","amount":100,"currency":"USD"}`)
	if failed.StatusCode != http.StatusInternalServerError {
		t.Errorf("payment with a fault armed = %d, want 500", failed.StatusCode)
	}
	failed.Body.Close()
	if injector.Fired(faults.BeforeCommit) == 0 {
		t.Error("no fault fired")
	}

	snap := decodeBody[map[string]any](t, h.do("GET", "/v1/ledger/snapshot", "", ""))
	inv, _ := snap["invariants"].(map[string]any)
	if ok, _ := inv["ok"].(bool); !ok {
		t.Error("ledger unbalanced after an aborted capture")
	}

	// Disarming restores normal service, and the same key is reusable
	// because the failed attempt rolled back.
	off := h.do("POST", "/v1/faults", "", `{"disable":true}`)
	off.Body.Close()
	ok := h.do("POST", "/v1/payments", "key-armed",
		`{"payer_account":"`+h.alice.String()+`","payee_account":"`+h.bob.String()+`","amount":100,"currency":"USD"}`)
	if ok.StatusCode != http.StatusCreated {
		t.Errorf("payment after disarming = %d, want 201", ok.StatusCode)
	}
	ok.Body.Close()
}

func TestFaultRequestValidation(t *testing.T) {
	h, _ := faultAPI(t)
	for _, tc := range []struct {
		name, body string
	}{
		{"unknown point", `{"point":"not.a.point","probability":0.5}`},
		{"probability above one", `{"point":"capture.before_commit","probability":1.5}`},
		{"negative probability", `{"point":"capture.before_commit","probability":-0.1}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp := h.do("POST", "/v1/faults", "", tc.body)
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400", resp.StatusCode)
			}
		})
	}
}
