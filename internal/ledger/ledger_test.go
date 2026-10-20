package ledger

import (
	"errors"
	"math/rand"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/arnavkasat/payment-engine/internal/money"
)

var now = time.Date(2026, 2, 1, 12, 0, 0, 0, time.UTC)

func TestNewCapturePostsZeroSumWithFee(t *testing.T) {
	payer, clearing, fees := uuid.New(), uuid.New(), uuid.New()
	e, err := NewCapture(uuid.New(), payer, clearing, fees, "USD", 1000, 25, now)
	if err != nil {
		t.Fatalf("NewCapture: %v", err)
	}
	if len(e.Postings) != 3 {
		t.Fatalf("got %d postings, want 3", len(e.Postings))
	}
	d := e.Deltas()
	if d[payer] != -1025 {
		t.Errorf("payer delta = %d, want -1025", d[payer])
	}
	if d[clearing] != 1000 {
		t.Errorf("clearing delta = %d, want 1000", d[clearing])
	}
	if d[fees] != 25 {
		t.Errorf("fee delta = %d, want 25", d[fees])
	}
	if err := e.Validate(); err != nil {
		t.Errorf("Validate: %v", err)
	}
	if e.ID == uuid.Nil || e.Postings[0].EntryID != e.ID {
		t.Error("entry and posting identifiers not stamped")
	}
}

func TestNewCaptureWithoutFeeIsTwoLegged(t *testing.T) {
	e, err := NewCapture(uuid.New(), uuid.New(), uuid.New(), uuid.Nil, "USD", 500, 0, now)
	if err != nil {
		t.Fatalf("NewCapture: %v", err)
	}
	if len(e.Postings) != 2 {
		t.Fatalf("got %d postings, want 2", len(e.Postings))
	}
}

func TestNewCaptureRejectsBadInput(t *testing.T) {
	for _, tc := range []struct {
		name        string
		amount, fee money.Minor
		feeAccount  uuid.UUID
	}{
		{"zero amount", 0, 0, uuid.Nil},
		{"negative amount", -1, 0, uuid.Nil},
		{"negative fee", 100, -1, uuid.New()},
		{"fee without account", 100, 5, uuid.Nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewCapture(uuid.New(), uuid.New(), uuid.New(), tc.feeAccount, "USD", tc.amount, tc.fee, now); err == nil {
				t.Fatal("want error, got nil")
			}
		})
	}
}

func TestValidateCatchesBrokenEntries(t *testing.T) {
	acct := uuid.New()
	for _, tc := range []struct {
		name  string
		entry Entry
		want  error
	}{
		{"no currency", Entry{Postings: []Posting{{AccountID: acct, Amount: 1}, {AccountID: uuid.New(), Amount: -1}}}, ErrMissingCurrency},
		{"one posting", Entry{Currency: "USD", Postings: []Posting{{AccountID: acct, Amount: 1}}}, ErrTooFewPostings},
		{"unbalanced", Entry{Currency: "USD", Postings: []Posting{{AccountID: acct, Amount: 10}, {AccountID: uuid.New(), Amount: -9}}}, ErrNotBalanced},
		{"zero leg", Entry{Currency: "USD", Postings: []Posting{{AccountID: acct, Amount: 0}, {AccountID: uuid.New(), Amount: 0}}}, ErrZeroPosting},
		{"duplicate account", Entry{Currency: "USD", Postings: []Posting{{AccountID: acct, Amount: 5}, {AccountID: acct, Amount: -5}}}, ErrDuplicateLeg},
		{"missing account", Entry{Currency: "USD", Postings: []Posting{{AccountID: uuid.Nil, Amount: 5}, {AccountID: acct, Amount: -5}}}, ErrMissingAccount},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.entry.Validate(); !errors.Is(err, tc.want) {
				t.Fatalf("Validate = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestSettlementAndReversalConserveValue(t *testing.T) {
	clearing, payee := uuid.New(), uuid.New()
	s, err := NewSettlement(uuid.New(), clearing, payee, "USD", 1000, now)
	if err != nil {
		t.Fatalf("NewSettlement: %v", err)
	}
	if s.Deltas()[clearing] != -1000 || s.Deltas()[payee] != 1000 {
		t.Fatalf("unexpected settlement deltas: %v", s.Deltas())
	}
	r, err := NewReversal(s, now)
	if err != nil {
		t.Fatalf("NewReversal: %v", err)
	}
	if r.Kind != KindReversal {
		t.Errorf("kind = %q, want %q", r.Kind, KindReversal)
	}
	// A settlement and its reversal together move nothing.
	for acct, delta := range s.Deltas() {
		if sum := delta + r.Deltas()[acct]; sum != 0 {
			t.Errorf("account %s nets %d after reversal, want 0", acct, sum)
		}
	}
}

// TestRandomCapturesAlwaysBalance is the conservation law stated as a
// property: no combination of amounts and fees the constructor accepts can
// produce an entry whose postings fail to sum to zero.
func TestRandomCapturesAlwaysBalance(t *testing.T) {
	rnd := rand.New(rand.NewSource(7))
	payer, clearing, fees := uuid.New(), uuid.New(), uuid.New()
	var running money.Minor
	for i := 0; i < 2000; i++ {
		amount := money.Minor(rnd.Int63n(1_000_000) + 1)
		fee := money.Minor(rnd.Int63n(1000))
		e, err := NewCapture(uuid.New(), payer, clearing, fees, "USD", amount, fee, now)
		if err != nil {
			t.Fatalf("NewCapture(%d, %d): %v", amount, fee, err)
		}
		if err := e.Validate(); err != nil {
			t.Fatalf("entry %d invalid: %v", i, err)
		}
		for _, p := range e.Postings {
			running += p.Amount
		}
		if running != 0 {
			t.Fatalf("running total after entry %d = %d, want 0", i, running)
		}
	}
}

func TestReportSummary(t *testing.T) {
	var r Report
	if !r.OK() || r.Err() != nil {
		t.Fatal("empty report should be OK")
	}
	if got := r.Summary(); got != "all invariants hold" {
		t.Errorf("Summary = %q", got)
	}
	r.Add(RuleEntryZeroSum, "entry %s sums to %d", "abc", 5)
	r.Add(RuleEntryZeroSum, "entry %s sums to %d", "def", 7)
	r.Add(RuleGlobalZeroSum, "total is %d", 12)
	if r.OK() || r.Err() == nil {
		t.Fatal("report with violations should not be OK")
	}
	got := r.Summary()
	want := "entry_zero_sum [entry abc sums to 5 (+1 more)]"
	if !contains(got, want) || !contains(got, "global_zero_sum [total is 12]") {
		t.Errorf("Summary = %q, want it to contain %q", got, want)
	}
}

func contains(haystack, needle string) bool {
	return len(haystack) >= len(needle) && (haystack == needle || indexOf(haystack, needle) >= 0)
}

func indexOf(h, n string) int {
	for i := 0; i+len(n) <= len(h); i++ {
		if h[i:i+len(n)] == n {
			return i
		}
	}
	return -1
}
