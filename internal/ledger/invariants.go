package ledger

import (
	"fmt"
	"sort"
	"strings"

	"github.com/arnavkasat/payment-engine/internal/money"
)

// Rule names the ledger invariant a violation broke. The names are stable:
// they appear in chaos-run output, in metrics and in alerts.
type Rule string

const (
	// RuleGlobalZeroSum: the signed amounts of every posting ever written
	// sum to zero. Value is only ever moved, never minted.
	RuleGlobalZeroSum Rule = "global_zero_sum"
	// RuleEntryZeroSum: each individual journal entry sums to zero, so no
	// single transaction can leave the books lopsided.
	RuleEntryZeroSum Rule = "entry_zero_sum"
	// RuleBalanceMatchesPostings: each account's stored balance equals the
	// sum of its postings. Catches a balance update that drifted from the
	// journal it was supposed to mirror.
	RuleBalanceMatchesPostings Rule = "balance_matches_postings"
	// RuleNoUnauthorisedOverdraft: accounts that forbid it are never negative.
	RuleNoUnauthorisedOverdraft Rule = "no_unauthorised_overdraft"
	// RuleSingleEntryPerKind: a payment has at most one capture and at most
	// one settlement. A duplicate means a retry was double-applied.
	RuleSingleEntryPerKind Rule = "single_entry_per_kind"
)

// AllRules lists every invariant, in report order.
var AllRules = []Rule{
	RuleGlobalZeroSum,
	RuleEntryZeroSum,
	RuleBalanceMatchesPostings,
	RuleNoUnauthorisedOverdraft,
	RuleSingleEntryPerKind,
}

// Violation is one broken invariant, with enough detail to find the rows.
type Violation struct {
	Rule   Rule   `json:"rule"`
	Detail string `json:"detail"`
}

func (v Violation) String() string { return string(v.Rule) + ": " + v.Detail }

// Report is the outcome of a full invariant verification.
type Report struct {
	Violations      []Violation `json:"violations"`
	Accounts        int         `json:"accounts"`
	Entries         int         `json:"entries"`
	Postings        int         `json:"postings"`
	Payments        int         `json:"payments"`
	TotalPostingSum money.Minor `json:"total_posting_sum"`
}

// Add records a violation.
func (r *Report) Add(rule Rule, format string, args ...any) {
	r.Violations = append(r.Violations, Violation{Rule: rule, Detail: fmt.Sprintf(format, args...)})
}

// OK reports whether every invariant held.
func (r Report) OK() bool { return len(r.Violations) == 0 }

// Err returns a non-nil error describing the violations, or nil if OK.
func (r Report) Err() error {
	if r.OK() {
		return nil
	}
	return fmt.Errorf("ledger invariants violated: %s", r.Summary())
}

// Summary renders the violations on one line, deduplicated by rule so that a
// million broken rows do not produce a million lines.
func (r Report) Summary() string {
	if r.OK() {
		return "all invariants hold"
	}
	byRule := map[Rule][]string{}
	for _, v := range r.Violations {
		byRule[v.Rule] = append(byRule[v.Rule], v.Detail)
	}
	rules := make([]string, 0, len(byRule))
	for rule := range byRule {
		rules = append(rules, string(rule))
	}
	sort.Strings(rules)
	parts := make([]string, 0, len(rules))
	for _, rule := range rules {
		details := byRule[Rule(rule)]
		head := details[0]
		if len(details) > 1 {
			head = fmt.Sprintf("%s (+%d more)", head, len(details)-1)
		}
		parts = append(parts, rule+" ["+head+"]")
	}
	return strings.Join(parts, "; ")
}
