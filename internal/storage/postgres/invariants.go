package postgres

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/arnavkasat/payment-engine/internal/ledger"
	"github.com/arnavkasat/payment-engine/internal/money"
)

// VerifyInvariants recomputes every ledger invariant from the journal, in the
// database. Nothing is loaded into the process, so the cost is a handful of
// aggregate scans rather than a function of how much history exists.
//
// The checks run inside one repeatable-read transaction so that they all see
// the same snapshot; otherwise a transfer committing between two queries
// would look like a violation.
func (s *Store) VerifyInvariants(ctx context.Context) (ledger.Report, error) {
	var rep ledger.Report

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return rep, mapError(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY`); err != nil {
		return rep, mapError(err)
	}

	var totalSum int64
	if err := tx.QueryRow(ctx, `
		SELECT COALESCE(SUM(amount), 0),
		       count(*),
		       (SELECT count(*) FROM accounts),
		       (SELECT count(*) FROM ledger_entries),
		       (SELECT count(*) FROM payments)
		  FROM ledger_postings`).
		Scan(&totalSum, &rep.Postings, &rep.Accounts, &rep.Entries, &rep.Payments); err != nil {
		return rep, mapError(err)
	}
	rep.TotalPostingSum = money.Minor(totalSum)
	if totalSum != 0 {
		rep.Add(ledger.RuleGlobalZeroSum, "postings sum to %d, want 0", totalSum)
	}

	// Entries whose legs do not cancel. The deferred constraint trigger
	// should make this impossible; the check is here because "should be
	// impossible" is not the same as "is".
	rows, err := tx.Query(ctx, `
		SELECT entry_id, SUM(amount)
		  FROM ledger_postings
		 GROUP BY entry_id
		HAVING SUM(amount) <> 0`)
	if err != nil {
		return rep, mapError(err)
	}
	for rows.Next() {
		var (
			id  uuid.UUID
			sum int64
		)
		if err := rows.Scan(&id, &sum); err != nil {
			rows.Close()
			return rep, mapError(err)
		}
		rep.Add(ledger.RuleEntryZeroSum, "entry %s sums to %d", id, sum)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return rep, mapError(err)
	}

	// Balances that have drifted from the postings that produced them.
	rows, err = tx.Query(ctx, `
		SELECT a.name, a.balance, COALESCE(p.total, 0)
		  FROM accounts a
		  LEFT JOIN (
		       SELECT account_id, SUM(amount) AS total
		         FROM ledger_postings
		        GROUP BY account_id
		  ) p ON p.account_id = a.id
		 WHERE a.balance <> COALESCE(p.total, 0)
		 ORDER BY a.name`)
	if err != nil {
		return rep, mapError(err)
	}
	for rows.Next() {
		var (
			name              string
			balance, postings int64
		)
		if err := rows.Scan(&name, &balance, &postings); err != nil {
			rows.Close()
			return rep, mapError(err)
		}
		rep.Add(ledger.RuleBalanceMatchesPostings, "account %s balance %d, postings sum %d", name, balance, postings)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return rep, mapError(err)
	}

	// Accounts that went negative without permission.
	rows, err = tx.Query(ctx, `
		SELECT name, balance FROM accounts
		 WHERE NOT allow_negative AND balance < 0
		 ORDER BY name`)
	if err != nil {
		return rep, mapError(err)
	}
	for rows.Next() {
		var (
			name    string
			balance int64
		)
		if err := rows.Scan(&name, &balance); err != nil {
			rows.Close()
			return rep, mapError(err)
		}
		rep.Add(ledger.RuleNoUnauthorisedOverdraft, "account %s balance %d", name, balance)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return rep, mapError(err)
	}

	// Payments that picked up a second capture or settlement, which is what
	// a double-applied retry would look like.
	rows, err = tx.Query(ctx, `
		SELECT payment_id, kind, count(*)
		  FROM ledger_entries
		 WHERE kind <> 'reversal'
		 GROUP BY payment_id, kind
		HAVING count(*) > 1`)
	if err != nil {
		return rep, mapError(err)
	}
	for rows.Next() {
		var (
			id   uuid.UUID
			kind string
			n    int
		)
		if err := rows.Scan(&id, &kind, &n); err != nil {
			rows.Close()
			return rep, mapError(err)
		}
		rep.Add(ledger.RuleSingleEntryPerKind, "%s/%s written %d times", id, kind, n)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return rep, mapError(err)
	}

	if err := tx.Commit(ctx); err != nil {
		return rep, fmt.Errorf("postgres: closing verification snapshot: %w", mapError(err))
	}
	return rep, nil
}
