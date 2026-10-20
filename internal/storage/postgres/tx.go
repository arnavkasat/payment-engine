package postgres

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/arnavkasat/payment-engine/internal/ledger"
	"github.com/arnavkasat/payment-engine/internal/money"
	"github.com/arnavkasat/payment-engine/internal/storage"
)

// tx is a storage.Tx backed by a pgx transaction.
type tx struct{ tx pgx.Tx }

var _ storage.Tx = (*tx)(nil)

// ClaimIdempotencyKey wins the key or reports who holds it. The insert and
// the read of the incumbent are two statements, but under READ COMMITTED the
// insert blocks on any competing uncommitted insert and the follow-up read
// takes a fresh snapshot, so the loser always sees the winner's row.
func (t *tx) ClaimIdempotencyKey(ctx context.Context, key, fingerprint string) (storage.IdempotencyRecord, bool, error) {
	rec, err := scanIdempotency(t.tx.QueryRow(ctx, `
		INSERT INTO idempotency_keys (key, fingerprint, state)
		VALUES ($1, $2, 'in_flight')
		ON CONFLICT (key) DO NOTHING
		RETURNING key, fingerprint, state, payment_id, status_code, response, created_at, completed_at`,
		key, fingerprint))
	if err == nil {
		return rec, true, nil
	}
	if !errors.Is(err, storage.ErrNotFound) {
		return storage.IdempotencyRecord{}, false, err
	}

	rec, err = scanIdempotency(t.tx.QueryRow(ctx, `
		SELECT key, fingerprint, state, payment_id, status_code, response, created_at, completed_at
		  FROM idempotency_keys WHERE key = $1`, key))
	if err != nil {
		// The incumbent vanished between the two statements, which only
		// happens if it rolled back. Reporting a conflict sends the caller
		// through the retry path rather than inventing a result.
		if errors.Is(err, storage.ErrNotFound) {
			return storage.IdempotencyRecord{}, false, fmt.Errorf("idempotency key %q: claim raced with a rollback: %w", key, err)
		}
		return storage.IdempotencyRecord{}, false, err
	}
	return rec, false, nil
}

func scanIdempotency(row pgx.Row) (storage.IdempotencyRecord, error) {
	var (
		rec       storage.IdempotencyRecord
		state     string
		paymentID *uuid.UUID
	)
	if err := row.Scan(&rec.Key, &rec.Fingerprint, &state, &paymentID,
		&rec.StatusCode, &rec.Response, &rec.CreatedAt, &rec.CompletedAt); err != nil {
		return storage.IdempotencyRecord{}, mapError(err)
	}
	rec.State = storage.IdempotencyState(state)
	if paymentID != nil {
		rec.PaymentID = *paymentID
	}
	return rec, nil
}

// CompleteIdempotencyKey stores the response for future replays.
func (t *tx) CompleteIdempotencyKey(ctx context.Context, key string, paymentID uuid.UUID, statusCode int, response []byte) error {
	tag, err := t.tx.Exec(ctx, `
		UPDATE idempotency_keys
		   SET state = 'completed', payment_id = $2, status_code = $3,
		       response = $4, completed_at = now()
		 WHERE key = $1`, key, paymentID, statusCode, response)
	if err != nil {
		return mapError(err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: idempotency key %q", storage.ErrNotFound, key)
	}
	return nil
}

// GetAccounts locks the named accounts for update. Rows are locked in
// ascending id order: every writer agrees on that order, so two concurrent
// transfers touching the same pair of accounts queue up instead of
// deadlocking.
func (t *tx) GetAccounts(ctx context.Context, ids []uuid.UUID) (map[uuid.UUID]storage.Account, error) {
	if len(ids) == 0 {
		return map[uuid.UUID]storage.Account{}, nil
	}
	unique := make([]uuid.UUID, 0, len(ids))
	seen := make(map[uuid.UUID]struct{}, len(ids))
	for _, id := range ids {
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		unique = append(unique, id)
	}

	rows, err := t.tx.Query(ctx, `
		SELECT `+accountColumns+`
		  FROM accounts
		 WHERE id = ANY($1)
		 ORDER BY id
		   FOR UPDATE`, unique)
	if err != nil {
		return nil, mapError(err)
	}
	defer rows.Close()

	out := make(map[uuid.UUID]storage.Account, len(unique))
	for rows.Next() {
		a, err := scanAccount(rows)
		if err != nil {
			return nil, err
		}
		out[a.ID] = a
	}
	if err := rows.Err(); err != nil {
		return nil, mapError(err)
	}
	if len(out) != len(unique) {
		for _, id := range unique {
			if _, ok := out[id]; !ok {
				return nil, fmt.Errorf("%w: account %s", storage.ErrNotFound, id)
			}
		}
	}
	return out, nil
}

// ApplyEntry writes an entry with its postings and moves the balances they
// imply, in one transaction. The locks taken by GetAccounts are held until
// commit, so no concurrent transfer can read a balance this entry is about
// to change.
func (t *tx) ApplyEntry(ctx context.Context, e ledger.Entry) error {
	if err := e.Validate(); err != nil {
		return err
	}

	accounts, err := t.GetAccounts(ctx, e.Accounts())
	if err != nil {
		return err
	}

	deltas := e.Deltas()
	ids := make([]uuid.UUID, 0, len(deltas))
	for id := range deltas {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i].String() < ids[j].String() })

	amounts := make([]int64, 0, len(ids))
	for _, id := range ids {
		a := accounts[id]
		if a.Currency != e.Currency {
			return fmt.Errorf("%w: account %s holds %s, entry is %s", storage.ErrCurrencyMismatch, a.Name, a.Currency, e.Currency)
		}
		updated, err := money.Add(a.Balance, deltas[id])
		if err != nil {
			return fmt.Errorf("account %s: %w", a.Name, err)
		}
		if updated < 0 && !a.AllowNegative {
			return fmt.Errorf("%w: account %s has %d, needs %d", storage.ErrInsufficientFunds, a.Name, a.Balance, -deltas[id])
		}
		amounts = append(amounts, int64(deltas[id]))
	}

	if _, err := t.tx.Exec(ctx, `
		INSERT INTO ledger_entries (id, payment_id, kind, currency, created_at)
		VALUES ($1, $2, $3, $4, $5)`,
		e.ID, e.PaymentID, string(e.Kind), string(e.Currency), e.CreatedAt); err != nil {
		return mapError(err)
	}

	batch := &pgx.Batch{}
	for _, p := range e.Postings {
		batch.Queue(`
			INSERT INTO ledger_postings (id, entry_id, account_id, amount)
			VALUES ($1, $2, $3, $4)`, p.ID, e.ID, p.AccountID, int64(p.Amount))
	}
	results := t.tx.SendBatch(ctx, batch)
	for range e.Postings {
		if _, err := results.Exec(); err != nil {
			_ = results.Close()
			return mapError(err)
		}
	}
	if err := results.Close(); err != nil {
		return mapError(err)
	}

	// One statement moves every balance, so the journal and the balances can
	// only ever commit together.
	tag, err := t.tx.Exec(ctx, `
		UPDATE accounts a
		   SET balance = a.balance + d.delta
		  FROM (SELECT unnest($1::uuid[]) AS id, unnest($2::bigint[]) AS delta) d
		 WHERE a.id = d.id`, ids, amounts)
	if err != nil {
		return mapError(err)
	}
	if int(tag.RowsAffected()) != len(ids) {
		return fmt.Errorf("%w: updated %d of %d account balances", storage.ErrNotFound, tag.RowsAffected(), len(ids))
	}
	return nil
}

// InsertPayment writes a new payment.
func (t *tx) InsertPayment(ctx context.Context, p storage.Payment) error {
	_, err := t.tx.Exec(ctx, `
		INSERT INTO payments (id, idempotency_key, payer_account, payee_account,
		                      amount, fee, currency, status, reference, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
		p.ID, p.IdempotencyKey, p.PayerAccount, p.PayeeAccount, int64(p.Amount), int64(p.Fee),
		string(p.Currency), string(p.Status), p.Reference, p.CreatedAt, p.UpdatedAt)
	return mapError(err)
}

// GetPayment reads a payment inside the transaction.
func (t *tx) GetPayment(ctx context.Context, id uuid.UUID) (storage.Payment, error) {
	p, err := scanPayment(t.tx.QueryRow(ctx, `SELECT `+paymentColumns+` FROM payments WHERE id = $1`, id))
	if err != nil {
		return storage.Payment{}, fmt.Errorf("payment %s: %w", id, err)
	}
	return p, nil
}

// TransitionPayment moves a payment between statuses. The expected status is
// part of the WHERE clause, so a redelivered event cannot advance a payment
// that already moved on.
func (t *tx) TransitionPayment(ctx context.Context, id uuid.UUID, from, to storage.PaymentStatus) error {
	tag, err := t.tx.Exec(ctx, `
		UPDATE payments SET status = $3, updated_at = now()
		 WHERE id = $1 AND status = $2`, id, string(from), string(to))
	if err != nil {
		return mapError(err)
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	var current string
	if err := t.tx.QueryRow(ctx, `SELECT status FROM payments WHERE id = $1`, id).Scan(&current); err != nil {
		return fmt.Errorf("payment %s: %w", id, mapError(err))
	}
	return fmt.Errorf("%w: payment %s is %q, expected %q", storage.ErrStatusConflict, id, current, from)
}

// Enqueue appends messages to the outbox in the caller's transaction.
func (t *tx) Enqueue(ctx context.Context, msgs ...storage.OutboxMessage) error {
	if len(msgs) == 0 {
		return nil
	}
	batch := &pgx.Batch{}
	for _, m := range msgs {
		if m.ID == uuid.Nil {
			m.ID = uuid.New()
		}
		if m.CreatedAt.IsZero() {
			batch.Queue(`
				INSERT INTO outbox (id, aggregate_id, type, payload)
				VALUES ($1, $2, $3, $4)`, m.ID, m.AggregateID, m.Type, m.Payload)
			continue
		}
		batch.Queue(`
			INSERT INTO outbox (id, aggregate_id, type, payload, created_at)
			VALUES ($1, $2, $3, $4, $5)`, m.ID, m.AggregateID, m.Type, m.Payload, m.CreatedAt)
	}
	results := t.tx.SendBatch(ctx, batch)
	for range msgs {
		if _, err := results.Exec(); err != nil {
			_ = results.Close()
			return mapError(err)
		}
	}
	return mapError(results.Close())
}

// MarkProcessed records a consumer's handling of a message. The insert is the
// dedupe: whoever writes the row first is the one delivery that counts, and
// because the row lives in the handler's transaction it is rolled back with
// everything else if the handler fails.
func (t *tx) MarkProcessed(ctx context.Context, consumer, messageID string) (bool, error) {
	tag, err := t.tx.Exec(ctx, `
		INSERT INTO processed_messages (consumer, message_id)
		VALUES ($1, $2)
		ON CONFLICT (consumer, message_id) DO NOTHING`, consumer, messageID)
	if err != nil {
		return false, mapError(err)
	}
	return tag.RowsAffected() == 1, nil
}
