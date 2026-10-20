package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/arnavkasat/payment-engine/internal/ledger"
	"github.com/arnavkasat/payment-engine/internal/money"
	"github.com/arnavkasat/payment-engine/internal/storage"
)

const accountColumns = `id, name, currency, kind, allow_negative, balance, created_at`

// scanAccount reads one account row in accountColumns order.
func scanAccount(row interface{ Scan(...any) error }) (storage.Account, error) {
	var (
		a        storage.Account
		currency string
		kind     string
		balance  int64
	)
	if err := row.Scan(&a.ID, &a.Name, &currency, &kind, &a.AllowNegative, &balance, &a.CreatedAt); err != nil {
		return storage.Account{}, mapError(err)
	}
	a.Currency = money.Currency(currency)
	a.Kind = storage.AccountKind(kind)
	a.Balance = money.Minor(balance)
	return a, nil
}

// CreateAccount inserts an account.
func (s *Store) CreateAccount(ctx context.Context, a storage.Account) (storage.Account, error) {
	if a.ID == uuid.Nil {
		a.ID = uuid.New()
	}
	if a.CreatedAt.IsZero() {
		a.CreatedAt = time.Now().UTC()
	}
	row := s.pool.QueryRow(ctx, `
		INSERT INTO accounts (id, name, currency, kind, allow_negative, balance, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		RETURNING `+accountColumns,
		a.ID, a.Name, string(a.Currency), string(a.Kind), a.AllowNegative, int64(a.Balance), a.CreatedAt)
	return scanAccount(row)
}

// GetAccount reads an account by id.
func (s *Store) GetAccount(ctx context.Context, id uuid.UUID) (storage.Account, error) {
	row := s.pool.QueryRow(ctx, `SELECT `+accountColumns+` FROM accounts WHERE id = $1`, id)
	a, err := scanAccount(row)
	if err != nil {
		return storage.Account{}, fmt.Errorf("account %s: %w", id, err)
	}
	return a, nil
}

// GetAccountByName reads an account by its unique name.
func (s *Store) GetAccountByName(ctx context.Context, name string) (storage.Account, error) {
	row := s.pool.QueryRow(ctx, `SELECT `+accountColumns+` FROM accounts WHERE name = $1`, name)
	a, err := scanAccount(row)
	if err != nil {
		return storage.Account{}, fmt.Errorf("account %q: %w", name, err)
	}
	return a, nil
}

// ListAccounts returns every account ordered by name.
func (s *Store) ListAccounts(ctx context.Context) ([]storage.Account, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+accountColumns+` FROM accounts ORDER BY name`)
	if err != nil {
		return nil, mapError(err)
	}
	defer rows.Close()
	var out []storage.Account
	for rows.Next() {
		a, err := scanAccount(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, mapError(rows.Err())
}

const paymentColumns = `id, idempotency_key, payer_account, payee_account, amount, fee, currency, status, reference, created_at, updated_at`

func scanPayment(row interface{ Scan(...any) error }) (storage.Payment, error) {
	var (
		p           storage.Payment
		amount, fee int64
		currency    string
		status      string
	)
	if err := row.Scan(&p.ID, &p.IdempotencyKey, &p.PayerAccount, &p.PayeeAccount,
		&amount, &fee, &currency, &status, &p.Reference, &p.CreatedAt, &p.UpdatedAt); err != nil {
		return storage.Payment{}, mapError(err)
	}
	p.Amount = money.Minor(amount)
	p.Fee = money.Minor(fee)
	p.Currency = money.Currency(currency)
	p.Status = storage.PaymentStatus(status)
	return p, nil
}

// GetPayment reads a payment by id.
func (s *Store) GetPayment(ctx context.Context, id uuid.UUID) (storage.Payment, error) {
	row := s.pool.QueryRow(ctx, `SELECT `+paymentColumns+` FROM payments WHERE id = $1`, id)
	p, err := scanPayment(row)
	if err != nil {
		return storage.Payment{}, fmt.Errorf("payment %s: %w", id, err)
	}
	return p, nil
}

// GetEntries returns a payment's journal entries with their postings.
func (s *Store) GetEntries(ctx context.Context, paymentID uuid.UUID) ([]ledger.Entry, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, payment_id, kind, currency, created_at
		  FROM ledger_entries
		 WHERE payment_id = $1
		 ORDER BY created_at, id`, paymentID)
	if err != nil {
		return nil, mapError(err)
	}
	defer rows.Close()

	var entries []ledger.Entry
	index := map[uuid.UUID]int{}
	for rows.Next() {
		var (
			e        ledger.Entry
			kind     string
			currency string
		)
		if err := rows.Scan(&e.ID, &e.PaymentID, &kind, &currency, &e.CreatedAt); err != nil {
			return nil, mapError(err)
		}
		e.Kind = ledger.EntryKind(kind)
		e.Currency = money.Currency(currency)
		index[e.ID] = len(entries)
		entries = append(entries, e)
	}
	if err := rows.Err(); err != nil {
		return nil, mapError(err)
	}
	if len(entries) == 0 {
		return nil, nil
	}

	postings, err := s.pool.Query(ctx, `
		SELECT p.id, p.entry_id, p.account_id, p.amount
		  FROM ledger_postings p
		  JOIN ledger_entries e ON e.id = p.entry_id
		 WHERE e.payment_id = $1
		 ORDER BY p.entry_id, p.id`, paymentID)
	if err != nil {
		return nil, mapError(err)
	}
	defer postings.Close()
	for postings.Next() {
		var (
			p      ledger.Posting
			amount int64
		)
		if err := postings.Scan(&p.ID, &p.EntryID, &p.AccountID, &amount); err != nil {
			return nil, mapError(err)
		}
		p.Amount = money.Minor(amount)
		if i, ok := index[p.EntryID]; ok {
			entries[i].Postings = append(entries[i].Postings, p)
		}
	}
	return entries, mapError(postings.Err())
}

const outboxColumns = `id, aggregate_id, type, payload, attempts, created_at, published_at, leased_until, lease_owner, last_error`

func scanOutbox(row interface{ Scan(...any) error }) (storage.OutboxMessage, error) {
	var m storage.OutboxMessage
	if err := row.Scan(&m.ID, &m.AggregateID, &m.Type, &m.Payload, &m.Attempts,
		&m.CreatedAt, &m.PublishedAt, &m.LeasedUntil, &m.LeaseOwner, &m.LastError); err != nil {
		return storage.OutboxMessage{}, mapError(err)
	}
	return m, nil
}

// LeaseOutbox claims unpublished messages whose lease has lapsed, skipping
// rows another worker currently holds.
func (s *Store) LeaseOutbox(ctx context.Context, owner string, limit int, lease time.Duration) ([]storage.OutboxMessage, error) {
	rows, err := s.pool.Query(ctx, `
		WITH claimed AS (
			SELECT id
			  FROM outbox
			 WHERE published_at IS NULL
			   AND (leased_until IS NULL OR leased_until < now())
			 ORDER BY created_at, id
			 LIMIT $2
			 FOR UPDATE SKIP LOCKED
		)
		UPDATE outbox o
		   SET leased_until = now() + make_interval(secs => $3),
		       lease_owner  = $1,
		       attempts     = o.attempts + 1
		  FROM claimed c
		 WHERE o.id = c.id
		RETURNING o.id, o.aggregate_id, o.type, o.payload, o.attempts,
		          o.created_at, o.published_at, o.leased_until, o.lease_owner, o.last_error`,
		owner, limit, lease.Seconds())
	if err != nil {
		return nil, mapError(err)
	}
	defer rows.Close()
	var out []storage.OutboxMessage
	for rows.Next() {
		m, err := scanOutbox(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, mapError(rows.Err())
}

// MarkOutboxPublished retires messages the broker accepted. Re-marking an
// already-published message is a no-op, because redeliveries race.
func (s *Store) MarkOutboxPublished(ctx context.Context, ids []uuid.UUID) error {
	if len(ids) == 0 {
		return nil
	}
	tag, err := s.pool.Exec(ctx, `
		UPDATE outbox
		   SET published_at = COALESCE(published_at, now()),
		       leased_until = NULL,
		       lease_owner  = ''
		 WHERE id = ANY($1)`, ids)
	if err != nil {
		return mapError(err)
	}
	if int(tag.RowsAffected()) != len(ids) {
		return fmt.Errorf("%w: marked %d of %d outbox messages", storage.ErrNotFound, tag.RowsAffected(), len(ids))
	}
	return nil
}

// ReleaseOutbox returns a message to the queue after a failed publish.
func (s *Store) ReleaseOutbox(ctx context.Context, id uuid.UUID, cause string) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE outbox
		   SET leased_until = NULL, lease_owner = '', last_error = $2
		 WHERE id = $1`, id, cause)
	if err != nil {
		return mapError(err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: outbox %s", storage.ErrNotFound, id)
	}
	return nil
}

// PendingOutbox counts messages still awaiting publication.
func (s *Store) PendingOutbox(ctx context.Context) (int, error) {
	var n int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE published_at IS NULL`).Scan(&n); err != nil {
		return 0, mapError(err)
	}
	return n, nil
}
