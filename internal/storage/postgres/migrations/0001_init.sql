-- Schema for the payment engine.
--
-- The double-entry rule is not left to application code alone: a deferred
-- constraint trigger re-checks, at COMMIT, that every entry touched by the
-- transaction sums to zero. Application bugs, ad-hoc psql sessions and future
-- services all hit the same wall.

CREATE TABLE accounts (
    id             UUID PRIMARY KEY,
    name           TEXT NOT NULL UNIQUE,
    currency       CHAR(3) NOT NULL,
    kind           TEXT NOT NULL CHECK (kind IN ('wallet', 'clearing', 'revenue', 'external')),
    allow_negative BOOLEAN NOT NULL DEFAULT FALSE,
    balance        BIGINT NOT NULL DEFAULT 0,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Accounts that have not opted into an overdraft can never be negative,
    -- whatever the application believes.
    CONSTRAINT accounts_balance_floor CHECK (allow_negative OR balance >= 0)
);

CREATE TABLE payments (
    id              UUID PRIMARY KEY,
    idempotency_key TEXT NOT NULL UNIQUE,
    payer_account   UUID NOT NULL REFERENCES accounts (id),
    payee_account   UUID NOT NULL REFERENCES accounts (id),
    amount          BIGINT NOT NULL CHECK (amount > 0),
    fee             BIGINT NOT NULL DEFAULT 0 CHECK (fee >= 0),
    currency        CHAR(3) NOT NULL,
    status          TEXT NOT NULL CHECK (status IN ('captured', 'settled', 'failed')),
    reference       TEXT NOT NULL DEFAULT '',
    created_at      TIMESTAMPTZ NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL,

    CONSTRAINT payments_distinct_accounts CHECK (payer_account <> payee_account)
);

CREATE INDEX payments_status_idx ON payments (status, created_at);

CREATE TABLE ledger_entries (
    id         UUID PRIMARY KEY,
    payment_id UUID NOT NULL,
    kind       TEXT NOT NULL CHECK (kind IN ('capture', 'settlement', 'reversal', 'deposit')),
    currency   CHAR(3) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX ledger_entries_payment_idx ON ledger_entries (payment_id, created_at);

-- A payment gets at most one capture and at most one settlement. If a retry
-- or a redelivered Kafka message ever slipped past the idempotency key and
-- the dedupe table, this index is the last line that stops it being applied
-- twice. Reversals are exempt: a payment may be reversed more than once.
CREATE UNIQUE INDEX ledger_entries_one_per_kind
    ON ledger_entries (payment_id, kind)
    WHERE kind <> 'reversal';

CREATE TABLE ledger_postings (
    id         UUID PRIMARY KEY,
    entry_id   UUID NOT NULL REFERENCES ledger_entries (id) ON DELETE CASCADE,
    account_id UUID NOT NULL REFERENCES accounts (id),
    amount     BIGINT NOT NULL CHECK (amount <> 0),

    -- One leg per account per entry keeps the balance delta unambiguous.
    CONSTRAINT ledger_postings_one_leg_per_account UNIQUE (entry_id, account_id)
);

CREATE INDEX ledger_postings_account_idx ON ledger_postings (account_id);
CREATE INDEX ledger_postings_entry_idx ON ledger_postings (entry_id);

-- Conservation of value, enforced by the database at COMMIT.
CREATE FUNCTION assert_entry_balanced() RETURNS TRIGGER
LANGUAGE plpgsql AS $$
DECLARE
    v_entry UUID := COALESCE(NEW.entry_id, OLD.entry_id);
    v_total BIGINT;
    v_legs  INT;
BEGIN
    SELECT COALESCE(SUM(amount), 0), COUNT(*)
      INTO v_total, v_legs
      FROM ledger_postings
     WHERE entry_id = v_entry;

    -- The entry may have been deleted outright (a cascade from its entry
    -- row); an entry with no postings is vacuously balanced.
    IF v_legs = 0 THEN
        RETURN NULL;
    END IF;

    IF v_legs < 2 THEN
        RAISE EXCEPTION 'ledger entry % has only % posting(s)', v_entry, v_legs
            USING ERRCODE = 'check_violation';
    END IF;

    IF v_total <> 0 THEN
        RAISE EXCEPTION 'ledger entry % does not balance: postings sum to %', v_entry, v_total
            USING ERRCODE = 'check_violation';
    END IF;

    RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER ledger_postings_must_balance
    AFTER INSERT OR UPDATE OR DELETE ON ledger_postings
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION assert_entry_balanced();

CREATE TABLE idempotency_keys (
    key          TEXT PRIMARY KEY,
    fingerprint  TEXT NOT NULL,
    state        TEXT NOT NULL CHECK (state IN ('in_flight', 'completed')),
    payment_id   UUID,
    status_code  INT NOT NULL DEFAULT 0,
    response     BYTEA,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,

    CONSTRAINT idempotency_completed_has_response
        CHECK (state <> 'completed' OR (payment_id IS NOT NULL AND completed_at IS NOT NULL))
);

-- Lets a janitor expire abandoned in-flight claims without a full scan.
CREATE INDEX idempotency_keys_in_flight_idx
    ON idempotency_keys (created_at)
    WHERE state = 'in_flight';

-- The transactional outbox. Rows are written in the same transaction as the
-- state change they describe, so an event exists if and only if the payment
-- that produced it exists. Publication is a separate, retryable step.
CREATE TABLE outbox (
    id           UUID PRIMARY KEY,
    aggregate_id UUID NOT NULL,
    type         TEXT NOT NULL,
    payload      BYTEA NOT NULL,
    attempts     INT NOT NULL DEFAULT 0,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ,
    leased_until TIMESTAMPTZ,
    lease_owner  TEXT NOT NULL DEFAULT '',
    last_error   TEXT NOT NULL DEFAULT ''
);

-- Publishers only ever scan unpublished rows, so the index excludes the rest
-- and stays small no matter how much history the table accumulates.
CREATE INDEX outbox_unpublished_idx
    ON outbox (created_at, id)
    WHERE published_at IS NULL;

-- Consumer-side deduplication. A handler writes its marker in the same
-- transaction as its side effects, which is what turns at-least-once
-- delivery into exactly-once effect.
CREATE TABLE processed_messages (
    consumer     TEXT NOT NULL,
    message_id   TEXT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (consumer, message_id)
);
