# payment-engine

A payment engine in Go that you can crash at any point and still balance the books.

It captures payments into a double-entry PostgreSQL ledger, deduplicates client retries
with idempotency keys, hands events to Kafka through a transactional outbox, and settles
them with consumers whose effects are exactly-once even though delivery is at-least-once.

The guarantee, stated so it can be falsified:

> Faults may lose work, duplicate work, or delay work. They may never move money twice,
> lose money, or leave a payment captured and unsettled once the faults stop.

`cmd/chaos` tries to falsify it, and the same scenarios run in CI as ordinary tests.

---

## Quickstart

No dependencies — the whole engine in one process, backed by an in-memory store:

```sh
make demo          # DATABASE_URL=memory go run ./cmd/api
```

The real topology — PostgreSQL, Kafka, the API and the settlement worker:

```sh
make up            # docker compose up --build -d
curl localhost:8080/readyz
```

Then move some money:

```sh
# Create two wallets.
alice=$(curl -sX POST localhost:8080/v1/accounts \
  -d '{"name":"alice","currency":"USD","kind":"wallet"}' | jq -r .id)
bob=$(curl -sX POST localhost:8080/v1/accounts \
  -d '{"name":"bob","currency":"USD","kind":"wallet"}' | jq -r .id)

# Fund Alice from the external counterparty.
curl -sX POST "localhost:8080/v1/accounts/$alice/deposits" \
  -H "Idempotency-Key: $(uuidgen)" \
  -d '{"amount":100000,"currency":"USD"}' | jq

# Pay Bob 250.00 with a 2.50 fee. Send it twice with the same key.
key=$(uuidgen)
for i in 1 2; do
  curl -sD- -o /dev/null -X POST localhost:8080/v1/payments \
    -H "Idempotency-Key: $key" \
    -d "{\"payer_account\":\"$alice\",\"payee_account\":\"$bob\",\"amount\":25000,\"fee\":250,\"currency\":\"USD\"}" \
    | grep -E 'HTTP|Idempotent-Replay'
done
```

The second request comes back `201` with `Idempotent-Replay: true` and the identical body.
Alice is charged once.

---

## The three claims, and how to check them yourself

### 1. The ledger always balances

Every movement is a journal entry whose postings sum to exactly zero. A payment of 250.00
with a 2.50 fee is three postings:

| account | delta |
| --- | --- |
| alice (wallet) | −25250 |
| clearing | +25000 |
| fees (revenue) | +250 |
| **sum** | **0** |

This is enforced in four independent places, because the one that matters is whichever one
is still working:

1. `ledger.Entry.Validate` refuses an unbalanced entry in the domain.
2. Each store re-validates before writing.
3. A **deferred constraint trigger** in PostgreSQL re-checks the sum at `COMMIT`, so an
   unbalanced entry cannot be written by this engine, a future service, or a person at a
   `psql` prompt.
4. `VerifyInvariants` recomputes everything from the journal and reports drift.

Check it on a running system — the endpoint returns `500` if anything is off:

```sh
curl -sf localhost:8080/v1/ledger/invariants | jq
```

The invariants, by name:

| rule | meaning |
| --- | --- |
| `global_zero_sum` | every posting ever written sums to zero |
| `entry_zero_sum` | each journal entry sums to zero on its own |
| `balance_matches_postings` | each stored balance equals the sum of its postings |
| `no_unauthorised_overdraft` | accounts that forbid it never go negative |
| `single_entry_per_kind` | a payment has at most one capture and one settlement |

### 2. Retries are free

Every write takes an `Idempotency-Key`. The key, the journal entry, the balance changes,
the payment row, the outbox message and the stored HTTP response all commit in **one**
transaction. A retry returns the stored bytes verbatim.

The key is bound to a SHA-256 fingerprint of the request, so reusing one for a *different*
payment is rejected with `422 idempotency_key_reused` rather than silently handing back
someone else's payment — the failure mode that turns an idempotency key into a liability.

```sh
go test ./internal/payments/ -run TestConcurrentRetriesCaptureExactlyOnce -v
```

Sixty-four concurrent requests with one key: exactly one capture, one payment id, one debit.

### 3. Crashes are survivable

`cmd/chaos` runs the full pipeline while failing it at six named durability boundaries, then
switches the faults off and waits for the system to drain.

```
$ make chaos

chaos run
=========
  requests      attempted 647, captured 391, replayed 103, failed 153
  faults        181 fired
                  capture.after_commit         82
                  capture.before_commit        71
                  consumer.after_dedupe        12
                  consumer.before_ack          11
                  outbox.after_publish         3
                  outbox.before_publish        2
  broker        824 deliveries, 12 recognised as duplicates
  settlement    406 settled, 0 unsettled, 0 events pending
  clearing      0 left parked
  recovery      610ms
  elapsed       1.092s

  invariants during faults : PASS
  invariants after recovery: PASS
  postings sum to 0 across 918 entries and 11 accounts

RESULT: PASS
```

The clearing balance is the sharpest check in there. Clearing receives exactly the amount of
every capture and gives up exactly the amount of every settlement, so a final balance of zero
means *every capture that ever committed has settled* — including the ones whose caller saw an
error and never learned the payment id.

Each injection point targets one specific window:

| point | what fails | what must hold |
| --- | --- | --- |
| `capture.before_commit` | dies just before `COMMIT` | payment, postings, balances and outbox row all vanish together; the key is reusable |
| `capture.after_commit` | dies before answering the client | the retry replays; the payer is not charged twice |
| `outbox.before_publish` | dies after leasing, before publishing | the lease lapses and another worker publishes |
| `outbox.after_publish` | broker accepted it, row not retired | the message is published again and deduplicated downstream |
| `consumer.after_dedupe` | dies after the marker, before the effect | both roll back; the redelivery is treated as the first |
| `consumer.before_ack` | effect committed, offset not | the redelivery finds the marker and changes nothing |

Run them one at a time with `go run ./cmd/chaos -points capture.after_commit`, or against a
real database with `make chaos-postgres`.

---

## How it works

```
      POST /v1/payments
      Idempotency-Key: k
              │
              ▼
    ┌─────────────────────────────────────────────┐
    │  ONE PostgreSQL transaction                 │
    │                                             │
    │   claim idempotency key k                   │
    │   lock payer + payee rows (ascending id)    │
    │   write journal entry (sums to zero)        │
    │   move balances                             │
    │   insert payment  (status: captured)        │
    │   insert outbox row  payment.captured       │
    │   store the HTTP response under k           │
    └─────────────────────────────────────────────┘
              │ COMMIT
              ▼
    payer ──▶ clearing  (funds held)
              │
              │   publisher leases the outbox row (FOR UPDATE SKIP LOCKED),
              │   publishes to Kafka, then retires the row
              ▼
         ┌─────────┐
         │  Kafka  │  partitioned by payment id
         └─────────┘
              │   at-least-once delivery
              ▼
    ┌─────────────────────────────────────────────┐
    │  ONE PostgreSQL transaction                 │
    │                                             │
    │   insert (group, message-id) marker         │
    │     └─ already there? skip, it's a dupe     │
    │   write settlement entry (sums to zero)     │
    │   move balances                             │
    │   payment: captured ──▶ settled             │
    │   insert outbox row  payment.settled        │
    └─────────────────────────────────────────────┘
              │ COMMIT
              ▼
         clearing ──▶ payee
```

Three ideas carry the whole design:

**The outbox makes the event as durable as the payment.** There is no window in which a
payment exists without its event, or an event without its payment, because they are the same
`COMMIT`. Publishing afterwards is a separate, retryable step — which is why delivery is
at-least-once and not exactly-once.

**The dedupe marker is in the same transaction as the effect.** This is what upgrades
at-least-once delivery to exactly-once *effect*. Neither half can commit without the other, so
there is no ordering of crashes that leaves a marker claiming work that never happened, or work
that happened without a marker. Markers are keyed on the outbox row's id, which is stable
across redeliveries — broker offsets are not, and using one for deduplication is the classic
way to get this wrong.

**Money is never in two places, and never in none.** Capture moves funds to clearing;
settlement moves them to the payee. A payment interrupted anywhere leaves its amount sitting
in clearing, visible and accounted for, until the pipeline catches up.

More detail, including the decisions that went the other way: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

---

## API

| method | path | notes |
| --- | --- | --- |
| `POST` | `/v1/payments` | requires `Idempotency-Key`; replays return the stored body with `Idempotent-Replay: true` |
| `GET` | `/v1/payments/{id}` | |
| `GET` | `/v1/payments/{id}/entries` | the journal behind a payment, with each leg and its sum |
| `POST` | `/v1/accounts` | |
| `GET` | `/v1/accounts` | |
| `GET` | `/v1/accounts/{id}` | |
| `POST` | `/v1/accounts/{id}/deposits` | requires `Idempotency-Key`; the only way value enters the ledger |
| `GET` | `/v1/ledger/invariants` | `200` when the books balance, `500` when they do not |
| `GET` | `/healthz`, `/readyz`, `/metrics` | Prometheus text exposition |

Errors are always `{"error":{"code":"...","message":"..."}}`. Codes are stable:

| code | status | |
| --- | --- | --- |
| `idempotency_key_required` | 400 | writes are refused without a key |
| `invalid_request` | 400 | |
| `idempotency_key_reused` | 422 | same key, different request body |
| `request_in_flight` | 409 | another request holds the key; `Retry-After: 1` |
| `insufficient_funds` | 422 | |
| `currency_mismatch` | 422 | |
| `not_found` | 404 | |
| `status_conflict` | 409 | the payment was not in the expected state |
| `retry_exhausted` | 503 | lock contention, not a client mistake — retry |

An unrecognised failure is a `500`, never a `4xx`. Telling a client not to retry something that
might have succeeded is worse than telling it to try again.

---

## Testing

```sh
make test               # unit tests and chaos scenarios, with -race
make test-integration   # the storage conformance suite against real PostgreSQL
make chaos              # failure injection, in-memory
make chaos-postgres     # failure injection, against PostgreSQL
make check              # what CI runs on a pull request
```

There are two `storage.Store` implementations — PostgreSQL and in-memory — and
[one conformance suite](internal/storage/storagetest/storagetest.go) that both must pass.
That is what lets the fast tests use the in-memory store without the two quietly diverging:
if the memory store ever gets lock ordering, rollback or outbox leasing wrong, the same test
that catches it also runs against PostgreSQL in CI.

Beyond the conformance suite, the tests worth reading are the ones that assert a specific
failure is survivable:

- `TestConcurrentRetriesCaptureExactlyOnce` — 64 racers, one key, one charge
- `TestCrashBeforeCommitLosesTheWholeCapture` / `TestCrashAfterCommitIsRepairedByTheRetry`
- `TestProcessedMarkerRollsBackWithItsTransaction` — the exactly-once-effect property
- `TestCrashAfterPublishRedelivers` — the duplicate the consumer must absorb
- `TestEachFaultPointInIsolation` — every injection point, one at a time
- `TestDeferredTriggerRejectsUnbalancedEntry` — the database refusing an imbalance with the application bypassed

---

## Performance

`cmd/loadgen` drives the HTTP API and reports exact percentiles from raw samples, not from
histogram buckets. It also sends duplicate requests on purpose, because a payment API's retry
path is part of its steady-state load.

```sh
make up
go run ./cmd/loadgen -url http://localhost:8080 -concurrency 32 -duration 30s
```

Numbers depend entirely on the hardware and on how many wallets the traffic spreads across —
an engine that takes a row lock per account is contention-bound, so `-wallets 4` and
`-wallets 256` measure very different things. [docs/BENCHMARKS.md](docs/BENCHMARKS.md) gives the
method, the knobs that matter, and the figures measured while building this, so you can compare
against your own run rather than take a headline number on trust.

---

## Configuration

Everything is read in [`internal/config`](internal/config/config.go) and nowhere else.

| variable | default | |
| --- | --- | --- |
| `HTTP_ADDR` | `:8080` | |
| `DATABASE_URL` | `postgres://payments:payments@localhost:5432/payments?sslmode=disable` | `memory` selects the in-memory store |
| `DATABASE_MAX_CONNS` | `32` | should exceed peak concurrent writes |
| `KAFKA_BROKERS` | `localhost:9092` | comma separated |
| `KAFKA_TOPIC` | `payments.events` | |
| `CONSUMER_GROUP` | `settlement` | changing it replays history |
| `CLEARING_ACCOUNT` / `FEE_ACCOUNT` | `clearing` / `fees` | resolved by name at startup |
| `OUTBOX_BATCH` | `128` | |
| `OUTBOX_INTERVAL` | `50ms` | poll delay after an empty pass |
| `OUTBOX_LEASE` | `30s` | must exceed a publish; validated at startup |
| `BOOTSTRAP_ACCOUNTS` | `true` | create the standard accounts if missing |
| `LOG_LEVEL` | `info` | |
| `SHUTDOWN_TIMEOUT` | `15s` | |
| `ENABLE_PUBLISHER` / `ENABLE_CONSUMER` | `true` | worker only; run the two halves separately to scale them separately |

---

## Layout

```
cmd/
  api/        HTTP API; also runs the pipeline in-process in demo mode
  worker/     outbox publisher and settlement consumer
  chaos/      failure-injection harness
  loadgen/    load generator with exact percentiles
internal/
  money/      exact amounts in minor units, overflow-checked
  ledger/     double-entry domain: entries, postings, the zero-sum rule, invariants
  payments/   the engine: idempotency, capture, settlement
  storage/    the persistence contract
    postgres/   pgx implementation, schema, SQL invariant queries
    memory/     reference implementation for tests
    storagetest/ the conformance suite both must pass
  outbox/     transactional outbox publisher
  consumer/   deduplicating message processor
  events/     broker-agnostic envelopes
    kafka/      segmentio/kafka-go producer and consumer
    inmem/      in-process bus that reproduces at-least-once delivery
  chaos/      the failure-injection harness, as a library so CI runs it
  faults/     named injection points
  httpapi/    handlers, error mapping, middleware
  metrics/    counters and histograms in Prometheus text format
  config/     environment configuration
  app/        wiring shared by both binaries
```

Dependencies: `jackc/pgx` for PostgreSQL, `segmentio/kafka-go` for Kafka, `google/uuid`.
HTTP routing, metrics, migrations and configuration are standard library.

## Licence

MIT — see [LICENSE](LICENSE).
