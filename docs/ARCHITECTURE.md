# Architecture

This document covers the decisions behind the engine, including the ones that went the
other way. The README covers what it does; this covers why.

## The sign convention

A posting's amount is the **signed delta applied to an account's balance**. Money leaving an
account is negative, money arriving is positive, and the postings of a journal entry must sum
to exactly zero.

In classical bookkeeping terms a positive delta is a debit to an asset account and a negative
delta is a credit to it. The ledger stores the delta rather than a direction flag and an
unsigned magnitude for one reason: it makes the invariant a plain `SUM(amount) = 0`, which the
database can check itself, in one index scan, without knowing anything about account types.

Account kinds exist for policy, not arithmetic:

| kind | may go negative | role |
| --- | --- | --- |
| `wallet` | no | customer money |
| `clearing` | no | captured funds, held until settlement |
| `revenue` | no | fees |
| `external` | **yes** | the world outside the ledger |

`external` is the counterweight that lets value enter the system while the global sum stays
zero. Its balance is, by construction, minus the total value the engine holds. A ledger with
no account allowed to go negative could never receive its first deposit.

## Why capture and settlement are separate

A single-step transfer — debit payer, credit payee, done — would be simpler, and would not
need Kafka at all. The two-step design exists because it makes the in-flight state *nameable*:

- After capture, the money is in `clearing`. It is not the payer's and not yet the payee's.
- After settlement, it is the payee's.

A crash anywhere in between leaves a defined, inspectable amount in a real account rather
than an ambiguity. The clearing balance is then a complete health metric: **non-zero clearing
after the pipeline drains means some capture never settled**, and no per-payment bookkeeping
is needed to detect it. The chaos harness uses exactly this as its strongest assertion.

It also makes the Kafka consumer load-bearing. A consumer that only wrote to a projection
could be broken for a week before anyone noticed; one that moves money is tested by every
payment.

## Isolation: READ COMMITTED, not SERIALIZABLE

The PostgreSQL store runs transactions at READ COMMITTED and takes explicit row locks:

```sql
SELECT ... FROM accounts WHERE id = ANY($1) ORDER BY id FOR UPDATE
```

Two properties make this safe:

1. **Every balance change takes the row lock first.** There is no path that reads a balance
   and writes it back without holding the lock, so the lost update READ COMMITTED would
   otherwise allow cannot occur.
2. **Locks are always acquired in ascending account-id order.** Both the PostgreSQL store and
   the in-memory store sort before locking. Two concurrent transfers touching the same pair of
   accounts in opposite directions therefore queue, rather than deadlock.

SERIALIZABLE would add nothing on top of that, and would cost throughput to serialisation
failures that are false positives for this access pattern. The level is configurable, and the
retry loop handles `40001` and `40P01` either way, so switching is a one-line change if a
future access pattern needs it.

## Why the idempotency key is fingerprinted

Storing just the key would mean that reusing a key with a different body returns the first
payment. The caller then believes a payment it never made has succeeded — a worse failure
than an error, because it is silent.

So each key stores a SHA-256 of the request's canonical form. The canonical form is built
field by field with length prefixes rather than by marshalling, because JSON field order,
whitespace and optional fields can all change without the request changing — and the length
prefix stops a reference of `"a|b"` colliding with two separate fields `"a"` and `"b"`.

A mismatch is `422`, never a replay.

## What happens to failed requests

A request that fails deterministically — insufficient funds, currency mismatch — rolls its
whole transaction back, which **releases the idempotency key**.

This is deliberate. The alternative, storing the failure under the key, means a payment
rejected at 09:00 for want of funds can never be retried with the same key at 09:05 once the
payer has topped up. Callers routinely reuse a key for the logical operation rather than the
attempt. Since the rejection is deterministic given the same state, a retry either fails the
same way or succeeds because the state legitimately changed.

## The outbox, and why it is not exactly-once

The outbox row is written in the capture transaction. Publishing happens later:

1. Lease a batch: `FOR UPDATE SKIP LOCKED`, setting `leased_until` and incrementing `attempts`.
2. Publish to Kafka and wait for all in-sync replicas to acknowledge.
3. Mark the rows published.

A crash between 2 and 3 means the broker has a message whose row is still pending. The lease
lapses, another worker publishes it again, and the consumer sees a duplicate. **This is not a
bug to be fixed; it is the price of not having a transaction that spans PostgreSQL and Kafka.**
The design pays that price deliberately and closes the gap on the consumer side, where closing
it is cheap.

The lease is used rather than holding a transaction open across the publish, because holding a
database transaction open across a network call to a broker ties the database's health to the
broker's.

`RequiredAcks` is `all`. Anything weaker lets the broker acknowledge a message it can still
lose — and the outbox row would already be retired, so it would never be retried.

## The consumer: at-least-once in, exactly-once effect out

```go
store.Tx(func(tx) error {
    first, err := tx.MarkProcessed(ctx, group, msg.ID)   // INSERT ... ON CONFLICT DO NOTHING
    if !first {
        return nil                                        // duplicate: skip
    }
    return handler(ctx, tx, msg)                          // the effect
})
```

The marker and the effect share one transaction. There is no ordering of crashes that leaves a
marker claiming work that never happened, or work that happened without a marker.

Two details matter more than they look:

- **The deduplication key is the outbox row's id**, carried in a Kafka header. It is stable
  across redeliveries. Broker offsets are not — a rebalance or a re-publish changes them — and
  using an offset here is the standard way this design is got wrong.
- **Markers are scoped to the consumer group**, so a second group (analytics, say) sees every
  message once on its own terms rather than finding them already marked.

The settlement handler additionally guards on payment status, so even a message that somehow
bypassed the marker cannot settle a settled payment. And beneath that, a partial unique index
allows only one capture and one settlement row per payment. Three independent layers, because
the cost of each is a few lines and the cost of a double settlement is real money.

## Offset commits are batched, on purpose

kafka-go can commit an offset synchronously after every message. The engine does
not, and the reason is the deduplication table.

A synchronous commit costs a broker round trip per message, which caps settlement
throughput at the inverse of that latency — a few hundred messages a second on a
healthy local broker, far less across a network. It buys durability of the
*offset*, not of the effect: the effect was already durable when its transaction
committed.

What a batched commit risks is replaying the last interval's worth of offsets
after a crash. Those messages arrive again, find their marker already written,
and are skipped. The trade is a bounded amount of redundant work after a crash —
precisely the work the engine is built to absorb — for an order of magnitude of
throughput. `CommitInterval` is configurable, and a negative value restores
synchronous commits for anyone who wants to pay for them.

The ordering that matters is untouched either way: `CommitMessages` is still
called only after the handler's transaction has committed, so an offset can
never advance past a message that was not applied.

## The topic is created explicitly

The worker creates its topic at startup rather than relying on the broker's
auto-creation, for three reasons. Auto-creation hands out whatever partition
count the broker defaults to, and the partition count is the ceiling on
settlement concurrency — a consumer group can never have more useful members
than partitions. It is disabled on most production clusters. And a client only
triggers it by asking for it in a metadata request, so a publisher that does not
ask simply fails against a missing topic, however the broker is configured.

That last point is not hypothetical: it is how the first CI run of this
repository failed. The broker had auto-creation enabled, the writer never asked
for it, every publish failed with `UNKNOWN_TOPIC_OR_PARTITION`, and the outbox
never drained. The engine behaved correctly throughout — nothing was lost, the
books stayed balanced, and the undelivered events sat in the outbox exactly as
designed — but nothing settled either.

## The dashboard is one embedded file

`internal/httpapi/dashboard.html` is served at `/` via `go:embed`. There is no
npm, no bundler and no build step, which keeps the repository a Go repository
and the deployable a single static binary.

It polls one endpoint, `GET /v1/ledger/snapshot`, which gathers the accounts,
the payment counts, the outbox depth and a full invariant verification in one
server-side call. Four separate polls would be four round trips showing four
slightly different moments; one call means the numbers on screen are consistent
with each other.

The fault controls it exposes are gated behind `ENABLE_FAULT_INJECTION`, which
defaults to false. The injector is shared by the service, the publisher and the
consumer, so arming a point affects the whole pipeline at once; when the flag is
off the injector is nil, and a nil injector costs one nil check and never fires.

## Defence in depth in the schema

The database enforces what it can, so that the invariants survive code that is not this code:

| mechanism | stops |
| --- | --- |
| deferred constraint trigger on `ledger_postings` | an entry whose postings do not sum to zero, checked at `COMMIT` |
| `CHECK (allow_negative OR balance >= 0)` | an unauthorised overdraft |
| `UNIQUE (entry_id, account_id)` | two legs against one account in one entry, which would make the delta ambiguous |
| `CHECK (amount <> 0)` | a posting that moves nothing |
| partial unique index on `(payment_id, kind)` | a second capture or settlement for a payment |
| `UNIQUE (idempotency_key)` on payments | a key producing two payments |
| `PRIMARY KEY (consumer, message_id)` | a message processed twice by one group |

The constraint trigger is deferred rather than immediate because an entry is written one
posting at a time; an immediate check would fire on the first leg, when the entry is correctly
unbalanced.

`internal/storage/postgres/conformance_test.go` tests these directly, with the application
bypassed, because a constraint nobody has watched fail is a constraint nobody knows works.

## Two implementations, one conformance suite

`storage.Store` has a PostgreSQL implementation and an in-memory one. The in-memory store is
not a mock: it implements the same semantics, including rollback (by snapshot), deterministic
lock ordering and outbox lease expiry.

Both run `internal/storage/storagetest`, which is the only thing that makes the in-memory
store trustworthy as a stand-in. Fifteen cases cover rollback, idempotency races, overdraft
refusal, lease expiry and the deduplication marker's transactional behaviour. The in-memory
store runs them on every `go test`; PostgreSQL runs them in CI.

The payoff is that `internal/payments`, `internal/outbox`, `internal/consumer` and
`internal/chaos` all have fast, dependency-free tests whose results mean something.

## Metrics without a metrics library

`internal/metrics` is about 300 lines: labelled counters, bucketed histograms, Prometheus text
exposition. It is hand-rolled because the engine needs exactly two metric shapes, and because
the latency numbers in the documentation should come from code in this repository that a
reader can check.

The HTTP middleware labels by **route pattern**, never raw path. Labelling by path would mint
a new time series per payment id — the standard way to take down a metrics backend.

Exact percentiles in `cmd/loadgen` come from sorted raw samples instead, because a bucketed
histogram's p95 is an interpolation and a benchmark claim should not be.

## What is deliberately missing

This is a focused demonstration, not a product. The following are absent, and would be the
first things to add:

- **Multi-currency.** Accounts carry a currency and cross-currency postings are rejected, but
  there is no FX entry kind that would make a cross-currency payment possible.
- **Authentication and tenancy.** There is no auth, and no notion of who owns an account.
- **Reversals and refunds.** `ledger.NewReversal` exists and is tested, but no API exposes it
  and no state machine governs when a payment may be reversed.
- **Outbox and idempotency retention.** Both tables grow forever. They need partitioning or a
  janitor; the index on in-flight idempotency claims is there to make the janitor cheap.
- **Dead-letter handling.** A message whose handler fails permanently blocks its partition.
  Production wants a retry topic and a dead-letter topic.
- **Schema registry.** Events are JSON with a type string. A real deployment wants versioned,
  validated schemas.
