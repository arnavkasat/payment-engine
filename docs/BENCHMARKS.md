# Benchmarks

Numbers from a payment engine mean nothing without the shape of the workload, so this
document is mostly method. The figures below are real and reproducible; run it yourself
and compare.

## Measured in CI

Every push runs a full-stack job: PostgreSQL 16, a single-node Kafka in KRaft mode, the
API and the settlement worker in containers, on a standard GitHub-hosted runner
(2 vCPU, shared). The load is 2,000 payments from 8 concurrent clients across 16 wallets,
with 8 clearing shards.

**Capture** — the synchronous path a client waits on:

| metric | value |
| --- | --- |
| throughput | 231 req/s |
| p50 | 10.6 ms |
| p90 | 25.8 ms |
| p95 | 36.5 ms |
| p99 | 1,013 ms |
| failures | 0 of 2,192 |
| retries absorbed | 192 |

**Settlement** — asynchronous, through Kafka. 3,087 payments drained in **8.2 seconds**,
roughly 500 settlements per second:

```
clearing=3087000  pending_events=15  awaiting_settlement=3087
clearing=2069000  pending_events=8   awaiting_settlement=2069
clearing=1049000  pending_events=8   awaiting_settlement=1049
clearing=95000    pending_events=2   awaiting_settlement=95
clearing=0        pending_events=0   awaiting_settlement=0
```

**Invariants afterwards**, recomputed from the journal rather than from a counter:

```json
{ "ok": true, "accounts": 33, "entries": 6622,
  "postings": 16547, "payments": 3303, "total_posting_sum": 0 }
```

Two honest caveats. A shared CI runner is not a benchmark rig — the database, both
services and the load generator are competing for two cores. And the p99 of 1.0s against
a p50 of 10ms is a real tail, not noise: it is transactions queueing on a contended
account row and then being retried. Sharding moved that tail off the clearing accounts;
what remains is contention on the sixteen wallets, which is a property of the test's own
shape rather than of the engine.

## Running your own

```sh
make up
go run ./cmd/loadgen -url http://localhost:8080 \
  -concurrency 32 -duration 30s -warmup 5s -wallets 64 -retry-rate 0.1
```

`cmd/loadgen` reports percentiles from sorted raw samples, not from histogram buckets, so
they are exact for the run it measured. `-json` emits the same summary for scripting.

## What actually moves the numbers

**Wallet count, more than anything else.** Every capture takes a row lock on the payer and
the payee. With `-wallets 4`, thirty-two concurrent clients queue for four rows and the
benchmark measures lock contention. With `-wallets 256` they rarely collide and it
measures the database's commit path. Both are legitimate questions; they are not the same
question. Always report the wallet count alongside the throughput.

**Clearing shards.** Every capture also posts against a clearing account and a fee
account. With one of each, the entire engine serialises on two rows — this was measured,
not theorised, and the story is in `docs/ARCHITECTURE.md`. `CLEARING_SHARDS` spreads that
across N rows. Past the point where the payer rows are the contended ones, raising it buys
nothing.

**Connection pool size.** `DATABASE_MAX_CONNS` must exceed peak concurrent writes, or
requests queue for a connection *before* they queue for a lock, and the latency histogram
grows a second mode that has nothing to do with the ledger.

**`fsync` and the disk.** Each capture is one commit. On a laptop SSD with `fsync` on,
commit latency dominates everything the Go code does. Benchmarks from a container on
network storage are measuring the storage.

**Retry rate.** `-retry-rate` sends duplicate requests with the same key. A replay is a
cheaper path than a capture — one indexed lookup, no locks, no writes — so a high retry
rate *raises* throughput. Reporting a number achieved at `-retry-rate 0.9` without saying
so would be dishonest.

**Settlement is a separate ceiling.** `cmd/loadgen` measures capture only; settlement
happens behind Kafka and is bounded by the consumer, not by the API. Measuring capture
rate and calling it the engine's throughput would be measuring the easy half. Watch the
whole pipeline instead:

```sh
watch -n1 "curl -s localhost:8080/v1/ledger/snapshot | \
  jq '{clearing: .clearing_balance, pending: .pending_outbox, awaiting: .in_flight}'"
```

Or open `http://localhost:8080` and watch the chart.

**The consumer's commit interval.** Settlement throughput is bounded by how often the
consumer flushes offsets. The default batches them every 100ms; setting `CommitInterval`
negative restores one synchronous broker round trip per message, which is roughly an order
of magnitude slower and buys nothing the deduplication table does not already provide.

## Where the time goes in a capture

One capture is one transaction containing:

1. `INSERT ... ON CONFLICT DO NOTHING` on `idempotency_keys` — one index write
2. `SELECT ... FOR UPDATE` on 2–3 accounts — index scans plus lock acquisition
3. `INSERT` one `ledger_entries` row
4. `INSERT` 2–3 `ledger_postings` rows, batched into one round trip
5. `UPDATE` 2–3 account balances in a single statement
6. `INSERT` one `payments` row
7. `INSERT` one `outbox` row
8. `UPDATE` the idempotency key with the stored response
9. `COMMIT` — the deferred zero-sum trigger fires here, plus one `fsync`

Nine statements and one `fsync`, batched where pgx allows. Under contention, step 2
dominates; without it, step 9 does.

A replayed request is step 1 and a single `SELECT`, and nothing else.

## Profiling

```sh
go test ./internal/payments/ -run TestManyPaymentsKeepTheBooksBalanced \
  -cpuprofile cpu.out -count 5
go tool pprof -top cpu.out
```

That path uses the in-memory store, so it profiles the engine's own work —
fingerprinting, entry construction, validation, JSON encoding — with the database
excluded. Note that the in-memory store serialises every transaction behind one mutex by
design; it is a correctness reference, not a performance target, and its throughput
numbers mean nothing. For the database path, profile the running API via
`net/http/pprof` or read `pg_stat_statements`.
