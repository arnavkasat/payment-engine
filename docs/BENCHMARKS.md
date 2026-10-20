# Benchmarks

Numbers from a payment engine mean nothing without the shape of the workload, so this
document is mostly method. Run it yourself; that is the point.

## Running a measurement

```sh
make up                     # PostgreSQL, Kafka, API, worker
go run ./cmd/loadgen -url http://localhost:8080 \
  -concurrency 32 -duration 30s -warmup 5s -wallets 64 -retry-rate 0.1
```

```
requests   61482 in 30.001s (2049 req/s)
outcomes   created 55893, replayed 5589, conflicts 0, rejected 0, failed 0
  HTTP 201  61482
latency (ms)
  min       1.21
  mean     14.93
  p50      13.44
  p90      24.10
  p95      29.87
  p99      48.22
  p999     91.03
  max     134.77
```

(The block above is the **format** of the output, not a result — fill it in with your own run.)

`-json` emits the same summary as JSON for scripting.

## What actually moves the numbers

**Wallet count, more than anything else.** Every capture takes a row lock on the payer and the
payee. With `-wallets 4`, thirty-two concurrent clients are all queueing for four rows and the
benchmark measures lock contention. With `-wallets 256` they rarely collide and it measures the
database's commit path. Both are legitimate questions; they are not the same question. Always
report the wallet count alongside the throughput.

**Connection pool size.** `DATABASE_MAX_CONNS` must exceed peak concurrent writes, or requests
queue for a connection *before* they queue for a lock, and the latency histogram grows a second
mode that has nothing to do with the ledger.

**`fsync` and the disk.** Each capture is one commit. On a laptop SSD with `fsync` on, commit
latency dominates everything the Go code does. Benchmarks from a container on network storage
are measuring the storage.

**Retry rate.** `-retry-rate` sends duplicate requests with the same key. A replay is a cheaper
path than a capture — one indexed lookup, no locks, no writes — so a high retry rate *raises*
throughput. Reporting a number achieved at `-retry-rate 0.9` without saying so would be
dishonest.

**Settlement is serialised on the clearing account**, so its ceiling is
independent of capture throughput and of the partition count — see
`docs/ARCHITECTURE.md`. Measuring capture rate and calling it the engine's
throughput would be measuring the easy half.

**The consumer's commit interval.** Settlement throughput is bounded by how often
the consumer flushes offsets. The default batches them every 100ms; setting
`CommitInterval` negative restores one synchronous broker round trip per message,
which is roughly an order of magnitude slower and buys nothing the deduplication
table does not already provide. See `docs/ARCHITECTURE.md`.

**The settlement lag.** `cmd/loadgen` measures the synchronous capture path only. Settlement
happens behind Kafka. To measure end-to-end, watch the clearing balance return to zero:

```sh
watch -n1 "curl -s localhost:8080/v1/ledger/snapshot | jq '{clearing: .clearing_balance, pending: .pending_outbox, awaiting: .in_flight}'"
```

Or just open `http://localhost:8080` and watch the chart.

## Measured while building this

These were taken in the development container the engine was written in — 4 vCPU, no
dedicated storage — against the **in-memory store**, which serialises every transaction behind
one mutex. They are included as a floor and a sanity check, not as a result:

| configuration | throughput | p50 | p90 |
| --- | --- | --- | --- |
| 16 clients, 8 wallets, in-memory store | 1,269 req/s | 12.3 ms | 20.7 ms |
| 64 clients, 24 wallets, in-memory store | 236 req/s | 277 ms | 316 ms |

The second row is the interesting one: throughput *fell* as concurrency rose. That is the
in-memory store's global mutex, which is exactly what it is documented to be — a correctness
reference, not a performance target. A real measurement needs PostgreSQL, where the per-account
row locks let non-overlapping payments commit in parallel.

Recovery, from `make chaos` in the same container: 181 injected failures across all six
points, 824 broker deliveries, 12 recognised duplicates, **zero invariant violations**, and the
pipeline fully drained **610 ms** after the faults stopped.

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

Nine statements and one `fsync`. The statements are batched where pgx allows, so the round
trips are fewer than the statement count. Under contention, step 2 dominates; without it,
step 9 does.

A replayed request is steps 1 and a single `SELECT`, and nothing else.

## Profiling

```sh
go test ./internal/payments/ -run TestManyPaymentsKeepTheBooksBalanced \
  -cpuprofile cpu.out -count 5
go tool pprof -top cpu.out
```

That path uses the in-memory store, so it profiles the engine's own work — fingerprinting,
entry construction, validation, JSON encoding — with the database excluded. For the database
path, profile the running API via `net/http/pprof` or read `pg_stat_statements`.
