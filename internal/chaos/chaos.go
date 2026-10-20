// Package chaos runs the whole pipeline — capture, outbox, broker, consumer,
// settlement — while killing it at every durability boundary, and then
// checks that the ledger still balances and every captured payment reached
// its payee.
//
// The claim being tested is narrow and falsifiable: faults may lose work,
// duplicate work or delay work, but they may never move money twice, lose
// money, or leave a payment captured and unsettled once the faults stop.
package chaos

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/arnavkasat/payment-engine/internal/consumer"
	"github.com/arnavkasat/payment-engine/internal/events"
	"github.com/arnavkasat/payment-engine/internal/events/inmem"
	"github.com/arnavkasat/payment-engine/internal/faults"
	"github.com/arnavkasat/payment-engine/internal/ledger"
	"github.com/arnavkasat/payment-engine/internal/metrics"
	"github.com/arnavkasat/payment-engine/internal/money"
	"github.com/arnavkasat/payment-engine/internal/outbox"
	"github.com/arnavkasat/payment-engine/internal/payments"
	"github.com/arnavkasat/payment-engine/internal/storage"
)

// Options configures a run.
type Options struct {
	// Store is the store under test. Required.
	Store storage.Store
	// Payments is how many distinct payments to attempt.
	Payments int
	// Workers is how many clients issue them concurrently.
	Workers int
	// Wallets is how many payer/payee wallets to spread the payments over.
	// More wallets means less lock contention; fewer means more.
	Wallets int
	// FaultProbability is the chance each injection point fires, in [0,1].
	FaultProbability float64
	// Points limits which injection points are armed. Nil means every
	// point; an empty non-nil slice arms none, which is the control run.
	Points []faults.Point
	// RetryRate is the chance a client immediately retries a request with
	// the same idempotency key, imitating a client that lost the response.
	RetryRate float64
	// Seed makes a run reproducible.
	Seed int64
	// RecoveryTimeout bounds how long the pipeline may take to drain once
	// the faults stop.
	RecoveryTimeout time.Duration
	// OutboxLease is how long a publisher holds a message. Short leases
	// recover quickly from a publisher that died mid-publish.
	OutboxLease time.Duration
	// Logger receives progress. Nil discards it.
	Logger *slog.Logger
}

func (o *Options) applyDefaults() error {
	if o.Store == nil {
		return errors.New("chaos: store is required")
	}
	if o.Payments <= 0 {
		o.Payments = 500
	}
	if o.Workers <= 0 {
		o.Workers = 8
	}
	if o.Wallets < 2 {
		o.Wallets = 8
	}
	if o.FaultProbability <= 0 {
		o.FaultProbability = 0.1
	}
	if o.Points == nil {
		o.Points = faults.AllPoints
	}
	if o.Seed == 0 {
		o.Seed = time.Now().UnixNano()
	}
	if o.RecoveryTimeout <= 0 {
		o.RecoveryTimeout = 30 * time.Second
	}
	if o.OutboxLease <= 0 {
		o.OutboxLease = 500 * time.Millisecond
	}
	if o.Logger == nil {
		o.Logger = slog.New(slog.DiscardHandler)
	}
	return nil
}

// Result is what a run observed.
type Result struct {
	// Attempted is how many capture calls were made, retries included.
	Attempted int
	// Captured is how many calls returned a fresh payment.
	Captured int
	// Replayed is how many returned a stored response instead of paying
	// again — the retries the idempotency keys absorbed.
	Replayed int
	// Failed is how many calls returned an error, injected or otherwise.
	Failed int

	// FaultsFired counts injected failures by point.
	FaultsFired map[faults.Point]int
	// TotalFaults is their sum.
	TotalFaults int

	// Deliveries is how many messages the broker handed to the consumer,
	// redeliveries included; Duplicates is how many of those the dedupe
	// table recognised and skipped.
	Deliveries int
	Duplicates int

	// Settled is how many payments reached their payee.
	Settled int
	// Unsettled is how many were still captured when the run ended. It must
	// be zero after a successful recovery.
	Unsettled int
	// PendingOutbox is how many events were still unpublished at the end.
	PendingOutbox int
	// ClearingBalance is what was left parked in the clearing account. It
	// is the strongest single check the harness has: clearing receives
	// exactly the amount of every capture and gives up exactly the amount
	// of every settlement, so a zero balance means every capture that ever
	// committed has settled — including ones whose caller saw an error and
	// never learned their id.
	ClearingBalance money.Minor

	// DuringFaults is the invariant report taken while faults were still
	// firing. The ledger must balance even mid-chaos.
	DuringFaults ledger.Report
	// Final is the invariant report after recovery.
	Final ledger.Report
	// RecoveryDuration is how long the pipeline took to drain completely
	// once the faults were switched off.
	RecoveryDuration time.Duration
	// Elapsed is the wall-clock time of the whole run.
	Elapsed time.Duration
}

// OK reports whether the run upheld every guarantee.
func (r Result) OK() bool {
	return r.Final.OK() && r.DuringFaults.OK() &&
		r.Unsettled == 0 && r.PendingOutbox == 0 && r.ClearingBalance == 0
}

// Run executes a chaos run and returns what it observed. It returns an error
// only when the harness itself could not complete; a violated invariant is
// reported in the Result, because the caller usually wants to print the
// detail rather than a bare failure.
func Run(ctx context.Context, opts Options) (Result, error) {
	if err := opts.applyDefaults(); err != nil {
		return Result{}, err
	}
	start := time.Now()

	injector := faults.New(opts.Seed)
	reg := metrics.NewRegistry()
	bus := inmem.New(4096, 2*time.Millisecond)
	defer func() { _ = bus.Close() }()

	world, clearing, fees, wallets, err := setupAccounts(ctx, opts)
	if err != nil {
		return Result{}, err
	}

	// One shard: the harness is testing correctness under failure, and a
	// single clearing account makes "did everything settle?" a single
	// balance read. Throughput sharding is orthogonal and tested elsewhere.
	svc, err := payments.New(opts.Store, payments.Config{
		Clearing: []uuid.UUID{clearing}, Fees: []uuid.UUID{fees}, External: world,
		Faults: injector, Metrics: reg,
	})
	if err != nil {
		return Result{}, err
	}
	if err := fundWallets(ctx, svc, wallets); err != nil {
		return Result{}, err
	}

	publisher, err := outbox.New(opts.Store, bus, outbox.Config{
		Owner:    "chaos-publisher",
		Batch:    64,
		Interval: 2 * time.Millisecond,
		Lease:    opts.OutboxLease,
		Faults:   injector,
		Metrics:  reg,
		Logger:   opts.Logger,
	})
	if err != nil {
		return Result{}, err
	}
	processor, err := consumer.New(opts.Store, consumer.Config{
		Group:   "settlement",
		Faults:  injector,
		Metrics: reg,
		Logger:  opts.Logger,
	})
	if err != nil {
		return Result{}, err
	}
	processor.Register(events.TypePaymentCaptured, svc.HandleCaptured)

	pipelineCtx, stopPipeline := context.WithCancel(ctx)
	defer stopPipeline()
	var pipeline sync.WaitGroup
	pipeline.Add(2)
	go func() { defer pipeline.Done(); _ = publisher.Run(pipelineCtx) }()
	go func() { defer pipeline.Done(); _ = bus.Run(pipelineCtx, processor.Handle) }()

	// Arm the faults and drive traffic through the pipeline.
	for _, p := range opts.Points {
		injector.Set(p, opts.FaultProbability)
	}
	opts.Logger.Info("chaos phase starting",
		"payments", opts.Payments, "workers", opts.Workers,
		"fault_probability", opts.FaultProbability, "points", len(opts.Points), "seed", opts.Seed)

	result, paymentIDs := drive(ctx, svc, opts, wallets)

	// The books must balance even now, with faults still firing and work
	// still in flight. A payment part-way through the pipeline is money
	// sitting in clearing, not money that vanished.
	if result.DuringFaults, err = opts.Store.VerifyInvariants(ctx); err != nil {
		return result, fmt.Errorf("chaos: verifying mid-run: %w", err)
	}
	opts.Logger.Info("chaos phase complete",
		"captured", result.Captured, "replayed", result.Replayed, "failed", result.Failed,
		"faults", injector.Total(), "invariants_ok", result.DuringFaults.OK())

	// Recovery: stop the faults and wait for the pipeline to drain.
	injector.Disable()
	recoveryStart := time.Now()
	settled, drainErr := waitForDrain(ctx, opts, clearing, paymentIDs)
	result.RecoveryDuration = time.Since(recoveryStart)
	result.Settled = settled

	stopPipeline()
	pipeline.Wait()

	result.FaultsFired = map[faults.Point]int{}
	for _, p := range faults.AllPoints {
		if n := injector.Fired(p); n > 0 {
			result.FaultsFired[p] = n
		}
	}
	result.TotalFaults = injector.Total()
	result.Deliveries = bus.Deliveries()
	result.Duplicates = int(reg.CounterValue(consumer.MetricDuplicates,
		metrics.Labels{"type": events.TypePaymentCaptured, "outcome": "duplicate"}))
	result.Unsettled = len(paymentIDs) - settled

	if result.PendingOutbox, err = opts.Store.PendingOutbox(ctx); err != nil {
		return result, fmt.Errorf("chaos: counting pending outbox: %w", err)
	}
	clearingAccount, err := opts.Store.GetAccount(ctx, clearing)
	if err != nil {
		return result, fmt.Errorf("chaos: reading clearing account: %w", err)
	}
	result.ClearingBalance = clearingAccount.Balance
	if result.Final, err = opts.Store.VerifyInvariants(ctx); err != nil {
		return result, fmt.Errorf("chaos: verifying after recovery: %w", err)
	}
	result.Elapsed = time.Since(start)

	if drainErr != nil {
		return result, drainErr
	}
	return result, nil
}

// drive issues the payment traffic and returns what the clients observed,
// along with the set of payments that were actually committed.
func drive(ctx context.Context, svc *payments.Service, opts Options, wallets []uuid.UUID) (Result, map[uuid.UUID]struct{}) {
	var (
		attempted, captured, replayed, failed atomic.Int64
		mu                                    sync.Mutex
		ids                                   = map[uuid.UUID]struct{}{}
	)

	jobs := make(chan int)
	var wg sync.WaitGroup
	for w := 0; w < opts.Workers; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			rnd := rand.New(rand.NewSource(opts.Seed + int64(worker)*7919))
			for n := range jobs {
				payer := wallets[rnd.Intn(len(wallets))]
				payee := wallets[rnd.Intn(len(wallets))]
				for payee == payer {
					payee = wallets[rnd.Intn(len(wallets))]
				}
				req := payments.CreateRequest{
					IdempotencyKey: fmt.Sprintf("chaos-%d", n),
					PayerAccount:   payer,
					PayeeAccount:   payee,
					Amount:         money.Minor(rnd.Intn(500) + 1),
					Fee:            money.Minor(rnd.Intn(5)),
					Currency:       "USD",
					Reference:      fmt.Sprintf("chaos/%d", n),
				}

				// Each payment is attempted, and sometimes attempted again
				// with the same key, exactly as a client that lost a
				// response would.
				tries := 1
				if rnd.Float64() < opts.RetryRate {
					tries = 2
				}
				for t := 0; t < tries; t++ {
					attempted.Add(1)
					res, err := svc.CreatePayment(ctx, req)
					switch {
					case err != nil:
						failed.Add(1)
					case res.Replayed:
						replayed.Add(1)
					default:
						captured.Add(1)
					}
					if err == nil && res.Payment.ID != uuid.Nil {
						mu.Lock()
						ids[res.Payment.ID] = struct{}{}
						mu.Unlock()
					}
				}
			}
		}(w)
	}

	for n := 0; n < opts.Payments; n++ {
		select {
		case <-ctx.Done():
			close(jobs)
			wg.Wait()
			goto done
		case jobs <- n:
		}
	}
	close(jobs)
	wg.Wait()
done:
	return Result{
		Attempted: int(attempted.Load()),
		Captured:  int(captured.Load()),
		Replayed:  int(replayed.Load()),
		Failed:    int(failed.Load()),
	}, ids
}

// waitForDrain blocks until the outbox is empty, every payment the clients
// know about has settled, and nothing is left parked in clearing — or the
// recovery timeout expires.
func waitForDrain(ctx context.Context, opts Options, clearing uuid.UUID, ids map[uuid.UUID]struct{}) (int, error) {
	deadline := time.Now().Add(opts.RecoveryTimeout)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		settled, err := countSettled(ctx, opts.Store, ids)
		if err != nil {
			return settled, err
		}
		pending, err := opts.Store.PendingOutbox(ctx)
		if err != nil {
			return settled, err
		}
		parked, err := opts.Store.GetAccount(ctx, clearing)
		if err != nil {
			return settled, err
		}
		if settled == len(ids) && pending == 0 && parked.Balance == 0 {
			return settled, nil
		}
		if time.Now().After(deadline) {
			return settled, fmt.Errorf("chaos: pipeline did not drain within %s: %d of %d payments settled, %d events pending, %d parked in clearing",
				opts.RecoveryTimeout, settled, len(ids), pending, parked.Balance)
		}
		select {
		case <-ctx.Done():
			return settled, ctx.Err()
		case <-ticker.C:
		}
	}
}

func countSettled(ctx context.Context, store storage.Store, ids map[uuid.UUID]struct{}) (int, error) {
	var n int
	for id := range ids {
		p, err := store.GetPayment(ctx, id)
		if err != nil {
			return n, fmt.Errorf("chaos: reading payment %s: %w", id, err)
		}
		if p.Status == storage.StatusSettled {
			n++
		}
	}
	return n, nil
}

// setupAccounts creates the system accounts and the wallets traffic flows
// between. Existing accounts are reused, so a run can be pointed at a
// database that already has them.
func setupAccounts(ctx context.Context, opts Options) (world, clearing, fees uuid.UUID, wallets []uuid.UUID, err error) {
	ensure := func(name string, kind storage.AccountKind, allowNegative bool) (uuid.UUID, error) {
		if a, err := opts.Store.GetAccountByName(ctx, name); err == nil {
			return a.ID, nil
		} else if !errors.Is(err, storage.ErrNotFound) {
			return uuid.Nil, err
		}
		a, err := opts.Store.CreateAccount(ctx, storage.Account{
			Name: name, Currency: "USD", Kind: kind, AllowNegative: allowNegative,
		})
		if err != nil {
			return uuid.Nil, err
		}
		return a.ID, nil
	}

	// The account names are suffixed with the seed so that repeated runs
	// against one database do not fight over the same wallets.
	suffix := fmt.Sprintf("-chaos-%d", opts.Seed)
	if world, err = ensure("world"+suffix, storage.KindExternal, true); err != nil {
		return
	}
	if clearing, err = ensure("clearing"+suffix, storage.KindClearing, false); err != nil {
		return
	}
	if fees, err = ensure("fees"+suffix, storage.KindRevenue, false); err != nil {
		return
	}
	for i := 0; i < opts.Wallets; i++ {
		id, wErr := ensure(fmt.Sprintf("wallet-%d%s", i, suffix), storage.KindWallet, false)
		if wErr != nil {
			return world, clearing, fees, wallets, wErr
		}
		wallets = append(wallets, id)
	}
	return
}

// fundWallets gives every wallet enough to pay with. The deposits go through
// the service, so they are idempotent and journalled like everything else.
func fundWallets(ctx context.Context, svc *payments.Service, wallets []uuid.UUID) error {
	for i, id := range wallets {
		if _, err := svc.Deposit(ctx, payments.DepositRequest{
			IdempotencyKey: fmt.Sprintf("chaos-fund-%s", id),
			Account:        id,
			Amount:         10_000_000,
			Currency:       "USD",
			Reference:      fmt.Sprintf("opening balance %d", i),
		}); err != nil {
			return fmt.Errorf("chaos: funding wallet %s: %w", id, err)
		}
	}
	return nil
}
