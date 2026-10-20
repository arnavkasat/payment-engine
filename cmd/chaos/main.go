// Command chaos runs the payment pipeline under injected failures and
// reports whether the ledger survived.
//
// It exits non-zero if any invariant was violated, any payment was left
// unsettled, or anything was left parked in the clearing account, so it can
// be wired straight into CI or a nightly job.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/arnavkasat/payment-engine/internal/chaos"
	"github.com/arnavkasat/payment-engine/internal/faults"
	"github.com/arnavkasat/payment-engine/internal/storage"
	"github.com/arnavkasat/payment-engine/internal/storage/memory"
	"github.com/arnavkasat/payment-engine/internal/storage/postgres"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "chaos: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		dsn       = flag.String("dsn", "", "PostgreSQL connection string; empty runs against the in-memory store")
		payments  = flag.Int("payments", 500, "number of distinct payments to attempt")
		workers   = flag.Int("workers", 8, "concurrent clients")
		wallets   = flag.Int("wallets", 8, "wallets to spread payments over; fewer means more lock contention")
		prob      = flag.Float64("fault-probability", 0.1, "chance each injection point fires, 0 to 1")
		retryRate = flag.Float64("retry-rate", 0.3, "chance a client retries a request with the same idempotency key")
		pointList = flag.String("points", "", "comma-separated injection points; empty means all")
		seed      = flag.Int64("seed", 0, "random seed; 0 picks one from the clock")
		timeout   = flag.Duration("recovery-timeout", 30*time.Second, "how long the pipeline may take to drain once faults stop")
		lease     = flag.Duration("outbox-lease", 500*time.Millisecond, "how long a publisher holds a leased message")
		asJSON    = flag.Bool("json", false, "print the result as JSON")
		verbose   = flag.Bool("v", false, "log pipeline progress")
	)
	flag.Parse()

	points, err := parsePoints(*pointList)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	store, closeStore, err := openStore(ctx, *dsn)
	if err != nil {
		return err
	}
	defer closeStore()

	logger := slog.New(slog.DiscardHandler)
	if *verbose {
		logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	}

	result, runErr := chaos.Run(ctx, chaos.Options{
		Store:            store,
		Payments:         *payments,
		Workers:          *workers,
		Wallets:          *wallets,
		FaultProbability: *prob,
		Points:           points,
		RetryRate:        *retryRate,
		Seed:             *seed,
		RecoveryTimeout:  *timeout,
		OutboxLease:      *lease,
		Logger:           logger,
	})

	if *asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(result); err != nil {
			return err
		}
	} else {
		printReport(result)
	}

	if runErr != nil {
		return runErr
	}
	if !result.OK() {
		return fmt.Errorf("invariants violated: %s", result.Final.Summary())
	}
	return nil
}

func openStore(ctx context.Context, dsn string) (storage.Store, func(), error) {
	if dsn == "" {
		s := memory.New()
		return s, func() { _ = s.Close() }, nil
	}
	s, err := postgres.Open(ctx, postgres.Config{DSN: dsn, MaxConns: 32})
	if err != nil {
		return nil, nil, err
	}
	if err := postgres.Migrate(ctx, s.Pool()); err != nil {
		_ = s.Close()
		return nil, nil, err
	}
	return s, func() { _ = s.Close() }, nil
}

func parsePoints(list string) ([]faults.Point, error) {
	if strings.TrimSpace(list) == "" {
		return nil, nil // nil means every point
	}
	valid := map[string]faults.Point{}
	for _, p := range faults.AllPoints {
		valid[string(p)] = p
	}
	var out []faults.Point
	for _, raw := range strings.Split(list, ",") {
		name := strings.TrimSpace(raw)
		if name == "" {
			continue
		}
		p, ok := valid[name]
		if !ok {
			names := make([]string, 0, len(valid))
			for n := range valid {
				names = append(names, n)
			}
			sort.Strings(names)
			return nil, fmt.Errorf("unknown injection point %q; known points are %s", name, strings.Join(names, ", "))
		}
		out = append(out, p)
	}
	return out, nil
}

func printReport(r chaos.Result) {
	fmt.Println("chaos run")
	fmt.Println("=========")
	fmt.Printf("  requests      attempted %d, captured %d, replayed %d, failed %d\n",
		r.Attempted, r.Captured, r.Replayed, r.Failed)
	fmt.Printf("  faults        %d fired\n", r.TotalFaults)

	pts := make([]string, 0, len(r.FaultsFired))
	for p := range r.FaultsFired {
		pts = append(pts, string(p))
	}
	sort.Strings(pts)
	for _, p := range pts {
		fmt.Printf("                  %-28s %d\n", p, r.FaultsFired[faults.Point(p)])
	}

	fmt.Printf("  broker        %d deliveries, %d recognised as duplicates\n", r.Deliveries, r.Duplicates)
	fmt.Printf("  settlement    %d settled, %d unsettled, %d events pending\n", r.Settled, r.Unsettled, r.PendingOutbox)
	fmt.Printf("  clearing      %d left parked\n", r.ClearingBalance)
	fmt.Printf("  recovery      %s\n", r.RecoveryDuration.Round(time.Millisecond))
	fmt.Printf("  elapsed       %s\n", r.Elapsed.Round(time.Millisecond))
	fmt.Println()
	fmt.Printf("  invariants during faults : %s\n", status(r.DuringFaults.OK()))
	fmt.Printf("  invariants after recovery: %s\n", status(r.Final.OK()))
	fmt.Printf("  postings sum to %d across %d entries and %d accounts\n",
		r.Final.TotalPostingSum, r.Final.Entries, r.Final.Accounts)
	if !r.Final.OK() {
		fmt.Printf("  violations: %s\n", r.Final.Summary())
	}
	fmt.Println()
	fmt.Printf("RESULT: %s\n", status(r.OK()))
}

func status(ok bool) string {
	if ok {
		return "PASS"
	}
	return "FAIL"
}
