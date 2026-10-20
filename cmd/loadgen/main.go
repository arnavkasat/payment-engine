// Command loadgen drives the HTTP API and reports throughput and latency.
//
// Percentiles are computed from the raw samples rather than from histogram
// buckets, so the numbers it prints are exact for the run it measured. It
// also issues duplicate requests on purpose, because a payment API's retry
// path is part of its steady-state load, not an exception.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "loadgen: %v\n", err)
		os.Exit(1)
	}
}

type options struct {
	baseURL     string
	concurrency int
	duration    time.Duration
	requests    int
	wallets     int
	amount      int64
	fee         int64
	retryRate   float64
	warmup      time.Duration
	asJSON      bool
	timeout     time.Duration
}

func run() error {
	var o options
	flag.StringVar(&o.baseURL, "url", "http://localhost:8080", "base URL of the API")
	flag.IntVar(&o.concurrency, "concurrency", 32, "concurrent clients")
	flag.DurationVar(&o.duration, "duration", 10*time.Second, "how long to send traffic; ignored when -requests is set")
	flag.IntVar(&o.requests, "requests", 0, "total requests to send; overrides -duration")
	flag.IntVar(&o.wallets, "wallets", 16, "wallets to spread traffic over; fewer means more row contention")
	flag.Int64Var(&o.amount, "amount", 1000, "payment amount in minor units")
	flag.Int64Var(&o.fee, "fee", 25, "fee in minor units")
	flag.Float64Var(&o.retryRate, "retry-rate", 0.1, "fraction of requests resent with the same idempotency key")
	flag.DurationVar(&o.warmup, "warmup", 2*time.Second, "warm-up period excluded from the results")
	flag.DurationVar(&o.timeout, "timeout", 30*time.Second, "per-request timeout")
	flag.BoolVar(&o.asJSON, "json", false, "print the summary as JSON")
	flag.Parse()

	if o.concurrency <= 0 {
		return fmt.Errorf("-concurrency must be positive")
	}
	if o.wallets < 2 {
		return fmt.Errorf("-wallets must be at least 2")
	}

	ctx := context.Background()
	client := &http.Client{
		Timeout: o.timeout,
		Transport: &http.Transport{
			// The default of 2 idle connections per host throttles a load
			// generator into measuring its own connection churn.
			MaxIdleConns:        o.concurrency * 2,
			MaxIdleConnsPerHost: o.concurrency * 2,
			MaxConnsPerHost:     o.concurrency * 2,
		},
	}

	fmt.Fprintf(os.Stderr, "preparing %d wallets at %s\n", o.wallets, o.baseURL)
	wallets, err := prepare(ctx, client, o)
	if err != nil {
		return err
	}

	if o.warmup > 0 {
		fmt.Fprintf(os.Stderr, "warming up for %s\n", o.warmup)
		warmCtx, cancel := context.WithTimeout(ctx, o.warmup)
		drive(warmCtx, client, o, wallets, &stats{}, 0)
		cancel()
	}

	var s stats
	fmt.Fprintf(os.Stderr, "sending traffic with %d clients\n", o.concurrency)
	start := time.Now()
	runCtx, cancel := context.WithCancel(ctx)
	if o.requests <= 0 {
		runCtx, cancel = context.WithTimeout(ctx, o.duration)
	}
	drive(runCtx, client, o, wallets, &s, o.requests)
	cancel()
	elapsed := time.Since(start)

	summary := s.summarise(elapsed)
	if o.asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(summary)
	}
	summary.print()
	return nil
}

// stats collects per-request outcomes. Latencies are kept in full so that
// percentiles are exact rather than interpolated.
type stats struct {
	mu        sync.Mutex
	latencies []time.Duration

	ok       atomic.Int64
	replayed atomic.Int64
	conflict atomic.Int64
	rejected atomic.Int64
	failed   atomic.Int64

	byStatus sync.Map // int -> *atomic.Int64
}

func (s *stats) record(d time.Duration, status int, replay bool, err error) {
	if err != nil {
		s.failed.Add(1)
		return
	}
	s.mu.Lock()
	s.latencies = append(s.latencies, d)
	s.mu.Unlock()

	counter, _ := s.byStatus.LoadOrStore(status, new(atomic.Int64))
	counter.(*atomic.Int64).Add(1)

	switch {
	case status == http.StatusCreated && replay:
		s.replayed.Add(1)
	case status == http.StatusCreated:
		s.ok.Add(1)
	case status == http.StatusConflict:
		s.conflict.Add(1)
	case status >= 400 && status < 500:
		s.rejected.Add(1)
	default:
		s.failed.Add(1)
	}
}

// summary is the report, in a shape that is also useful as JSON.
type summary struct {
	Requests  int                `json:"requests"`
	Elapsed   string             `json:"elapsed"`
	TPS       float64            `json:"tps"`
	Created   int64              `json:"created"`
	Replayed  int64              `json:"replayed"`
	Conflicts int64              `json:"conflicts"`
	Rejected  int64              `json:"rejected"`
	Failed    int64              `json:"failed"`
	ByStatus  map[string]int64   `json:"by_status"`
	LatencyMs map[string]float64 `json:"latency_ms"`
}

func (s *stats) summarise(elapsed time.Duration) summary {
	s.mu.Lock()
	latencies := append([]time.Duration(nil), s.latencies...)
	s.mu.Unlock()
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })

	byStatus := map[string]int64{}
	s.byStatus.Range(func(k, v any) bool {
		byStatus[fmt.Sprint(k)] = v.(*atomic.Int64).Load()
		return true
	})

	total := len(latencies)
	out := summary{
		Requests:  total,
		Elapsed:   elapsed.Round(time.Millisecond).String(),
		Created:   s.ok.Load(),
		Replayed:  s.replayed.Load(),
		Conflicts: s.conflict.Load(),
		Rejected:  s.rejected.Load(),
		Failed:    s.failed.Load(),
		ByStatus:  byStatus,
		LatencyMs: map[string]float64{},
	}
	if elapsed > 0 {
		out.TPS = float64(total) / elapsed.Seconds()
	}
	for name, q := range map[string]float64{"p50": 0.50, "p90": 0.90, "p95": 0.95, "p99": 0.99, "p999": 0.999} {
		out.LatencyMs[name] = millis(percentile(latencies, q))
	}
	if total > 0 {
		out.LatencyMs["min"] = millis(latencies[0])
		out.LatencyMs["max"] = millis(latencies[total-1])
		var sum time.Duration
		for _, d := range latencies {
			sum += d
		}
		out.LatencyMs["mean"] = millis(sum / time.Duration(total))
	}
	return out
}

// percentile returns the nearest-rank percentile of a sorted slice.
func percentile(sorted []time.Duration, q float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	rank := int(q*float64(len(sorted))+0.5) - 1
	if rank < 0 {
		rank = 0
	}
	if rank >= len(sorted) {
		rank = len(sorted) - 1
	}
	return sorted[rank]
}

func millis(d time.Duration) float64 {
	return float64(d.Microseconds()) / 1000
}

func (s summary) print() {
	fmt.Printf("requests   %d in %s (%.0f req/s)\n", s.Requests, s.Elapsed, s.TPS)
	fmt.Printf("outcomes   created %d, replayed %d, conflicts %d, rejected %d, failed %d\n",
		s.Created, s.Replayed, s.Conflicts, s.Rejected, s.Failed)

	codes := make([]string, 0, len(s.ByStatus))
	for code := range s.ByStatus {
		codes = append(codes, code)
	}
	sort.Strings(codes)
	for _, code := range codes {
		fmt.Printf("  HTTP %s  %d\n", code, s.ByStatus[code])
	}

	fmt.Println("latency (ms)")
	for _, name := range []string{"min", "mean", "p50", "p90", "p95", "p99", "p999", "max"} {
		if v, ok := s.LatencyMs[name]; ok {
			fmt.Printf("  %-5s %8.2f\n", name, v)
		}
	}
}

// drive sends traffic until the context is cancelled or the request budget
// is spent.
func drive(ctx context.Context, client *http.Client, o options, wallets []uuid.UUID, s *stats, budget int) {
	var sent atomic.Int64
	var wg sync.WaitGroup
	for w := 0; w < o.concurrency; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			rnd := rand.New(rand.NewSource(time.Now().UnixNano() + int64(worker)*7919))
			for {
				if ctx.Err() != nil {
					return
				}
				if budget > 0 && sent.Add(1) > int64(budget) {
					return
				}

				payer := wallets[rnd.Intn(len(wallets))]
				payee := wallets[rnd.Intn(len(wallets))]
				for payee == payer {
					payee = wallets[rnd.Intn(len(wallets))]
				}
				key := uuid.NewString()
				body := fmt.Sprintf(
					`{"payer_account":%q,"payee_account":%q,"amount":%d,"fee":%d,"currency":"USD","reference":"loadgen"}`,
					payer, payee, o.amount, o.fee)

				post(ctx, client, o, s, key, body)
				// Some clients lose the response and ask again. Those
				// retries cost a database round trip and must never cost a
				// second payment.
				if rnd.Float64() < o.retryRate {
					post(ctx, client, o, s, key, body)
				}
			}
		}(w)
	}
	wg.Wait()
}

func post(ctx context.Context, client *http.Client, o options, s *stats, key, body string) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, o.baseURL+"/v1/payments", bytes.NewReader([]byte(body)))
	if err != nil {
		s.record(0, 0, false, err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", key)

	start := time.Now()
	resp, err := client.Do(req)
	elapsed := time.Since(start)
	if err != nil {
		if ctx.Err() == nil {
			s.record(elapsed, 0, false, err)
		}
		return
	}
	replay := resp.Header.Get("Idempotent-Replay") == "true"
	// The body must be drained for the connection to be reused; a load
	// generator that leaks connections measures the leak.
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	s.record(elapsed, resp.StatusCode, replay, nil)
}

// prepare creates the wallets and funds them, so that the measured phase
// never fails for want of money.
func prepare(ctx context.Context, client *http.Client, o options) ([]uuid.UUID, error) {
	run := uuid.NewString()[:8]
	wallets := make([]uuid.UUID, 0, o.wallets)
	for i := 0; i < o.wallets; i++ {
		name := fmt.Sprintf("loadgen-%s-%d", run, i)
		var account struct {
			ID uuid.UUID `json:"id"`
		}
		if err := call(ctx, client, o.baseURL+"/v1/accounts", "",
			fmt.Sprintf(`{"name":%q,"currency":"USD","kind":"wallet"}`, name), &account); err != nil {
			return nil, fmt.Errorf("creating wallet %s: %w", name, err)
		}
		// Fund generously: the run must not end because a wallet ran dry.
		if err := call(ctx, client,
			fmt.Sprintf("%s/v1/accounts/%s/deposits", o.baseURL, account.ID),
			"loadgen-fund-"+account.ID.String(),
			`{"amount":1000000000,"currency":"USD","reference":"loadgen float"}`, nil); err != nil {
			return nil, fmt.Errorf("funding wallet %s: %w", name, err)
		}
		wallets = append(wallets, account.ID)
	}
	return wallets, nil
}

func call(ctx context.Context, client *http.Client, url, key, body string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader([]byte(body)))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("%s: %s", resp.Status, bytes.TrimSpace(raw))
	}
	if out == nil {
		return nil
	}
	return json.Unmarshal(raw, out)
}
