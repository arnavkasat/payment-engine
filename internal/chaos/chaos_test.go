package chaos_test

import (
	"context"
	"testing"
	"time"

	"github.com/arnavkasat/payment-engine/internal/chaos"
	"github.com/arnavkasat/payment-engine/internal/faults"
	"github.com/arnavkasat/payment-engine/internal/storage/memory"
)

// TestChaosRunKeepsTheLedgerBalanced is the headline property, run as an
// ordinary test so that CI enforces it on every commit.
func TestChaosRunKeepsTheLedgerBalanced(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	result, err := chaos.Run(ctx, chaos.Options{
		Store:            memory.New(),
		Payments:         300,
		Workers:          8,
		Wallets:          6,
		FaultProbability: 0.15,
		RetryRate:        0.3,
		Seed:             20260201,
		RecoveryTimeout:  30 * time.Second,
	})
	if err != nil {
		t.Fatalf("chaos.Run: %v", err)
	}

	if result.TotalFaults == 0 {
		t.Fatal("no faults fired; the run proved nothing")
	}
	if !result.DuringFaults.OK() {
		t.Errorf("ledger was unbalanced while faults were firing: %s", result.DuringFaults.Summary())
	}
	if !result.Final.OK() {
		t.Errorf("ledger was unbalanced after recovery: %s", result.Final.Summary())
	}
	if result.Final.TotalPostingSum != 0 {
		t.Errorf("postings sum to %d, want 0", result.Final.TotalPostingSum)
	}
	if result.Unsettled != 0 {
		t.Errorf("%d payments never settled", result.Unsettled)
	}
	if result.PendingOutbox != 0 {
		t.Errorf("%d events never published", result.PendingOutbox)
	}
	if result.ClearingBalance != 0 {
		t.Errorf("%d left parked in clearing: a capture never settled", result.ClearingBalance)
	}
	if !result.OK() {
		t.Error("Result.OK() = false")
	}
	if result.Captured == 0 {
		t.Error("no payments were captured")
	}
	if result.Replayed == 0 {
		t.Error("no retries were absorbed by idempotency keys")
	}

	t.Logf("attempted=%d captured=%d replayed=%d failed=%d faults=%d deliveries=%d duplicates=%d recovery=%s",
		result.Attempted, result.Captured, result.Replayed, result.Failed,
		result.TotalFaults, result.Deliveries, result.Duplicates, result.RecoveryDuration)
}

// TestEachFaultPointInIsolation pins down which guarantee each injection
// point exercises: with that one point firing on every opportunity, the
// system must still end balanced and fully settled.
func TestEachFaultPointInIsolation(t *testing.T) {
	for _, point := range faults.AllPoints {
		t.Run(string(point), func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()

			result, err := chaos.Run(ctx, chaos.Options{
				Store:            memory.New(),
				Payments:         40,
				Workers:          4,
				Wallets:          4,
				Points:           []faults.Point{point},
				FaultProbability: 0.5,
				RetryRate:        1.0, // every request is retried
				Seed:             7,
				RecoveryTimeout:  30 * time.Second,
				OutboxLease:      200 * time.Millisecond,
			})
			if err != nil {
				t.Fatalf("chaos.Run: %v", err)
			}
			if result.FaultsFired[point] == 0 {
				t.Fatalf("no faults fired at %s", point)
			}
			if !result.OK() {
				t.Errorf("run not OK: final=%s unsettled=%d pending=%d clearing=%d",
					result.Final.Summary(), result.Unsettled, result.PendingOutbox, result.ClearingBalance)
			}
			t.Logf("%s: fired=%d captured=%d replayed=%d failed=%d recovery=%s",
				point, result.FaultsFired[point], result.Captured, result.Replayed,
				result.Failed, result.RecoveryDuration)
		})
	}
}

// TestCleanRunHasNoFailures is the control: with no faults armed, nothing
// should fail and every payment should settle promptly.
func TestCleanRunHasNoFailures(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	result, err := chaos.Run(ctx, chaos.Options{
		Store:            memory.New(),
		Payments:         100,
		Workers:          4,
		Wallets:          4,
		Points:           []faults.Point{},
		FaultProbability: 0,
		RetryRate:        0.5,
		Seed:             1,
		RecoveryTimeout:  30 * time.Second,
	})
	if err != nil {
		t.Fatalf("chaos.Run: %v", err)
	}
	if result.Captured != 100 {
		t.Errorf("captured %d payments, want 100", result.Captured)
	}
	if result.Failed != 0 {
		t.Errorf("%d requests failed with no faults armed", result.Failed)
	}
	if !result.OK() {
		t.Errorf("clean run not OK: %s", result.Final.Summary())
	}
}
