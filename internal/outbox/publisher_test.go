package outbox_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/arnavkasat/payment-engine/internal/events"
	"github.com/arnavkasat/payment-engine/internal/events/inmem"
	"github.com/arnavkasat/payment-engine/internal/faults"
	"github.com/arnavkasat/payment-engine/internal/metrics"
	"github.com/arnavkasat/payment-engine/internal/outbox"
	"github.com/arnavkasat/payment-engine/internal/storage"
	"github.com/arnavkasat/payment-engine/internal/storage/memory"
)

// recorder is an events.Publisher that remembers what it was given and can
// be told to fail.
type recorder struct {
	mu   sync.Mutex
	got  []events.Message
	fail error
}

func (r *recorder) Publish(ctx context.Context, msgs ...events.Message) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.fail != nil {
		return r.fail
	}
	r.got = append(r.got, msgs...)
	return nil
}

func (r *recorder) Close() error { return nil }

func (r *recorder) messages() []events.Message {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]events.Message(nil), r.got...)
}

func (r *recorder) setFailure(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.fail = err
}

func enqueue(t *testing.T, store storage.Store, n int) []uuid.UUID {
	t.Helper()
	ids := make([]uuid.UUID, 0, n)
	if err := store.Tx(context.Background(), func(ctx context.Context, tx storage.Tx) error {
		for i := 0; i < n; i++ {
			id := uuid.New()
			ids = append(ids, id)
			if err := tx.Enqueue(ctx, storage.OutboxMessage{
				ID:          id,
				AggregateID: uuid.New(),
				Type:        events.TypePaymentCaptured,
				Payload:     []byte(`{"n":1}`),
				CreatedAt:   time.Now().UTC(),
			}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	return ids
}

func TestDrainPublishesEverythingPending(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	pub := &recorder{}
	reg := metrics.NewRegistry()

	p, err := outbox.New(store, pub, outbox.Config{Batch: 2, Metrics: reg})
	if err != nil {
		t.Fatalf("outbox.New: %v", err)
	}
	ids := enqueue(t, store, 5)

	n, err := p.Drain(ctx)
	if err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if n != 5 {
		t.Errorf("published %d, want 5", n)
	}
	if got := len(pub.messages()); got != 5 {
		t.Errorf("broker received %d messages, want 5", got)
	}
	if pending, _ := store.PendingOutbox(ctx); pending != 0 {
		t.Errorf("pending = %d, want 0", pending)
	}
	if got := reg.CounterValue(outbox.MetricPublished, nil); got != 5 {
		t.Errorf("published counter = %d, want 5", got)
	}

	// The envelope must carry the outbox row id, because that is the only
	// identifier a consumer can deduplicate on across redeliveries.
	seen := map[string]bool{}
	for _, m := range pub.messages() {
		seen[m.ID] = true
		if m.Key == "" || m.Type != events.TypePaymentCaptured {
			t.Errorf("envelope = %+v", m)
		}
	}
	for _, id := range ids {
		if !seen[id.String()] {
			t.Errorf("message %s never reached the broker", id)
		}
	}

	// A second drain has nothing to do.
	if n, err := p.Drain(ctx); err != nil || n != 0 {
		t.Errorf("second Drain = %d, %v, want 0, nil", n, err)
	}
}

func TestPublishFailureReleasesMessagesForRetry(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	boom := errors.New("broker unreachable")
	pub := &recorder{fail: boom}
	reg := metrics.NewRegistry()

	p, err := outbox.New(store, pub, outbox.Config{Batch: 10, Metrics: reg})
	if err != nil {
		t.Fatalf("outbox.New: %v", err)
	}
	enqueue(t, store, 3)

	if _, err := p.Drain(ctx); !errors.Is(err, boom) {
		t.Fatalf("Drain = %v, want the broker error", err)
	}
	if pending, _ := store.PendingOutbox(ctx); pending != 3 {
		t.Errorf("pending = %d, want 3: a failed publish must not retire rows", pending)
	}
	if got := reg.CounterValue(outbox.MetricPublishFails, nil); got != 3 {
		t.Errorf("failure counter = %d, want 3", got)
	}

	// Released rows are available again at once, without waiting for the
	// lease to lapse, and carry the cause for operators to read.
	leased, err := store.LeaseOutbox(ctx, "inspector", 10, time.Minute)
	if err != nil {
		t.Fatalf("LeaseOutbox: %v", err)
	}
	if len(leased) != 3 {
		t.Fatalf("re-leased %d messages, want 3", len(leased))
	}
	if leased[0].LastError != boom.Error() {
		t.Errorf("LastError = %q, want %q", leased[0].LastError, boom)
	}

	// Once the broker recovers, the same rows publish successfully.
	for _, m := range leased {
		if err := store.ReleaseOutbox(ctx, m.ID, ""); err != nil {
			t.Fatalf("ReleaseOutbox: %v", err)
		}
	}
	pub.setFailure(nil)
	if n, err := p.Drain(ctx); err != nil || n != 3 {
		t.Fatalf("Drain after recovery = %d, %v, want 3, nil", n, err)
	}
	if pending, _ := store.PendingOutbox(ctx); pending != 0 {
		t.Errorf("pending = %d, want 0", pending)
	}
}

// TestCrashAfterPublishRedelivers covers the gap the whole consumer-side
// dedupe exists to close: the broker accepted the message, the process died
// before the row could be retired, so the message is published twice.
func TestCrashAfterPublishRedelivers(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	pub := &recorder{}
	injector := faults.New(1)
	injector.Set(faults.OutboxAfterPublish, 1.0)

	p, err := outbox.New(store, pub, outbox.Config{
		Batch:  10,
		Lease:  40 * time.Millisecond,
		Faults: injector,
	})
	if err != nil {
		t.Fatalf("outbox.New: %v", err)
	}
	enqueue(t, store, 2)

	if _, err := p.Drain(ctx); !errors.Is(err, faults.ErrInjected) {
		t.Fatalf("Drain = %v, want an injected failure", err)
	}
	if got := len(pub.messages()); got != 2 {
		t.Fatalf("broker received %d messages, want 2", got)
	}
	if pending, _ := store.PendingOutbox(ctx); pending != 2 {
		t.Errorf("pending = %d, want 2: the rows were never retired", pending)
	}

	// Within the lease, no other worker picks them up.
	if again, _ := store.LeaseOutbox(ctx, "other", 10, time.Minute); len(again) != 0 {
		t.Errorf("another worker leased %d messages while the lease was live", len(again))
	}

	time.Sleep(80 * time.Millisecond)
	injector.Disable()

	if n, err := p.Drain(ctx); err != nil || n != 2 {
		t.Fatalf("Drain after lease expiry = %d, %v, want 2, nil", n, err)
	}
	if got := len(pub.messages()); got != 4 {
		t.Errorf("broker received %d messages, want 4: two originals and two duplicates", got)
	}
	if pending, _ := store.PendingOutbox(ctx); pending != 0 {
		t.Errorf("pending = %d, want 0", pending)
	}
}

func TestRunStopsOnContextCancellation(t *testing.T) {
	store := memory.New()
	bus := inmem.New(64, time.Millisecond)
	t.Cleanup(func() { _ = bus.Close() })

	p, err := outbox.New(store, bus, outbox.Config{Batch: 4, Interval: time.Millisecond})
	if err != nil {
		t.Fatalf("outbox.New: %v", err)
	}
	enqueue(t, store, 4)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()

	deadline := time.After(2 * time.Second)
	for {
		pending, err := store.PendingOutbox(context.Background())
		if err != nil {
			t.Fatalf("PendingOutbox: %v", err)
		}
		if pending == 0 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("publisher did not drain the outbox: %d still pending", pending)
		case <-time.After(5 * time.Millisecond):
		}
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Run = %v, want nil on cancellation", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}
	if got := bus.Depth(); got != 4 {
		t.Errorf("bus holds %d messages, want 4", got)
	}
}

func TestNewRejectsMissingCollaborators(t *testing.T) {
	if _, err := outbox.New(nil, &recorder{}, outbox.Config{}); err == nil {
		t.Error("outbox.New with no store returned no error")
	}
	if _, err := outbox.New(memory.New(), nil, outbox.Config{}); err == nil {
		t.Error("outbox.New with no publisher returned no error")
	}
}
