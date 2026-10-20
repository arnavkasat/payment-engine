package consumer_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"

	"github.com/arnavkasat/payment-engine/internal/consumer"
	"github.com/arnavkasat/payment-engine/internal/events"
	"github.com/arnavkasat/payment-engine/internal/faults"
	"github.com/arnavkasat/payment-engine/internal/metrics"
	"github.com/arnavkasat/payment-engine/internal/storage"
	"github.com/arnavkasat/payment-engine/internal/storage/memory"
)

func message(id string) events.Message {
	return events.Message{
		ID:      id,
		Key:     uuid.NewString(),
		Type:    events.TypePaymentCaptured,
		Payload: []byte(`{}`),
	}
}

// TestRedeliveriesRunTheHandlerOnce is the exactly-once-effect claim: a
// message delivered many times changes state once.
func TestRedeliveriesRunTheHandlerOnce(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	reg := metrics.NewRegistry()

	p, err := consumer.New(store, consumer.Config{Group: "settlement", Metrics: reg})
	if err != nil {
		t.Fatalf("consumer.New: %v", err)
	}
	var applied atomic.Int64
	p.Register(events.TypePaymentCaptured, func(ctx context.Context, tx storage.Tx, m events.Message) error {
		applied.Add(1)
		return nil
	})

	m := message("msg-1")
	for i := 0; i < 10; i++ {
		if err := p.Handle(ctx, m); err != nil {
			t.Fatalf("delivery %d: %v", i, err)
		}
	}
	if got := applied.Load(); got != 1 {
		t.Fatalf("handler ran %d times across 10 deliveries, want 1", got)
	}
	if got := reg.CounterValue(consumer.MetricDuplicates, metrics.Labels{"type": events.TypePaymentCaptured, "outcome": "duplicate"}); got != 9 {
		t.Errorf("duplicate counter = %d, want 9", got)
	}
	if got := reg.CounterValue(consumer.MetricHandled, metrics.Labels{"type": events.TypePaymentCaptured, "outcome": "applied"}); got != 1 {
		t.Errorf("applied counter = %d, want 1", got)
	}
}

func TestConcurrentRedeliveriesRunTheHandlerOnce(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	p, err := consumer.New(store, consumer.Config{Group: "settlement"})
	if err != nil {
		t.Fatalf("consumer.New: %v", err)
	}
	var applied atomic.Int64
	p.Register(events.TypePaymentCaptured, func(ctx context.Context, tx storage.Tx, m events.Message) error {
		applied.Add(1)
		return nil
	})

	m := message("msg-1")
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := p.Handle(ctx, m); err != nil {
				t.Errorf("Handle: %v", err)
			}
		}()
	}
	wg.Wait()
	if got := applied.Load(); got != 1 {
		t.Fatalf("handler ran %d times across 32 concurrent deliveries, want 1", got)
	}
}

// TestFailedHandlerIsRetriable proves the marker is not durable on its own:
// if the handler fails, the next delivery must be treated as the first.
func TestFailedHandlerIsRetriable(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	p, err := consumer.New(store, consumer.Config{Group: "settlement"})
	if err != nil {
		t.Fatalf("consumer.New: %v", err)
	}

	boom := errors.New("downstream down")
	var attempts atomic.Int64
	p.Register(events.TypePaymentCaptured, func(ctx context.Context, tx storage.Tx, m events.Message) error {
		if attempts.Add(1) <= 3 {
			return boom
		}
		return nil
	})

	m := message("msg-1")
	for i := 0; i < 3; i++ {
		if err := p.Handle(ctx, m); !errors.Is(err, boom) {
			t.Fatalf("delivery %d = %v, want the handler error", i, err)
		}
	}
	if err := p.Handle(ctx, m); err != nil {
		t.Fatalf("fourth delivery: %v", err)
	}
	if got := attempts.Load(); got != 4 {
		t.Errorf("handler ran %d times, want 4", got)
	}
	// Having succeeded, further deliveries are duplicates again.
	if err := p.Handle(ctx, m); err != nil {
		t.Fatalf("fifth delivery: %v", err)
	}
	if got := attempts.Load(); got != 4 {
		t.Errorf("handler ran %d times after success, want 4", got)
	}
}

// TestCrashBetweenDedupeAndEffectRollsBackBoth covers the window where the
// marker is written but the effect is not.
func TestCrashBetweenDedupeAndEffectRollsBackBoth(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	injector := faults.New(1)
	injector.Set(faults.ConsumerAfterDedupe, 1.0)

	p, err := consumer.New(store, consumer.Config{Group: "settlement", Faults: injector})
	if err != nil {
		t.Fatalf("consumer.New: %v", err)
	}
	var applied atomic.Int64
	p.Register(events.TypePaymentCaptured, func(ctx context.Context, tx storage.Tx, m events.Message) error {
		applied.Add(1)
		return nil
	})

	m := message("msg-1")
	if err := p.Handle(ctx, m); !errors.Is(err, faults.ErrInjected) {
		t.Fatalf("Handle = %v, want an injected failure", err)
	}
	if got := applied.Load(); got != 0 {
		t.Fatalf("handler ran %d times, want 0", got)
	}

	injector.Disable()
	if err := p.Handle(ctx, m); err != nil {
		t.Fatalf("redelivery: %v", err)
	}
	if got := applied.Load(); got != 1 {
		t.Errorf("handler ran %d times after recovery, want 1", got)
	}
}

// TestCrashBeforeAckDoesNotReapplyTheEffect covers the opposite window: the
// effect committed but the broker was never told.
func TestCrashBeforeAckDoesNotReapplyTheEffect(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	injector := faults.New(1)
	injector.Set(faults.ConsumerBeforeAck, 1.0)

	p, err := consumer.New(store, consumer.Config{Group: "settlement", Faults: injector})
	if err != nil {
		t.Fatalf("consumer.New: %v", err)
	}
	var applied atomic.Int64
	p.Register(events.TypePaymentCaptured, func(ctx context.Context, tx storage.Tx, m events.Message) error {
		applied.Add(1)
		return nil
	})

	m := message("msg-1")
	if err := p.Handle(ctx, m); !errors.Is(err, faults.ErrInjected) {
		t.Fatalf("Handle = %v, want an injected failure", err)
	}
	if got := applied.Load(); got != 1 {
		t.Fatalf("handler ran %d times, want 1: the effect committed", got)
	}

	injector.Disable()
	if err := p.Handle(ctx, m); err != nil {
		t.Fatalf("redelivery: %v", err)
	}
	if got := applied.Load(); got != 1 {
		t.Errorf("handler ran %d times after redelivery, want 1", got)
	}
}

func TestConsumerGroupsAreIndependent(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	m := message("msg-1")

	var counts [2]atomic.Int64
	for i, group := range []string{"settlement", "analytics"} {
		p, err := consumer.New(store, consumer.Config{Group: group})
		if err != nil {
			t.Fatalf("consumer.New(%q): %v", group, err)
		}
		i := i
		p.Register(events.TypePaymentCaptured, func(ctx context.Context, tx storage.Tx, m events.Message) error {
			counts[i].Add(1)
			return nil
		})
		for j := 0; j < 3; j++ {
			if err := p.Handle(ctx, m); err != nil {
				t.Fatalf("%s delivery %d: %v", group, j, err)
			}
		}
	}
	for i, want := range []int64{1, 1} {
		if got := counts[i].Load(); got != want {
			t.Errorf("group %d handled %d times, want %d", i, got, want)
		}
	}
}

func TestUnknownTypesAreIgnoredNotRetriedForever(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	reg := metrics.NewRegistry()
	p, err := consumer.New(store, consumer.Config{Group: "settlement", Metrics: reg})
	if err != nil {
		t.Fatalf("consumer.New: %v", err)
	}
	m := message("msg-1")
	m.Type = "payment.invented_next_year"
	if err := p.Handle(ctx, m); err != nil {
		t.Fatalf("Handle = %v, want nil for an unknown type", err)
	}
	if got := reg.CounterValue(consumer.MetricHandled, metrics.Labels{"type": m.Type, "outcome": "ignored"}); got != 1 {
		t.Errorf("ignored counter = %d, want 1", got)
	}
}

func TestMessageWithoutAnIDIsRejected(t *testing.T) {
	store := memory.New()
	p, err := consumer.New(store, consumer.Config{Group: "settlement"})
	if err != nil {
		t.Fatalf("consumer.New: %v", err)
	}
	p.Register(events.TypePaymentCaptured, func(context.Context, storage.Tx, events.Message) error { return nil })
	m := message("")
	if err := p.Handle(context.Background(), m); err == nil {
		t.Fatal("Handle accepted a message with no id to deduplicate on")
	}
}

func TestNewValidatesConfig(t *testing.T) {
	if _, err := consumer.New(nil, consumer.Config{Group: "g"}); err == nil {
		t.Error("consumer.New with no store returned no error")
	}
	if _, err := consumer.New(memory.New(), consumer.Config{}); err == nil {
		t.Error("consumer.New with no group returned no error")
	}
}
