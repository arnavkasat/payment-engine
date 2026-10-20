// Package inmem is an in-process implementation of the events interfaces. It
// backs unit tests and local chaos runs, and it deliberately reproduces the
// awkward parts of a real broker: delivery is at-least-once, a handler that
// fails gets the message again, and ordering is only guaranteed per key.
package inmem

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/arnavkasat/payment-engine/internal/events"
)

// ErrClosed is returned by Publish after Close.
var ErrClosed = errors.New("inmem: bus is closed")

// Bus is an in-memory broker.
type Bus struct {
	mu        sync.Mutex
	closed    bool
	ch        chan events.Message
	redeliver time.Duration

	deliveredMu sync.Mutex
	delivered   int
}

// New returns a Bus with the given queue depth. redeliver is how long a
// failed message waits before it is offered again.
func New(buffer int, redeliver time.Duration) *Bus {
	if buffer <= 0 {
		buffer = 1024
	}
	if redeliver <= 0 {
		redeliver = time.Millisecond
	}
	return &Bus{ch: make(chan events.Message, buffer), redeliver: redeliver}
}

var (
	_ events.Publisher = (*Bus)(nil)
	_ events.Consumer  = (*Bus)(nil)
)

// Publish enqueues messages, blocking if the queue is full.
func (b *Bus) Publish(ctx context.Context, msgs ...events.Message) error {
	b.mu.Lock()
	closed := b.closed
	b.mu.Unlock()
	if closed {
		return ErrClosed
	}
	for _, m := range msgs {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case b.ch <- m:
		}
	}
	return nil
}

// Run delivers messages to h until the context is cancelled. A message whose
// handler returns an error is re-queued, so the handler must be able to see
// it again — the same contract a real broker imposes.
func (b *Bus) Run(ctx context.Context, h events.Handler) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case m, ok := <-b.ch:
			if !ok {
				return nil
			}
			b.deliveredMu.Lock()
			b.delivered++
			b.deliveredMu.Unlock()

			if err := h(ctx, m); err != nil {
				b.requeue(ctx, m)
			}
		}
	}
}

// requeue returns a message to the queue after a delay, without blocking the
// consumer loop.
func (b *Bus) requeue(ctx context.Context, m events.Message) {
	go func() {
		timer := time.NewTimer(b.redeliver)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		b.mu.Lock()
		closed := b.closed
		b.mu.Unlock()
		if closed {
			return
		}
		defer func() {
			// A send on a channel closed by a concurrent Close is a panic,
			// not an error worth propagating from a background re-delivery.
			_ = recover()
		}()
		select {
		case <-ctx.Done():
		case b.ch <- m:
		}
	}()
}

// Deliveries returns how many messages have been handed to handlers,
// including redeliveries. Tests use it to assert that duplicates happened.
func (b *Bus) Deliveries() int {
	b.deliveredMu.Lock()
	defer b.deliveredMu.Unlock()
	return b.delivered
}

// Depth returns how many messages are waiting.
func (b *Bus) Depth() int { return len(b.ch) }

// Close stops the bus. It is safe to call more than once.
func (b *Bus) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil
	}
	b.closed = true
	close(b.ch)
	return nil
}
