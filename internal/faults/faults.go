// Package faults provides deterministic fault injection at named points in
// the payment pipeline. Production binaries run with a nil *Injector, whose
// methods are no-ops, so the call sites cost one nil check and nothing else.
package faults

import (
	"errors"
	"fmt"
	"math/rand"
	"sync"
)

// Point names a place in the pipeline where a process may be killed or an
// error returned. The interesting points are the ones that straddle a
// durability boundary: if the system is correct, a crash at any of them
// leaves the ledger balanced.
type Point string

const (
	// BeforeCommit aborts the capture transaction just before COMMIT. The
	// payment, its postings and its outbox row must all vanish together.
	BeforeCommit Point = "capture.before_commit"
	// AfterCommit fails after COMMIT but before the client is answered, so
	// the client retries a payment that already exists. Idempotency keys
	// must collapse the retry onto the committed payment.
	AfterCommit Point = "capture.after_commit"
	// OutboxAfterPublish fails after the broker accepted the message but
	// before the outbox row is marked published, forcing a redelivery.
	OutboxAfterPublish Point = "outbox.after_publish"
	// OutboxBeforePublish fails after the row is leased but before publish,
	// so the lease must expire and another worker must pick it up.
	OutboxBeforePublish Point = "outbox.before_publish"
	// ConsumerAfterDedupe fails after the dedupe row is written but before
	// the settlement postings commit. Both are in one transaction, so the
	// rollback must take the dedupe row with it.
	ConsumerAfterDedupe Point = "consumer.after_dedupe"
	// ConsumerBeforeAck fails after the settlement committed but before the
	// broker offset is acked, redelivering an already-applied message.
	ConsumerBeforeAck Point = "consumer.before_ack"
)

// AllPoints lists every injection point, for harnesses that sweep them.
var AllPoints = []Point{
	BeforeCommit,
	AfterCommit,
	OutboxBeforePublish,
	OutboxAfterPublish,
	ConsumerAfterDedupe,
	ConsumerBeforeAck,
}

// ErrInjected is returned by Check when a fault fires. Callers distinguish it
// from real failures only for accounting; the recovery path must be identical.
var ErrInjected = errors.New("faults: injected failure")

// Injector fires faults at configured points with configured probabilities.
// The zero value and the nil pointer are both usable and inert.
type Injector struct {
	mu     sync.Mutex
	rnd    *rand.Rand
	probs  map[Point]float64
	counts map[Point]int
}

// New returns an Injector seeded for reproducible runs.
func New(seed int64) *Injector {
	return &Injector{
		rnd:    rand.New(rand.NewSource(seed)),
		probs:  make(map[Point]float64),
		counts: make(map[Point]int),
	}
}

// Set configures the probability in [0,1] that Check fires at p.
func (i *Injector) Set(p Point, probability float64) {
	if i == nil {
		return
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.probs == nil {
		i.probs = make(map[Point]float64)
	}
	i.probs[p] = probability
}

// Check reports an injected failure at p according to its probability. A nil
// Injector never fires, which is how production runs.
func (i *Injector) Check(p Point) error {
	if i == nil {
		return nil
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	prob := i.probs[p]
	if prob <= 0 || i.rnd.Float64() >= prob {
		return nil
	}
	if i.counts == nil {
		i.counts = make(map[Point]int)
	}
	i.counts[p]++
	return fmt.Errorf("%w at %s", ErrInjected, p)
}

// Fired returns how many times a fault fired at p.
func (i *Injector) Fired(p Point) int {
	if i == nil {
		return 0
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.counts[p]
}

// Total returns how many faults fired across every point.
func (i *Injector) Total() int {
	if i == nil {
		return 0
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	var n int
	for _, c := range i.counts {
		n += c
	}
	return n
}

// Disable clears every configured probability, letting a harness run a clean
// recovery phase without rebuilding the pipeline.
func (i *Injector) Disable() {
	if i == nil {
		return
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	i.probs = make(map[Point]float64)
}
