package faults

import (
	"errors"
	"testing"
)

func TestNilInjectorIsInert(t *testing.T) {
	var i *Injector
	i.Set(BeforeCommit, 1.0) // must not panic
	if err := i.Check(BeforeCommit); err != nil {
		t.Fatalf("nil injector fired: %v", err)
	}
	if i.Total() != 0 || i.Fired(BeforeCommit) != 0 {
		t.Fatal("nil injector counted a fault")
	}
	i.Disable()
}

func TestCertaintyAndCounting(t *testing.T) {
	i := New(1)
	i.Set(BeforeCommit, 1.0)
	for n := 0; n < 10; n++ {
		if err := i.Check(BeforeCommit); !errors.Is(err, ErrInjected) {
			t.Fatalf("Check = %v, want ErrInjected", err)
		}
	}
	if got := i.Fired(BeforeCommit); got != 10 {
		t.Errorf("Fired = %d, want 10", got)
	}
	if err := i.Check(AfterCommit); err != nil {
		t.Errorf("unconfigured point fired: %v", err)
	}
	if got := i.Total(); got != 10 {
		t.Errorf("Total = %d, want 10", got)
	}
	i.Disable()
	if err := i.Check(BeforeCommit); err != nil {
		t.Errorf("fired after Disable: %v", err)
	}
}

func TestSeededRunsAreReproducible(t *testing.T) {
	run := func() []bool {
		i := New(42)
		i.Set(OutboxAfterPublish, 0.5)
		var out []bool
		for n := 0; n < 50; n++ {
			out = append(out, i.Check(OutboxAfterPublish) != nil)
		}
		return out
	}
	a, b := run(), run()
	for n := range a {
		if a[n] != b[n] {
			t.Fatalf("run diverged at %d", n)
		}
	}
	var fired int
	for _, f := range a {
		if f {
			fired++
		}
	}
	if fired == 0 || fired == len(a) {
		t.Fatalf("probability 0.5 fired %d/%d times", fired, len(a))
	}
}
