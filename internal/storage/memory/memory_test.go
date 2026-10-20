package memory_test

import (
	"testing"

	"github.com/google/uuid"

	"github.com/arnavkasat/payment-engine/internal/money"
	"github.com/arnavkasat/payment-engine/internal/storage/memory"
	"github.com/arnavkasat/payment-engine/internal/storage/storagetest"
)

func TestConformance(t *testing.T) {
	storagetest.Run(t, func(t *testing.T) storagetest.Harness {
		s := memory.New()
		t.Cleanup(func() { _ = s.Close() })
		return storagetest.Harness{
			Store: s,
			Corrupt: func(t *testing.T, id uuid.UUID, delta money.Minor) {
				s.Corrupt(id, delta)
			},
		}
	})
}
