package payments

import (
	"encoding/binary"
	"fmt"

	"github.com/google/uuid"
)

// Sharding exists because of a lock, not a disk.
//
// Every capture posts a leg against the clearing account and another against
// the fee account, and every settlement posts against clearing again. A single
// account of each kind means PostgreSQL takes a row lock on the same two rows
// for every write in the system, so the entire engine serialises on them no
// matter how many API replicas, partitions or consumers are running.
//
// Splitting each into N accounts spreads that lock across N rows. The shard is
// derived from the payment id rather than stored, so a settlement always finds
// the same clearing account its capture used without reading anything first,
// and the ledger invariants are unchanged: the shards are ordinary accounts and
// the books still sum to zero across them.

// shardIndex picks a shard for a payment. It reads the last four bytes of the
// id, which are random in a v4 UUID — the first bytes carry version and
// variant bits and would bias the distribution.
func shardIndex(id uuid.UUID, n int) int {
	if n <= 1 {
		return 0
	}
	return int(binary.BigEndian.Uint32(id[12:16]) % uint32(n))
}

// clearingFor returns the clearing account a payment's legs belong in.
func (s *Service) clearingFor(paymentID uuid.UUID) uuid.UUID {
	return s.cfg.Clearing[shardIndex(paymentID, len(s.cfg.Clearing))]
}

// feesFor returns the fee account a payment's fee belongs in, or uuid.Nil
// when the deployment charges no fees.
func (s *Service) feesFor(paymentID uuid.UUID) uuid.UUID {
	if len(s.cfg.Fees) == 0 {
		return uuid.Nil
	}
	return s.cfg.Fees[shardIndex(paymentID, len(s.cfg.Fees))]
}

// validateShards rejects a configuration that would post against nothing.
func validateShards(clearing []uuid.UUID) error {
	if len(clearing) == 0 {
		return fmt.Errorf("payments: at least one clearing account is required")
	}
	for i, id := range clearing {
		if id == uuid.Nil {
			return fmt.Errorf("payments: clearing account %d is the nil uuid", i)
		}
	}
	return nil
}
