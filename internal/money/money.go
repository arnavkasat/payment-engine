// Package money models monetary amounts as signed integers in a currency's
// minor units (cents, pence, paise). Floating point is never used: every
// amount in the ledger is exact, and arithmetic is checked for overflow.
package money

import (
	"errors"
	"fmt"
	"strings"
)

// Minor is a signed amount in a currency's minor unit. The sign carries
// meaning in the ledger: a posting's Minor is the delta applied to an
// account's balance, and the deltas of a journal entry must sum to zero.
type Minor int64

// Currency is an ISO-4217 alphabetic code.
type Currency string

var (
	ErrOverflow        = errors.New("money: amount overflow")
	ErrInvalidCurrency = errors.New("money: invalid currency code")
)

// ParseCurrency validates and normalises an ISO-4217 alphabetic code.
func ParseCurrency(s string) (Currency, error) {
	if len(s) != 3 {
		return "", fmt.Errorf("%w: %q", ErrInvalidCurrency, s)
	}
	up := strings.ToUpper(s)
	for i := 0; i < len(up); i++ {
		if up[i] < 'A' || up[i] > 'Z' {
			return "", fmt.Errorf("%w: %q", ErrInvalidCurrency, s)
		}
	}
	return Currency(up), nil
}

// Add returns a+b, or ErrOverflow if the result does not fit in an int64.
func Add(a, b Minor) (Minor, error) {
	sum := a + b
	// Overflow happened iff the operands share a sign that the result does not.
	if (a > 0 && b > 0 && sum < 0) || (a < 0 && b < 0 && sum >= 0) {
		return 0, ErrOverflow
	}
	return sum, nil
}

// SumAll adds every amount, reporting overflow rather than wrapping.
func SumAll(amounts ...Minor) (Minor, error) {
	var total Minor
	for _, a := range amounts {
		var err error
		if total, err = Add(total, a); err != nil {
			return 0, err
		}
	}
	return total, nil
}

// Neg returns -a, reporting overflow for the single un-negatable int64.
func Neg(a Minor) (Minor, error) {
	if a == Minor(-1<<63) {
		return 0, ErrOverflow
	}
	return -a, nil
}

// String renders the amount in major units with two decimal places. It is a
// debugging aid only: exponents other than 2 are not modelled.
func (m Minor) String() string {
	neg := m < 0
	v := int64(m)
	if neg {
		v = -v
	}
	s := fmt.Sprintf("%d.%02d", v/100, v%100)
	if neg {
		return "-" + s
	}
	return s
}
