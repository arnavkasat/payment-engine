package money

import (
	"errors"
	"math"
	"testing"
)

func TestParseCurrency(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want Currency
		ok   bool
	}{
		{"usd", "USD", true},
		{"EUR", "EUR", true},
		{"US", "", false},
		{"USDD", "", false},
		{"US1", "", false},
		{"", "", false},
	} {
		got, err := ParseCurrency(tc.in)
		if tc.ok && err != nil {
			t.Errorf("ParseCurrency(%q) = error %v, want %q", tc.in, err, tc.want)
		}
		if !tc.ok && err == nil {
			t.Errorf("ParseCurrency(%q) = %q, want error", tc.in, got)
		}
		if got != tc.want {
			t.Errorf("ParseCurrency(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestAddOverflow(t *testing.T) {
	max := Minor(math.MaxInt64)
	min := Minor(math.MinInt64)
	if _, err := Add(max, 1); !errors.Is(err, ErrOverflow) {
		t.Errorf("Add(max,1) err = %v, want ErrOverflow", err)
	}
	if _, err := Add(min, -1); !errors.Is(err, ErrOverflow) {
		t.Errorf("Add(min,-1) err = %v, want ErrOverflow", err)
	}
	if got, err := Add(max, min); err != nil || got != -1 {
		t.Errorf("Add(max,min) = %v, %v, want -1, nil", got, err)
	}
	if got, err := Add(5, -7); err != nil || got != -2 {
		t.Errorf("Add(5,-7) = %v, %v, want -2, nil", got, err)
	}
}

func TestSumAllZeroSum(t *testing.T) {
	got, err := SumAll(-1025, 1000, 25)
	if err != nil || got != 0 {
		t.Fatalf("SumAll = %v, %v, want 0, nil", got, err)
	}
}

func TestNeg(t *testing.T) {
	if _, err := Neg(Minor(math.MinInt64)); !errors.Is(err, ErrOverflow) {
		t.Errorf("Neg(minInt64) err = %v, want ErrOverflow", err)
	}
	if got, _ := Neg(42); got != -42 {
		t.Errorf("Neg(42) = %v, want -42", got)
	}
}

func TestString(t *testing.T) {
	for _, tc := range []struct {
		in   Minor
		want string
	}{{0, "0.00"}, {5, "0.05"}, {1234, "12.34"}, {-1234, "-12.34"}} {
		if got := tc.in.String(); got != tc.want {
			t.Errorf("Minor(%d).String() = %q, want %q", tc.in, got, tc.want)
		}
	}
}
