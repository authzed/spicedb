package caveats

import (
	"fmt"
	"testing"

	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/testutil"
)

func TestShortcircuitedOr(t *testing.T) {
	tcs := []struct {
		first    *v1.CaveatExpression
		second   *v1.CaveatExpression
		expected *v1.CaveatExpression
	}{
		{
			nil,
			nil,
			nil,
		},
		{
			CaveatExprForTesting("first"),
			nil,
			nil,
		},
		{
			nil,
			CaveatExprForTesting("first"),
			nil,
		},
		{
			CaveatExprForTesting("first"),
			CaveatExprForTesting("second"),
			Or(CaveatExprForTesting("first"), CaveatExprForTesting("second")),
		},
	}

	for _, tc := range tcs {
		t.Run(fmt.Sprintf("%v-%v", tc.first, tc.second), func(t *testing.T) {
			testutil.RequireProtoEqual(t, tc.expected, ShortcircuitedOr(tc.first, tc.second), "mismatch")
		})
	}
}

func TestOr(t *testing.T) {
	tcs := []struct {
		first    *v1.CaveatExpression
		second   *v1.CaveatExpression
		expected *v1.CaveatExpression
	}{
		{
			nil,
			nil,
			nil,
		},
		{
			CaveatExprForTesting("first"),
			nil,
			CaveatExprForTesting("first"),
		},
		{
			nil,
			CaveatExprForTesting("first"),
			CaveatExprForTesting("first"),
		},
		{
			CaveatExprForTesting("first"),
			CaveatExprForTesting("first"),
			CaveatExprForTesting("first"),
		},
		{
			CaveatExprForTesting("first"),
			CaveatExprForTesting("second"),
			&v1.CaveatExpression{
				OperationOrCaveat: &v1.CaveatExpression_Operation{
					Operation: &v1.CaveatOperation{
						Op:       v1.CaveatOperation_OR,
						Children: []*v1.CaveatExpression{CaveatExprForTesting("first"), CaveatExprForTesting("second")},
					},
				},
			},
		},
	}

	for _, tc := range tcs {
		t.Run(fmt.Sprintf("%v-%v", tc.first, tc.second), func(t *testing.T) {
			testutil.RequireProtoEqual(t, tc.expected, Or(tc.first, tc.second), "mismatch")
		})
	}
}

func TestAnd(t *testing.T) {
	tcs := []struct {
		first    *v1.CaveatExpression
		second   *v1.CaveatExpression
		expected *v1.CaveatExpression
	}{
		{
			nil,
			nil,
			nil,
		},
		{
			CaveatExprForTesting("first"),
			nil,
			CaveatExprForTesting("first"),
		},
		{
			nil,
			CaveatExprForTesting("first"),
			CaveatExprForTesting("first"),
		},
		{
			CaveatExprForTesting("first"),
			CaveatExprForTesting("first"),
			CaveatExprForTesting("first"),
		},
		{
			CaveatExprForTesting("first"),
			CaveatExprForTesting("second"),
			&v1.CaveatExpression{
				OperationOrCaveat: &v1.CaveatExpression_Operation{
					Operation: &v1.CaveatOperation{
						Op:       v1.CaveatOperation_AND,
						Children: []*v1.CaveatExpression{CaveatExprForTesting("first"), CaveatExprForTesting("second")},
					},
				},
			},
		},
	}

	for _, tc := range tcs {
		t.Run(fmt.Sprintf("%v-%v", tc.first, tc.second), func(t *testing.T) {
			testutil.RequireProtoEqual(t, tc.expected, And(tc.first, tc.second), "mismatch")
		})
	}
}

func TestInvert(t *testing.T) {
	tcs := []struct {
		first    *v1.CaveatExpression
		expected *v1.CaveatExpression
	}{
		{
			nil,
			nil,
		},
		{
			CaveatExprForTesting("first"),
			&v1.CaveatExpression{
				OperationOrCaveat: &v1.CaveatExpression_Operation{
					Operation: &v1.CaveatOperation{
						Op:       v1.CaveatOperation_NOT,
						Children: []*v1.CaveatExpression{CaveatExprForTesting("first")},
					},
				},
			},
		},
	}

	for _, tc := range tcs {
		t.Run(fmt.Sprintf("%v", tc.first), func(t *testing.T) {
			testutil.RequireProtoEqual(t, tc.expected, Invert(tc.first), "mismatch")
		})
	}
}
