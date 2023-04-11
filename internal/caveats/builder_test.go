package caveats

import (
	"fmt"
	"testing"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/testutil"
)

func TestShortcircuitedOr(t *testing.T) {
	tcs := []struct {
		first    *core.CaveatExpression
		second   *core.CaveatExpression
		expected *core.CaveatExpression
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
		tc := tc
		t.Run(fmt.Sprintf("%v-%v", tc.first, tc.second), func(t *testing.T) {
			testutil.RequireProtoEqual(t, tc.expected, ShortcircuitedOr(tc.first, tc.second), "mismatch")
		})
	}
}

func TestOr(t *testing.T) {
	tcs := []struct {
		first    *core.CaveatExpression
		second   *core.CaveatExpression
		expected *core.CaveatExpression
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
			&core.CaveatExpression{
				OperationOrCaveat: &core.CaveatExpression_Operation{
					Operation: &core.CaveatOperation{
						Op:       core.CaveatOperation_OR,
						Children: []*core.CaveatExpression{CaveatExprForTesting("first"), CaveatExprForTesting("second")},
					},
				},
			},
		},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(fmt.Sprintf("%v-%v", tc.first, tc.second), func(t *testing.T) {
			testutil.RequireProtoEqual(t, tc.expected, Or(tc.first, tc.second), "mismatch")
		})
	}
}

func TestAnd(t *testing.T) {
	tcs := []struct {
		first    *core.CaveatExpression
		second   *core.CaveatExpression
		expected *core.CaveatExpression
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
			&core.CaveatExpression{
				OperationOrCaveat: &core.CaveatExpression_Operation{
					Operation: &core.CaveatOperation{
						Op:       core.CaveatOperation_AND,
						Children: []*core.CaveatExpression{CaveatExprForTesting("first"), CaveatExprForTesting("second")},
					},
				},
			},
		},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(fmt.Sprintf("%v-%v", tc.first, tc.second), func(t *testing.T) {
			testutil.RequireProtoEqual(t, tc.expected, And(tc.first, tc.second), "mismatch")
		})
	}
}

func TestInvert(t *testing.T) {
	tcs := []struct {
		first    *core.CaveatExpression
		expected *core.CaveatExpression
	}{
		{
			nil,
			nil,
		},
		{
			CaveatExprForTesting("first"),
			&core.CaveatExpression{
				OperationOrCaveat: &core.CaveatExpression_Operation{
					Operation: &core.CaveatOperation{
						Op:       core.CaveatOperation_NOT,
						Children: []*core.CaveatExpression{CaveatExprForTesting("first")},
					},
				},
			},
		},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(fmt.Sprintf("%v", tc.first), func(t *testing.T) {
			testutil.RequireProtoEqual(t, tc.expected, Invert(tc.first), "mismatch")
		})
	}
}
