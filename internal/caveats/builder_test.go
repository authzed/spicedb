package caveats

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

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

func TestCaveatAsExpr(t *testing.T) {
	tcs := []struct {
		name     string
		caveat   *core.ContextualizedCaveat
		expected *core.CaveatExpression
	}{
		{
			"nil caveat",
			nil,
			nil,
		},
		{
			"basic caveat",
			&core.ContextualizedCaveat{
				CaveatName: "test",
			},
			&core.CaveatExpression{
				OperationOrCaveat: &core.CaveatExpression_Caveat{
					Caveat: &core.ContextualizedCaveat{
						CaveatName: "test",
					},
				},
			},
		},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			testutil.RequireProtoEqual(t, tc.expected, CaveatAsExpr(tc.caveat), "mismatch")
		})
	}
}

func TestCaveatForTesting(t *testing.T) {
	result := CaveatForTesting("test_caveat")
	expected := &core.ContextualizedCaveat{
		CaveatName: "test_caveat",
	}
	testutil.RequireProtoEqual(t, expected, result, "mismatch")
}

func TestMustCaveatExprForTestingWithContext(t *testing.T) {
	context := map[string]any{
		"key1": "value1",
		"key2": 42,
	}

	result := MustCaveatExprForTestingWithContext("test_caveat", context)
	require.NotNil(t, result)
	require.NotNil(t, result.GetCaveat())
	require.Equal(t, "test_caveat", result.GetCaveat().CaveatName)
	require.NotNil(t, result.GetCaveat().Context)

	contextMap := result.GetCaveat().Context.AsMap()
	require.Equal(t, "value1", contextMap["key1"])
	require.Equal(t, float64(42), contextMap["key2"]) //nolint:testifyrequire these are known/static values
}

func TestMustCaveatExprForTestingWithContextPanic(t *testing.T) {
	// Test that invalid context causes panic
	require.Panics(t, func() {
		MustCaveatExprForTestingWithContext("test", map[string]any{
			"invalid": func() {}, // Functions can't be converted to structpb
		})
	})
}

func TestSubtract(t *testing.T) {
	tcs := []struct {
		name       string
		caveat     *core.CaveatExpression
		subtracted *core.CaveatExpression
		expected   *core.CaveatExpression
	}{
		{
			"both nil",
			nil,
			nil,
			Invert(nil),
		},
		{
			"caveat nil, subtracted not nil",
			nil,
			CaveatExprForTesting("subtract"),
			Invert(CaveatExprForTesting("subtract")),
		},
		{
			"caveat not nil, subtracted nil",
			CaveatExprForTesting("base"),
			nil,
			CaveatExprForTesting("base"),
		},
		{
			"both not nil",
			CaveatExprForTesting("base"),
			CaveatExprForTesting("subtract"),
			&core.CaveatExpression{
				OperationOrCaveat: &core.CaveatExpression_Operation{
					Operation: &core.CaveatOperation{
						Op: core.CaveatOperation_AND,
						Children: []*core.CaveatExpression{
							CaveatExprForTesting("base"),
							Invert(CaveatExprForTesting("subtract")),
						},
					},
				},
			},
		},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			result := Subtract(tc.caveat, tc.subtracted)
			testutil.RequireProtoEqual(t, tc.expected, result, "mismatch")
		})
	}
}
