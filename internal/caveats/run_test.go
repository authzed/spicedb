package caveats_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/caveats"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/testfixtures"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

func TestRunCaveatExpressions(t *testing.T) {
	tcs := []struct {
		name          string
		expression    *v1.CaveatExpression
		context       map[string]any
		expectedValue bool
	}{
		{
			"basic",
			caveatexpr("firstCaveat"),
			map[string]any{
				"first": "42",
			},
			true,
		},
		{
			"another basic",
			caveatexpr("firstCaveat"),
			map[string]any{
				"first": "12",
			},
			false,
		},
		{
			"short circuited or",
			caveatOr(
				caveatexpr("firstCaveat"),
				caveatexpr("secondCaveat"),
			),
			map[string]any{
				"first": "42",
			},
			true,
		},
		{
			"non-short circuited or",
			caveatOr(
				caveatexpr("firstCaveat"),
				caveatexpr("secondCaveat"),
			),
			map[string]any{
				"first":  "12",
				"second": "hello",
			},
			true,
		},
		{
			"false or",
			caveatOr(
				caveatexpr("firstCaveat"),
				caveatexpr("secondCaveat"),
			),
			map[string]any{
				"first":  "12",
				"second": "hi",
			},
			false,
		},
		{
			"short circuited and",
			caveatAnd(
				caveatexpr("firstCaveat"),
				caveatexpr("secondCaveat"),
			),
			map[string]any{
				"first": "12",
			},
			false,
		},
		{
			"non-short circuited and",
			caveatAnd(
				caveatexpr("firstCaveat"),
				caveatexpr("secondCaveat"),
			),
			map[string]any{
				"first":  "12",
				"second": "hello",
			},
			false,
		},
		{
			"false or",
			caveatAnd(
				caveatexpr("firstCaveat"),
				caveatexpr("secondCaveat"),
			),
			map[string]any{
				"first":  "42",
				"second": "hi",
			},
			false,
		},
		{
			"inversion",
			caveatInvert(
				caveatexpr("firstCaveat"),
			),
			map[string]any{
				"first": "12",
			},
			true,
		},
		{
			"nested",
			caveatAnd(
				caveatOr(
					caveatexpr("firstCaveat"),
					caveatexpr("secondCaveat"),
				),
				caveatInvert(
					caveatexpr("thirdCaveat"),
				),
			),
			map[string]any{
				"first":  "12",
				"second": "hi",
				"third":  false,
			},
			false,
		},
		{
			"nested true",
			caveatAnd(
				caveatOr(
					caveatexpr("firstCaveat"),
					caveatexpr("secondCaveat"),
				),
				caveatInvert(
					caveatexpr("thirdCaveat"),
				),
			),
			map[string]any{
				"first":  "42",
				"second": "hi",
				"third":  false,
			},
			true,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			req := require.New(t)

			rawDS, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
			req.NoError(err)

			ds, _ := testfixtures.DatastoreFromSchemaAndTestRelationships(rawDS, `
				caveat firstCaveat(first int) {
					first == 42
				}

				caveat secondCaveat(second string) {
					second == 'hello'
				}

				caveat thirdCaveat(third bool) {
					third
				}
				`, nil, req)
			headRevision, err := ds.HeadRevision(context.Background())
			req.NoError(err)

			reader := ds.SnapshotReader(headRevision)

			for _, debugOption := range []caveats.RunCaveatExpressionDebugOption{
				caveats.RunCaveatExpressionNoDebugging,
				caveats.RunCaveatExpressionWithDebugInformation,
			} {
				t.Run(fmt.Sprintf("%v", debugOption), func(t *testing.T) {
					req := require.New(t)

					result, err := caveats.RunCaveatExpression(context.Background(), tc.expression, tc.context, reader, debugOption)
					req.NoError(err)
					req.Equal(tc.expectedValue, result.Value())
				})
			}
		})
	}
}

// TODO(jschorr): Move these into helper methods to be shared by all tests
func caveat(name string) *core.ContextualizedCaveat {
	return &core.ContextualizedCaveat{
		CaveatName: name,
	}
}

func caveatexpr(name string) *v1.CaveatExpression {
	return &v1.CaveatExpression{
		OperationOrCaveat: &v1.CaveatExpression_Caveat{
			Caveat: caveat(name),
		},
	}
}

func caveatOr(first *v1.CaveatExpression, second *v1.CaveatExpression) *v1.CaveatExpression {
	return &v1.CaveatExpression{
		OperationOrCaveat: &v1.CaveatExpression_Operation{
			Operation: &v1.CaveatOperation{
				Op:       v1.CaveatOperation_OR,
				Children: []*v1.CaveatExpression{first, second},
			},
		},
	}
}

func caveatAnd(first *v1.CaveatExpression, second *v1.CaveatExpression) *v1.CaveatExpression {
	return &v1.CaveatExpression{
		OperationOrCaveat: &v1.CaveatExpression_Operation{
			Operation: &v1.CaveatOperation{
				Op:       v1.CaveatOperation_AND,
				Children: []*v1.CaveatExpression{first, second},
			},
		},
	}
}

func caveatInvert(ce *v1.CaveatExpression) *v1.CaveatExpression {
	return &v1.CaveatExpression{
		OperationOrCaveat: &v1.CaveatExpression_Operation{
			Operation: &v1.CaveatOperation{
				Op:       v1.CaveatOperation_NOT,
				Children: []*v1.CaveatExpression{ce},
			},
		},
	}
}
