package caveats

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/dsfortesting"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/testfixtures"
	pkgcaveats "github.com/authzed/spicedb/pkg/caveats"
	"github.com/authzed/spicedb/pkg/caveats/types"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/genutil/mapz"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

var (
	caveatexpr   = CaveatExprForTesting
	caveatAnd    = And
	caveatOr     = Or
	caveatInvert = Invert
)

func TestRunCaveatExpressions(t *testing.T) {
	tcs := []struct {
		name                   string
		expression             *core.CaveatExpression
		context                map[string]any
		expectedValue          bool
		expectedMissingContext []string
		expectedExprString     string
	}{
		{
			"basic",
			caveatexpr("firstCaveat"),
			map[string]any{
				"first": "42",
			},
			true,
			nil,
			"first == 42",
		},
		{
			"another basic",
			caveatexpr("firstCaveat"),
			map[string]any{
				"first": "12",
			},
			false,
			nil,
			"first == 42",
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
			nil,
			`(first == 42) || (second == "hello")`,
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
			nil,
			`(first == 42) || (second == "hello")`,
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
			nil,
			`(first == 42) || (second == "hello")`,
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
			nil,
			`(first == 42) && (second == "hello")`,
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
			nil,
			`(first == 42) && (second == "hello")`,
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
			nil,
			`(first == 42) && (second == "hello")`,
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
			nil,
			`!(first == 42)`,
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
			nil,
			`((first == 42) || (second == "hello")) && (!(third))`,
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
			nil,
			`((first == 42) || (second == "hello")) && (!(third))`,
		},
		{
			"missing context on left side of and branch, right branch is true",
			caveatAnd(
				caveatexpr("firstCaveat"),
				caveatexpr("secondCaveat"),
			),
			map[string]any{
				"second": "hello",
			},
			false,
			[]string{"first"},
			`(first == 42) && (second == "hello")`,
		},
		{
			"missing context on right side of and branch, left branch is true",
			caveatAnd(
				caveatexpr("firstCaveat"),
				caveatexpr("secondCaveat"),
			),
			map[string]any{
				"first": "42",
			},
			false,
			[]string{"second"},
			`(first == 42) && (second == "hello")`,
		},
		{
			"missing context on left side of or branch, right branch is true",
			caveatOr(
				caveatexpr("firstCaveat"),
				caveatexpr("secondCaveat"),
			),
			map[string]any{
				"second": "hello",
			},
			true,
			nil,
			`(first == 42) || (second == "hello")`,
		},
		{
			"missing context on right side of or branch, left branch is true",
			caveatOr(
				caveatexpr("firstCaveat"),
				caveatexpr("secondCaveat"),
			),
			map[string]any{
				"first": "42",
			},
			true,
			nil,
			`(first == 42) || (second == "hello")`,
		},
		{
			"missing context on left side of and branch, right branch is false",
			caveatAnd(
				caveatexpr("firstCaveat"),
				caveatexpr("secondCaveat"),
			),
			map[string]any{
				"second": "hi",
			},
			false,
			nil,
			`(first == 42) && (second == "hello")`,
		},
		{
			"missing context on right side of and branch, left branch is false",
			caveatAnd(
				caveatexpr("firstCaveat"),
				caveatexpr("secondCaveat"),
			),
			map[string]any{
				"first": "41",
			},
			false,
			nil,
			`(first == 42) && (second == "hello")`,
		},
		{
			"missing context on left side of or branch, right branch is false",
			caveatOr(
				caveatexpr("firstCaveat"),
				caveatexpr("secondCaveat"),
			),
			map[string]any{
				"second": "hi",
			},
			false,
			[]string{"first"},
			`(first == 42) || (second == "hello")`,
		},
		{
			"missing context on right side of or branch, left branch is false",
			caveatOr(
				caveatexpr("firstCaveat"),
				caveatexpr("secondCaveat"),
			),
			map[string]any{
				"first": "41",
			},
			false,
			[]string{"second"},
			`(first == 42) || (second == "hello")`,
		},
		{
			"missing context on both sides of and branch",
			caveatAnd(
				caveatexpr("firstCaveat"),
				caveatexpr("secondCaveat"),
			),
			map[string]any{},
			false,
			[]string{"first", "second"},
			`(first == 42) && (second == "hello")`,
		},
		{
			"missing context on both sides of or branch",
			caveatOr(
				caveatexpr("firstCaveat"),
				caveatexpr("secondCaveat"),
			),
			map[string]any{},
			false,
			[]string{"first", "second"},
			`(first == 42) || (second == "hello")`,
		},
		{
			"missing context under invert",
			caveatInvert(
				caveatexpr("firstCaveat"),
			),
			map[string]any{},
			false,
			[]string{"first"},
			`!(first == 42)`,
		},
		{
			"missing context with invert under and with true right branch",
			caveatAnd(
				caveatInvert(
					caveatexpr("firstCaveat"),
				),
				caveatexpr("secondCaveat"),
			),
			map[string]any{
				"second": "hello",
			},
			false,
			[]string{"first"},
			`(!(first == 42)) && (second == "hello")`,
		},
		{
			"missing context with invert under and with true left branch",
			caveatAnd(
				caveatexpr("secondCaveat"),
				caveatInvert(
					caveatexpr("firstCaveat"),
				),
			),
			map[string]any{
				"second": "hello",
			},
			false,
			[]string{"first"},
			`(second == "hello") && (!(first == 42))`,
		},
		{
			"missing context with invert under and with false right branch",
			caveatAnd(
				caveatInvert(
					caveatexpr("firstCaveat"),
				),
				caveatexpr("secondCaveat"),
			),
			map[string]any{
				"second": "hi",
			},
			false,
			nil,
			`(!(first == 42)) && (second == "hello")`,
		},
		{
			"missing context with invert under and with false left branch",
			caveatAnd(
				caveatexpr("secondCaveat"),
				caveatInvert(
					caveatexpr("firstCaveat"),
				),
			),
			map[string]any{
				"second": "hi",
			},
			false,
			nil,
			`(second == "hello") && (!(first == 42))`,
		},
		{
			"missing context with invert under or with true right branch",
			caveatOr(
				caveatInvert(
					caveatexpr("firstCaveat"),
				),
				caveatexpr("secondCaveat"),
			),
			map[string]any{
				"second": "hello",
			},
			true,
			nil,
			`(!(first == 42)) || (second == "hello")`,
		},
		{
			"missing context with invert under or with true left branch",
			caveatOr(
				caveatexpr("secondCaveat"),
				caveatInvert(
					caveatexpr("firstCaveat"),
				),
			),
			map[string]any{
				"second": "hello",
			},
			true,
			nil,
			`(second == "hello") || (!(first == 42))`,
		},
		{
			"missing context with invert under or with false right branch",
			caveatOr(
				caveatInvert(
					caveatexpr("firstCaveat"),
				),
				caveatexpr("secondCaveat"),
			),
			map[string]any{
				"second": "hi",
			},
			false,
			[]string{"first"},
			`(!(first == 42)) || (second == "hello")`,
		},
		{
			"missing context with invert under or with false left branch",
			caveatOr(
				caveatexpr("secondCaveat"),
				caveatInvert(
					caveatexpr("firstCaveat"),
				),
			),
			map[string]any{
				"second": "hi",
			},
			false,
			[]string{"first"},
			`(second == "hello") || (!(first == 42))`,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			req := require.New(t)

			rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, 0, memdb.DisableGC)
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
			headRevision, err := ds.HeadRevision(t.Context())
			req.NoError(err)

			dl := datalayer.NewDataLayer(ds)
			sr, err := dl.SnapshotReader(headRevision).ReadSchema()
			req.NoError(err)

			for _, debugOption := range []RunCaveatExpressionDebugOption{
				RunCaveatExpressionNoDebugging,
				RunCaveatExpressionWithDebugInformation,
			} {
				t.Run(fmt.Sprintf("%v", debugOption), func(t *testing.T) {
					req := require.New(t)

					result, err := RunSingleCaveatExpression(t.Context(), types.Default.TypeSet, tc.expression, tc.context, sr, debugOption)
					req.NoError(err)
					req.Equal(tc.expectedValue, result.Value())

					if len(tc.expectedMissingContext) > 0 {
						require.False(t, result.Value())
						require.True(t, result.IsPartial())

						missingFields, err := result.MissingVarNames()
						require.NoError(t, err)
						require.ElementsMatch(t, tc.expectedMissingContext, missingFields)
					} else {
						require.False(t, result.IsPartial())
					}

					if debugOption == RunCaveatExpressionWithDebugInformation {
						exprString, _, err := BuildDebugInformation(result)
						require.NoError(t, err)
						require.NotEmpty(t, exprString)
						require.Equal(t, tc.expectedExprString, exprString)
					}
				})
			}
		})
	}
}

func TestRunCaveatWithMissingMap(t *testing.T) {
	req := require.New(t)

	rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, 0, memdb.DisableGC)
	req.NoError(err)

	ds, _ := testfixtures.DatastoreFromSchemaAndTestRelationships(rawDS, `
				caveat some_caveat(themap map<any>) {
					themap.first == 42
				}
				`, nil, req)

	headRevision, err := ds.HeadRevision(t.Context())
	req.NoError(err)

	dl := datalayer.NewDataLayer(ds)
	sr, err := dl.SnapshotReader(headRevision).ReadSchema()
	req.NoError(err)

	result, err := RunSingleCaveatExpression(
		t.Context(),
		types.Default.TypeSet,
		caveatexpr("some_caveat"),
		map[string]any{},
		sr,
		RunCaveatExpressionNoDebugging,
	)
	req.NoError(err)
	req.True(result.IsPartial())
	req.False(result.Value())
}

func TestRunCaveatWithEmptyMap(t *testing.T) {
	req := require.New(t)

	rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, 0, memdb.DisableGC)
	req.NoError(err)

	ds, _ := testfixtures.DatastoreFromSchemaAndTestRelationships(rawDS, `
				caveat some_caveat(themap map<any>) {
					themap.first == 42
				}
				`, nil, req)

	headRevision, err := ds.HeadRevision(t.Context())
	req.NoError(err)

	dl := datalayer.NewDataLayer(ds)
	sr, err := dl.SnapshotReader(headRevision).ReadSchema()
	req.NoError(err)

	_, err = RunSingleCaveatExpression(
		t.Context(),
		types.Default.TypeSet,
		caveatexpr("some_caveat"),
		map[string]any{
			"themap": map[string]any{},
		},
		sr,
		RunCaveatExpressionNoDebugging,
	)
	req.Error(err)
	var evalErr EvaluationError
	req.ErrorAs(err, &evalErr)
}

func TestRunCaveatMultipleTimes(t *testing.T) {
	req := require.New(t)

	rawDS, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
	req.NoError(err)

	ds, _ := testfixtures.DatastoreFromSchemaAndTestRelationships(rawDS, `
				caveat some_caveat(themap map<any>) {
					themap.first == 42
				}

				caveat another_caveat(somecondition int) {
					somecondition == 42
				}
				`, nil, req)

	headRevision, err := ds.HeadRevision(t.Context())
	req.NoError(err)

	dl := datalayer.NewDataLayer(ds)
	sr, err := dl.SnapshotReader(headRevision).ReadSchema()
	req.NoError(err)

	runner := NewCaveatRunner(types.Default.TypeSet)

	// Run the first caveat.
	result, err := runner.RunCaveatExpression(t.Context(), caveatexpr("some_caveat"), map[string]any{
		"themap": map[string]any{
			"first": 42,
		},
	}, sr, RunCaveatExpressionNoDebugging)
	req.NoError(err)
	req.True(result.Value())

	// Run the first caveat again, giving a reader that will error if it is used, ensuring
	// the cache is used.
	result, err = runner.RunCaveatExpression(t.Context(), caveatexpr("some_caveat"), map[string]any{
		"themap": map[string]any{
			"first": 41,
		},
	}, noCaveatsReader{}, RunCaveatExpressionNoDebugging)
	req.NoError(err)
	req.False(result.Value())

	// Run the second caveat.
	result, err = runner.RunCaveatExpression(t.Context(), caveatexpr("another_caveat"), map[string]any{
		"somecondition": int64(42),
	}, sr, RunCaveatExpressionNoDebugging)
	req.NoError(err)
	req.True(result.Value())
}

type noCaveatsReader struct {
	datastore.Reader
}

func (f noCaveatsReader) LegacyReadCaveatByName(ctx context.Context, name string) (caveat *core.CaveatDefinition, lastWritten datastore.Revision, err error) {
	return nil, nil, errors.New("should not be called")
}

func (f noCaveatsReader) LegacyLookupCaveatsWithNames(ctx context.Context, names []string) ([]datastore.RevisionedCaveat, error) {
	return nil, errors.New("should not be called")
}

func (f noCaveatsReader) LegacyListAllCaveats(ctx context.Context) ([]datastore.RevisionedCaveat, error) {
	return nil, errors.New("should not be called")
}

func (f noCaveatsReader) LookupCaveatDefinitionsByNames(ctx context.Context, names []string) (map[string]datastore.CaveatDefinition, error) {
	return nil, errors.New("should not be called")
}

func TestRunCaveatWithMissingDefinition(t *testing.T) {
	req := require.New(t)

	rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, 0, memdb.DisableGC)
	req.NoError(err)

	ds, _ := testfixtures.DatastoreFromSchemaAndTestRelationships(rawDS, `
		caveat existing_caveat(param int) {
			param == 42
		}
		`, nil, req)

	headRevision, err := ds.HeadRevision(t.Context())
	req.NoError(err)

	dl := datalayer.NewDataLayer(ds)
	sr, err := dl.SnapshotReader(headRevision).ReadSchema()
	req.NoError(err)

	// Try to run a caveat that doesn't exist
	_, err = RunSingleCaveatExpression(
		t.Context(),
		types.Default.TypeSet,
		caveatexpr("nonexistent_caveat"),
		map[string]any{},
		sr,
		RunCaveatExpressionNoDebugging,
	)
	req.Error(err)
	req.Contains(err.Error(), "caveat with name `nonexistent_caveat` not found")
}

func TestCaveatRunnerPopulateCaveatDefinitionsForExpr(t *testing.T) {
	req := require.New(t)

	rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, 0, memdb.DisableGC)
	req.NoError(err)

	ds, _ := testfixtures.DatastoreFromSchemaAndTestRelationships(rawDS, `
		caveat first_caveat(firstparam int) {
			firstparam == 42
		}
		caveat second_caveat(secondparam string) {
			secondparam == "hello"
		}
		`, nil, req)

	headRevision, err := ds.HeadRevision(t.Context())
	req.NoError(err)

	dl := datalayer.NewDataLayer(ds)
	sr, err := dl.SnapshotReader(headRevision).ReadSchema()
	req.NoError(err)

	runner := NewCaveatRunner(types.Default.TypeSet)

	// Test populating definitions for complex expression
	expr := caveatAnd(
		caveatexpr("first_caveat"),
		caveatexpr("second_caveat"),
	)

	err = runner.PopulateCaveatDefinitionsForExpr(t.Context(), expr, sr)
	req.NoError(err)

	// Should be able to run the expression now without additional lookups
	result, err := runner.RunCaveatExpression(
		t.Context(),
		expr,
		map[string]any{
			"firstparam": int64(42),
		},
		noCaveatsReader{},
		RunCaveatExpressionNoDebugging,
	)
	req.NoError(err)
	req.False(result.Value()) // Should be false because second_caveat is missing context
	req.True(result.IsPartial())
}

func TestCaveatRunnerEmptyExpression(t *testing.T) {
	req := require.New(t)

	rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, 0, memdb.DisableGC)
	req.NoError(err)

	ds, _ := testfixtures.DatastoreFromSchemaAndTestRelationships(rawDS, `
		caveat test_caveat(param int) {
			param == 42
		}
		`, nil, req)

	headRevision, err := ds.HeadRevision(t.Context())
	req.NoError(err)

	dl := datalayer.NewDataLayer(ds)
	sr, err := dl.SnapshotReader(headRevision).ReadSchema()
	req.NoError(err)

	runner := NewCaveatRunner(types.Default.TypeSet)

	// Test with an expression that has no caveats (empty operation)
	expr := &core.CaveatExpression{
		OperationOrCaveat: &core.CaveatExpression_Operation{
			Operation: &core.CaveatOperation{
				Op:       core.CaveatOperation_AND,
				Children: []*core.CaveatExpression{},
			},
		},
	}

	err = runner.PopulateCaveatDefinitionsForExpr(t.Context(), expr, sr)
	req.Error(err)
	req.Contains(err.Error(), "received empty caveat expression")
}

func TestSyntheticResultMissingVarNames(t *testing.T) {
	// Test MissingVarNames on a non-partial result
	sr := syntheticResult{
		value:           true,
		isPartialResult: false,
	}

	_, err := sr.MissingVarNames()
	require.Error(t, err)
	require.Contains(t, err.Error(), "not a partial value")

	// Test MissingVarNames on a partial result with no missing variables tracked
	sr2 := syntheticResult{
		value:           false,
		isPartialResult: true,
		missingVarNames: mapz.NewSet[string](),
	}

	missingVars, err := sr2.MissingVarNames()
	require.NoError(t, err)
	require.Empty(t, missingVars)

	// Test MissingVarNames on a partial result with debug results
	env := pkgcaveats.NewEnvironmentWithDefaultTypeSet()
	require.NoError(t, env.AddVariable("param", types.Default.AnyType))

	caveat, err := pkgcaveats.CompileCaveatWithName(env, "param == 5", "test")
	require.NoError(t, err)

	// Create a partial result by not providing the parameter
	partialResult, err := pkgcaveats.EvaluateCaveat(caveat, map[string]any{})
	require.NoError(t, err)
	require.True(t, partialResult.IsPartial())

	sr3 := syntheticResult{
		value:               false,
		isPartialResult:     true,
		exprResultsForDebug: []ExpressionResult{partialResult},
	}

	missingVars, err = sr3.MissingVarNames()
	require.NoError(t, err)
	require.Contains(t, missingVars, "param")
}

func TestUnknownCaveatOperation(t *testing.T) {
	req := require.New(t)

	rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, 0, memdb.DisableGC)
	req.NoError(err)

	ds, _ := testfixtures.DatastoreFromSchemaAndTestRelationships(rawDS, `
		caveat test_caveat(param int) {
			param == 42
		}
		`, nil, req)

	headRevision, err := ds.HeadRevision(t.Context())
	req.NoError(err)

	dl := datalayer.NewDataLayer(ds)
	sr, err := dl.SnapshotReader(headRevision).ReadSchema()
	req.NoError(err)

	runner := NewCaveatRunner(types.Default.TypeSet)

	// Create an expression with an unknown operation
	expr := &core.CaveatExpression{
		OperationOrCaveat: &core.CaveatExpression_Operation{
			Operation: &core.CaveatOperation{
				Op:       999, // Invalid operation
				Children: []*core.CaveatExpression{caveatexpr("test_caveat")},
			},
		},
	}

	err = runner.PopulateCaveatDefinitionsForExpr(t.Context(), expr, sr)
	req.NoError(err)

	require.Panics(t, func() {
		_, err = runner.RunCaveatExpression(
			t.Context(),
			expr,
			map[string]any{"param": int64(42)},
			sr,
			RunCaveatExpressionNoDebugging,
		)
	})
}
