package caveats_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/caveats"
	"github.com/authzed/spicedb/internal/datastore/dsfortesting"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

var (
	caveatexpr   = caveats.CaveatExprForTesting
	caveatAnd    = caveats.And
	caveatOr     = caveats.Or
	caveatInvert = caveats.Invert
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
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			req := require.New(t)

			rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(0, 0, memdb.DisableGC)
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
				debugOption := debugOption
				t.Run(fmt.Sprintf("%v", debugOption), func(t *testing.T) {
					req := require.New(t)

					result, err := caveats.RunSingleCaveatExpression(context.Background(), tc.expression, tc.context, reader, debugOption)
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

					if debugOption == caveats.RunCaveatExpressionWithDebugInformation {
						exprString, _, err := caveats.BuildDebugInformation(result)
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

	rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(0, 0, memdb.DisableGC)
	req.NoError(err)

	ds, _ := testfixtures.DatastoreFromSchemaAndTestRelationships(rawDS, `
				caveat some_caveat(themap map<any>) {
					themap.first == 42
				}
				`, nil, req)

	headRevision, err := ds.HeadRevision(context.Background())
	req.NoError(err)

	reader := ds.SnapshotReader(headRevision)

	result, err := caveats.RunSingleCaveatExpression(
		context.Background(),
		caveatexpr("some_caveat"),
		map[string]any{},
		reader,
		caveats.RunCaveatExpressionNoDebugging,
	)
	req.NoError(err)
	req.True(result.IsPartial())
	req.False(result.Value())
}

func TestRunCaveatWithEmptyMap(t *testing.T) {
	req := require.New(t)

	rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(0, 0, memdb.DisableGC)
	req.NoError(err)

	ds, _ := testfixtures.DatastoreFromSchemaAndTestRelationships(rawDS, `
				caveat some_caveat(themap map<any>) {
					themap.first == 42
				}
				`, nil, req)

	headRevision, err := ds.HeadRevision(context.Background())
	req.NoError(err)

	reader := ds.SnapshotReader(headRevision)

	_, err = caveats.RunSingleCaveatExpression(
		context.Background(),
		caveatexpr("some_caveat"),
		map[string]any{
			"themap": map[string]any{},
		},
		reader,
		caveats.RunCaveatExpressionNoDebugging,
	)
	req.Error(err)
	req.True(errors.As(err, &caveats.EvaluationError{}))
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

	headRevision, err := ds.HeadRevision(context.Background())
	req.NoError(err)

	reader := ds.SnapshotReader(headRevision)

	runner := caveats.NewCaveatRunner()

	// Run the first caveat.
	result, err := runner.RunCaveatExpression(context.Background(), caveatexpr("some_caveat"), map[string]any{
		"themap": map[string]any{
			"first": 42,
		},
	}, reader, caveats.RunCaveatExpressionNoDebugging)
	req.NoError(err)
	req.True(result.Value())

	// Run the first caveat again, giving a reader that will error if it is used, ensuring
	// the cache is used.
	result, err = runner.RunCaveatExpression(context.Background(), caveatexpr("some_caveat"), map[string]any{
		"themap": map[string]any{
			"first": 41,
		},
	}, noCaveatsReader{reader}, caveats.RunCaveatExpressionNoDebugging)
	req.NoError(err)
	req.False(result.Value())

	// Run the second caveat.
	result, err = runner.RunCaveatExpression(context.Background(), caveatexpr("another_caveat"), map[string]any{
		"somecondition": int64(42),
	}, reader, caveats.RunCaveatExpressionNoDebugging)
	req.NoError(err)
	req.True(result.Value())
}

type noCaveatsReader struct {
	datastore.Reader
}

func (f noCaveatsReader) ReadCaveatByName(ctx context.Context, name string) (caveat *core.CaveatDefinition, lastWritten datastore.Revision, err error) {
	return nil, nil, errors.New("should not be called")
}

func (f noCaveatsReader) LookupCaveatsWithNames(ctx context.Context, names []string) ([]datastore.RevisionedCaveat, error) {
	return nil, errors.New("should not be called")
}

func (f noCaveatsReader) ListAllCaveats(ctx context.Context) ([]datastore.RevisionedCaveat, error) {
	return nil, errors.New("should not be called")
}
