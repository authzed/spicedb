package caveats_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/caveats"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/testfixtures"
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
		name          string
		expression    *core.CaveatExpression
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
		tc := tc
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
				debugOption := debugOption
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

func TestRunCaveatWithMissingMap(t *testing.T) {
	req := require.New(t)

	rawDS, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
	req.NoError(err)

	ds, _ := testfixtures.DatastoreFromSchemaAndTestRelationships(rawDS, `
				caveat some_caveat(themap map<any>) {
					themap.first == 42
				}
				`, nil, req)

	headRevision, err := ds.HeadRevision(context.Background())
	req.NoError(err)

	reader := ds.SnapshotReader(headRevision)

	result, err := caveats.RunCaveatExpression(
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

	rawDS, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
	req.NoError(err)

	ds, _ := testfixtures.DatastoreFromSchemaAndTestRelationships(rawDS, `
				caveat some_caveat(themap map<any>) {
					themap.first == 42
				}
				`, nil, req)

	headRevision, err := ds.HeadRevision(context.Background())
	req.NoError(err)

	reader := ds.SnapshotReader(headRevision)

	_, err = caveats.RunCaveatExpression(
		context.Background(),
		caveatexpr("some_caveat"),
		map[string]any{
			"themap": map[string]any{},
		},
		reader,
		caveats.RunCaveatExpressionNoDebugging,
	)
	req.Error(err)
	req.True(errors.As(err, &caveats.EvaluationErr{}))
}
