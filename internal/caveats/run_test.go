package caveats_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/caveats"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/testfixtures"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

var (
	caveatexpr   = caveats.CaveatExprForTesting
	caveatAnd    = caveats.And
	caveatOr     = caveats.Or
	caveatInvert = caveats.Invert
)

func TestRunCaveatExpressions(t *testing.T) {
	t.Parallel()
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
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
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
