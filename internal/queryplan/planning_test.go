package queryplan

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/revisions"
	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/queryopttest"
	"github.com/authzed/spicedb/internal/querysignalstest"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	dispatchv1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/query"
	"github.com/authzed/spicedb/pkg/query/queryopt"
	"github.com/authzed/spicedb/pkg/query/signals"
)

// cachedCheckDispatcher isolates the result-cache boundary. A hit must avoid
// schema conversion and dispatch, while preserving the symbolic caveat path.
type cachedCheckDispatcher struct {
	dispatch.Dispatcher
	lookups int
	lookup  dispatch.PlanCheckLookup
}

func (d *cachedCheckDispatcher) LookupPlanCheck(_ context.Context, lookup dispatch.PlanCheckLookup) (*dispatchv1.ResultPath, bool, error) {
	d.lookups++
	d.lookup = lookup
	return dispatch.QueryPathToResultPath(&query.Path{
		Resource: query.NewObject("document", "d"),
		Subject:  query.NewObjectAndRelation("u", "user", "..."),
		Caveat: &core.CaveatExpression{OperationOrCaveat: &core.CaveatExpression_Caveat{
			Caveat: &core.ContextualizedCaveat{CaveatName: "enabled"},
		}},
	}), true, nil
}

func TestCheckCacheHitAvoidsPlanningAndKeepsRequestContextOutOfCache(t *testing.T) {
	planner := Planner{Profile: checkKey(t, 2).Profile}
	dispatcher := &cachedCheckDispatcher{}
	request := CheckRequest{
		Request: Request{
			Revision:      revisions.NewForTransactionID(7),
			SchemaHash:    "schema",
			CaveatContext: map[string]any{"ok": true},
		},
		Permission:   "view",
		Resource:     query.NewObject("document", "d"),
		Subject:      query.NewObjectAndRelation("u", "user", "..."),
		Dispatcher:   dispatcher,
		MaximumDepth: 50,
	}

	// No schema reader or execution dependencies are supplied: a cache hit must
	// return before touching them, and must leave the original request values intact.
	path, err := planner.Check(t.Context(), request)
	require.NoError(t, err)
	require.NotNil(t, path)
	require.Equal(t, "enabled", path.Caveat.GetCaveat().CaveatName)
	require.Nil(t, dispatcher.lookup.PlanContext.CaveatContext)
	require.Equal(t, int32(50), dispatcher.lookup.PlanContext.MaxRecursionDepth)
	require.Equal(t, "document#view", dispatcher.lookup.CanonicalKey)
	require.Equal(t, true, request.CaveatContext["ok"])

	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = planner.Check(canceled, request)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, dispatcher.lookups)
}

func TestReceivedPreparationReusesOnlyFixedPolicyAndKeepsLedgersIndependent(t *testing.T) {
	outline := queryopttest.Canonical(t, queryopttest.Union(
		queryopttest.Fixed(t, "document:d#reader@user:u"),
		queryopttest.Fixed(t, "document:d#writer@user:u"),
	))
	original, err := outline.Compile()
	require.NoError(t, err)
	var wire bytes.Buffer
	require.NoError(t, original.Serialize(&wire))
	request := Request{
		Schema:     schemaReader{stored: querysignalstest.Stored("schema")},
		Revision:   revisions.NewForTransactionID(7),
		SchemaHash: "schema",
		Params:     queryopt.RequestParams{Operation: query.OperationCheck},
	}

	for _, tc := range []struct {
		name  string
		fixed bool
	}{
		{name: "learned"},
		{name: "fixed", fixed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			planner := Planner{Signals: signals.NewDefaultOwner()}

			if tc.fixed {
				planner.Profile = checkKey(t, 2).Profile
			}

			first, err := planner.FromWire(t.Context(), request, wire.Bytes())
			require.NoError(t, err)
			defer first.Flush()
			second, err := planner.FromWire(t.Context(), request, wire.Bytes())
			require.NoError(t, err)
			defer second.Flush()
			require.Equal(t, original.CanonicalKey(), first.Iterator.CanonicalKey())

			if tc.fixed {
				require.Same(t, first.Iterator, second.Iterator)
				require.NotSame(t, first.HintLedger, second.HintLedger)
				require.Len(t, first.HintLedger.Hints(), 1)
			} else {
				require.NotSame(t, first.Iterator, second.Iterator)
				require.NotNil(t, first.collector)
				require.NotSame(t, first.collector, second.collector)
			}
		})
	}
}

func TestReceivedLookupsRetainCurrentPolicy(t *testing.T) {
	iterator := query.NewFixedIterator()
	var wire bytes.Buffer
	require.NoError(t, iterator.Serialize(&wire))
	planner := Planner{Profile: checkKey(t, 2).Profile, Signals: signals.NewDefaultOwner()}

	for _, operation := range []query.Operation{query.OperationIterSubjects, query.OperationIterResources} {
		t.Run(fmt.Sprint(operation), func(t *testing.T) {
			request := Request{
				Schema:     schemaReader{stored: querysignalstest.Stored("schema")},
				Revision:   revisions.NewForTransactionID(7),
				SchemaHash: "schema",
				Params:     queryopt.RequestParams{Operation: operation},
			}
			prepared, err := planner.FromWire(t.Context(), request, wire.Bytes())
			require.NoError(t, err)
			defer prepared.Flush()

			// Native parity is a Check-only policy. Receiver lookups must keep
			// collecting normal evidence and must not inherit its Check ceiling.
			require.NotNil(t, prepared.collector)
			ctx := query.NewLocalContext(t.Context(), prepared.Options...)
			require.EqualValues(t, 1, ctx.CheckConcurrencyLimit())
		})
	}
}
