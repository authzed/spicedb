package graph

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/revisions"
	"github.com/authzed/spicedb/internal/queryopttest"
	"github.com/authzed/spicedb/internal/queryplan"
	"github.com/authzed/spicedb/internal/querysignalstest"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	dispatchv1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/query"
	"github.com/authzed/spicedb/pkg/query/planhint"
	"github.com/authzed/spicedb/pkg/query/queryopt"
	"github.com/authzed/spicedb/pkg/query/queryprofile"
)

func TestReceivedCheckPlansReuseExactWireAndLocalPolicy(t *testing.T) {
	profile, err := queryprofile.Resolve(queryprofile.NativeParityV1, queryprofile.Options{CheckConcurrency: 2, DispatchChunkSize: 100})
	require.NoError(t, err)
	receiver := &localDispatcher{queryProfile: profile}
	revision := revisions.NewForTransactionID(7)
	reader := &signalReader{schema: &signalSchema{stored: querysignalstest.Stored("a")}}
	ctx := datalayer.ContextWithDataLayer(t.Context(), &signalDataLayer{revision: revision, reader: reader, t: t})
	outline := queryopttest.Canonical(t, queryopttest.Union(
		queryopttest.Fixed(t, "document:d#reader@user:u"),
		queryopttest.Fixed(t, "document:d#writer@user:u"),
	))
	original, err := outline.Compile()
	require.NoError(t, err)
	var wire bytes.Buffer
	require.NoError(t, original.Serialize(&wire))
	request := &dispatchv1.DispatchQueryPlanRequest{Plan: wire.Bytes(), Operation: dispatchv1.PlanOperation_PLAN_OPERATION_CHECK}
	first, firstLedger, err := prepareReceivedCheck(t, receiver, ctx, revision, "a", request)
	require.NoError(t, err)
	require.Equal(t, original.CanonicalKey(), first.CanonicalKey())
	require.Equal(t, uint16(2), firstLedger.Hints()[0].Payload.(planhint.CheckConcurrency).Limit)

	for _, operation := range []dispatchv1.PlanOperation{
		dispatchv1.PlanOperation_PLAN_OPERATION_CHECK,
		dispatchv1.PlanOperation_PLAN_OPERATION_CHECK_MANY_RESOURCES,
		dispatchv1.PlanOperation_PLAN_OPERATION_CHECK_MANY_SUBJECTS,
	} {
		request.Operation = operation
		reused, ledger, err := prepareReceivedCheck(t, receiver, ctx, revision, "a", request)
		require.NoError(t, err)
		require.Same(t, first, reused)
		require.NotSame(t, firstLedger, ledger)
	}

	// Equal wire bytes use the receiver's resolved scheduling policy. A changed
	// receiver limit must produce another completed tree and corresponding hints.
	receiver.queryProfile, err = queryprofile.Resolve(
		queryprofile.NativeParityV1,
		queryprofile.Options{CheckConcurrency: 3, DispatchChunkSize: 100},
	)
	require.NoError(t, err)
	changedPolicy, changedLedger, err := prepareReceivedCheck(t, receiver, ctx, revision, "a", request)
	require.NoError(t, err)
	require.NotSame(t, first, changedPolicy)
	require.Equal(t, uint16(3), changedLedger.Hints()[0].Payload.(planhint.CheckConcurrency).Limit)

	// Every cache probe still reads the requested revision's schema. Empty or
	// malformed wire data must not acquire a usable entry from an earlier request.
	require.Equal(t, 5, reader.reads)
	request.Plan = []byte("invalid-plan")
	_, _, err = prepareReceivedCheck(t, receiver, ctx, revision, "a", request)
	require.Error(t, err)
	require.Equal(t, 6, reader.reads)
}

func TestReceivedCacheDistinguishesEqualKeysWithDifferentPaths(t *testing.T) {
	profile, err := queryprofile.Resolve(queryprofile.NativeParityV1, queryprofile.Options{CheckConcurrency: 2, DispatchChunkSize: 100})
	require.NoError(t, err)
	receiver := &localDispatcher{queryProfile: profile}
	revision := revisions.NewForTransactionID(7)
	reader := &signalReader{schema: &signalSchema{stored: querysignalstest.Stored("a")}}
	ctx := datalayer.ContextWithDataLayer(t.Context(), &signalDataLayer{revision: revision, reader: reader, t: t})
	plans := make([]query.Iterator, 0, 2)

	for _, subject := range []string{"u", "v"} {
		outline := queryopttest.Canonical(t, queryopttest.Fixed(t, "document:d#reader@user:"+subject))
		original, err := outline.Compile()
		require.NoError(t, err)
		var wire bytes.Buffer
		require.NoError(t, original.Serialize(&wire))
		request := &dispatchv1.DispatchQueryPlanRequest{Plan: wire.Bytes(), Operation: dispatchv1.PlanOperation_PLAN_OPERATION_CHECK}
		compiled, ledger, err := prepareReceivedCheck(t, receiver, ctx, revision, "a", request)
		require.NoError(t, err)
		plans = append(plans, compiled)
		execution := query.NewLocalContext(t.Context(), query.WithHintLedger(ledger))
		path, err := execution.Check(compiled, query.NewObject("document", "d"), query.NewObjectAndRelation("u", "user", "..."))
		require.NoError(t, err)

		if subject == "u" {
			require.NotNil(t, path)
		} else {
			require.Nil(t, path)
		}
	}

	// Logical keys deliberately identify only the fixed population count. Exact
	// wire identity must keep these different granting populations separate.
	require.Equal(t, plans[0].CanonicalKey(), plans[1].CanonicalKey())
	require.NotSame(t, plans[0], plans[1])
}

// prepareReceivedCheck exercises the shared preparation entrypoint with the same
// snapshot schema acquisition used by DispatchQueryPlan, without executing RPCs.
func prepareReceivedCheck(t *testing.T, receiver *localDispatcher, ctx context.Context,
	revision datastore.Revision, hash datalayer.SchemaHash, request *dispatchv1.DispatchQueryPlanRequest,
) (query.Iterator, *planhint.Ledger, error) {
	t.Helper()
	reader, err := datalayer.MustFromContext(ctx).SnapshotReader(revision, hash).ReadSchema(ctx)
	if err != nil {
		return nil, nil, err
	}

	operation, err := planOperationToQueryOperation(request.Operation)
	if err != nil {
		return nil, nil, err
	}

	planner := queryplan.Planner{Profile: receiver.queryProfile, Signals: receiver.querySignals}
	plan, err := planner.FromWire(ctx, queryplan.Request{
		Schema:     reader,
		Revision:   revision,
		SchemaHash: hash,
		Params:     queryopt.RequestParams{Operation: operation},
	}, request.Plan)
	if err != nil {
		return nil, nil, err
	}

	t.Cleanup(plan.Flush)
	return plan.Iterator, plan.HintLedger, nil
}
