package graph

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/queryopttest"
	"github.com/authzed/spicedb/internal/queryplan"
	"github.com/authzed/spicedb/internal/querysignalstest"
	"github.com/authzed/spicedb/pkg/genutil/execscope"
	"github.com/authzed/spicedb/pkg/query"
	"github.com/authzed/spicedb/pkg/query/queryopt"
	"github.com/authzed/spicedb/pkg/query/queryprofile"
)

// barrierExecutor holds leaf Checks until both receiver branches have started.
type barrierExecutor struct {
	query.LocalExecutor
	started chan struct{}
	release <-chan struct{}
}

func (e *barrierExecutor) Check(
	ctx *query.Context,
	it query.Iterator,
	resource query.Object,
	subject query.ObjectAndRelation,
) (*query.Path, error) {
	if len(it.Subiterators()) == 0 {
		e.started <- struct{}{}

		select {
		case <-e.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	return e.LocalExecutor.Check(ctx, it, resource, subject)
}

func TestReceivedParallelPolicyPreservesKeysAndReceiverLimit(t *testing.T) {
	sender, err := queryprofile.Resolve(queryprofile.NativeParityV1, queryprofile.Options{CheckConcurrency: 8, DispatchChunkSize: 10})
	require.NoError(t, err)

	// Model independent permission operands with fixed leaves, avoiding datastore
	// timing variance while retaining the ownership marker used by real schemas.
	root := queryopttest.Intersection(
		queryopttest.Fixed(t, "document:d#reader@user:u"),
		queryopttest.Fixed(t, "document:d#writer@user:u"),
	)
	root.IndependentCheckBranches = true
	co := queryopttest.Canonical(t, root)
	params := queryopt.RequestParams{Operation: query.OperationCheck}
	co, err = queryopt.ApplyOptimizations(co, sender.StructuralOptimizers(params), params)
	require.NoError(t, err)
	original, err := co.Compile()
	require.NoError(t, err)
	var wire bytes.Buffer
	require.NoError(t, dispatch.NewDispatchIterator(original).Serialize(&wire))
	received, err := query.Deserialize(bytes.NewReader(wire.Bytes()), &query.DeserializeContext{})
	require.NoError(t, err)
	receiver, err := queryprofile.Resolve(queryprofile.NativeParityV1, queryprofile.Options{CheckConcurrency: 2, DispatchChunkSize: 10})
	require.NoError(t, err)
	planner := queryplan.Planner{Profile: receiver}
	planned, err := planner.FromWire(t.Context(), queryplan.Request{
		Schema: &signalSchema{stored: querysignalstest.Stored("schema")}, Params: params,
	}, wire.Bytes())
	require.NoError(t, err)
	require.Len(t, planned.HintLedger.Hints(), 1)
	compiled := planned.Iterator
	require.Equal(t, received.CanonicalKey(), compiled.CanonicalKey())
	require.Equal(t, original.CanonicalKey(), compiled.Subiterators()[0].CanonicalKey())
	release := make(chan struct{})
	executor := &barrierExecutor{started: make(chan struct{}, 2), release: release}
	scope := execscope.New()
	options := append(receiver.ContextOptions(), query.WithHintLedger(planned.HintLedger))
	deadline, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	ctx := query.NewQueryContext(scope.Context(deadline), executor, options...)
	done := make(chan error, 1)
	go func() {
		_, err := ctx.Check(compiled, query.NewObject("document", "d"), query.NewObjectAndRelation("u", "user", "..."))
		done <- err
	}()

	for range 2 {
		select {
		case <-executor.started:
		case <-deadline.Done():
			t.Fatal("receiver branches failed to start concurrently")
		}
	}

	close(release)
	require.NoError(t, <-done)
	scope.Wait()
}
