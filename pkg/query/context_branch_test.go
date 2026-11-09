package query

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/caveats"
	"github.com/authzed/spicedb/pkg/caveats/types"
)

func TestCheckBranchesIsolateTargetsAndDepth(t *testing.T) {
	parent := NewLocalContext(t.Context(), WithCaveatRunner(caveats.NewCaveatRunner(types.Default.TypeSet)))
	parent.TargetSubjectType = ObjectType{Type: "user", Subrelation: "..."}
	parent.targetedDepth = 2
	ancestor := &RecursiveIterator{}
	parent.targetedRecursions = map[string]*RecursiveIterator{"ancestor": ancestor}
	root := NewUnionIterator(NewFixedIterator(), NewFixedIterator())
	parent.MarkAsOperation(root, OperationCheck)
	branches := []*Context{parent.forkForCheck(parent.Context), parent.forkForCheck(parent.Context)}
	var workers sync.WaitGroup

	for index, branch := range branches {
		require.Equal(t, parent.TargetSubjectType, branch.TargetSubjectType)
		require.Equal(t, 2, branch.targetedDepth)
		require.Same(t, ancestor, branch.targetedRecursions["ancestor"])
		require.NotSame(t, parent.CaveatRunner, branch.CaveatRunner)
		require.False(t, branch.MarkAsOperation(NewFixedIterator(), OperationIterSubjects))
		workers.Go(func() {
			branch.TargetSubjectType = ObjectType{Type: fmt.Sprintf("branch%d", index)}
			branch.targetedDepth++
			branch.targetedRecursions["child"] = &RecursiveIterator{}
		})
	}

	workers.Wait()
	require.Equal(t, "user", parent.TargetSubjectType.Type)
	require.Equal(t, 2, parent.targetedDepth)
	require.Len(t, parent.targetedRecursions, 1)
	require.NotEqual(t, branches[0].TargetSubjectType, branches[1].TargetSubjectType)
}

type branchCompletionObserver struct{ events []ExecutionObservation }

func (o *branchCompletionObserver) ObserveExecution(event ExecutionObservation) {
	o.events = append(o.events, event)
}

func TestCheckBranchesShareUniqueInvocationIDs(t *testing.T) {
	observer := &branchCompletionObserver{}
	parent := NewLocalContext(t.Context(), WithExecutionObserver(observer))
	leaf := NewFixedIterator()
	root := NewUnionIterator(leaf, leaf)
	parent.MarkAsOperation(root, OperationCheck)
	parent.executionStart(root, OperationCheck, "", "single")
	var workers sync.WaitGroup

	for range 32 {
		branch := parent.forkForCheck(context.Background())
		workers.Go(func() {
			_, err := branch.Check(leaf, NewObject("document", "d"), NewObjectAndRelation("u", "user", "..."))
			if err != nil {
				t.Error(err)
			}
		})
	}

	workers.Wait()
	require.Len(t, observer.events, 32)
	seen := map[uint64]bool{}

	for _, event := range observer.events {
		require.False(t, seen[event.Invocation])
		seen[event.Invocation] = true
		require.NotEmpty(t, event.Key.Identity)
	}
}

func TestFixedCheckReturnsOwnedNestedState(t *testing.T) {
	path := MustPathFromString("document:d#view@user:u")
	path.Caveat = caveats.CaveatExprForTesting("visible")
	path.Metadata = map[string]any{"nested": map[string]any{"value": "original"}}
	iterator := NewFixedIterator(*path)
	ctx := NewLocalContext(t.Context())
	first, err := ctx.Check(iterator, path.Resource, path.Subject)
	require.NoError(t, err)
	first.Caveat.GetCaveat().CaveatName = "changed"
	first.Metadata["nested"].(map[string]any)["value"] = "changed"

	second, err := ctx.Check(iterator, path.Resource, path.Subject)
	require.NoError(t, err)
	require.Equal(t, "visible", second.Caveat.GetCaveat().CaveatName)
	require.Equal(t, "original", second.Metadata["nested"].(map[string]any)["value"])
}
