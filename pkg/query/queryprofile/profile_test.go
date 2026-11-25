package queryprofile_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/queryopttest"
	"github.com/authzed/spicedb/pkg/query"
	"github.com/authzed/spicedb/pkg/query/queryopt"
	"github.com/authzed/spicedb/pkg/query/queryprofile"
)

func TestNativeProfileExcludesOptionalOptimizers(t *testing.T) {
	profile, err := queryprofile.Resolve(queryprofile.NativeParityV1, queryprofile.Options{CheckConcurrency: 50, DispatchChunkSize: 100})
	require.NoError(t, err)
	passes := profile.StructuralOptimizers(queryopt.RequestParams{Operation: query.OperationCheck})
	require.Len(t, passes, 1)
	require.Equal(t, "fixed-check-concurrency", passes[0].Name)
	require.Empty(t, profile.DecisionOptimizers())
	require.False(t, profile.CollectSignals())
	require.Len(t, profile.ContextOptions(), 2)

	_, err = queryprofile.Resolve("unknown", queryprofile.Options{})
	require.Error(t, err)
	_, err = queryprofile.Resolve(queryprofile.NativeParityV1, queryprofile.Options{})
	require.Error(t, err)
}

func TestNativeProfilePreservesExecutionShape(t *testing.T) {
	profile, err := queryprofile.Resolve(queryprofile.NativeParityV1, queryprofile.Options{CheckConcurrency: 2, DispatchChunkSize: 10})
	require.NoError(t, err)

	outline := queryopttest.FromSchema(t, `
		definition user {}
		definition document {
			relation reader: user
			relation writer: user
			relation owner: user
			permission inner = reader + writer
			permission view = inner + owner
		}`, "document", "view", profile.BuildOptions()...)
	require.Equal(t, query.AliasIteratorType, outline.Root.Type)
	require.Equal(t, query.UnionIteratorType, outline.Root.SubOutlines[0].Type)
	require.Len(t, outline.Root.SubOutlines[0].SubOutlines, 2)
	require.NotZero(t, outline.Root.ID)
	_, err = outline.Compile()
	require.NoError(t, err)
}

func TestNativeProfileLeavesCaveatsSymbolic(t *testing.T) {
	profile, err := queryprofile.Resolve(queryprofile.NativeParityV1, queryprofile.Options{
		CheckConcurrency: 50, DispatchChunkSize: 100,
	})
	require.NoError(t, err)
	outline := queryopttest.FromSchema(t, `
  caveat enabled(ok bool) { ok }
  definition user {}
  definition document {
   relation viewer: user with enabled
   permission view = viewer
  }`, "document", "view", profile.BuildOptions()...)

	// Public native-parity dispatch omits request context. Its compiled tree
	// must carry expressions rather than inserting context-evaluating filters.
	var assertSymbolic func(query.Outline)
	assertSymbolic = func(node query.Outline) {
		require.NotEqual(t, query.CaveatIteratorType, node.Type)

		for _, child := range node.SubOutlines {
			assertSymbolic(child)
		}
	}
	assertSymbolic(outline.Root)
	_, err = outline.Compile()
	require.NoError(t, err)
}
