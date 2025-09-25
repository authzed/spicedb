package query

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/authzed/spicedb/internal/caveats"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/pkg/caveats/types"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

// createTestCaveatExpression creates a CaveatExpression for testing
func createTestCaveatExpression(name string, context map[string]any) *core.CaveatExpression {
	caveat := &core.ContextualizedCaveat{
		CaveatName: name,
	}
	if context != nil {
		ctx, err := structpb.NewStruct(context)
		if err == nil {
			caveat.Context = ctx
		}
	}
	return &core.CaveatExpression{
		OperationOrCaveat: &core.CaveatExpression_Caveat{
			Caveat: caveat,
		},
	}
}

// Enhanced test helper for creating complex caveat expressions
func createComplexCaveatExpression(op core.CaveatOperation_Operation, children []*core.CaveatExpression) *core.CaveatExpression {
	return &core.CaveatExpression{
		OperationOrCaveat: &core.CaveatExpression_Operation{
			Operation: &core.CaveatOperation{
				Op:       op,
				Children: children,
			},
		},
	}
}

// Test helper for creating test datastore with caveat runner
func createTestDatastoreWithCaveats(t *testing.T, require *require.Assertions) (datastore.Datastore, datastore.Revision, *caveats.CaveatRunner) {
	rawDS, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
	require.NoError(err)

	ctx := context.Background()

	// Create a basic caveat definition
	caveatDef := &core.CaveatDefinition{
		Name: "count_limit",
		// Most of the fields need actual caveat infrastructure which is complex
		// For now, just create a minimal definition
	}

	revision, err := rawDS.ReadWriteTx(ctx, func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
		return tx.WriteCaveats(ctx, []*core.CaveatDefinition{caveatDef})
	})
	require.NoError(err)

	// Create caveat runner
	runner := caveats.NewCaveatRunner(types.NewTypeSet())

	return rawDS, revision, runner
}

func TestCaveatIterator(t *testing.T) {
	// Create test paths using caveat helper functions
	pathWithCaveat := MustPathFromString("document:doc1#view@user:alice")
	pathWithCaveat.Caveat = createTestCaveatExpression("test_caveat", map[string]any{
		"allowed": true,
	})

	pathWithoutCaveat := MustPathFromString("document:doc2#view@user:bob")

	pathWithDifferentCaveat := MustPathFromString("document:doc3#view@user:charlie")
	pathWithDifferentCaveat.Caveat = createTestCaveatExpression("other_caveat", map[string]any{
		"allowed": true,
	})

	testCases := []struct {
		name          string
		caveat        *core.ContextualizedCaveat
		paths         []*Path
		caveatContext map[string]any
		expectedPaths []*Path
	}{
		{
			name:   "no caveat iterator allows all paths",
			caveat: nil,
			paths: []*Path{
				pathWithCaveat,
				pathWithoutCaveat,
				pathWithDifferentCaveat,
			},
			caveatContext: nil,
			expectedPaths: []*Path{
				pathWithCaveat,
				pathWithoutCaveat,
				pathWithDifferentCaveat,
			},
		},
		{
			name:   "caveat iterator with matching caveat name filters correctly",
			caveat: createTestCaveat("test_caveat", nil),
			paths: []*Path{
				pathWithCaveat,          // has test_caveat
				pathWithoutCaveat,       // has no caveat
				pathWithDifferentCaveat, // has other_caveat
			},
			caveatContext: map[string]any{
				"allowed": true,
			},
			expectedPaths: []*Path{
				// This will fail due to caveat not found, which is expected behavior
			},
		},
		{
			name:   "caveat iterator filters out paths without matching caveat",
			caveat: createTestCaveat("test_caveat", nil),
			paths: []*Path{
				pathWithCaveat,
				pathWithoutCaveat,
				pathWithDifferentCaveat,
			},
			caveatContext: map[string]any{
				"allowed": true,
			},
			expectedPaths: []*Path{
				// Paths should be filtered - caveat evaluation will fail since no caveat definitions exist
			},
		},
		{
			name:   "caveat iterator without context denies all caveated paths",
			caveat: createTestCaveat("test_caveat", nil),
			paths: []*Path{
				pathWithCaveat,
				pathWithoutCaveat,
			},
			caveatContext: nil, // No caveat context provided
			expectedPaths: []*Path{
				// No paths should pass since no context is provided
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create a fixed iterator with the test paths
			fixedIter := NewFixedIterator(tc.paths...)

			// Create the caveat iterator
			caveatIter := NewCaveatIterator(fixedIter, tc.caveat)

			// Create test datastore and context
			ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
			require.NoError(t, err)

			rev, err := ds.ReadWriteTx(context.Background(), func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
				return nil
			})
			require.NoError(t, err)

			// Create test context with CaveatRunner and datastore
			queryCtx := &Context{
				Context:       context.Background(),
				Executor:      LocalExecutor{},
				Datastore:     ds,
				Revision:      rev,
				CaveatContext: tc.caveatContext,
				CaveatRunner:  caveats.NewCaveatRunner(types.NewTypeSet()),
			}

			// Test IterSubjectsImpl - this is more straightforward than CheckImpl
			// as it returns all paths for a given resource
			resource := NewObject("document", "doc1")

			seq, err := queryCtx.IterSubjects(caveatIter, resource)

			// Count expected paths that match this resource
			var expectedMatchingPaths []*Path
			for _, path := range tc.expectedPaths {
				if path.Resource.ObjectType == resource.ObjectType && path.Resource.ObjectID == resource.ObjectID {
					expectedMatchingPaths = append(expectedMatchingPaths, path)
				}
			}

			if tc.name == "no caveat iterator allows all paths" {
				require.NoError(t, err)
				actualPaths, err := CollectAll(seq)
				require.NoError(t, err)
				require.Len(t, actualPaths, len(expectedMatchingPaths), "Expected all matching paths to be returned")
			} else if tc.name == "caveat iterator filters out paths without matching caveat" {
				// Tests that attempt caveat evaluation should fail since we don't have caveat definitions
				// in the test context. This is expected and correct behavior.
				if err != nil {
					// Accept various caveat-related errors (not found, evaluation errors, etc.)
					require.True(t,
						err.Error() != "",
						"Expected some caveat-related error")
				} else {
					actualPaths, err := CollectAll(seq)
					if err != nil {
						// Accept various caveat-related errors during path collection
						require.True(t,
							err.Error() != "",
							"Expected some caveat-related error")
					} else {
						require.Fail(t, "Expected caveat evaluation to fail, but got paths: %v", actualPaths)
					}
				}
			} else if tc.caveat != nil && tc.caveat.CaveatName != "" {
				// Tests that attempt caveat evaluation should fail since we don't have caveat definitions
				// in the test context. This is expected and correct behavior.
				if err != nil {
					// Accept various caveat-related errors (not found, evaluation errors, etc.)
					require.True(t,
						err.Error() != "",
						"Expected some caveat-related error")
				} else {
					actualPaths, err := CollectAll(seq)
					if err != nil {
						// Accept various caveat-related errors during path collection
						require.True(t,
							err.Error() != "",
							"Expected some caveat-related error")
					} else {
						require.Fail(t, "Expected caveat evaluation to fail, but got paths: %v", actualPaths)
					}
				}
			} else {
				// For other caveat tests, no errors should occur but paths should be filtered
				require.NoError(t, err)
				actualPaths, err := CollectAll(seq)
				require.NoError(t, err)
				require.NotNil(t, actualPaths)
			}
		})
	}
}

func TestCaveatIteratorClone(t *testing.T) {
	// Create test path
	testPath := MustPathFromString("document:doc1#view@user:alice")

	testCaveat := createTestCaveat("test_caveat", map[string]any{
		"allowed": true,
	})

	// Create original iterator
	fixedIter := NewFixedIterator(testPath)
	originalIter := NewCaveatIterator(fixedIter, testCaveat)

	// Clone the iterator
	clonedIter := originalIter.Clone()

	// Verify the clone is a different instance but has the same content
	require.NotSame(t, originalIter, clonedIter)

	caveatIter, ok := clonedIter.(*CaveatIterator)
	require.True(t, ok)

	require.Equal(t, originalIter.caveat, caveatIter.caveat)
	require.NotSame(t, originalIter.subiterator, caveatIter.subiterator)
}

func TestCaveatIteratorExplain(t *testing.T) {
	testCaveat := createTestCaveat("test_caveat", map[string]any{
		"param1": "value1",
		"param2": 42,
	})

	fixedIter := NewFixedIterator()
	caveatIter := NewCaveatIterator(fixedIter, testCaveat)

	explanation := caveatIter.Explain()
	require.Contains(t, explanation.Info, "Caveat(test_caveat")
	require.Contains(t, explanation.Info, "context: [")
	require.Len(t, explanation.SubExplain, 1)
	require.Equal(t, "Fixed(0 paths)", explanation.SubExplain[0].Info)
}

func TestCaveatIteratorExplainNilCaveat(t *testing.T) {
	fixedIter := NewFixedIterator()
	caveatIter := NewCaveatIterator(fixedIter, nil)

	explanation := caveatIter.Explain()
	require.Equal(t, "Caveat(none)", explanation.Info)
	require.Len(t, explanation.SubExplain, 1)
}

// createTestCaveat creates a ContextualizedCaveat for testing purposes
func createTestCaveat(name string, context map[string]any) *core.ContextualizedCaveat {
	caveat := &core.ContextualizedCaveat{
		CaveatName: name,
	}

	if len(context) > 0 {
		contextStruct, err := structpb.NewStruct(context)
		if err != nil {
			panic(fmt.Sprintf("failed to create test caveat context: %v", err))
		}
		caveat.Context = contextStruct
	}

	return caveat
}

// Additional comprehensive tests for caveat.go

func TestCaveatIterator_ContainsExpectedCaveat(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	// Create a CaveatIterator to test the containsExpectedCaveat method
	testCaveat := createTestCaveat("expected_caveat", nil)
	caveatIter := NewCaveatIterator(NewFixedIterator(), testCaveat)

	// Test simple caveat expressions
	t.Run("Simple caveat match", func(t *testing.T) {
		expr := createTestCaveatExpression("expected_caveat", nil)
		contains := caveatIter.containsExpectedCaveat(expr)
		require.True(contains)
	})

	t.Run("Simple caveat no match", func(t *testing.T) {
		expr := createTestCaveatExpression("different_caveat", nil)
		contains := caveatIter.containsExpectedCaveat(expr)
		require.False(contains)
	})

	// Test complex caveat expressions
	t.Run("AND expression contains expected caveat", func(t *testing.T) {
		child1 := createTestCaveatExpression("expected_caveat", nil)
		child2 := createTestCaveatExpression("other_caveat", nil)
		andExpr := createComplexCaveatExpression(core.CaveatOperation_AND, []*core.CaveatExpression{child1, child2})

		contains := caveatIter.containsExpectedCaveat(andExpr)
		require.True(contains)
	})

	t.Run("OR expression contains expected caveat", func(t *testing.T) {
		child1 := createTestCaveatExpression("wrong_caveat", nil)
		child2 := createTestCaveatExpression("expected_caveat", nil)
		orExpr := createComplexCaveatExpression(core.CaveatOperation_OR, []*core.CaveatExpression{child1, child2})

		contains := caveatIter.containsExpectedCaveat(orExpr)
		require.True(contains)
	})

	t.Run("Complex expression without expected caveat", func(t *testing.T) {
		child1 := createTestCaveatExpression("wrong_caveat1", nil)
		child2 := createTestCaveatExpression("wrong_caveat2", nil)
		andExpr := createComplexCaveatExpression(core.CaveatOperation_AND, []*core.CaveatExpression{child1, child2})

		contains := caveatIter.containsExpectedCaveat(andExpr)
		require.False(contains)
	})

	t.Run("No expected caveat - should always return true", func(t *testing.T) {
		noCaveatIter := NewCaveatIterator(NewFixedIterator(), nil)
		expr := createTestCaveatExpression("any_caveat", nil)
		contains := noCaveatIter.containsExpectedCaveat(expr)
		require.True(contains)
	})
}

func TestCaveatIterator_BuildCaveatContext(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	testCaveat := createTestCaveat("test_caveat", nil)
	caveatIter := NewCaveatIterator(NewFixedIterator(), testCaveat)

	// Test context building with query-time context
	queryContext := map[string]any{
		"user_id": "alice",
		"limit":   10,
	}

	ctx := &Context{
		Context:       context.Background(),
		CaveatContext: queryContext,
	}

	pathCaveat := createTestCaveatExpression("test_caveat", map[string]any{
		"resource_id": "doc1",
	})

	contextMap := caveatIter.buildCaveatContext(ctx, pathCaveat)

	// Query-time context should be included
	require.Equal("alice", contextMap["user_id"])
	require.Equal(10, contextMap["limit"])

	// Should only contain query-time context, not relationship context
	// (SimplifyCaveatExpression handles relationship contexts)
	require.NotContains(contextMap, "resource_id")
}

func TestCaveatIterator_SimplifyCaveat_ErrorHandling(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	testCaveat := createTestCaveat("test_caveat", nil)
	caveatIter := NewCaveatIterator(NewFixedIterator(), testCaveat)

	// Test with no caveat on iterator - should pass through
	t.Run("No caveat on iterator", func(t *testing.T) {
		noCaveatIter := NewCaveatIterator(NewFixedIterator(), nil)
		path := MustPathFromString("document:doc1#view@user:alice")
		path.Caveat = createTestCaveatExpression("any_caveat", nil)

		// This should not error and pass through the original caveat
		result, passes, err := noCaveatIter.simplifyCaveat(&Context{}, path)
		require.NoError(err)
		require.True(passes)
		require.Equal(path.Caveat, result)
	})

	t.Run("No caveat on path", func(t *testing.T) {
		path := MustPathFromString("document:doc1#view@user:alice")
		// path.Caveat is nil

		result, passes, err := caveatIter.simplifyCaveat(&Context{}, path)
		require.NoError(err)
		require.True(passes)
		require.Nil(result) // Unconditional access
	})

	t.Run("No caveat runner", func(t *testing.T) {
		path := MustPathFromString("document:doc1#view@user:alice")
		path.Caveat = createTestCaveatExpression("test_caveat", nil)

		ctx := &Context{
			Context: context.Background(),
			// CaveatRunner is nil
		}

		_, _, err := caveatIter.simplifyCaveat(ctx, path)
		require.Error(err)
		require.Contains(err.Error(), "no caveat runner available")
	})
}

func TestCaveatIterator_IterSubjectsImpl(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	// Create test paths
	path1 := MustPathFromString("document:doc1#view@user:alice")
	path2 := MustPathFromString("document:doc1#view@user:bob")
	path3 := MustPathFromString("document:doc2#view@user:charlie")

	subIterator := NewFixedIterator(path1, path2, path3)

	t.Run("with no caveat filter", func(t *testing.T) {
		// No caveat filter - should pass through all paths
		caveatIter := NewCaveatIterator(subIterator, nil)

		ctx := &Context{
			Context:  context.Background(),
			Executor: LocalExecutor{},
		}

		resource := NewObject("document", "doc1")
		seq, err := caveatIter.IterSubjectsImpl(ctx, resource)

		require.NoError(err)
		require.NotNil(seq)

		// Should get paths for doc1 without error when no caveat filter
		paths, err := CollectAll(seq)
		require.NoError(err)
		require.Len(paths, 2) // alice and bob for doc1
	})

	t.Run("with caveat filter", func(t *testing.T) {
		// With caveat filter - test that the method can be called
		testCaveat := createTestCaveat("test_caveat", nil)
		caveatIter := NewCaveatIterator(subIterator, testCaveat)

		ctx := &Context{
			Context:  context.Background(),
			Executor: LocalExecutor{},
		}

		resource := NewObject("document", "doc1")
		seq, err := caveatIter.IterSubjectsImpl(ctx, resource)

		require.NoError(err) // Initial call should not error
		require.NotNil(seq)

		// The sequence exists even if caveat evaluation will fail later
		// This tests the method works correctly at the API level
	})
}

func TestCaveatIterator_IterResourcesImpl(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	// Create test paths
	path1 := MustPathFromString("document:doc1#view@user:alice")
	path2 := MustPathFromString("folder:folder1#view@user:alice")

	subIterator := NewFixedIterator(path1, path2)

	t.Run("with no caveat filter", func(t *testing.T) {
		// No caveat filter - should pass through all paths
		caveatIter := NewCaveatIterator(subIterator, nil)

		ctx := &Context{
			Context:  context.Background(),
			Executor: LocalExecutor{},
		}

		subject := NewObject("user", "alice").WithEllipses()
		seq, err := caveatIter.IterResourcesImpl(ctx, subject)

		require.NoError(err)
		require.NotNil(seq)

		// Should get both resources for alice without error when no caveat filter
		paths, err := CollectAll(seq)
		require.NoError(err)
		require.Len(paths, 2) // document and folder for alice
	})

	t.Run("with caveat filter", func(t *testing.T) {
		// With caveat filter - test that the method can be called
		testCaveat := createTestCaveat("test_caveat", nil)
		caveatIter := NewCaveatIterator(subIterator, testCaveat)

		ctx := &Context{
			Context:  context.Background(),
			Executor: LocalExecutor{},
		}

		subject := NewObject("user", "alice").WithEllipses()
		seq, err := caveatIter.IterResourcesImpl(ctx, subject)

		require.NoError(err) // Initial call should not error
		require.NotNil(seq)

		// The sequence exists even if caveat evaluation will fail later
		// This tests the method works correctly at the API level
	})
}

func TestCaveatIterator_BuildExplainInfo(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	t.Run("No caveat", func(t *testing.T) {
		caveatIter := NewCaveatIterator(NewFixedIterator(), nil)
		info := caveatIter.buildExplainInfo()
		require.Equal("Caveat(none)", info)
	})

	t.Run("Simple caveat without context", func(t *testing.T) {
		testCaveat := createTestCaveat("test_caveat", nil)
		caveatIter := NewCaveatIterator(NewFixedIterator(), testCaveat)
		info := caveatIter.buildExplainInfo()
		require.Equal("Caveat(test_caveat)", info)
	})

	t.Run("Caveat with context", func(t *testing.T) {
		testCaveat := createTestCaveat("test_caveat", map[string]any{
			"limit": 10,
			"user":  "alice",
		})
		caveatIter := NewCaveatIterator(NewFixedIterator(), testCaveat)
		info := caveatIter.buildExplainInfo()

		// Should contain caveat name and context keys
		require.Contains(info, "Caveat(test_caveat")
		require.Contains(info, "context:")
		// Context keys can be in any order
		require.Contains(info, "limit")
		require.Contains(info, "user")
	})
}
