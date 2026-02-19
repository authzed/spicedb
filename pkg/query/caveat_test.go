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
	"github.com/authzed/spicedb/pkg/datalayer"
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

func TestCaveatIteratorNoCaveat(t *testing.T) {
	// Test that when no caveat is set on the iterator, all paths are allowed through
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
		paths         []Path
		caveatContext map[string]any
		expectedPaths []Path
	}{
		{
			name: "allows all paths through",
			paths: []Path{
				pathWithCaveat,
				pathWithoutCaveat,
				pathWithDifferentCaveat,
			},
			caveatContext: nil,
			expectedPaths: []Path{
				pathWithCaveat,
				pathWithoutCaveat,
				pathWithDifferentCaveat,
			},
		},
		{
			name: "allows paths even with context provided",
			paths: []Path{
				pathWithCaveat,
				pathWithoutCaveat,
			},
			caveatContext: map[string]any{
				"allowed": true,
			},
			expectedPaths: []Path{
				pathWithCaveat,
				pathWithoutCaveat,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			fixedIter := NewFixedIterator(tc.paths...)
			caveatIter := NewCaveatIterator(fixedIter, nil)

			ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
			require.NoError(t, err)

			dl := datalayer.NewDataLayer(ds)
			rev, err := dl.ReadWriteTx(context.Background(), func(ctx context.Context, tx datalayer.ReadWriteTransaction) error {
				return nil
			})
			require.NoError(t, err)

			queryCtx := NewLocalContext(context.Background(),
				WithReader(dl.SnapshotReader(rev)),
				WithCaveatContext(tc.caveatContext),
				WithCaveatRunner(caveats.NewCaveatRunner(types.NewTypeSet())))

			resource := NewObject("document", "doc1")
			seq, err := queryCtx.IterSubjects(caveatIter, resource, NoObjectFilter())

			var expectedMatchingPaths []Path
			for _, path := range tc.expectedPaths {
				if path.Resource.ObjectType == resource.ObjectType && path.Resource.ObjectID == resource.ObjectID {
					expectedMatchingPaths = append(expectedMatchingPaths, path)
				}
			}

			require.NoError(t, err)
			actualPaths, err := CollectAll(seq)
			require.NoError(t, err)
			require.Len(t, actualPaths, len(expectedMatchingPaths), "Expected all matching paths to be returned")
		})
	}
}

func TestCaveatIteratorWithCaveat(t *testing.T) {
	// Test that when a caveat is set, it attempts to filter/evaluate paths
	// These tests expect errors because no actual caveat definitions exist in the test context
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
		paths         []Path
		caveatContext map[string]any
	}{
		{
			name:   "filters by matching caveat name",
			caveat: createTestCaveat("test_caveat", nil),
			paths: []Path{
				pathWithCaveat,          // has test_caveat
				pathWithoutCaveat,       // has no caveat
				pathWithDifferentCaveat, // has other_caveat
			},
			caveatContext: map[string]any{
				"allowed": true,
			},
		},
		{
			name:   "filters out paths without matching caveat",
			caveat: createTestCaveat("test_caveat", nil),
			paths: []Path{
				pathWithCaveat,
				pathWithoutCaveat,
				pathWithDifferentCaveat,
			},
			caveatContext: map[string]any{
				"allowed": true,
			},
		},
		{
			name:   "denies caveated paths without context",
			caveat: createTestCaveat("test_caveat", nil),
			paths: []Path{
				pathWithCaveat,
				pathWithoutCaveat,
			},
			caveatContext: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			fixedIter := NewFixedIterator(tc.paths...)
			caveatIter := NewCaveatIterator(fixedIter, tc.caveat)

			ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
			require.NoError(t, err)

			dl := datalayer.NewDataLayer(ds)
			rev, err := dl.ReadWriteTx(context.Background(), func(ctx context.Context, tx datalayer.ReadWriteTransaction) error {
				return nil
			})
			require.NoError(t, err)

			queryCtx := NewLocalContext(context.Background(),
				WithReader(dl.SnapshotReader(rev)),
				WithCaveatContext(tc.caveatContext),
				WithCaveatRunner(caveats.NewCaveatRunner(types.NewTypeSet())))

			resource := NewObject("document", "doc1")
			seq, err := queryCtx.IterSubjects(caveatIter, resource, NoObjectFilter())

			// These tests expect caveat-related errors because no actual caveat definitions exist
			if err != nil {
				require.NotEmpty(t, err.Error(), "Expected some caveat-related error")
			} else {
				actualPaths, err := CollectAll(seq)
				if err != nil {
					require.NotEmpty(t, err.Error(), "Expected some caveat-related error")
				} else {
					require.Fail(t, "Expected caveat evaluation to fail, but got paths:", actualPaths)
				}
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
		t.Parallel()
		expr := createTestCaveatExpression("expected_caveat", nil)
		contains := caveatIter.containsExpectedCaveat(expr)
		require.True(contains)
	})

	t.Run("Simple caveat no match", func(t *testing.T) {
		t.Parallel()
		expr := createTestCaveatExpression("different_caveat", nil)
		contains := caveatIter.containsExpectedCaveat(expr)
		require.False(contains)
	})

	// Test complex caveat expressions
	t.Run("AND expression contains expected caveat", func(t *testing.T) {
		t.Parallel()
		child1 := createTestCaveatExpression("expected_caveat", nil)
		child2 := createTestCaveatExpression("other_caveat", nil)
		andExpr := createComplexCaveatExpression(core.CaveatOperation_AND, []*core.CaveatExpression{child1, child2})

		contains := caveatIter.containsExpectedCaveat(andExpr)
		require.True(contains)
	})

	t.Run("OR expression contains expected caveat", func(t *testing.T) {
		t.Parallel()
		child1 := createTestCaveatExpression("wrong_caveat", nil)
		child2 := createTestCaveatExpression("expected_caveat", nil)
		orExpr := createComplexCaveatExpression(core.CaveatOperation_OR, []*core.CaveatExpression{child1, child2})

		contains := caveatIter.containsExpectedCaveat(orExpr)
		require.True(contains)
	})

	t.Run("Complex expression without expected caveat", func(t *testing.T) {
		t.Parallel()
		child1 := createTestCaveatExpression("wrong_caveat1", nil)
		child2 := createTestCaveatExpression("wrong_caveat2", nil)
		andExpr := createComplexCaveatExpression(core.CaveatOperation_AND, []*core.CaveatExpression{child1, child2})

		contains := caveatIter.containsExpectedCaveat(andExpr)
		require.False(contains)
	})

	t.Run("No expected caveat - should always return true", func(t *testing.T) {
		t.Parallel()
		noCaveatIter := NewCaveatIterator(NewFixedIterator(), nil)
		expr := createTestCaveatExpression("any_caveat", nil)
		contains := noCaveatIter.containsExpectedCaveat(expr)
		require.True(contains)
	})
}

func TestCaveatIterator_SimplifyCaveat_ErrorHandling(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	testCaveat := createTestCaveat("test_caveat", nil)
	caveatIter := NewCaveatIterator(NewFixedIterator(), testCaveat)

	// Test with no caveat on iterator - should pass through
	t.Run("No caveat on iterator", func(t *testing.T) {
		t.Parallel()
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
		t.Parallel()
		path := MustPathFromString("document:doc1#view@user:alice")
		// path.Caveat is nil

		result, passes, err := caveatIter.simplifyCaveat(&Context{}, path)
		require.NoError(err)
		require.True(passes)
		require.Nil(result) // Unconditional access
	})

	t.Run("No caveat runner", func(t *testing.T) {
		t.Parallel()
		path := MustPathFromString("document:doc1#view@user:alice")
		path.Caveat = createTestCaveatExpression("test_caveat", nil)

		ctx := NewLocalContext(context.Background())
		// CaveatRunner is nil in the returned context

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
		t.Parallel()
		// No caveat filter - should pass through all paths
		caveatIter := NewCaveatIterator(subIterator, nil)

		ctx := NewLocalContext(context.Background())

		resource := NewObject("document", "doc1")
		seq, err := caveatIter.IterSubjectsImpl(ctx, resource, NoObjectFilter())

		require.NoError(err)
		require.NotNil(seq)

		// Should get paths for doc1 without error when no caveat filter
		paths, err := CollectAll(seq)
		require.NoError(err)
		require.Len(paths, 2) // alice and bob for doc1
	})

	t.Run("with caveat filter", func(t *testing.T) {
		t.Parallel()
		// With caveat filter - test that the method can be called
		testCaveat := createTestCaveat("test_caveat", nil)
		caveatIter := NewCaveatIterator(subIterator, testCaveat)

		ctx := NewLocalContext(context.Background())

		resource := NewObject("document", "doc1")
		seq, err := caveatIter.IterSubjectsImpl(ctx, resource, NoObjectFilter())

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
		t.Parallel()
		// No caveat filter - should pass through all paths
		caveatIter := NewCaveatIterator(subIterator, nil)

		ctx := NewLocalContext(context.Background())

		subject := NewObject("user", "alice").WithEllipses()
		seq, err := caveatIter.IterResourcesImpl(ctx, subject, NoObjectFilter())

		require.NoError(err)
		require.NotNil(seq)

		// Should get both resources for alice without error when no caveat filter
		paths, err := CollectAll(seq)
		require.NoError(err)
		require.Len(paths, 2) // document and folder for alice
	})

	t.Run("with caveat filter", func(t *testing.T) {
		t.Parallel()
		// With caveat filter - test that the method can be called
		testCaveat := createTestCaveat("test_caveat", nil)
		caveatIter := NewCaveatIterator(subIterator, testCaveat)

		ctx := NewLocalContext(context.Background())

		subject := NewObject("user", "alice").WithEllipses()
		seq, err := caveatIter.IterResourcesImpl(ctx, subject, NoObjectFilter())

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
		t.Parallel()
		caveatIter := NewCaveatIterator(NewFixedIterator(), nil)
		info := caveatIter.buildExplainInfo()
		require.Equal("Caveat(none)", info)
	})

	t.Run("Simple caveat without context", func(t *testing.T) {
		t.Parallel()
		testCaveat := createTestCaveat("test_caveat", nil)
		caveatIter := NewCaveatIterator(NewFixedIterator(), testCaveat)
		info := caveatIter.buildExplainInfo()
		require.Equal("Caveat(test_caveat)", info)
	})

	t.Run("Caveat with context", func(t *testing.T) {
		t.Parallel()
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

func TestCaveatIterator_Types(t *testing.T) {
	t.Parallel()

	t.Run("ResourceType", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		// Create a caveat iterator with a subiterator
		path := MustPathFromString("document:doc1#viewer@user:alice")
		subIter := NewFixedIterator(path)
		testCaveat := createTestCaveat("test_caveat", nil)
		caveatIter := NewCaveatIterator(subIter, testCaveat)

		resourceType, err := caveatIter.ResourceType()
		require.NoError(err)
		require.Len(resourceType, 1)
		require.Equal("document", resourceType[0].Type) // From subiterator
	})

	t.Run("SubjectTypes", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		// Create a caveat iterator with a subiterator
		path := MustPathFromString("document:doc1#viewer@user:alice")
		subIter := NewFixedIterator(path)
		testCaveat := createTestCaveat("test_caveat", nil)
		caveatIter := NewCaveatIterator(subIter, testCaveat)

		subjectTypes, err := caveatIter.SubjectTypes()
		require.NoError(err)
		require.Len(subjectTypes, 1) // From subiterator
		require.Equal("user", subjectTypes[0].Type)
	})
}
