package query

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFilterResourcesByType(t *testing.T) {
	t.Parallel()

	// Helper to create a PathSeq from paths
	createSeq := func(paths []Path) PathSeq {
		return func(yield func(Path, error) bool) {
			for _, path := range paths {
				if !yield(path, nil) {
					return
				}
			}
		}
	}

	t.Run("EmptyFilter_ReturnsAllPaths", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		// Create test paths with different resource types
		paths := []Path{
			MustPathFromString("document:doc1#viewer@user:alice"),
			MustPathFromString("folder:folder1#viewer@user:bob"),
			MustPathFromString("document:doc2#editor@user:carol"),
		}

		seq := createSeq(paths)

		// Filter with empty ObjectType (no filtering)
		filtered := FilterResourcesByType(seq, NoObjectFilter())

		results, err := CollectAll(filtered)
		require.NoError(err)
		require.Len(results, 3, "Empty filter should return all paths")
	})

	t.Run("FilterByType_OnlyReturnsMatchingResources", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		paths := []Path{
			MustPathFromString("document:doc1#viewer@user:alice"),
			MustPathFromString("folder:folder1#viewer@user:bob"),
			MustPathFromString("document:doc2#editor@user:carol"),
		}

		seq := createSeq(paths)

		// Filter for only document resources
		filter := ObjectType{Type: "document"}
		filtered := FilterResourcesByType(seq, filter)

		results, err := CollectAll(filtered)
		require.NoError(err)
		require.Len(results, 2, "Should only return document resources")

		for _, path := range results {
			require.Equal("document", path.Resource.ObjectType)
		}
	})

	t.Run("FilterByTypeAndSubrelation_OnlyReturnsMatchingResources", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		paths := []Path{
			MustPathFromString("document:doc1#viewer@user:alice"),
			MustPathFromString("document:doc2#editor@user:bob"),
			MustPathFromString("document:doc3#viewer@user:carol"),
		}

		seq := createSeq(paths)

		// Filter for document resources with viewer relation
		filter := ObjectType{Type: "document", Subrelation: "viewer"}
		filtered := FilterResourcesByType(seq, filter)

		results, err := CollectAll(filtered)
		require.NoError(err)
		require.Len(results, 2, "Should only return document#viewer resources")

		for _, path := range results {
			require.Equal("document", path.Resource.ObjectType)
			require.Equal("viewer", path.Relation)
		}
	})

	t.Run("NoMatchingResources_ReturnsEmpty", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		paths := []Path{
			MustPathFromString("document:doc1#viewer@user:alice"),
			MustPathFromString("document:doc2#editor@user:bob"),
		}

		seq := createSeq(paths)

		// Filter for resources that don't exist
		filter := ObjectType{Type: "folder"}
		filtered := FilterResourcesByType(seq, filter)

		results, err := CollectAll(filtered)
		require.NoError(err)
		require.Empty(results, "Should return empty when no resources match")
	})

	t.Run("ErrorPropagation", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		// Create a sequence that yields an error
		testErr := errors.New("test error")
		errorSeq := func(yield func(Path, error) bool) {
			yield(Path{}, testErr)
		}

		// Filter should propagate the error
		filtered := FilterResourcesByType(errorSeq, ObjectType{Type: "document"})

		results, err := CollectAll(filtered)
		require.Error(err)
		require.ErrorIs(err, testErr)
		require.Empty(results)
	})
}

func TestFilterSubjectsByType(t *testing.T) {
	t.Parallel()

	// Helper to create a PathSeq from paths
	createSeq := func(paths []Path) PathSeq {
		return func(yield func(Path, error) bool) {
			for _, path := range paths {
				if !yield(path, nil) {
					return
				}
			}
		}
	}

	t.Run("EmptyFilter_ReturnsAllPaths", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		paths := []Path{
			MustPathFromString("document:doc1#viewer@user:alice"),
			MustPathFromString("document:doc2#viewer@group:group1"),
			MustPathFromString("document:doc3#editor@user:bob"),
		}

		seq := createSeq(paths)

		// Filter with empty ObjectType (no filtering)
		filtered := FilterSubjectsByType(seq, NoObjectFilter())

		results, err := CollectAll(filtered)
		require.NoError(err)
		require.Len(results, 3, "Empty filter should return all paths")
	})

	t.Run("FilterByType_OnlyReturnsMatchingSubjects", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		paths := []Path{
			MustPathFromString("document:doc1#viewer@user:alice"),
			MustPathFromString("document:doc2#viewer@group:group1"),
			MustPathFromString("document:doc3#editor@user:bob"),
		}

		seq := createSeq(paths)

		// Filter for only user subjects
		filter := ObjectType{Type: "user"}
		filtered := FilterSubjectsByType(seq, filter)

		results, err := CollectAll(filtered)
		require.NoError(err)
		require.Len(results, 2, "Should only return user subjects")

		for _, path := range results {
			require.Equal("user", path.Subject.ObjectType)
		}
	})

	t.Run("FilterByTypeAndSubrelation_OnlyReturnsMatchingSubjects", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		paths := []Path{
			MustPathFromString("document:doc1#viewer@group:group1#member"),
			MustPathFromString("document:doc2#viewer@group:group2#..."),
			MustPathFromString("document:doc3#viewer@group:group3#member"),
		}

		seq := createSeq(paths)

		// Filter for group subjects with member subrelation
		filter := ObjectType{Type: "group", Subrelation: "member"}
		filtered := FilterSubjectsByType(seq, filter)

		results, err := CollectAll(filtered)
		require.NoError(err)
		require.Len(results, 2, "Should only return group#member subjects")

		for _, path := range results {
			require.Equal("group", path.Subject.ObjectType)
			require.Equal("member", path.Subject.Relation)
		}
	})

	t.Run("NoMatchingSubjects_ReturnsEmpty", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		paths := []Path{
			MustPathFromString("document:doc1#viewer@user:alice"),
			MustPathFromString("document:doc2#editor@user:bob"),
		}

		seq := createSeq(paths)

		// Filter for subjects that don't exist
		filter := ObjectType{Type: "group"}
		filtered := FilterSubjectsByType(seq, filter)

		results, err := CollectAll(filtered)
		require.NoError(err)
		require.Empty(results, "Should return empty when no subjects match")
	})

	t.Run("ErrorPropagation", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		// Create a sequence that yields an error
		testErr := errors.New("test error")
		errorSeq := func(yield func(Path, error) bool) {
			yield(Path{}, testErr)
		}

		// Filter should propagate the error
		filtered := FilterSubjectsByType(errorSeq, ObjectType{Type: "user"})

		results, err := CollectAll(filtered)
		require.Error(err)
		require.ErrorIs(err, testErr)
		require.Empty(results)
	})
}

func TestNoObjectFilter(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	filter := NoObjectFilter()
	require.Empty(filter.Type, "NoObjectFilter should return empty Type")
	require.Empty(filter.Subrelation, "NoObjectFilter should return empty Subrelation")
}
