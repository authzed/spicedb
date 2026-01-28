package query

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRecursiveSentinel_Types(t *testing.T) {
	t.Parallel()

	t.Run("ResourceType", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		sentinel := NewRecursiveSentinel("folder", "parent", false)

		resourceType, err := sentinel.ResourceType()
		require.NoError(err)
		require.Equal("folder", resourceType.Type)
		require.Equal("parent", resourceType.Subrelation)
	})

	t.Run("SubjectTypes_WithoutSubRelations", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		sentinel := NewRecursiveSentinel("folder", "parent", false)

		subjectTypes, err := sentinel.SubjectTypes()
		require.NoError(err)
		require.Len(subjectTypes, 1)
		require.Equal("folder", subjectTypes[0].Type)
		require.Equal("parent", subjectTypes[0].Subrelation)
	})

	t.Run("SubjectTypes_WithSubRelations", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		sentinel := NewRecursiveSentinel("folder", "parent", true)

		subjectTypes, err := sentinel.SubjectTypes()
		require.NoError(err)
		require.Len(subjectTypes, 1)
		require.Equal("folder", subjectTypes[0].Type)
		require.Empty(subjectTypes[0].Subrelation) // Unknown during construction
	})
}
