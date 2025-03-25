package common

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/datastore/queryshape"
)

func TestExpectedIndexesForShape(t *testing.T) {
	schema := SchemaInformation{
		Indexes: []IndexDefinition{
			{
				Name: "idx1",
				Shapes: []queryshape.Shape{
					queryshape.CheckPermissionSelectDirectSubjects,
				},
			},
			{
				Name: "idx2",
				Shapes: []queryshape.Shape{
					queryshape.CheckPermissionSelectDirectSubjects,
					queryshape.CheckPermissionSelectIndirectSubjects,
				},
			},
		},
	}

	expectedIndexes := schema.expectedIndexesForShape(queryshape.Unspecified)
	require.Empty(t, expectedIndexes)

	expectedIndexes = schema.expectedIndexesForShape(queryshape.CheckPermissionSelectDirectSubjects)
	require.Equal(t, []string{"idx1", "idx2"}, expectedIndexes.ExpectedIndexNames)

	expectedIndexes = schema.expectedIndexesForShape(queryshape.CheckPermissionSelectIndirectSubjects)
	require.Equal(t, []string{"idx2"}, expectedIndexes.ExpectedIndexNames)
}
