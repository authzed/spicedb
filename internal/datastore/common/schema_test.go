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
			{
				// A maintenance index serving no query shape (e.g. GC).
				Name: "idx_maintenance",
			},
		},
	}

	// All indexes serving a shape are reported as shape-serving, regardless of the requested
	// shape; the maintenance index is never reported.
	allShapeServing := []string{"idx1", "idx2"}

	expectedIndexes := schema.expectedIndexesForShape(queryshape.Unspecified)
	require.Empty(t, expectedIndexes.ExpectedIndexNames)
	require.Equal(t, expectedIndexes.ShapeServingIndexNames, allShapeServing)

	expectedIndexes = schema.expectedIndexesForShape(queryshape.CheckPermissionSelectDirectSubjects)
	require.Equal(t, []string{"idx1", "idx2"}, expectedIndexes.ExpectedIndexNames)
	require.Equal(t, expectedIndexes.ShapeServingIndexNames, allShapeServing)

	expectedIndexes = schema.expectedIndexesForShape(queryshape.CheckPermissionSelectIndirectSubjects)
	require.Equal(t, []string{"idx2"}, expectedIndexes.ExpectedIndexNames)
	require.Equal(t, expectedIndexes.ShapeServingIndexNames, allShapeServing)
}

func TestSortByResourceColumnOrderColumns(t *testing.T) {
	schema := SchemaInformation{
		SortByResourceColumnOrder: []string{"custom1", "custom2"},
	}

	expectedColumns := schema.sortByResourceColumnOrderColumns()
	require.Equal(t, []string{"custom1", "custom2"}, expectedColumns)

	schema.SortByResourceColumnOrder = nil
	expectedColumns = schema.sortByResourceColumnOrderColumns()
	require.Equal(t, []string{
		schema.ColNamespace,
		schema.ColObjectID,
		schema.ColRelation,
		schema.ColUsersetNamespace,
		schema.ColUsersetObjectID,
		schema.ColUsersetRelation,
	}, expectedColumns)
}

func TestSortBySubjectColumnOrderColumns(t *testing.T) {
	schema := SchemaInformation{
		SortBySubjectColumnOrder: []string{"custom1", "custom2"},
	}

	expectedColumns := schema.sortBySubjectColumnOrderColumns()
	require.Equal(t, []string{"custom1", "custom2"}, expectedColumns)

	schema.SortBySubjectColumnOrder = nil
	expectedColumns = schema.sortBySubjectColumnOrderColumns()
	require.Equal(t, []string{
		schema.ColUsersetNamespace,
		schema.ColUsersetObjectID,
		schema.ColUsersetRelation,
		schema.ColNamespace,
		schema.ColObjectID,
		schema.ColRelation,
	}, expectedColumns)
}
