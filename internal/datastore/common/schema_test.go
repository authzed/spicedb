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
