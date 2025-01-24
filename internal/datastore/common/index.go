package common

import "github.com/authzed/spicedb/pkg/datastore/queryshape"

// IndexDefinition is a definition of an index for a datastore.
type IndexDefinition struct {
	// Name is the unique name for the index.
	Name string

	// ColumnsSQL is the SQL fragment of the columns over which this index will apply.
	ColumnsSQL string

	// Shapes are those query shapes for which this index should be used.
	Shapes []queryshape.Shape

	// OnlyShapes are those query shapes for which this index should be used as part of
	// an index-only lookup.
	OnlyShapes []queryshape.Shape
}
