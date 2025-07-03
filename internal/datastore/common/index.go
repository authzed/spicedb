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

	// IsDeprecated is true if this index is deprecated and should not be used.
	IsDeprecated bool
}

// matchesShape returns true if the index matches the given shape.
func (id IndexDefinition) matchesShape(shape queryshape.Shape) bool {
	for _, s := range id.Shapes {
		if s == shape {
			return true
		}
	}
	return false
}
