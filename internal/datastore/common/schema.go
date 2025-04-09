package common

import (
	sq "github.com/Masterminds/squirrel"

	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/datastore/queryshape"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

const (
	relationshipStandardColumnCount   = 8 // ColNamespace, ColObjectID, ColRelation, ColUsersetNamespace, ColUsersetObjectID, ColUsersetRelation, ResourceObjectData, SubjectObjectData
	relationshipCaveatColumnCount     = 2 // ColCaveatName, ColCaveatContext
	relationshipExpirationColumnCount = 1 // ColExpiration
	relationshipIntegrityColumnCount  = 3 // ColIntegrityKeyID, ColIntegrityHash, ColIntegrityTimestamp
)

// SchemaInformation holds the schema information from the SQL datastore implementation.
//
//go:generate go run github.com/ecordell/optgen -output zz_generated.schema_options.go . SchemaInformation
type SchemaInformation struct {
	RelationshipTableName string `debugmap:"visible"`

	ColNamespace        string `debugmap:"visible"`
	ColObjectID         string `debugmap:"visible"`
	ColRelation         string `debugmap:"visible"`
	ColUsersetNamespace string `debugmap:"visible"`
	ColUsersetObjectID  string `debugmap:"visible"`
	ColUsersetRelation  string `debugmap:"visible"`

	ColCaveatName    string `debugmap:"visible"`
	ColCaveatContext string `debugmap:"visible"`

	ColExpiration string `debugmap:"visible"`

	ColIntegrityKeyID     string `debugmap:"visible"`
	ColIntegrityHash      string `debugmap:"visible"`
	ColIntegrityTimestamp string `debugmap:"visible"`

	// Indexes are the indexes to use for this schema.
	Indexes []IndexDefinition `debugmap:"visible"`

	// PaginationFilterType is the type of pagination filter to use for this schema.
	PaginationFilterType PaginationFilterType `debugmap:"visible"`

	// PlaceholderFormat is the format of placeholders to use for this schema.
	PlaceholderFormat sq.PlaceholderFormat `debugmap:"visible"`

	// NowFunction is the function to use to get the current time in the datastore.
	NowFunction string `debugmap:"visible"`

	// ColumnOptimization is the optimization to use for columns in the schema, if any.
	ColumnOptimization ColumnOptimizationOption `debugmap:"visible"`

	// IntegrityEnabled is a flag to indicate if the schema has integrity columns.
	IntegrityEnabled bool `debugmap:"visible"`

	// ExpirationDisabled is a flag to indicate whether expiration support is disabled.
	ExpirationDisabled bool `debugmap:"visible"`
}

// expectedIndexesForShape returns the expected index names for a given query shape.
func (si SchemaInformation) expectedIndexesForShape(shape queryshape.Shape) options.SQLIndexInformation {
	expectedIndexes := options.SQLIndexInformation{}
	for _, index := range si.Indexes {
		if index.matchesShape(shape) {
			expectedIndexes.ExpectedIndexNames = append(expectedIndexes.ExpectedIndexNames, index.Name)
		}
	}
	return expectedIndexes
}

func (si SchemaInformation) debugValidate() {
	spiceerrors.DebugAssert(func() bool {
		si.mustValidate()
		return true
	}, "SchemaInformation failed to validate")
}

func (si SchemaInformation) mustValidate() {
	if si.RelationshipTableName == "" {
		panic("RelationshipTableName is required")
	}

	if si.ColNamespace == "" {
		panic("ColNamespace is required")
	}

	if si.ColObjectID == "" {
		panic("ColObjectID is required")
	}

	if si.ColRelation == "" {
		panic("ColRelation is required")
	}

	if si.ColUsersetNamespace == "" {
		panic("ColUsersetNamespace is required")
	}

	if si.ColUsersetObjectID == "" {
		panic("ColUsersetObjectID is required")
	}

	if si.ColUsersetRelation == "" {
		panic("ColUsersetRelation is required")
	}

	if si.ColCaveatName == "" {
		panic("ColCaveatName is required")
	}

	if si.ColCaveatContext == "" {
		panic("ColCaveatContext is required")
	}

	if si.ColExpiration == "" {
		panic("ColExpiration is required")
	}

	if si.IntegrityEnabled {
		if si.ColIntegrityKeyID == "" {
			panic("ColIntegrityKeyID is required")
		}

		if si.ColIntegrityHash == "" {
			panic("ColIntegrityHash is required")
		}

		if si.ColIntegrityTimestamp == "" {
			panic("ColIntegrityTimestamp is required")
		}
	}

	if si.NowFunction == "" {
		panic("NowFunction is required")
	}

	if si.ColumnOptimization == ColumnOptimizationOptionUnknown {
		panic("ColumnOptimization is required")
	}

	if si.PaginationFilterType == PaginationFilterTypeUnknown {
		panic("PaginationFilterType is required")
	}

	if si.PlaceholderFormat == nil {
		panic("PlaceholderFormat is required")
	}
}
