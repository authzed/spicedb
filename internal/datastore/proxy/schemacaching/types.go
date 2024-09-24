package schemacaching

import "github.com/authzed/spicedb/pkg/schemadsl/compiler"

// *DefinitionSizeVTMultiplier are the mulitipliers to be used for
// estimating the in-memory cost of a SchemaDefinition based on its
// on-wire size, as returned by SizeVT. This was determined by testing
// all existing definitions found in consistency tests and is
// enforced via the estimatedsize_test.
const (
	namespaceDefinitionSizeVTMultiplier = 10
	namespaceDefinitionMinimumSize      = 150

	caveatDefinitionSizeVTMultiplier = 10
	caveatDefinitionMinimumSize      = 150
)

type schemaDefinition interface {
	compiler.SchemaDefinition
	SizeVT() int
}
