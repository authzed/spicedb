package schemacaching

import "github.com/authzed/spicedb/pkg/schemadsl/compiler"

// *DefinitionSizeVTMultiplier are the mulitipliers to be used for
// estimating the in-memory cost of a SchemaDefinition based on its
// on-wire size, as returned by SizeVT. This was determined by testing
// all existing definitions found in consistency tests and is
// enforced via the estimatedsize_test.
//
// NOTE: the namespace multiplier was raised from 10 to 12 when decorators were
// added. A nil `Decorators` slice costs 24 bytes in memory on NamespaceDefinition,
// Relation and AllowedRelation, but contributes nothing to SizeVT, so the in-memory
// cost of a definition grew while its on-wire size did not. Any future field added
// to those messages has the same effect and may require raising this again.
const (
	namespaceDefinitionSizeVTMultiplier = 12
	namespaceDefinitionMinimumSize      = 150

	caveatDefinitionSizeVTMultiplier = 10
	caveatDefinitionMinimumSize      = 150
)

type schemaDefinition interface {
	compiler.SchemaDefinition
	SizeVT() int
}
