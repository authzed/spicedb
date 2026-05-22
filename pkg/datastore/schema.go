package datastore

import (
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

// TypeDefinition is a type alias for a pointer to a namespace definition.
type TypeDefinition = *core.NamespaceDefinition

// CaveatDefinition is a type alias for a pointer to a caveat definition.
type CaveatDefinition = *core.CaveatDefinition

// RevisionedTypeDefinition is a revisioned version of a type definition.
type RevisionedTypeDefinition = RevisionedDefinition[*core.NamespaceDefinition]
