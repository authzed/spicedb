package datastore

import (
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

// RevisionedTypeDefinition is a revisioned version of a type definition.
type RevisionedTypeDefinition = RevisionedDefinition[*core.NamespaceDefinition]
