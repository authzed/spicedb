package datastore

import (
	"context"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

// DeleteNamespacesRelationshipsOption is an option for deleting namespaces and their relationships.
type DeleteNamespacesRelationshipsOption int

const (
	// DeleteNamespacesOnly indicates that only namespaces should be deleted.
	// It is therefore the caller's responsibility to delete any relationships in those namespaces.
	DeleteNamespacesOnly DeleteNamespacesRelationshipsOption = iota

	// DeleteNamespacesAndRelationships indicates that namespaces and all relationships
	// in those namespaces should be deleted.
	DeleteNamespacesAndRelationships
)

// RevisionedCaveat is a revisioned version of a caveat definition.
type RevisionedCaveat = RevisionedDefinition[*core.CaveatDefinition]

type LegacySchemaReader interface {
	// LegacyReadCaveatByName returns a caveat with the provided name.
	// It returns an instance of CaveatNotFoundError if not found.
	LegacyReadCaveatByName(ctx context.Context, name string) (caveat *core.CaveatDefinition, lastWritten Revision, err error)

	// LegacyListAllCaveats returns all caveats stored in the system.
	LegacyListAllCaveats(ctx context.Context) ([]RevisionedCaveat, error)

	// LegacyLookupCaveatsWithNames finds all caveats with the matching names.
	LegacyLookupCaveatsWithNames(ctx context.Context, names []string) ([]RevisionedCaveat, error)

	// LegacyReadNamespaceByName reads a namespace definition and the revision at which it was created or
	// last written. It returns an instance of NamespaceNotFoundError if not found.
	LegacyReadNamespaceByName(ctx context.Context, nsName string) (ns *core.NamespaceDefinition, lastWritten Revision, err error)

	// LegacyListAllNamespaces lists all namespaces defined.
	LegacyListAllNamespaces(ctx context.Context) ([]RevisionedNamespace, error)

	// LegacyLookupNamespacesWithNames finds all namespaces with the matching names.
	LegacyLookupNamespacesWithNames(ctx context.Context, nsNames []string) ([]RevisionedNamespace, error)
}

type LegacySchemaWriter interface {
	LegacySchemaReader

	// LegacyWriteCaveats stores the provided caveats, and returns the assigned IDs
	// Each element of the returning slice corresponds by position to the input slice
	LegacyWriteCaveats(context.Context, []*core.CaveatDefinition) error

	// LegacyDeleteCaveats deletes the provided caveats by name
	LegacyDeleteCaveats(ctx context.Context, names []string) error

	// LegacyWriteNamespaces takes proto namespace definitions and persists them.
	LegacyWriteNamespaces(ctx context.Context, newConfigs ...*core.NamespaceDefinition) error

	// LegacyDeleteNamespaces deletes namespaces.
	LegacyDeleteNamespaces(ctx context.Context, nsNames []string, delOption DeleteNamespacesRelationshipsOption) error
}
