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
	// ReadCaveatByName returns a caveat with the provided name.
	// It returns an instance of CaveatNotFoundError if not found.
	ReadCaveatByName(ctx context.Context, name string) (caveat *core.CaveatDefinition, lastWritten Revision, err error)

	// ListAllCaveats returns all caveats stored in the system.
	ListAllCaveats(ctx context.Context) ([]RevisionedCaveat, error)

	// LookupCaveatsWithNames finds all caveats with the matching names.
	LookupCaveatsWithNames(ctx context.Context, names []string) ([]RevisionedCaveat, error)

	// ReadNamespaceByName reads a namespace definition and the revision at which it was created or
	// last written. It returns an instance of NamespaceNotFoundError if not found.
	ReadNamespaceByName(ctx context.Context, nsName string) (ns *core.NamespaceDefinition, lastWritten Revision, err error)

	// ListAllNamespaces lists all namespaces defined.
	ListAllNamespaces(ctx context.Context) ([]RevisionedNamespace, error)

	// LookupNamespacesWithNames finds all namespaces with the matching names.
	LookupNamespacesWithNames(ctx context.Context, nsNames []string) ([]RevisionedNamespace, error)
}

type LegacySchemaWriter interface {
	LegacySchemaReader

	// WriteCaveats stores the provided caveats, and returns the assigned IDs
	// Each element of the returning slice corresponds by position to the input slice
	WriteCaveats(context.Context, []*core.CaveatDefinition) error

	// DeleteCaveats deletes the provided caveats by name
	DeleteCaveats(ctx context.Context, names []string) error

	// WriteNamespaces takes proto namespace definitions and persists them.
	WriteNamespaces(ctx context.Context, newConfigs ...*core.NamespaceDefinition) error

	// DeleteNamespaces deletes namespaces.
	DeleteNamespaces(ctx context.Context, nsNames []string, delOption DeleteNamespacesRelationshipsOption) error
}
