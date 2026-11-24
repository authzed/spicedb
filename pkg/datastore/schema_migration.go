package datastore

import "context"

// SchemaMigrationBoundary identifies revisions for which unified schema storage
// is authoritative. Earlier revisions can retain a missing or stale unified copy.
type SchemaMigrationBoundary interface {
	ContainsRevision(Revision) bool
}

// SchemaMigrationBoundaryFunc adapts an engine-specific revision predicate.
type SchemaMigrationBoundaryFunc func(Revision) bool

func (f SchemaMigrationBoundaryFunc) ContainsRevision(rev Revision) bool { return f(rev) }

// SchemaMigrationBoundaryReader is implemented by stores with a durable schema
// migration boundary. A nil boundary means no authority marker exists yet.
type SchemaMigrationBoundaryReader interface {
	ReadSchemaMigrationBoundary(context.Context) (SchemaMigrationBoundary, error)
}
