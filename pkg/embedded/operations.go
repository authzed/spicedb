package embedded

import (
	"context"
	"errors"
	"iter"
	"reflect"

	"github.com/authzed/spicedb/internal/relationships"
	"github.com/authzed/spicedb/internal/services/shared"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
	"github.com/authzed/spicedb/pkg/tuple"
)

// ReadSchemaResult contains the schema visible to the reader.
type ReadSchemaResult struct{ SchemaText string }

// WriteSchemaResult contains the committed schema revision.
type WriteSchemaResult struct{ Revision datastore.Revision }

// WriteRelationshipsResult contains the committed revision, or NoRevision for staged writes.
type WriteRelationshipsResult struct{ Revision datastore.Revision }

// HeadRevisionResult contains a fresh committed revision.
type HeadRevisionResult struct{ Revision datastore.Revision }

// CommitResult contains the revision of a successfully committed transaction.
type CommitResult struct{ Revision datastore.Revision }

// ReadRelationshipsResult contains a single-use streaming iterator.
type ReadRelationshipsResult struct{ Relationships RelationshipIterator }

// RelationshipIterator yields relationships or a terminal read error.
type RelationshipIterator iter.Seq2[tuple.Relationship, error]

// RevisionedReader reads schema, relationships, and permissions at a fixed committed revision.
// It holds no database connection and does not prevent history from being garbage collected.
type RevisionedReader struct {
	permissions *Permissions
	revision    datastore.Revision
}

// SnapshotReader constructs a reader without performing I/O. Read methods report invalid revisions.
func (p *Permissions) SnapshotReader(revision datastore.Revision) *RevisionedReader {
	return &RevisionedReader{p, revision}
}

// HeadRevision selects a fresh committed revision for a SnapshotReader.
func (p *Permissions) HeadRevision(ctx context.Context) (HeadRevisionResult, error) {
	rev, _, err := p.dl.HeadRevision(ctx)
	if err != nil {
		return HeadRevisionResult{datastore.NoRevision}, err
	}
	return HeadRevisionResult{rev}, nil
}

func (r *RevisionedReader) validate(ctx context.Context) error {
	if r.revision == nil || r.revision == datastore.NoRevision {
		return errors.New("embedded: a committed revision is required")
	}
	// Revision implementations assume operands from the same backend. Parse only
	// to determine the backend's native type; retain the original full revision.
	native, err := r.permissions.dl.RevisionFromString(r.revision.String())
	if err != nil || reflect.TypeOf(native) != reflect.TypeOf(r.revision) {
		return datastore.NewInvalidRevisionErr(r.revision, datastore.CouldNotDetermineRevision)
	}
	return r.permissions.dl.CheckRevision(ctx, r.revision)
}

func (r *RevisionedReader) reader() datalayer.RevisionedReader {
	return r.permissions.dl.SnapshotReader(r.revision, datalayer.NoSchemaHashForExplicitRevision)
}

// Check evaluates a permission against this reader's committed revision.
func (r *RevisionedReader) Check(ctx context.Context, req CheckRequest) (CheckResult, error) {
	if err := r.validate(ctx); err != nil {
		return CheckResult{}, err
	}
	return r.permissions.checkAtRevision(ctx, req, r.revision, datalayer.NoSchemaHashForExplicitRevision)
}

// ReadSchema returns the schema at this reader's revision.
func (r *RevisionedReader) ReadSchema(ctx context.Context) (ReadSchemaResult, error) {
	if err := r.validate(ctx); err != nil {
		return ReadSchemaResult{}, err
	}
	return readSchema(ctx, r.reader())
}

func readSchema(ctx context.Context, r datalayer.RevisionedReader) (ReadSchemaResult, error) {
	sr, err := r.ReadSchema(ctx)
	if err != nil {
		return ReadSchemaResult{}, err
	}
	text, err := sr.SchemaText(ctx)
	return ReadSchemaResult{SchemaText: text}, err
}

// ReadRelationships streams matching relationships at this reader's revision.
func (r *RevisionedReader) ReadRelationships(ctx context.Context, filter datastore.RelationshipsFilter, opts ...options.QueryOptionsOption) (ReadRelationshipsResult, error) {
	if err := r.validate(ctx); err != nil {
		return ReadRelationshipsResult{}, err
	}
	return readRelationships(ctx, r.reader(), filter, opts...)
}

func readRelationships(ctx context.Context, r datalayer.RevisionedReader, filter datastore.RelationshipsFilter, opts ...options.QueryOptionsOption) (ReadRelationshipsResult, error) {
	if err := relationships.ValidateFilter(ctx, r, filter); err != nil {
		return ReadRelationshipsResult{}, err
	}
	it, err := r.QueryRelationships(ctx, filter, opts...)
	return ReadRelationshipsResult{Relationships: RelationshipIterator(it)}, err
}

// WriteSchema validates and commits a complete schema, rejecting incompatible changes.
func (p *Permissions) WriteSchema(ctx context.Context, text string) (WriteSchemaResult, error) {
	failed := WriteSchemaResult{datastore.NoRevision}
	opts := []compiler.Option{compiler.DisallowImportFlag(), compiler.CaveatTypeSet(p.cts)}
	if !p.config.ExpiringRelationshipsEnabled {
		opts = append(opts, compiler.DisallowExpirationFlag())
	}
	compiled, err := compiler.Compile(compiler.InputSchema{Source: input.Source("schema"), SchemaString: text}, compiler.AllowUnprefixedObjectType(), opts...)
	if err != nil {
		return failed, err
	}
	validated, err := shared.ValidateSchemaChanges(ctx, compiled, p.cts, false, text)
	if err != nil {
		return failed, err
	}
	rev, err := p.dl.ReadWriteTx(ctx, func(ctx context.Context, tx datalayer.ReadWriteTransaction) error {
		_, err := shared.ApplySchemaChanges(ctx, tx, p.cts, validated)
		return err
	}, options.WithSchemaHashPreconditionExclusive(true))
	if err != nil {
		return failed, err
	}
	return WriteSchemaResult{rev}, nil
}

// WriteRelationships validates and commits relationship updates atomically.
func (p *Permissions) WriteRelationships(ctx context.Context, updates []tuple.RelationshipUpdate) (WriteRelationshipsResult, error) {
	rev, err := p.dl.ReadWriteTx(ctx, func(ctx context.Context, tx datalayer.ReadWriteTransaction) error {
		return p.writeRelationships(ctx, tx, updates)
	})
	if err != nil {
		return WriteRelationshipsResult{datastore.NoRevision}, err
	}
	return WriteRelationshipsResult{rev}, nil
}

func (p *Permissions) writeRelationships(ctx context.Context, tx datalayer.ReadWriteTransaction, updates []tuple.RelationshipUpdate) error {
	if err := relationships.ValidateNativeUpdates(updates, p.config.MaxUpdatesPerWrite, p.config.MaxRelationshipContextSize, p.config.ExpiringRelationshipsEnabled); err != nil {
		return err
	}
	sr, err := tx.ReadSchema(ctx)
	if err != nil {
		return err
	}
	if err := relationships.ValidateRelationshipUpdates(ctx, sr, p.cts, updates); err != nil {
		return err
	}
	return tx.WriteRelationships(ctx, updates)
}
