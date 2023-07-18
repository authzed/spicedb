package testfixtures

import (
	"context"
	"errors"
	"fmt"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/genutil/mapz"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

type validatingDatastore struct {
	datastore.Datastore
}

// NewValidatingDatastore creates a proxy which runs validation on all call parameters before
// passing the call onward.
func NewValidatingDatastore(delegate datastore.Datastore) datastore.Datastore {
	return validatingDatastore{Datastore: delegate}
}

func (vd validatingDatastore) SnapshotReader(revision datastore.Revision) datastore.Reader {
	return validatingSnapshotReader{vd.Datastore.SnapshotReader(revision)}
}

func (vd validatingDatastore) ReadWriteTx(
	ctx context.Context,
	f datastore.TxUserFunc,
	opts ...options.RWTOptionsOption,
) (datastore.Revision, error) {
	if f == nil {
		return datastore.NoRevision, fmt.Errorf("nil delegate function")
	}

	return vd.Datastore.ReadWriteTx(ctx, func(rwt datastore.ReadWriteTransaction) error {
		txDelegate := validatingReadWriteTransaction{validatingSnapshotReader{rwt}, rwt}
		return f(txDelegate)
	}, opts...)
}

func (vd validatingDatastore) Unwrap() datastore.Datastore {
	return vd.Datastore
}

type validatingSnapshotReader struct {
	delegate datastore.Reader
}

func (vsr validatingSnapshotReader) ListAllNamespaces(
	ctx context.Context,
) ([]datastore.RevisionedNamespace, error) {
	read, err := vsr.delegate.ListAllNamespaces(ctx)
	if err != nil {
		return nil, err
	}

	for _, ns := range read {
		err := ns.Definition.Validate()
		if err != nil {
			return nil, err
		}
	}

	return read, err
}

func (vsr validatingSnapshotReader) LookupNamespacesWithNames(
	ctx context.Context,
	nsNames []string,
) ([]datastore.RevisionedNamespace, error) {
	read, err := vsr.delegate.LookupNamespacesWithNames(ctx, nsNames)
	if err != nil {
		return read, err
	}

	for _, ns := range read {
		err := ns.Definition.Validate()
		if err != nil {
			return nil, err
		}
	}

	return read, nil
}

func (vsr validatingSnapshotReader) QueryRelationships(ctx context.Context,
	filter datastore.RelationshipsFilter,
	opts ...options.QueryOptionsOption,
) (datastore.RelationshipIterator, error) {
	return vsr.delegate.QueryRelationships(ctx, filter, opts...)
}

func (vsr validatingSnapshotReader) ReadNamespaceByName(
	ctx context.Context,
	nsName string,
) (*core.NamespaceDefinition, datastore.Revision, error) {
	read, createdAt, err := vsr.delegate.ReadNamespaceByName(ctx, nsName)
	if err != nil {
		return read, createdAt, err
	}

	err = read.Validate()
	return read, createdAt, err
}

func (vsr validatingSnapshotReader) ReverseQueryRelationships(ctx context.Context,
	subjectsFilter datastore.SubjectsFilter,
	opts ...options.ReverseQueryOptionsOption,
) (datastore.RelationshipIterator, error) {
	queryOpts := options.NewReverseQueryOptionsWithOptions(opts...)
	if queryOpts.ResRelation != nil {
		if queryOpts.ResRelation.Namespace == "" {
			return nil, errors.New("resource relation on reverse query missing namespace")
		}
		if queryOpts.ResRelation.Relation == "" {
			return nil, errors.New("resource relation on reverse query missing relation")
		}
	}

	return vsr.delegate.ReverseQueryRelationships(ctx, subjectsFilter, opts...)
}

func (vsr validatingSnapshotReader) ReadCaveatByName(ctx context.Context, name string) (*core.CaveatDefinition, datastore.Revision, error) {
	read, createdAt, err := vsr.delegate.ReadCaveatByName(ctx, name)
	if err != nil {
		return read, createdAt, err
	}

	err = read.Validate()
	return read, createdAt, err
}

func (vsr validatingSnapshotReader) LookupCaveatsWithNames(ctx context.Context, caveatNames []string) ([]datastore.RevisionedCaveat, error) {
	read, err := vsr.delegate.LookupCaveatsWithNames(ctx, caveatNames)
	if err != nil {
		return nil, err
	}

	for _, caveat := range read {
		err := caveat.Definition.Validate()
		if err != nil {
			return nil, err
		}
	}

	return read, err
}

func (vsr validatingSnapshotReader) ListAllCaveats(ctx context.Context) ([]datastore.RevisionedCaveat, error) {
	read, err := vsr.delegate.ListAllCaveats(ctx)
	if err != nil {
		return nil, err
	}

	for _, caveat := range read {
		err := caveat.Definition.Validate()
		if err != nil {
			return nil, err
		}
	}

	return read, err
}

type validatingReadWriteTransaction struct {
	validatingSnapshotReader
	delegate datastore.ReadWriteTransaction
}

func (vrwt validatingReadWriteTransaction) WriteNamespaces(ctx context.Context, newConfigs ...*core.NamespaceDefinition) error {
	for _, newConfig := range newConfigs {
		if err := newConfig.Validate(); err != nil {
			return err
		}
	}
	return vrwt.delegate.WriteNamespaces(ctx, newConfigs...)
}

func (vrwt validatingReadWriteTransaction) DeleteNamespaces(ctx context.Context, nsNames ...string) error {
	return vrwt.delegate.DeleteNamespaces(ctx, nsNames...)
}

func (vrwt validatingReadWriteTransaction) WriteRelationships(ctx context.Context, mutations []*core.RelationTupleUpdate) error {
	if err := validateUpdatesToWrite(mutations...); err != nil {
		return err
	}

	// Ensure there are no duplicate mutations.
	tupleSet := mapz.NewSet[string]()
	for _, mutation := range mutations {
		if err := mutation.Validate(); err != nil {
			return err
		}

		if !tupleSet.Add(tuple.StringWithoutCaveat(mutation.Tuple)) {
			return fmt.Errorf("found duplicate update for relationship %s", tuple.StringWithoutCaveat(mutation.Tuple))
		}
	}

	return vrwt.delegate.WriteRelationships(ctx, mutations)
}

func (vrwt validatingReadWriteTransaction) DeleteRelationships(ctx context.Context, filter *v1.RelationshipFilter) error {
	if err := filter.Validate(); err != nil {
		return err
	}

	return vrwt.delegate.DeleteRelationships(ctx, filter)
}

func (vrwt validatingReadWriteTransaction) WriteCaveats(ctx context.Context, caveats []*core.CaveatDefinition) error {
	return vrwt.delegate.WriteCaveats(ctx, caveats)
}

func (vrwt validatingReadWriteTransaction) DeleteCaveats(ctx context.Context, names []string) error {
	return vrwt.delegate.DeleteCaveats(ctx, names)
}

func (vrwt validatingReadWriteTransaction) BulkLoad(ctx context.Context, source datastore.BulkWriteRelationshipSource) (uint64, error) {
	return vrwt.delegate.BulkLoad(ctx, source)
}

// validateUpdatesToWrite performs basic validation on relationship updates going into datastores.
func validateUpdatesToWrite(updates ...*core.RelationTupleUpdate) error {
	for _, update := range updates {
		err := tuple.UpdateToRelationshipUpdate(update).HandwrittenValidate()
		if err != nil {
			return err
		}
		if update.Tuple.Subject.Relation == "" {
			return fmt.Errorf("expected ... instead of an empty relation string relation in %v", update.Tuple)
		}
		if update.Tuple.Subject.ObjectId == tuple.PublicWildcard && update.Tuple.Subject.Relation != tuple.Ellipsis {
			return fmt.Errorf(
				"attempt to write a wildcard relationship (`%s`) with a non-empty relation `%v`. Please report this bug",
				tuple.MustString(update.Tuple),
				update.Tuple.Subject.Relation,
			)
		}
	}

	return nil
}

var (
	_ datastore.Datastore            = validatingDatastore{}
	_ datastore.Reader               = validatingSnapshotReader{}
	_ datastore.ReadWriteTransaction = validatingReadWriteTransaction{}
)
