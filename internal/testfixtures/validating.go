package testfixtures

import (
	"context"
	"errors"
	"fmt"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb/internal/datastore/options"
	"github.com/authzed/spicedb/pkg/util"
	"github.com/authzed/spicedb/pkg/datastore"
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
) (datastore.Revision, error) {
	if f == nil {
		return datastore.NoRevision, fmt.Errorf("nil delegate function")
	}

	return vd.Datastore.ReadWriteTx(ctx, func(rwt datastore.ReadWriteTransaction) error {
		txDelegate := validatingReadWriteTransaction{validatingSnapshotReader{rwt}, rwt}
		return f(txDelegate)
	})
}

type validatingSnapshotReader struct {
	delegate datastore.Reader
}

func (vsr validatingSnapshotReader) ListNamespaces(
	ctx context.Context,
) ([]*core.NamespaceDefinition, error) {
	read, err := vsr.delegate.ListNamespaces(ctx)
	if err != nil {
		return nil, err
	}

	for _, nsDef := range read {
		err := nsDef.Validate()
		if err != nil {
			return nil, err
		}
	}

	return read, err
}

func (vsr validatingSnapshotReader) LookupNamespaces(
	ctx context.Context,
	nsNames []string,
) ([]*core.NamespaceDefinition, error) {
	read, err := vsr.delegate.LookupNamespaces(ctx, nsNames)
	if err != nil {
		return read, err
	}

	for _, nsDef := range read {
		err := nsDef.Validate()
		if err != nil {
			return nil, err
		}
	}

	return read, err
}

func (vsr validatingSnapshotReader) QueryRelationships(ctx context.Context,
	filter datastore.RelationshipsFilter,
	opts ...options.QueryOptionsOption,
) (datastore.RelationshipIterator, error) {
	queryOpts := options.NewQueryOptionsWithOptions(opts...)
	for _, sub := range queryOpts.Usersets {
		if err := sub.Validate(); err != nil {
			return nil, err
		}
	}

	return vsr.delegate.QueryRelationships(ctx, filter, opts...)
}

func (vsr validatingSnapshotReader) ReadNamespace(
	ctx context.Context,
	nsName string,
) (*core.NamespaceDefinition, datastore.Revision, error) {
	read, createdAt, err := vsr.delegate.ReadNamespace(ctx, nsName)
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

func (vsr validatingSnapshotReader) ListCaveats(ctx context.Context, caveatNames ...string) ([]*core.CaveatDefinition, error) {
	read, err := vsr.delegate.ListCaveats(ctx, caveatNames...)
	if err != nil {
		return nil, err
	}

	for _, caveatDef := range read {
		err := caveatDef.Validate()
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
	tupleSet := util.NewSet[string]()
	for _, mutation := range mutations {
		if err := mutation.Validate(); err != nil {
			return err
		}

		if !tupleSet.Add(tuple.String(mutation.Tuple)) {
			return fmt.Errorf("found duplicate update for relationship %s", tuple.String(mutation.Tuple))
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
				tuple.String(update.Tuple),
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
