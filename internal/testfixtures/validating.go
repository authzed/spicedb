package testfixtures

import (
	"context"
	"fmt"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/datastore/queryshape"
	"github.com/authzed/spicedb/pkg/genutil/mapz"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
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
		return datastore.NoRevision, spiceerrors.MustBugf("nil delegate function")
	}

	return vd.Datastore.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		txDelegate := validatingReadWriteTransaction{validatingSnapshotReader{rwt}, rwt}
		return f(ctx, txDelegate)
	}, opts...)
}

func (vd validatingDatastore) Unwrap() datastore.Datastore {
	return vd.Datastore
}

type validatingSnapshotReader struct {
	delegate datastore.Reader
}

func (vsr validatingSnapshotReader) LegacyListAllNamespaces(
	ctx context.Context,
) ([]datastore.RevisionedNamespace, error) {
	read, err := vsr.delegate.LegacyListAllNamespaces(ctx)
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

func (vsr validatingSnapshotReader) LegacyLookupNamespacesWithNames(
	ctx context.Context,
	nsNames []string,
) ([]datastore.RevisionedNamespace, error) {
	read, err := vsr.delegate.LegacyLookupNamespacesWithNames(ctx, nsNames)
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

func (vsr validatingSnapshotReader) CountRelationships(ctx context.Context, name string) (int, error) {
	return vsr.delegate.CountRelationships(ctx, name)
}

func (vsr validatingSnapshotReader) LookupCounters(ctx context.Context) ([]datastore.RelationshipCounter, error) {
	return vsr.delegate.LookupCounters(ctx)
}

func (vsr validatingSnapshotReader) QueryRelationships(ctx context.Context,
	filter datastore.RelationshipsFilter,
	opts ...options.QueryOptionsOption,
) (datastore.RelationshipIterator, error) {
	queryOpts := options.NewQueryOptionsWithOptions(opts...)
	if queryOpts.QueryShape == "" || queryOpts.QueryShape == queryshape.Unspecified {
		return nil, spiceerrors.MustBugf("query shape is unspecified")
	}

	return vsr.delegate.QueryRelationships(ctx, filter, opts...)
}

func (vsr validatingSnapshotReader) LegacyReadNamespaceByName(
	ctx context.Context,
	nsName string,
) (*core.NamespaceDefinition, datastore.Revision, error) {
	read, createdAt, err := vsr.delegate.LegacyReadNamespaceByName(ctx, nsName)
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
			return nil, spiceerrors.MustBugf("resource relation on reverse query missing namespace")
		}
		if queryOpts.ResRelation.Relation == "" {
			return nil, spiceerrors.MustBugf("resource relation on reverse query missing relation")
		}
	}

	if queryOpts.QueryShapeForReverse == "" || queryOpts.QueryShapeForReverse == queryshape.Unspecified {
		return nil, spiceerrors.MustBugf("query shape for reverse query is unspecified")
	}

	return vsr.delegate.ReverseQueryRelationships(ctx, subjectsFilter, opts...)
}

func (vsr validatingSnapshotReader) LegacyReadCaveatByName(ctx context.Context, name string) (*core.CaveatDefinition, datastore.Revision, error) {
	read, createdAt, err := vsr.delegate.LegacyReadCaveatByName(ctx, name)
	if err != nil {
		return read, createdAt, err
	}

	err = read.Validate()
	return read, createdAt, err
}

func (vsr validatingSnapshotReader) LegacyLookupCaveatsWithNames(ctx context.Context, caveatNames []string) ([]datastore.RevisionedCaveat, error) {
	read, err := vsr.delegate.LegacyLookupCaveatsWithNames(ctx, caveatNames)
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

func (vsr validatingSnapshotReader) LegacyListAllCaveats(ctx context.Context) ([]datastore.RevisionedCaveat, error) {
	read, err := vsr.delegate.LegacyListAllCaveats(ctx)
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

func (vrwt validatingReadWriteTransaction) RegisterCounter(ctx context.Context, name string, filter *core.RelationshipFilter) error {
	if err := filter.Validate(); err != nil {
		return err
	}

	return vrwt.delegate.RegisterCounter(ctx, name, filter)
}

func (vrwt validatingReadWriteTransaction) UnregisterCounter(ctx context.Context, name string) error {
	return vrwt.delegate.UnregisterCounter(ctx, name)
}

func (vrwt validatingReadWriteTransaction) StoreCounterValue(ctx context.Context, name string, value int, computedAtRevision datastore.Revision) error {
	return vrwt.delegate.StoreCounterValue(ctx, name, value, computedAtRevision)
}

func (vrwt validatingReadWriteTransaction) LegacyWriteNamespaces(ctx context.Context, newConfigs ...*core.NamespaceDefinition) error {
	for _, newConfig := range newConfigs {
		if err := newConfig.Validate(); err != nil {
			return err
		}
	}
	return vrwt.delegate.LegacyWriteNamespaces(ctx, newConfigs...)
}

func (vrwt validatingReadWriteTransaction) LegacyDeleteNamespaces(ctx context.Context, nsNames []string, delOption datastore.DeleteNamespacesRelationshipsOption) error {
	return vrwt.delegate.LegacyDeleteNamespaces(ctx, nsNames, delOption)
}

func (vrwt validatingReadWriteTransaction) WriteRelationships(ctx context.Context, mutations []tuple.RelationshipUpdate) error {
	if err := validateUpdatesToWrite(mutations...); err != nil {
		return err
	}

	// Ensure there are no duplicate mutations.
	tupleSet := mapz.NewSet[string]()
	for _, mutation := range mutations {
		if !tupleSet.Add(tuple.StringWithoutCaveatOrExpiration(mutation.Relationship)) {
			return fmt.Errorf("found duplicate update for relationship %s", tuple.StringWithoutCaveatOrExpiration(mutation.Relationship))
		}
	}

	return vrwt.delegate.WriteRelationships(ctx, mutations)
}

func (vrwt validatingReadWriteTransaction) DeleteRelationships(ctx context.Context, filter *v1.RelationshipFilter, options ...options.DeleteOptionsOption) (uint64, bool, error) {
	if err := filter.Validate(); err != nil {
		return 0, false, err
	}

	return vrwt.delegate.DeleteRelationships(ctx, filter, options...)
}

func (vrwt validatingReadWriteTransaction) LegacyWriteCaveats(ctx context.Context, caveats []*core.CaveatDefinition) error {
	return vrwt.delegate.LegacyWriteCaveats(ctx, caveats)
}

func (vrwt validatingReadWriteTransaction) LegacyDeleteCaveats(ctx context.Context, names []string) error {
	return vrwt.delegate.LegacyDeleteCaveats(ctx, names)
}

func (vrwt validatingReadWriteTransaction) BulkLoad(ctx context.Context, source datastore.BulkWriteRelationshipSource) (uint64, error) {
	return vrwt.delegate.BulkLoad(ctx, source)
}

// validateUpdatesToWrite performs basic validation on relationship updates going into datastores.
func validateUpdatesToWrite(updates ...tuple.RelationshipUpdate) error {
	for _, update := range updates {
		up, err := tuple.UpdateToV1RelationshipUpdate(update)
		if err != nil {
			return err
		}

		if err := up.HandwrittenValidate(); err != nil {
			return err
		}
		if update.Relationship.Subject.Relation == "" {
			return spiceerrors.MustBugf("expected ... instead of an empty relation string relation in %v", update.Relationship)
		}
		if update.Relationship.Subject.ObjectID == tuple.PublicWildcard && update.Relationship.Subject.Relation != tuple.Ellipsis {
			return spiceerrors.MustBugf(
				"attempt to write a wildcard relationship (`%s`) with a non-empty relation `%v`. Please report this bug",
				tuple.MustString(update.Relationship),
				update.Relationship.Subject.Relation,
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
