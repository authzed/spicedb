package testfixtures

import (
	"context"
	"errors"
	"fmt"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/options"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

type validatingDatastore struct {
	delegate datastore.Datastore
}

// NewValidatingDatastore creates a proxy which runs validation on all call parameters before
// passing the call onward.
func NewValidatingDatastore(delegate datastore.Datastore) datastore.Datastore {
	return validatingDatastore{delegate: delegate}
}

func (vd validatingDatastore) Close() error {
	return vd.delegate.Close()
}

func (vd validatingDatastore) IsReady(ctx context.Context) (bool, error) {
	return vd.delegate.IsReady(ctx)
}

func (vd validatingDatastore) SnapshotReader(revision datastore.Revision) datastore.Reader {
	return validatingSnapshotReader{vd.delegate.SnapshotReader(revision)}
}

func (vd validatingDatastore) ReadWriteTx(
	ctx context.Context,
	f datastore.TxUserFunc,
) (datastore.Revision, error) {
	if f == nil {
		return datastore.NoRevision, fmt.Errorf("nil delegate function")
	}

	return vd.delegate.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		txDelegate := validatingReadWriteTransaction{validatingSnapshotReader{rwt}, rwt}
		return f(ctx, txDelegate)
	})
}

func (vd validatingDatastore) OptimizedRevision(ctx context.Context) (datastore.Revision, error) {
	return vd.delegate.OptimizedRevision(ctx)
}

func (vd validatingDatastore) HeadRevision(ctx context.Context) (datastore.Revision, error) {
	return vd.delegate.HeadRevision(ctx)
}

func (vd validatingDatastore) CheckRevision(ctx context.Context, revision datastore.Revision) error {
	return vd.delegate.CheckRevision(ctx, revision)
}

func (vd validatingDatastore) Watch(ctx context.Context, afterRevision datastore.Revision) (<-chan *datastore.RevisionChanges, <-chan error) {
	return vd.delegate.Watch(ctx, afterRevision)
}

func (vd validatingDatastore) Statistics(ctx context.Context) (datastore.Stats, error) {
	return vd.delegate.Statistics(ctx)
}

type validatingSnapshotReader struct {
	delegate datastore.Reader
}

func (vsr validatingSnapshotReader) ListNamespaces(
	ctx context.Context,
) ([]*core.NamespaceDefinition, error) {
	read, err := vsr.delegate.ListNamespaces(ctx)
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
	filter *v1.RelationshipFilter,
	opts ...options.QueryOptionsOption,
) (datastore.RelationshipIterator, error) {
	if err := filter.Validate(); err != nil {
		return nil, err
	}

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
	subjectFilter *v1.SubjectFilter,
	opts ...options.ReverseQueryOptionsOption,
) (datastore.RelationshipIterator, error) {
	if err := subjectFilter.Validate(); err != nil {
		return nil, err
	}

	queryOpts := options.NewReverseQueryOptionsWithOptions(opts...)
	if queryOpts.ResRelation != nil {
		if queryOpts.ResRelation.Namespace == "" {
			return nil, errors.New("resource relation on reverse query missing namespace")
		}
		if queryOpts.ResRelation.Relation == "" {
			return nil, errors.New("resource relation on reverse query missing relation")
		}
	}

	return vsr.delegate.ReverseQueryRelationships(ctx, subjectFilter, opts...)
}

type validatingReadWriteTransaction struct {
	validatingSnapshotReader
	delegate datastore.ReadWriteTransaction
}

func (vrwt validatingReadWriteTransaction) WriteNamespaces(newConfigs ...*core.NamespaceDefinition) error {
	for _, newConfig := range newConfigs {
		if err := newConfig.Validate(); err != nil {
			return err
		}
	}
	return vrwt.delegate.WriteNamespaces(newConfigs...)
}

func (vrwt validatingReadWriteTransaction) DeleteNamespace(nsName string) error {
	return vrwt.delegate.DeleteNamespace(nsName)
}

func (vrwt validatingReadWriteTransaction) WriteRelationships(mutations []*v1.RelationshipUpdate) error {
	if err := common.ValidateUpdatesToWrite(mutations); err != nil {
		return err
	}
	for _, mutation := range mutations {
		if err := mutation.Validate(); err != nil {
			return err
		}
	}

	return vrwt.delegate.WriteRelationships(mutations)
}

func (vrwt validatingReadWriteTransaction) DeleteRelationships(filter *v1.RelationshipFilter) error {
	if err := filter.Validate(); err != nil {
		return err
	}

	return vrwt.delegate.DeleteRelationships(filter)
}

var (
	_ datastore.Datastore            = validatingDatastore{}
	_ datastore.Reader               = validatingSnapshotReader{}
	_ datastore.ReadWriteTransaction = validatingReadWriteTransaction{}
)
