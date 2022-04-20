package testfixtures

import (
	"context"
	"errors"
	"fmt"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/options"
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

func (vd validatingDatastore) DeleteRelationships(ctx context.Context, preconditions []*v1.Precondition, preconditionRevision datastore.Revision, filter *v1.RelationshipFilter) (datastore.Revision, error) {
	for _, precondition := range preconditions {
		err := precondition.Validate()
		if err != nil {
			return datastore.NoRevision, err
		}
	}

	if err := filter.Validate(); err != nil {
		return datastore.NoRevision, err
	}

	return vd.delegate.DeleteRelationships(ctx, preconditions, preconditionRevision, filter)
}

func (vd validatingDatastore) WriteTuples(ctx context.Context, preconditions []*v1.Precondition, preconditionRevision datastore.Revision, mutations []*v1.RelationshipUpdate) (datastore.Revision, error) {
	for _, precondition := range preconditions {
		err := precondition.Validate()
		if err != nil {
			return datastore.NoRevision, err
		}
	}

	err := common.ValidateUpdatesToWrite(mutations)
	if err != nil {
		return datastore.NoRevision, err
	}

	for _, mutation := range mutations {
		err = mutation.Validate()
		if err != nil {
			return datastore.NoRevision, err
		}
	}

	return vd.delegate.WriteTuples(ctx, preconditions, preconditionRevision, mutations)
}

func (vd validatingDatastore) OptimizedRevision(ctx context.Context) (datastore.Revision, error) {
	return vd.delegate.OptimizedRevision(ctx)
}

func (vd validatingDatastore) HeadRevision(ctx context.Context) (datastore.Revision, error) {
	return vd.delegate.HeadRevision(ctx)
}

func (vd validatingDatastore) Watch(ctx context.Context, afterRevision datastore.Revision) (<-chan *datastore.RevisionChanges, <-chan error) {
	return vd.delegate.Watch(ctx, afterRevision)
}

func (vd validatingDatastore) WriteNamespace(ctx context.Context, newConfig *core.NamespaceDefinition) (datastore.Revision, error) {
	if err := newConfig.Validate(); err != nil {
		return datastore.NoRevision, err
	}
	return vd.delegate.WriteNamespace(ctx, newConfig)
}

func (vd validatingDatastore) ReadNamespace(
	ctx context.Context,
	nsName string,
	revision datastore.Revision,
) (*core.NamespaceDefinition, datastore.Revision, error) {
	read, createdAt, err := vd.delegate.ReadNamespace(ctx, nsName, revision)
	if err != nil {
		return read, createdAt, err
	}

	err = read.Validate()
	return read, createdAt, err
}

func (vd validatingDatastore) DeleteNamespace(ctx context.Context, nsName string) (datastore.Revision, error) {
	return vd.delegate.DeleteNamespace(ctx, nsName)
}

func (vd validatingDatastore) QueryTuples(ctx context.Context,
	filter *v1.RelationshipFilter,
	revision datastore.Revision,
	opts ...options.QueryOptionsOption,
) (datastore.TupleIterator, error) {
	if err := filter.Validate(); err != nil {
		return nil, err
	}

	queryOpts := options.NewQueryOptionsWithOptions(opts...)
	for _, sub := range queryOpts.Usersets {
		if err := sub.Validate(); err != nil {
			return nil, err
		}
	}

	return vd.delegate.QueryTuples(ctx, filter, revision, opts...)
}

func (vd validatingDatastore) ReverseQueryTuples(ctx context.Context,
	subjectFilter *v1.SubjectFilter,
	revision datastore.Revision,
	opts ...options.ReverseQueryOptionsOption,
) (datastore.TupleIterator, error) {
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

	return vd.delegate.ReverseQueryTuples(ctx, subjectFilter, revision, opts...)
}

func (vd validatingDatastore) CheckRevision(ctx context.Context, revision datastore.Revision) error {
	return vd.delegate.CheckRevision(ctx, revision)
}

func (vd validatingDatastore) ListNamespaces(
	ctx context.Context,
	revision datastore.Revision,
) ([]*core.NamespaceDefinition, error) {
	read, err := vd.delegate.ListNamespaces(ctx, revision)
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

func (vd validatingDatastore) NamespaceCacheKey(namespaceName string, revision datastore.Revision) (string, error) {
	return fmt.Sprintf("%s@%s", namespaceName, revision), nil
}

func (vd validatingDatastore) Statistics(ctx context.Context) (datastore.Stats, error) {
	return vd.delegate.Statistics(ctx)
}
