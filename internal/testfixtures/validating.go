package testfixtures

import (
	"context"
	"fmt"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb/internal/datastore"
)

type validatingDatastore struct {
	delegate datastore.Datastore
}

// NewValidatingDatastore creates a proxy which runs validation on all call parameters before
// passing the call onward.
func NewValidatingDatastore(delegate datastore.Datastore) datastore.Datastore {
	return validatingDatastore{delegate: delegate}
}

func (vd validatingDatastore) IsReady(ctx context.Context) (bool, error) {
	return vd.delegate.IsReady(ctx)
}

func (vd validatingDatastore) DeleteRelationships(ctx context.Context, preconditions []*v1.Precondition, filter *v1.RelationshipFilter) (datastore.Revision, error) {
	for _, precondition := range preconditions {
		err := precondition.Validate()
		if err != nil {
			return datastore.NoRevision, err
		}
	}

	err := filter.Validate()
	if err != nil {
		return datastore.NoRevision, err
	}

	return vd.delegate.DeleteRelationships(ctx, preconditions, filter)
}

func (vd validatingDatastore) WriteTuples(ctx context.Context, preconditions []*v1.Precondition, mutations []*v1.RelationshipUpdate) (datastore.Revision, error) {
	for _, precondition := range preconditions {
		err := precondition.Validate()
		if err != nil {
			return datastore.NoRevision, err
		}
	}

	for _, mutation := range mutations {
		err := mutation.Validate()
		if err != nil {
			return datastore.NoRevision, err
		}
	}

	return vd.delegate.WriteTuples(ctx, preconditions, mutations)
}

func (vd validatingDatastore) Revision(ctx context.Context) (datastore.Revision, error) {
	return vd.delegate.Revision(ctx)
}

func (vd validatingDatastore) SyncRevision(ctx context.Context) (datastore.Revision, error) {
	return vd.delegate.SyncRevision(ctx)
}

func (vd validatingDatastore) Watch(ctx context.Context, afterRevision datastore.Revision) (<-chan *datastore.RevisionChanges, <-chan error) {
	return vd.delegate.Watch(ctx, afterRevision)
}

func (vd validatingDatastore) WriteNamespace(ctx context.Context, newConfig *v0.NamespaceDefinition) (datastore.Revision, error) {
	err := newConfig.Validate()
	if err != nil {
		return datastore.NoRevision, err
	}
	return vd.delegate.WriteNamespace(ctx, newConfig)
}

func (vd validatingDatastore) ReadNamespace(ctx context.Context, nsName string) (*v0.NamespaceDefinition, datastore.Revision, error) {
	read, rev, err := vd.delegate.ReadNamespace(ctx, nsName)
	if err != nil {
		return read, rev, err
	}

	err = read.Validate()
	return read, rev, err
}

func (vd validatingDatastore) DeleteNamespace(ctx context.Context, nsName string) (datastore.Revision, error) {
	return vd.delegate.DeleteNamespace(ctx, nsName)
}

func (vd validatingDatastore) QueryTuples(filter datastore.TupleQueryResourceFilter, revision datastore.Revision) datastore.TupleQuery {
	var err error
	if filter.ResourceType == "" {
		err = fmt.Errorf("missing required resource type")
	}
	return validatingTupleQuery{vd.delegate.QueryTuples(filter, revision), nil, nil, err}
}

func (vd validatingDatastore) ReverseQueryTuplesFromSubjectNamespace(subjectNamespace string, revision datastore.Revision) datastore.ReverseTupleQuery {
	if subjectNamespace == "" {
		return validatingTupleQuery{
			nil,
			vd.delegate.ReverseQueryTuplesFromSubjectNamespace(subjectNamespace, revision),
			nil,
			fmt.Errorf("Empty subject namespace given to ReverseQueryTuplesFromSubjectNamespace"),
		}
	}

	return validatingTupleQuery{nil, vd.delegate.ReverseQueryTuplesFromSubjectNamespace(subjectNamespace, revision), nil, nil}
}

func (vd validatingDatastore) ReverseQueryTuplesFromSubject(subject *v0.ObjectAndRelation, revision datastore.Revision) datastore.ReverseTupleQuery {
	err := subject.Validate()
	return validatingTupleQuery{nil, vd.delegate.ReverseQueryTuplesFromSubject(subject, revision), nil, err}
}

func (vd validatingDatastore) ReverseQueryTuplesFromSubjectRelation(subjectNamespace, subjectRelation string, revision datastore.Revision) datastore.ReverseTupleQuery {
	if subjectNamespace == "" {
		return validatingTupleQuery{
			nil,
			vd.delegate.ReverseQueryTuplesFromSubjectNamespace(subjectNamespace, revision),
			nil,
			fmt.Errorf("Empty subject namespace given to ReverseQueryTuplesFromSubjectRelation"),
		}
	}

	if subjectRelation == "" {
		return validatingTupleQuery{
			nil,
			vd.delegate.ReverseQueryTuplesFromSubjectNamespace(subjectNamespace, revision),
			nil,
			fmt.Errorf("Empty subject relation given to ReverseQueryTuplesFromSubjectRelation"),
		}
	}

	return validatingTupleQuery{nil, vd.delegate.ReverseQueryTuplesFromSubjectRelation(subjectNamespace, subjectRelation, revision), nil, nil}
}

func (vd validatingDatastore) CheckRevision(ctx context.Context, revision datastore.Revision) error {
	return vd.delegate.CheckRevision(ctx, revision)
}

func (vd validatingDatastore) ListNamespaces(ctx context.Context) ([]*v0.NamespaceDefinition, error) {
	read, err := vd.delegate.ListNamespaces(ctx)
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

type validatingTupleQuery struct {
	wrapped        datastore.TupleQuery
	wrappedReverse datastore.ReverseTupleQuery
	wrappedCommon  datastore.CommonTupleQuery
	foundErr       error
}

func (vd validatingTupleQuery) WithSubjectFilter(filter *v1.SubjectFilter) datastore.TupleQuery {
	if vd.foundErr != nil {
		return vd
	}

	err := filter.Validate()
	return validatingTupleQuery{
		wrapped:  vd.wrapped.WithSubjectFilter(filter),
		foundErr: err,
	}
}

func (vd validatingTupleQuery) WithUsersets(usersets []*v0.ObjectAndRelation) datastore.TupleQuery {
	if vd.foundErr != nil {
		return vd
	}

	var err error
	for _, userset := range usersets {
		err := userset.Validate()
		if err != nil {
			break
		}
	}

	return validatingTupleQuery{
		wrapped:  vd.wrapped.WithUsersets(usersets),
		foundErr: err,
	}
}

func (vd validatingTupleQuery) WithObjectRelation(namespace string, relation string) datastore.ReverseTupleQuery {
	if namespace == "" {
		return validatingTupleQuery{
			wrapped:        nil,
			wrappedReverse: vd.wrappedReverse,
			foundErr:       fmt.Errorf("Empty namespace given to WithObjectRelation"),
		}
	}

	if relation == "" {
		return validatingTupleQuery{
			wrapped:        nil,
			wrappedReverse: vd.wrappedReverse,
			foundErr:       fmt.Errorf("Empty relation given to WithObjectRelation"),
		}
	}

	return validatingTupleQuery{
		wrapped:        nil,
		wrappedReverse: vd.wrappedReverse.WithObjectRelation(namespace, relation),
		foundErr:       vd.foundErr,
	}
}

func (vd validatingTupleQuery) Limit(limit uint64) datastore.CommonTupleQuery {
	return validatingTupleQuery{
		wrapped:       nil,
		wrappedCommon: vd.wrapped.Limit(limit),
		foundErr:      vd.foundErr,
	}
}

func (vd validatingTupleQuery) Execute(ctx context.Context) (datastore.TupleIterator, error) {
	if vd.foundErr != nil {
		return nil, vd.foundErr
	}

	if vd.wrapped != nil {
		return vd.wrapped.Execute(ctx)
	}

	if vd.wrappedReverse != nil {
		return vd.wrappedReverse.Execute(ctx)
	}

	return vd.wrappedCommon.Execute(ctx)
}
