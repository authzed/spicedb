package proxy

import (
	"context"
	"fmt"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/datastore/options"
	"github.com/authzed/spicedb/pkg/namespace"
)

const (
	errTranslation = "namespace translation error: %w"

	defaultWatchBufferLength = 128
)

type mappingProxy struct {
	delegate          datastore.Datastore
	mapper            namespace.Mapper
	watchBufferLength uint16
}

// NewMappingProxy creates a proxy which maps user provided namespaces names to
// encoded namespace names according to a mapping.
func NewMappingProxy(delegate datastore.Datastore, mapper namespace.Mapper, watchBufferLength uint16) datastore.Datastore {
	if watchBufferLength == 0 {
		watchBufferLength = defaultWatchBufferLength
	}

	return mappingProxy{delegate, mapper, watchBufferLength}
}

func (mp mappingProxy) NamespaceCacheKey(namespaceName string, revision datastore.Revision) (string, error) {
	mapped, err := mp.mapper.Encode(namespaceName)
	if err != nil {
		return "", err
	}
	return mp.delegate.NamespaceCacheKey(mapped, revision)
}

func (mp mappingProxy) Close() error {
	return mp.delegate.Close()
}

func (mp mappingProxy) IsReady(ctx context.Context) (bool, error) {
	return mp.delegate.IsReady(ctx)
}

func (mp mappingProxy) WriteTuples(ctx context.Context, preconditions []*v1.Precondition, mutations []*v1.RelationshipUpdate) (datastore.Revision, error) {
	translatedPreconditions := make([]*v1.Precondition, 0, len(preconditions))
	for _, pc := range preconditions {
		translatedPC, err := translatePrecondition(pc, mp.mapper.Encode)
		if err != nil {
			return datastore.NoRevision, fmt.Errorf(errTranslation, err)
		}
		translatedPreconditions = append(translatedPreconditions, translatedPC)
	}

	translatedMutations := make([]*v1.RelationshipUpdate, 0, len(mutations))
	for _, mut := range mutations {
		translatedRel, err := translateRelationship(mut.Relationship, mp.mapper.Encode)
		if err != nil {
			return datastore.NoRevision, fmt.Errorf(errTranslation, err)
		}
		translatedMutations = append(translatedMutations, &v1.RelationshipUpdate{
			Operation:    mut.Operation,
			Relationship: translatedRel,
		})
	}

	return mp.delegate.WriteTuples(ctx, translatedPreconditions, translatedMutations)
}

func (mp mappingProxy) DeleteRelationships(ctx context.Context, preconditions []*v1.Precondition, filter *v1.RelationshipFilter) (datastore.Revision, error) {
	translatedPreconditions := make([]*v1.Precondition, 0, len(preconditions))
	for _, pc := range preconditions {
		translatedPC, err := translatePrecondition(pc, mp.mapper.Encode)
		if err != nil {
			return datastore.NoRevision, fmt.Errorf(errTranslation, err)
		}
		translatedPreconditions = append(translatedPreconditions, translatedPC)
	}

	translatedFilter, err := translateRelFilter(filter, mp.mapper.Encode)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(errTranslation, err)
	}

	return mp.delegate.DeleteRelationships(ctx, translatedPreconditions, translatedFilter)
}

func (mp mappingProxy) OptimizedRevision(ctx context.Context) (datastore.Revision, error) {
	return mp.delegate.OptimizedRevision(ctx)
}

func (mp mappingProxy) HeadRevision(ctx context.Context) (datastore.Revision, error) {
	return mp.delegate.HeadRevision(ctx)
}

func (mp mappingProxy) Watch(ctx context.Context, afterRevision datastore.Revision) (<-chan *datastore.RevisionChanges, <-chan error) {
	changeChan, errChan := mp.delegate.Watch(ctx, afterRevision)

	newChangeChan := make(chan *datastore.RevisionChanges, mp.watchBufferLength)
	newErrChan := make(chan error, 1)

	go func() {
		defer close(newChangeChan)

		done := false
		for !done {
			select {
			case change, ok := <-changeChan:
				if ok {
					translatedChanges := make([]*core.RelationTupleUpdate, 0, len(change.Changes))
					for _, update := range change.Changes {
						translatedTuple, err := translateTuple(update.Tuple, mp.mapper.Reverse)
						if err != nil {
							newErrChan <- fmt.Errorf(errTranslation, err)
						}
						translatedChanges = append(translatedChanges, &core.RelationTupleUpdate{
							Operation: update.Operation,
							Tuple:     translatedTuple,
						})
					}

					newChangeChan <- &datastore.RevisionChanges{
						Revision: change.Revision,
						Changes:  translatedChanges,
					}
				}
			case err, ok := <-errChan:
				if ok {
					newErrChan <- err
				} else {
					close(newErrChan)
					done = true
				}
			}
		}
	}()

	return newChangeChan, newErrChan
}

func (mp mappingProxy) WriteNamespace(ctx context.Context, newConfig *core.NamespaceDefinition) (datastore.Revision, error) {
	translatedNamespaceName, err := mp.mapper.Encode(newConfig.Name)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(errTranslation, err)
	}

	return mp.delegate.WriteNamespace(ctx, &core.NamespaceDefinition{
		Name:     translatedNamespaceName,
		Relation: newConfig.Relation,
		Metadata: newConfig.Metadata,
	})
}

func (mp mappingProxy) ReadNamespace(ctx context.Context, nsName string, revision datastore.Revision) (*core.NamespaceDefinition, datastore.Revision, error) {
	storedNamespaceName, err := mp.mapper.Encode(nsName)
	if err != nil {
		return nil, datastore.NoRevision, fmt.Errorf(errTranslation, err)
	}

	ns, rev, err := mp.delegate.ReadNamespace(ctx, storedNamespaceName, revision)
	if err != nil {
		return ns, rev, err
	}

	originalNamespaceName, err := mp.mapper.Reverse(ns.Name)
	if err != nil {
		return nil, datastore.NoRevision, fmt.Errorf(errTranslation, err)
	}
	ns.Name = originalNamespaceName

	return ns, rev, err
}

func (mp mappingProxy) DeleteNamespace(ctx context.Context, nsName string) (datastore.Revision, error) {
	storedNamespaceName, err := mp.mapper.Encode(nsName)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(errTranslation, err)
	}

	return mp.delegate.DeleteNamespace(ctx, storedNamespaceName)
}

func (mp mappingProxy) QueryTuples(
	ctx context.Context,
	filter *v1.RelationshipFilter,
	revision datastore.Revision,
	opts ...options.QueryOptionsOption,
) (datastore.TupleIterator, error) {
	resourceType, err := mp.mapper.Encode(filter.ResourceType)
	if err != nil {
		return nil, fmt.Errorf(errTranslation, err)
	}

	var subFilter *v1.SubjectFilter
	if filter.OptionalSubjectFilter != nil {
		subResourceType, err := mp.mapper.Encode(filter.OptionalSubjectFilter.SubjectType)
		if err != nil {
			return nil, fmt.Errorf(errTranslation, err)
		}

		subFilter = &v1.SubjectFilter{
			SubjectType:       subResourceType,
			OptionalSubjectId: filter.OptionalSubjectFilter.OptionalSubjectId,
			OptionalRelation:  filter.OptionalSubjectFilter.OptionalRelation,
		}
	}

	queryOpts := options.NewQueryOptionsWithOptions(opts...)
	translatedUsersets := make([]*core.ObjectAndRelation, 0, len(queryOpts.Usersets))
	for _, userset := range queryOpts.Usersets {
		translatedUserset, err := translateONR(userset, mp.mapper.Encode)
		if err != nil {
			return nil, fmt.Errorf(errTranslation, err)
		}
		translatedUsersets = append(translatedUsersets, translatedUserset)
	}

	rawIter, err := mp.delegate.QueryTuples(ctx, &v1.RelationshipFilter{
		ResourceType:          resourceType,
		OptionalResourceId:    filter.OptionalResourceId,
		OptionalRelation:      filter.OptionalRelation,
		OptionalSubjectFilter: subFilter,
	}, revision, options.WithLimit(queryOpts.Limit), options.SetUsersets(translatedUsersets))
	if err != nil {
		return nil, err
	}

	return &mappingTupleIterator{rawIter, mp.mapper, nil}, nil
}

func (mp mappingProxy) ReverseQueryTuples(
	ctx context.Context,
	filter *v1.SubjectFilter,
	revision datastore.Revision,
	opts ...options.ReverseQueryOptionsOption,
) (datastore.TupleIterator, error) {
	subjectType, err := mp.mapper.Encode(filter.SubjectType)
	if err != nil {
		return nil, fmt.Errorf(errTranslation, err)
	}

	queryOpts := options.NewReverseQueryOptionsWithOptions(opts...)

	translatedOptions := []options.ReverseQueryOptionsOption{
		options.WithReverseLimit(queryOpts.ReverseLimit),
	}
	if queryOpts.ResRelation != nil {
		translatedResourceType, err := mp.mapper.Encode(queryOpts.ResRelation.Namespace)
		if err != nil {
			return nil, fmt.Errorf(errTranslation, err)
		}

		translatedOptions = append(translatedOptions, options.WithResRelation(
			&options.ResourceRelation{
				Namespace: translatedResourceType,
				Relation:  queryOpts.ResRelation.Relation,
			},
		))
	}

	rawIter, err := mp.delegate.ReverseQueryTuples(ctx, &v1.SubjectFilter{
		SubjectType:       subjectType,
		OptionalSubjectId: filter.OptionalSubjectId,
		OptionalRelation:  filter.OptionalRelation,
	}, revision, translatedOptions...)
	if err != nil {
		return nil, err
	}

	return &mappingTupleIterator{rawIter, mp.mapper, nil}, nil
}

func (mp mappingProxy) CheckRevision(ctx context.Context, revision datastore.Revision) error {
	return mp.delegate.CheckRevision(ctx, revision)
}

func (mp mappingProxy) Statistics(ctx context.Context) (datastore.Stats, error) {
	return mp.delegate.Statistics(ctx)
}

func (mp mappingProxy) ListNamespaces(ctx context.Context, revision datastore.Revision) ([]*core.NamespaceDefinition, error) {
	nsDefs, err := mp.delegate.ListNamespaces(ctx, revision)
	if err != nil {
		return nil, err
	}

	for _, nsDef := range nsDefs {
		originalNamespaceName, err := mp.mapper.Reverse(nsDef.Name)
		if err != nil {
			return nil, err
		}
		nsDef.Name = originalNamespaceName
	}
	return nsDefs, nil
}

type mappingTupleIterator struct {
	delegate datastore.TupleIterator
	mapper   namespace.Mapper
	err      error
}

func (mti *mappingTupleIterator) Next() *core.RelationTuple {
	nextTuple := mti.delegate.Next()
	if nextTuple != nil {
		translated, err := translateTuple(nextTuple, mti.mapper.Reverse)
		if err != nil {
			mti.err = err
			return nil
		}

		return translated
	}
	return nil
}

func (mti *mappingTupleIterator) Err() error {
	if mti.err != nil {
		return mti.err
	}
	return mti.delegate.Err()
}

func (mti *mappingTupleIterator) Close() {
	mti.delegate.Close()
}

// MapperFunc is used to translate ObjectTypes into another form before
// interacting with a datastore.
type MapperFunc func(string) (string, error)

func translateRelationship(in *v1.Relationship, mapper MapperFunc) (*v1.Relationship, error) {
	translatedObjectType, err := mapper(in.Resource.ObjectType)
	if err != nil {
		return nil, err
	}

	translatedSubjectType, err := mapper(in.Subject.Object.ObjectType)
	if err != nil {
		return nil, err
	}

	return &v1.Relationship{
		Resource: &v1.ObjectReference{
			ObjectType: translatedObjectType,
			ObjectId:   in.Resource.ObjectId,
		},
		Relation: in.Relation,
		Subject: &v1.SubjectReference{
			Object: &v1.ObjectReference{
				ObjectType: translatedSubjectType,
				ObjectId:   in.Subject.Object.ObjectId,
			},
			OptionalRelation: in.Subject.OptionalRelation,
		},
	}, nil
}

func translateTuple(in *core.RelationTuple, mapper MapperFunc) (*core.RelationTuple, error) {
	translatedObject, err := translateONR(in.ObjectAndRelation, mapper)
	if err != nil {
		return nil, err
	}

	userset := in.User.GetUserset()
	translatedUserset, err := translateONR(userset, mapper)
	if err != nil {
		return nil, err
	}

	return &core.RelationTuple{
		ObjectAndRelation: translatedObject,
		User: &core.User{
			UserOneof: &core.User_Userset{
				Userset: translatedUserset,
			},
		},
	}, nil
}

func translateONR(in *core.ObjectAndRelation, mapper MapperFunc) (*core.ObjectAndRelation, error) {
	newNamespace, err := mapper(in.Namespace)
	if err != nil {
		return nil, err
	}

	return &core.ObjectAndRelation{
		Namespace: newNamespace,
		ObjectId:  in.ObjectId,
		Relation:  in.Relation,
	}, nil
}

func translateRelFilter(filter *v1.RelationshipFilter, mapper MapperFunc) (*v1.RelationshipFilter, error) {
	resourceType, err := mapper(filter.ResourceType)
	if err != nil {
		return nil, err
	}

	var subject *v1.SubjectFilter
	if filter.OptionalSubjectFilter != nil {
		subject, err = translateSubjectFilter(filter.OptionalSubjectFilter, mapper)
		if err != nil {
			return nil, err
		}
	}

	return &v1.RelationshipFilter{
		ResourceType:          resourceType,
		OptionalResourceId:    filter.OptionalResourceId,
		OptionalRelation:      filter.OptionalRelation,
		OptionalSubjectFilter: subject,
	}, nil
}

func translatePrecondition(in *v1.Precondition, mapper MapperFunc) (*v1.Precondition, error) {
	filter, err := translateRelFilter(in.Filter, mapper)
	if err != nil {
		return nil, err
	}

	return &v1.Precondition{
		Operation: in.Operation,
		Filter:    filter,
	}, nil
}

func translateSubjectFilter(in *v1.SubjectFilter, mapper MapperFunc) (*v1.SubjectFilter, error) {
	if in == nil {
		return nil, nil
	}

	newObjectType, err := mapper(in.SubjectType)
	if err != nil {
		return nil, err
	}

	return &v1.SubjectFilter{
		SubjectType:       newObjectType,
		OptionalSubjectId: in.OptionalSubjectId,
		OptionalRelation:  in.OptionalRelation,
	}, nil
}
