package proxy

import (
	"context"
	"fmt"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	"github.com/authzed/spicedb/internal/datastore"
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

func (mp mappingProxy) WriteTuples(ctx context.Context, preconditions []*v0.RelationTuple, mutations []*v0.RelationTupleUpdate) (datastore.Revision, error) {
	translatedPreconditions := make([]*v0.RelationTuple, 0, len(preconditions))
	for _, pc := range preconditions {
		translatedPC, err := translateTuple(pc, mp.mapper.Encode)
		if err != nil {
			return datastore.NoRevision, fmt.Errorf(errTranslation, err)
		}
		translatedPreconditions = append(translatedPreconditions, translatedPC)
	}

	translatedMutations := make([]*v0.RelationTupleUpdate, 0, len(mutations))
	for _, mut := range mutations {
		translatedMutationTuple, err := translateTuple(mut.Tuple, mp.mapper.Encode)
		if err != nil {
			return datastore.NoRevision, fmt.Errorf(errTranslation, err)
		}
		translatedMutations = append(translatedMutations, &v0.RelationTupleUpdate{
			Operation: mut.Operation,
			Tuple:     translatedMutationTuple,
		})
	}

	return mp.delegate.WriteTuples(ctx, translatedPreconditions, translatedMutations)
}

func (mp mappingProxy) Revision(ctx context.Context) (datastore.Revision, error) {
	return mp.delegate.Revision(ctx)
}

func (mp mappingProxy) SyncRevision(ctx context.Context) (datastore.Revision, error) {
	return mp.delegate.SyncRevision(ctx)
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
					translatedChanges := make([]*v0.RelationTupleUpdate, 0, len(change.Changes))
					for _, update := range change.Changes {
						translatedTuple, err := translateTuple(update.Tuple, mp.mapper.Reverse)
						if err != nil {
							newErrChan <- fmt.Errorf(errTranslation, err)
						}
						translatedChanges = append(translatedChanges, &v0.RelationTupleUpdate{
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

func (mp mappingProxy) WriteNamespace(ctx context.Context, newConfig *v0.NamespaceDefinition) (datastore.Revision, error) {
	translatedNamespaceName, err := mp.mapper.Encode(newConfig.Name)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(errTranslation, err)
	}

	return mp.delegate.WriteNamespace(ctx, &v0.NamespaceDefinition{
		Name:     translatedNamespaceName,
		Relation: newConfig.Relation,
		Metadata: newConfig.Metadata,
	})
}

func (mp mappingProxy) ReadNamespace(ctx context.Context, nsName string) (*v0.NamespaceDefinition, datastore.Revision, error) {
	storedNamespaceName, err := mp.mapper.Encode(nsName)
	if err != nil {
		return nil, datastore.NoRevision, fmt.Errorf(errTranslation, err)
	}

	ns, rev, err := mp.delegate.ReadNamespace(ctx, storedNamespaceName)
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

func (mp mappingProxy) QueryTuples(namespace string, revision datastore.Revision) datastore.TupleQuery {
	translatedNamespace, err := mp.mapper.Encode(namespace)
	return mappingTupleQuery{mp.delegate.QueryTuples(translatedNamespace, revision), mp.mapper, err}
}

func (mp mappingProxy) ReverseQueryTuplesFromSubject(subject *v0.ObjectAndRelation, revision datastore.Revision) datastore.ReverseTupleQuery {
	translatedONR, err := translateONR(subject, mp.mapper.Encode)
	return mappingReverseTupleQuery{mp.delegate.ReverseQueryTuplesFromSubject(translatedONR, revision), mp.mapper, err}
}

func (mp mappingProxy) ReverseQueryTuplesFromSubjectRelation(subjectNamespace, subjectRelation string, revision datastore.Revision) datastore.ReverseTupleQuery {
	translatedNamespace, err := mp.mapper.Encode(subjectNamespace)
	return mappingReverseTupleQuery{
		mp.delegate.ReverseQueryTuplesFromSubjectRelation(translatedNamespace, subjectRelation, revision),
		mp.mapper,
		err,
	}
}

func (mp mappingProxy) CheckRevision(ctx context.Context, revision datastore.Revision) error {
	return mp.delegate.CheckRevision(ctx, revision)
}

func (mp mappingProxy) IsEmpty(ctx context.Context) (bool, error) {
	return mp.delegate.IsEmpty(ctx)
}

type mappingTupleIterator struct {
	delegate datastore.TupleIterator
	mapper   namespace.Mapper
	err      error
}

func (mti *mappingTupleIterator) Next() *v0.RelationTuple {
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

type mappingTupleQuery struct {
	delegate datastore.TupleQuery
	mapper   namespace.Mapper
	err      error
}

func (mtq mappingTupleQuery) Execute(ctx context.Context) (datastore.TupleIterator, error) {
	if mtq.err != nil {
		return nil, mtq.err
	}

	delegateIterator, err := mtq.delegate.Execute(ctx)
	if err != nil {
		return nil, err
	}

	return &mappingTupleIterator{delegateIterator, mtq.mapper, nil}, nil
}

func (mtq mappingTupleQuery) Limit(limit uint64) datastore.CommonTupleQuery {
	return mappingCommonTupleQuery{
		mtq.delegate.Limit(limit),
		mtq.mapper,
		mtq.err,
	}
}

func (mtq mappingTupleQuery) WithObjectID(objectID string) datastore.TupleQuery {
	mtq.delegate = mtq.delegate.WithObjectID(objectID)
	return mtq
}

func (mtq mappingTupleQuery) WithRelation(relation string) datastore.TupleQuery {
	mtq.delegate = mtq.delegate.WithRelation(relation)
	return mtq
}

func (mtq mappingTupleQuery) WithUserset(userset *v0.ObjectAndRelation) datastore.TupleQuery {
	if mtq.err != nil {
		return mtq
	}

	translatedUserset, err := translateONR(userset, mtq.mapper.Encode)
	if err != nil {
		mtq.err = err
		return mtq
	}

	mtq.delegate = mtq.delegate.WithUserset(translatedUserset)
	return mtq
}

func (mtq mappingTupleQuery) WithUsersets(usersets []*v0.ObjectAndRelation) datastore.TupleQuery {
	if mtq.err != nil {
		return mtq
	}

	translatedUsersets := make([]*v0.ObjectAndRelation, 0, len(usersets))
	for _, userset := range usersets {
		translated, err := translateONR(userset, mtq.mapper.Encode)
		if err != nil {
			mtq.err = err
			return mtq
		}

		translatedUsersets = append(translatedUsersets, translated)
	}

	mtq.delegate = mtq.delegate.WithUsersets(translatedUsersets)
	return mtq
}

type mappingReverseTupleQuery struct {
	delegate datastore.ReverseTupleQuery
	mapper   namespace.Mapper
	err      error
}

func (mrtq mappingReverseTupleQuery) Execute(ctx context.Context) (datastore.TupleIterator, error) {
	if mrtq.err != nil {
		return nil, mrtq.err
	}

	delegateIterator, err := mrtq.delegate.Execute(ctx)
	if err != nil {
		return nil, err
	}

	return &mappingTupleIterator{delegateIterator, mrtq.mapper, nil}, nil
}

func (mrtq mappingReverseTupleQuery) Limit(limit uint64) datastore.CommonTupleQuery {
	return mappingCommonTupleQuery{
		mrtq.delegate.Limit(limit),
		mrtq.mapper,
		mrtq.err,
	}
}

func (mrtq mappingReverseTupleQuery) WithObjectRelation(namespace string, relation string) datastore.ReverseTupleQuery {
	if mrtq.err != nil {
		return mrtq
	}

	translatedNamespace, err := mrtq.mapper.Encode(namespace)
	if err != nil {
		mrtq.err = err
		return mrtq
	}

	mrtq.delegate = mrtq.delegate.WithObjectRelation(translatedNamespace, relation)
	return mrtq
}

func translateTuple(in *v0.RelationTuple, mapper func(string) (string, error)) (*v0.RelationTuple, error) {
	translatedObject, err := translateONR(in.ObjectAndRelation, mapper)
	if err != nil {
		return nil, err
	}

	userset := in.User.GetUserset()
	translatedUserset, err := translateONR(userset, mapper)
	if err != nil {
		return nil, err
	}

	return &v0.RelationTuple{
		ObjectAndRelation: translatedObject,
		User: &v0.User{
			UserOneof: &v0.User_Userset{
				Userset: translatedUserset,
			},
		},
	}, nil
}

func translateONR(in *v0.ObjectAndRelation, mapper func(string) (string, error)) (*v0.ObjectAndRelation, error) {
	newNamespace, err := mapper(in.Namespace)
	if err != nil {
		return nil, err
	}

	return &v0.ObjectAndRelation{
		Namespace: newNamespace,
		ObjectId:  in.ObjectId,
		Relation:  in.Relation,
	}, nil
}

type mappingCommonTupleQuery struct {
	delegate datastore.CommonTupleQuery
	mapper   namespace.Mapper
	err      error
}

func (mctq mappingCommonTupleQuery) Limit(limit uint64) datastore.CommonTupleQuery {
	mctq.delegate = mctq.delegate.Limit(limit)
	return mctq
}

func (mctq mappingCommonTupleQuery) Execute(ctx context.Context) (datastore.TupleIterator, error) {
	if mctq.err != nil {
		return nil, mctq.err
	}

	delegateIterator, err := mctq.delegate.Execute(ctx)
	if err != nil {
		return nil, err
	}

	return &mappingTupleIterator{delegateIterator, mctq.mapper, nil}, nil
}
