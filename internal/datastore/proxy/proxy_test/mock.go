package proxy_test

import (
	"context"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/stretchr/testify/mock"

	"github.com/authzed/spicedb/internal/datastore/options"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

type MockDatastore struct {
	mock.Mock
}

func (dm *MockDatastore) SnapshotReader(rev datastore.Revision) datastore.Reader {
	args := dm.Called(rev)
	return args.Get(0).(datastore.Reader)
}

func (dm *MockDatastore) ReadWriteTx(
	ctx context.Context,
	f datastore.TxUserFunc,
) (datastore.Revision, error) {
	args := dm.Called()
	mockRWT := args.Get(0).(datastore.ReadWriteTransaction)

	if err := f(ctx, mockRWT); err != nil {
		return datastore.NoRevision, err
	}

	return args.Get(1).(datastore.Revision), args.Error(2)
}

func (dm *MockDatastore) OptimizedRevision(ctx context.Context) (datastore.Revision, error) {
	args := dm.Called()
	return args.Get(0).(datastore.Revision), args.Error(1)
}

func (dm *MockDatastore) HeadRevision(ctx context.Context) (datastore.Revision, error) {
	args := dm.Called()
	return args.Get(0).(datastore.Revision), args.Error(1)
}

func (dm *MockDatastore) CheckRevision(ctx context.Context, revision datastore.Revision) error {
	args := dm.Called(revision)
	return args.Error(0)
}

func (dm *MockDatastore) Watch(ctx context.Context, afterRevision datastore.Revision) (<-chan *datastore.RevisionChanges, <-chan error) {
	args := dm.Called(afterRevision)
	return args.Get(0).(<-chan *datastore.RevisionChanges), args.Get(1).(<-chan error)
}

func (dm *MockDatastore) IsReady(ctx context.Context) (bool, error) {
	args := dm.Called()
	return args.Bool(0), args.Error(1)
}

func (dm *MockDatastore) Features(ctx context.Context) (*datastore.Features, error) {
	args := dm.Called()
	return args.Get(0).(*datastore.Features), args.Error(1)
}

func (dm *MockDatastore) Statistics(ctx context.Context) (datastore.Stats, error) {
	args := dm.Called()
	return args.Get(0).(datastore.Stats), args.Error(1)
}

func (dm *MockDatastore) Close() error {
	args := dm.Called()
	return args.Error(0)
}

type MockReader struct {
	mock.Mock
}

func (dm *MockReader) ReadNamespace(
	ctx context.Context,
	nsName string,
) (*core.NamespaceDefinition, datastore.Revision, error) {
	args := dm.Called(nsName)

	var def *core.NamespaceDefinition
	if args.Get(0) != nil {
		def = args.Get(0).(*core.NamespaceDefinition)
	}

	return def, args.Get(1).(datastore.Revision), args.Error(2)
}

func (dm *MockReader) QueryRelationships(
	ctx context.Context,
	filter datastore.RelationshipsFilter,
	options ...options.QueryOptionsOption,
) (datastore.RelationshipIterator, error) {
	callArgs := make([]interface{}, 0, len(options)+1)
	callArgs = append(callArgs, filter)
	for _, option := range options {
		callArgs = append(callArgs, option)
	}

	args := dm.Called(callArgs...)
	var results datastore.RelationshipIterator
	if args.Get(0) != nil {
		results = args.Get(0).(datastore.RelationshipIterator)
	}

	return results, args.Error(1)
}

func (dm *MockReader) ReverseQueryRelationships(
	ctx context.Context,
	subjectsFilter datastore.SubjectsFilter,
	options ...options.ReverseQueryOptionsOption,
) (datastore.RelationshipIterator, error) {
	callArgs := make([]interface{}, 0, len(options)+1)
	callArgs = append(callArgs, subjectsFilter)
	for _, option := range options {
		callArgs = append(callArgs, option)
	}

	args := dm.Called(callArgs...)
	var results datastore.RelationshipIterator
	if args.Get(0) != nil {
		results = args.Get(0).(datastore.RelationshipIterator)
	}

	return results, args.Error(1)
}

func (dm *MockReader) ListNamespaces(ctx context.Context) ([]*core.NamespaceDefinition, error) {
	args := dm.Called()
	return args.Get(0).([]*core.NamespaceDefinition), args.Error(1)
}

type MockReadWriteTransaction struct {
	mock.Mock
}

func (dm *MockReadWriteTransaction) ReadNamespace(
	ctx context.Context,
	nsName string,
) (*core.NamespaceDefinition, datastore.Revision, error) {
	args := dm.Called(nsName)

	var def *core.NamespaceDefinition
	if args.Get(0) != nil {
		def = args.Get(0).(*core.NamespaceDefinition)
	}

	return def, args.Get(1).(datastore.Revision), args.Error(2)
}

func (dm *MockReadWriteTransaction) QueryRelationships(
	ctx context.Context,
	filter datastore.RelationshipsFilter,
	options ...options.QueryOptionsOption,
) (datastore.RelationshipIterator, error) {
	callArgs := make([]interface{}, 0, len(options)+1)
	callArgs = append(callArgs, filter)
	for _, option := range options {
		callArgs = append(callArgs, option)
	}

	args := dm.Called(callArgs...)
	var results datastore.RelationshipIterator
	if args.Get(0) != nil {
		results = args.Get(0).(datastore.RelationshipIterator)
	}

	return results, args.Error(1)
}

func (dm *MockReadWriteTransaction) ReverseQueryRelationships(
	ctx context.Context,
	subjectsFilter datastore.SubjectsFilter,
	options ...options.ReverseQueryOptionsOption,
) (datastore.RelationshipIterator, error) {
	callArgs := make([]interface{}, 0, len(options)+1)
	callArgs = append(callArgs, subjectsFilter)
	for _, option := range options {
		callArgs = append(callArgs, option)
	}

	args := dm.Called(callArgs...)
	var results datastore.RelationshipIterator
	if args.Get(0) != nil {
		results = args.Get(0).(datastore.RelationshipIterator)
	}

	return results, args.Error(1)
}

func (dm *MockReadWriteTransaction) ListNamespaces(ctx context.Context) ([]*core.NamespaceDefinition, error) {
	args := dm.Called()
	return args.Get(0).([]*core.NamespaceDefinition), args.Error(1)
}

func (dm *MockReadWriteTransaction) WriteRelationships(mutations []*core.RelationTupleUpdate) error {
	args := dm.Called(mutations)
	return args.Error(0)
}

func (dm *MockReadWriteTransaction) DeleteRelationships(filter *v1.RelationshipFilter) error {
	args := dm.Called(filter)
	return args.Error(0)
}

func (dm *MockReadWriteTransaction) WriteNamespaces(newConfigs ...*core.NamespaceDefinition) error {
	args := dm.Called(newConfigs)
	return args.Error(0)
}

func (dm *MockReadWriteTransaction) DeleteNamespace(nsName string) error {
	args := dm.Called(nsName)
	return args.Error(0)
}

var (
	_ datastore.Datastore            = &MockDatastore{}
	_ datastore.Reader               = &MockReader{}
	_ datastore.ReadWriteTransaction = &MockReadWriteTransaction{}
)
