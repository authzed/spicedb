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

	if err := f(mockRWT); err != nil {
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

func (dm *MockDatastore) RevisionFromString(s string) (datastore.Revision, error) {
	args := dm.Called(s)
	return args.Get(0).(datastore.Revision), args.Error(1)
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

func (dm *MockReader) ReadNamespaceByName(
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

func (dm *MockReader) ListAllNamespaces(ctx context.Context) ([]datastore.RevisionedNamespace, error) {
	args := dm.Called()
	return args.Get(0).([]datastore.RevisionedNamespace), args.Error(1)
}

func (dm *MockReader) LookupNamespacesWithNames(ctx context.Context, nsNames []string) ([]datastore.RevisionedNamespace, error) {
	args := dm.Called(nsNames)
	return args.Get(0).([]datastore.RevisionedNamespace), args.Error(1)
}

func (dm *MockReader) ReadCaveatByName(ctx context.Context, name string) (*core.CaveatDefinition, datastore.Revision, error) {
	args := dm.Called(name)

	var def *core.CaveatDefinition
	if args.Get(0) != nil {
		def = args.Get(0).(*core.CaveatDefinition)
	}

	return def, args.Get(1).(datastore.Revision), args.Error(2)
}

func (dm *MockReader) LookupCaveatsWithNames(ctx context.Context, caveatNames []string) ([]datastore.RevisionedCaveat, error) {
	args := dm.Called(caveatNames)
	return args.Get(0).([]datastore.RevisionedCaveat), args.Error(1)
}

func (dm *MockReader) ListAllCaveats(ctx context.Context) ([]datastore.RevisionedCaveat, error) {
	args := dm.Called()
	return args.Get(0).([]datastore.RevisionedCaveat), args.Error(1)
}

type MockReadWriteTransaction struct {
	mock.Mock
}

func (dm *MockReadWriteTransaction) ReadNamespaceByName(
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

func (dm *MockReadWriteTransaction) ListAllNamespaces(ctx context.Context) ([]datastore.RevisionedNamespace, error) {
	args := dm.Called()
	return args.Get(0).([]datastore.RevisionedNamespace), args.Error(1)
}

func (dm *MockReadWriteTransaction) LookupNamespacesWithNames(ctx context.Context, nsNames []string) ([]datastore.RevisionedNamespace, error) {
	args := dm.Called(nsNames)
	return args.Get(0).([]datastore.RevisionedNamespace), args.Error(1)
}

func (dm *MockReadWriteTransaction) WriteRelationships(ctx context.Context, mutations []*core.RelationTupleUpdate) error {
	args := dm.Called(mutations)
	return args.Error(0)
}

func (dm *MockReadWriteTransaction) DeleteRelationships(ctx context.Context, filter *v1.RelationshipFilter) error {
	args := dm.Called(filter)
	return args.Error(0)
}

func (dm *MockReadWriteTransaction) WriteNamespaces(ctx context.Context, newConfigs ...*core.NamespaceDefinition) error {
	args := dm.Called(newConfigs)
	return args.Error(0)
}

func (dm *MockReadWriteTransaction) DeleteNamespaces(ctx context.Context, nsNames ...string) error {
	xs := make([]any, 0, len(nsNames))
	for _, nsName := range nsNames {
		xs = append(xs, nsName)
	}

	args := dm.Called(xs...)
	return args.Error(0)
}

func (dm *MockReadWriteTransaction) ReadCaveatByName(ctx context.Context, name string) (*core.CaveatDefinition, datastore.Revision, error) {
	args := dm.Called(name)

	var def *core.CaveatDefinition
	if args.Get(0) != nil {
		def = args.Get(0).(*core.CaveatDefinition)
	}

	return def, args.Get(1).(datastore.Revision), args.Error(2)
}

func (dm *MockReadWriteTransaction) LookupCaveatsWithNames(ctx context.Context, caveatNames []string) ([]datastore.RevisionedCaveat, error) {
	args := dm.Called(caveatNames)
	return args.Get(0).([]datastore.RevisionedCaveat), args.Error(1)
}

func (dm *MockReadWriteTransaction) ListAllCaveats(ctx context.Context) ([]datastore.RevisionedCaveat, error) {
	args := dm.Called()
	return args.Get(0).([]datastore.RevisionedCaveat), args.Error(1)
}

func (dm *MockReadWriteTransaction) WriteCaveats(ctx context.Context, caveats []*core.CaveatDefinition) error {
	args := dm.Called(caveats)
	return args.Error(0)
}

func (dm *MockReadWriteTransaction) DeleteCaveats(ctx context.Context, names []string) error {
	panic("not used")
}

var (
	_ datastore.Datastore            = &MockDatastore{}
	_ datastore.Reader               = &MockReader{}
	_ datastore.ReadWriteTransaction = &MockReadWriteTransaction{}
)
