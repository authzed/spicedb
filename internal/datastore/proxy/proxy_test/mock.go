package proxy_test

import (
	"context"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/ccoveille/go-safecast"
	"github.com/stretchr/testify/mock"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

type MockDatastore struct {
	mock.Mock
}

func (dm *MockDatastore) SnapshotReader(rev datastore.Revision) datastore.Reader {
	args := dm.Called(rev)
	return args.Get(0).(datastore.Reader)
}

func (dm *MockDatastore) MetricsID() (string, error) {
	return "mock", nil
}

func (dm *MockDatastore) ReadWriteTx(
	ctx context.Context,
	f datastore.TxUserFunc,
	opts ...options.RWTOptionsOption,
) (datastore.Revision, error) {
	args := dm.Called(opts)
	mockRWT := args.Get(0).(datastore.ReadWriteTransaction)

	if err := f(ctx, mockRWT); err != nil {
		return datastore.NoRevision, err
	}

	return args.Get(1).(datastore.Revision), args.Error(2)
}

func (dm *MockDatastore) OptimizedRevision(_ context.Context) (datastore.Revision, error) {
	args := dm.Called()
	return args.Get(0).(datastore.Revision), args.Error(1)
}

func (dm *MockDatastore) HeadRevision(_ context.Context) (datastore.Revision, error) {
	args := dm.Called()
	return args.Get(0).(datastore.Revision), args.Error(1)
}

func (dm *MockDatastore) CheckRevision(_ context.Context, revision datastore.Revision) error {
	args := dm.Called(revision)
	return args.Error(0)
}

func (dm *MockDatastore) RevisionFromString(s string) (datastore.Revision, error) {
	args := dm.Called(s)
	return args.Get(0).(datastore.Revision), args.Error(1)
}

func (dm *MockDatastore) Watch(_ context.Context, afterRevision datastore.Revision, _ datastore.WatchOptions) (<-chan datastore.RevisionChanges, <-chan error) {
	args := dm.Called(afterRevision)
	return args.Get(0).(<-chan datastore.RevisionChanges), args.Get(1).(<-chan error)
}

func (dm *MockDatastore) ReadyState(_ context.Context) (datastore.ReadyState, error) {
	args := dm.Called()
	return args.Get(0).(datastore.ReadyState), args.Error(1)
}

func (dm *MockDatastore) Features(_ context.Context) (*datastore.Features, error) {
	args := dm.Called()
	return args.Get(0).(*datastore.Features), args.Error(1)
}

func (dm *MockDatastore) OfflineFeatures() (*datastore.Features, error) {
	args := dm.Called()
	return args.Get(0).(*datastore.Features), args.Error(1)
}

func (dm *MockDatastore) Statistics(_ context.Context) (datastore.Stats, error) {
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
	_ context.Context,
	nsName string,
) (*core.NamespaceDefinition, datastore.Revision, error) {
	args := dm.Called(nsName)

	var def *core.NamespaceDefinition
	if args.Get(0) != nil {
		def = args.Get(0).(*core.NamespaceDefinition)
	}

	return def, args.Get(1).(datastore.Revision), args.Error(2)
}

func (dm *MockReader) CountRelationships(ctx context.Context, name string) (int, error) {
	args := dm.Called(name)
	return args.Get(0).(int), args.Error(1)
}

func (dm *MockReader) LookupCounters(ctx context.Context) ([]datastore.RelationshipCounter, error) {
	args := dm.Called()
	return args.Get(0).([]datastore.RelationshipCounter), args.Error(1)
}

func (dm *MockReader) QueryRelationships(
	_ context.Context,
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
	_ context.Context,
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

func (dm *MockReader) ListAllNamespaces(_ context.Context) ([]datastore.RevisionedNamespace, error) {
	args := dm.Called()
	return args.Get(0).([]datastore.RevisionedNamespace), args.Error(1)
}

func (dm *MockReader) LookupNamespacesWithNames(_ context.Context, nsNames []string) ([]datastore.RevisionedNamespace, error) {
	args := dm.Called(nsNames)
	return args.Get(0).([]datastore.RevisionedNamespace), args.Error(1)
}

func (dm *MockReader) ReadCaveatByName(_ context.Context, name string) (*core.CaveatDefinition, datastore.Revision, error) {
	args := dm.Called(name)

	var def *core.CaveatDefinition
	if args.Get(0) != nil {
		def = args.Get(0).(*core.CaveatDefinition)
	}

	return def, args.Get(1).(datastore.Revision), args.Error(2)
}

func (dm *MockReader) LookupCaveatsWithNames(_ context.Context, caveatNames []string) ([]datastore.RevisionedCaveat, error) {
	args := dm.Called(caveatNames)
	return args.Get(0).([]datastore.RevisionedCaveat), args.Error(1)
}

func (dm *MockReader) ListAllCaveats(_ context.Context) ([]datastore.RevisionedCaveat, error) {
	args := dm.Called()
	return args.Get(0).([]datastore.RevisionedCaveat), args.Error(1)
}

type MockReadWriteTransaction struct {
	mock.Mock
}

func (dm *MockReadWriteTransaction) CountRelationships(ctx context.Context, name string) (int, error) {
	args := dm.Called(name)
	return args.Get(0).(int), args.Error(1)
}

func (dm *MockReadWriteTransaction) LookupCounters(ctx context.Context) ([]datastore.RelationshipCounter, error) {
	args := dm.Called()
	return args.Get(0).([]datastore.RelationshipCounter), args.Error(1)
}

func (dm *MockReadWriteTransaction) ReadNamespaceByName(
	_ context.Context,
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
	_ context.Context,
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
	_ context.Context,
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

func (dm *MockReadWriteTransaction) ListAllNamespaces(_ context.Context) ([]datastore.RevisionedNamespace, error) {
	args := dm.Called()
	return args.Get(0).([]datastore.RevisionedNamespace), args.Error(1)
}

func (dm *MockReadWriteTransaction) LookupNamespacesWithNames(_ context.Context, nsNames []string) ([]datastore.RevisionedNamespace, error) {
	args := dm.Called(nsNames)
	return args.Get(0).([]datastore.RevisionedNamespace), args.Error(1)
}

func (dm *MockReadWriteTransaction) WriteRelationships(_ context.Context, mutations []tuple.RelationshipUpdate) error {
	args := dm.Called(mutations)
	return args.Error(0)
}

func (dm *MockReadWriteTransaction) DeleteRelationships(_ context.Context, filter *v1.RelationshipFilter, options ...options.DeleteOptionsOption) (uint64, bool, error) {
	args := dm.Called(filter)
	return 0, false, args.Error(0)
}

func (dm *MockReadWriteTransaction) WriteNamespaces(_ context.Context, newConfigs ...*core.NamespaceDefinition) error {
	args := dm.Called(newConfigs)
	return args.Error(0)
}

func (dm *MockReadWriteTransaction) DeleteNamespaces(_ context.Context, nsNames ...string) error {
	xs := make([]any, 0, len(nsNames))
	for _, nsName := range nsNames {
		xs = append(xs, nsName)
	}

	args := dm.Called(xs...)
	return args.Error(0)
}

func (dm *MockReadWriteTransaction) BulkLoad(_ context.Context, iter datastore.BulkWriteRelationshipSource) (uint64, error) {
	args := dm.Called(iter)
	// We're assuming this is non-negative.
	uintArg, _ := safecast.ToUint64(args.Int(0))
	return uintArg, args.Error(1)
}

func (dm *MockReadWriteTransaction) ReadCaveatByName(_ context.Context, name string) (*core.CaveatDefinition, datastore.Revision, error) {
	args := dm.Called(name)

	var def *core.CaveatDefinition
	if args.Get(0) != nil {
		def = args.Get(0).(*core.CaveatDefinition)
	}

	return def, args.Get(1).(datastore.Revision), args.Error(2)
}

func (dm *MockReadWriteTransaction) LookupCaveatsWithNames(_ context.Context, caveatNames []string) ([]datastore.RevisionedCaveat, error) {
	args := dm.Called(caveatNames)
	return args.Get(0).([]datastore.RevisionedCaveat), args.Error(1)
}

func (dm *MockReadWriteTransaction) ListAllCaveats(_ context.Context) ([]datastore.RevisionedCaveat, error) {
	args := dm.Called()
	return args.Get(0).([]datastore.RevisionedCaveat), args.Error(1)
}

func (dm *MockReadWriteTransaction) WriteCaveats(_ context.Context, caveats []*core.CaveatDefinition) error {
	args := dm.Called(caveats)
	return args.Error(0)
}

func (dm *MockReadWriteTransaction) DeleteCaveats(_ context.Context, _ []string) error {
	panic("not used")
}

func (dm *MockReadWriteTransaction) RegisterCounter(ctx context.Context, name string, filter *core.RelationshipFilter) error {
	args := dm.Called(name, filter)
	return args.Error(0)
}

func (dm *MockReadWriteTransaction) UnregisterCounter(ctx context.Context, name string) error {
	args := dm.Called(name)
	return args.Error(0)
}

func (dm *MockReadWriteTransaction) StoreCounterValue(ctx context.Context, name string, value int, computedAtRevision datastore.Revision) error {
	args := dm.Called(name, value, computedAtRevision)
	return args.Error(0)
}

var (
	_ datastore.Datastore            = &MockDatastore{}
	_ datastore.Reader               = &MockReader{}
	_ datastore.ReadWriteTransaction = &MockReadWriteTransaction{}
)
