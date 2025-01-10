package schemacaching

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/authzed/spicedb/internal/datastore/dsfortesting"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/datastore/proxy/proxy_test"
	"github.com/authzed/spicedb/internal/datastore/revisions"
	"github.com/authzed/spicedb/pkg/caveats"
	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/genutil/mapz"
	ns "github.com/authzed/spicedb/pkg/namespace"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/testutil"
)

var (
	old  = revisions.NewForTransactionID(0)
	zero = revisions.NewForTransactionID(1)
	one  = revisions.NewForTransactionID(2)
	two  = revisions.NewForTransactionID(3)

	nilOpts []options.RWTOptionsOption
)

const (
	nsA = "namespace_a"
	nsB = "namespace_b"
)

// TestNilUnmarshal asserts that if we get a nil NamespaceDefinition from a
// datastore implementation, the process of inserting it into the cache and
// back does not break anything.
func TestNilUnmarshal(t *testing.T) {
	nsDef := (*core.NamespaceDefinition)(nil)
	marshalled, err := nsDef.MarshalVT()
	require.Nil(t, err)

	var newDef *core.NamespaceDefinition
	err = nsDef.UnmarshalVT(marshalled)
	require.Nil(t, err)
	require.Equal(t, nsDef, newDef)
}

type testerDef struct {
	name                   string
	readSingleFunctionName string
	readSingleFunc         func(ctx context.Context, reader datastore.Reader, name string) (datastore.SchemaDefinition, datastore.Revision, error)

	lookupFunctionName string
	lookupFunc         func(ctx context.Context, reader datastore.Reader, names []string) ([]datastore.SchemaDefinition, error)

	notFoundErr error

	writeFunctionName string
	writeFunc         func(rwt datastore.ReadWriteTransaction, def datastore.SchemaDefinition) error

	createDef      func(name string) datastore.SchemaDefinition
	wrap           func(def datastore.SchemaDefinition) any
	wrapRevisioned func(def datastore.SchemaDefinition) any
}

var testers = []testerDef{
	{
		"namespace",

		"ReadNamespaceByName",
		func(ctx context.Context, reader datastore.Reader, name string) (datastore.SchemaDefinition, datastore.Revision, error) {
			return reader.ReadNamespaceByName(ctx, name)
		},

		"LookupNamespacesWithNames",
		func(ctx context.Context, reader datastore.Reader, names []string) ([]datastore.SchemaDefinition, error) {
			defs, err := reader.LookupNamespacesWithNames(ctx, names)
			if err != nil {
				return nil, err
			}
			schemaDefs := []datastore.SchemaDefinition{}
			for _, def := range defs {
				schemaDefs = append(schemaDefs, def.Definition)
			}
			return schemaDefs, nil
		},

		datastore.NamespaceNotFoundError{},

		"WriteNamespaces",
		func(rwt datastore.ReadWriteTransaction, def datastore.SchemaDefinition) error {
			return rwt.WriteNamespaces(context.Background(), def.(*core.NamespaceDefinition))
		},

		func(name string) datastore.SchemaDefinition { return &core.NamespaceDefinition{Name: name} },
		func(def datastore.SchemaDefinition) any {
			return []*core.NamespaceDefinition{def.(*core.NamespaceDefinition)}
		},
		func(def datastore.SchemaDefinition) any {
			return []datastore.RevisionedNamespace{{Definition: def.(*core.NamespaceDefinition)}}
		},
	},
	{
		"caveat",
		"ReadCaveatByName",
		func(ctx context.Context, reader datastore.Reader, name string) (datastore.SchemaDefinition, datastore.Revision, error) {
			return reader.ReadCaveatByName(ctx, name)
		},

		"LookupCaveatsWithNames",
		func(ctx context.Context, reader datastore.Reader, names []string) ([]datastore.SchemaDefinition, error) {
			defs, err := reader.LookupCaveatsWithNames(ctx, names)
			if err != nil {
				return nil, err
			}
			schemaDefs := []datastore.SchemaDefinition{}
			for _, def := range defs {
				schemaDefs = append(schemaDefs, def.Definition)
			}
			return schemaDefs, nil
		},

		datastore.CaveatNameNotFoundError{},

		"WriteCaveats",
		func(rwt datastore.ReadWriteTransaction, def datastore.SchemaDefinition) error {
			return rwt.WriteCaveats(context.Background(), []*core.CaveatDefinition{def.(*core.CaveatDefinition)})
		},

		func(name string) datastore.SchemaDefinition { return &core.CaveatDefinition{Name: name} },
		func(def datastore.SchemaDefinition) any {
			return []*core.CaveatDefinition{def.(*core.CaveatDefinition)}
		},
		func(def datastore.SchemaDefinition) any {
			return []datastore.RevisionedCaveat{{Definition: def.(*core.CaveatDefinition)}}
		},
	},
}

func TestSnapshotCaching(t *testing.T) {
	for _, tester := range testers {
		tester := tester
		t.Run(tester.name, func(t *testing.T) {
			dsMock := &proxy_test.MockDatastore{}

			oneReader := &proxy_test.MockReader{}
			dsMock.On("SnapshotReader", one).Return(oneReader)
			oneReader.On(tester.readSingleFunctionName, nsA).Return(nil, old, nil).Once()
			oneReader.On(tester.readSingleFunctionName, nsB).Return(nil, zero, nil).Once()

			twoReader := &proxy_test.MockReader{}
			dsMock.On("SnapshotReader", two).Return(twoReader)
			twoReader.On(tester.readSingleFunctionName, nsA).Return(nil, zero, nil).Once()
			twoReader.On(tester.readSingleFunctionName, nsB).Return(nil, one, nil).Once()

			require := require.New(t)
			ds := NewCachingDatastoreProxy(dsMock, DatastoreProxyTestCache(t), 1*time.Hour, JustInTimeCaching, 100*time.Millisecond)

			_, updatedOneA, err := tester.readSingleFunc(context.Background(), ds.SnapshotReader(one), nsA)
			require.NoError(err)
			require.True(old.Equal(updatedOneA))

			_, updatedOneAAgain, err := tester.readSingleFunc(context.Background(), ds.SnapshotReader(one), nsA)
			require.NoError(err)
			require.True(old.Equal(updatedOneAAgain))

			_, updatedOneB, err := tester.readSingleFunc(context.Background(), ds.SnapshotReader(one), nsB)
			require.NoError(err)
			require.True(zero.Equal(updatedOneB))

			_, updatedOneBAgain, err := tester.readSingleFunc(context.Background(), ds.SnapshotReader(one), nsB)
			require.NoError(err)
			require.True(zero.Equal(updatedOneBAgain))

			_, updatedTwoA, err := tester.readSingleFunc(context.Background(), ds.SnapshotReader(two), nsA)
			require.NoError(err)
			require.True(zero.Equal(updatedTwoA))

			_, updatedTwoAAgain, err := tester.readSingleFunc(context.Background(), ds.SnapshotReader(two), nsA)
			require.NoError(err)
			require.True(zero.Equal(updatedTwoAAgain))

			_, updatedTwoB, err := tester.readSingleFunc(context.Background(), ds.SnapshotReader(two), nsB)
			require.NoError(err)
			require.True(one.Equal(updatedTwoB))

			_, updatedTwoBAgain, err := tester.readSingleFunc(context.Background(), ds.SnapshotReader(two), nsB)
			require.NoError(err)
			require.True(one.Equal(updatedTwoBAgain))

			dsMock.AssertExpectations(t)
			oneReader.AssertExpectations(t)
			twoReader.AssertExpectations(t)
		})
	}
}

func TestRWTCaching(t *testing.T) {
	for _, tester := range testers {
		tester := tester
		t.Run(tester.name, func(t *testing.T) {
			dsMock := &proxy_test.MockDatastore{}
			rwtMock := &proxy_test.MockReadWriteTransaction{}

			require := require.New(t)

			dsMock.On("ReadWriteTx", nilOpts).Return(rwtMock, one, nil).Once()
			rwtMock.On(tester.readSingleFunctionName, nsA).Return(nil, zero, nil).Once()

			ctx := context.Background()

			ds := NewCachingDatastoreProxy(dsMock, nil, 1*time.Hour, JustInTimeCaching, 100*time.Millisecond)

			rev, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
				_, updatedA, err := tester.readSingleFunc(ctx, rwt, nsA)
				require.NoError(err)
				require.True(zero.Equal(updatedA))

				// This will not call out the mock RWT again, the mock will panic if it does.
				_, updatedA, err = tester.readSingleFunc(ctx, rwt, nsA)
				require.NoError(err)
				require.True(zero.Equal(updatedA))

				return nil
			})
			require.True(one.Equal(rev))
			require.NoError(err)

			dsMock.AssertExpectations(t)
			rwtMock.AssertExpectations(t)
		})
	}
}

func TestRWTCacheWithWrites(t *testing.T) {
	for _, tester := range testers {
		tester := tester
		t.Run(tester.name, func(t *testing.T) {
			dsMock := &proxy_test.MockDatastore{}
			rwtMock := &proxy_test.MockReadWriteTransaction{}

			require := require.New(t)

			dsMock.On("ReadWriteTx", nilOpts).Return(rwtMock, one, nil).Once()
			rwtMock.On(tester.readSingleFunctionName, nsA).Return(nil, zero, tester.notFoundErr).Once()

			ctx := context.Background()

			ds := NewCachingDatastoreProxy(dsMock, nil, 1*time.Hour, JustInTimeCaching, 100*time.Millisecond)

			rev, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
				// Cache the 404
				_, _, err := tester.readSingleFunc(ctx, rwt, nsA)
				require.Error(err, tester.notFoundErr)

				// This will not call out the mock RWT again, the mock will panic if it does.
				_, _, err = tester.readSingleFunc(ctx, rwt, nsA)
				require.Error(err, tester.notFoundErr)

				// Write nsA
				def := tester.createDef(nsA)
				rwtMock.On(tester.writeFunctionName, tester.wrap(def)).Return(nil).Once()
				require.NoError(tester.writeFunc(rwt, def))

				// Call Read* on nsA and we should flow through to the mock
				rwtMock.On(tester.readSingleFunctionName, nsA).Return(def, zero, nil).Once()
				def, updatedA, err := tester.readSingleFunc(ctx, rwt, nsA)

				require.True(updatedA.Equal(zero))
				require.NotNil(def)
				require.NoError(err)

				return nil
			})
			require.True(one.Equal(rev))
			require.NoError(err)

			dsMock.AssertExpectations(t)
			rwtMock.AssertExpectations(t)
		})
	}
}

func TestSingleFlight(t *testing.T) {
	for _, tester := range testers {
		tester := tester
		t.Run(tester.name, func(t *testing.T) {
			dsMock := &proxy_test.MockDatastore{}

			oneReader := &proxy_test.MockReader{}
			dsMock.On("SnapshotReader", one).Return(oneReader)
			oneReader.
				On(tester.readSingleFunctionName, nsA).
				WaitUntil(time.After(50*time.Millisecond)).
				Return(nil, old, nil).
				Once()

			require := require.New(t)

			ds := NewCachingDatastoreProxy(dsMock, nil, 1*time.Hour, JustInTimeCaching, 100*time.Millisecond)

			readNamespace := func() error {
				_, updatedAt, err := tester.readSingleFunc(context.Background(), ds.SnapshotReader(one), nsA)
				require.NoError(err)
				require.True(old.Equal(updatedAt))
				return err
			}

			g := errgroup.Group{}
			g.Go(readNamespace)
			g.Go(readNamespace)

			require.NoError(g.Wait())

			dsMock.AssertExpectations(t)
			oneReader.AssertExpectations(t)
		})
	}
}

func TestSnapshotCachingRealDatastore(t *testing.T) {
	tcs := []struct {
		name          string
		nsDef         *core.NamespaceDefinition
		namespaceName string
		caveatDef     *core.CaveatDefinition
		caveatName    string
	}{
		{
			"missing",
			nil,
			"somenamespace",
			nil,
			"somecaveat",
		},
		{
			"defined",
			ns.Namespace(
				"document",
				ns.MustRelation("owner",
					nil,
					ns.AllowedRelation("user", "..."),
				),
				ns.MustRelation("editor",
					nil,
					ns.AllowedRelation("user", "..."),
				),
			),
			"document",
			ns.MustCaveatDefinition(caveats.MustEnvForVariables(
				map[string]caveattypes.VariableType{
					"somevar": caveattypes.IntType,
				},
			), "somecaveat", "somevar < 42"),
			"somecaveat",
		},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(0, 0, memdb.DisableGC)
			require.NoError(t, err)

			ctx := context.Background()
			ds := NewCachingDatastoreProxy(rawDS, nil, 1*time.Hour, JustInTimeCaching, 100*time.Millisecond)

			if tc.nsDef != nil {
				_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
					err := rwt.WriteNamespaces(ctx, tc.nsDef)
					if err != nil {
						return err
					}

					return rwt.WriteCaveats(ctx, []*core.CaveatDefinition{tc.caveatDef})
				})
				require.NoError(t, err)
			}

			headRev, err := ds.HeadRevision(ctx)
			require.NoError(t, err)

			reader := ds.SnapshotReader(headRev)
			ns, _, _ := reader.ReadNamespaceByName(ctx, tc.namespaceName)
			testutil.RequireProtoEqual(t, tc.nsDef, ns, "found different namespaces")

			ns2, _, _ := reader.ReadNamespaceByName(ctx, tc.namespaceName)
			testutil.RequireProtoEqual(t, tc.nsDef, ns2, "found different namespaces")

			c1, _, _ := reader.ReadCaveatByName(ctx, tc.caveatName)
			testutil.RequireProtoEqual(t, tc.caveatDef, c1, "found different caveats")

			c2, _, _ := reader.ReadCaveatByName(ctx, tc.caveatName)
			testutil.RequireProtoEqual(t, tc.caveatDef, c2, "found different caveats")
		})
	}
}

type reader struct {
	proxy_test.MockReader
}

func (r *reader) ReadNamespaceByName(ctx context.Context, namespace string) (ns *core.NamespaceDefinition, lastWritten datastore.Revision, err error) {
	time.Sleep(10 * time.Millisecond)
	if errors.Is(ctx.Err(), context.Canceled) {
		return nil, old, fmt.Errorf("error")
	}
	return &core.NamespaceDefinition{Name: namespace}, old, nil
}

func (r *reader) ReadCaveatByName(ctx context.Context, name string) (*core.CaveatDefinition, datastore.Revision, error) {
	time.Sleep(10 * time.Millisecond)
	if errors.Is(ctx.Err(), context.Canceled) {
		return nil, old, fmt.Errorf("error")
	}
	return &core.CaveatDefinition{Name: name}, old, nil
}

func TestSingleFlightCancelled(t *testing.T) {
	for _, tester := range testers {
		tester := tester
		t.Run(tester.name, func(t *testing.T) {
			dsMock := &proxy_test.MockDatastore{}
			ctx1, cancel1 := context.WithCancel(context.Background())
			ctx2, cancel2 := context.WithCancel(context.Background())
			defer cancel2()
			defer cancel1()

			dsMock.On("SnapshotReader", one).Return(&reader{MockReader: proxy_test.MockReader{}})

			ds := NewCachingDatastoreProxy(dsMock, nil, 1*time.Hour, JustInTimeCaching, 100*time.Millisecond)

			g := sync.WaitGroup{}
			var d2 datastore.SchemaDefinition
			g.Add(2)
			go func() {
				_, _, _ = tester.readSingleFunc(ctx1, ds.SnapshotReader(one), nsA)
				g.Done()
			}()
			go func() {
				time.Sleep(5 * time.Millisecond)
				d2, _, _ = tester.readSingleFunc(ctx2, ds.SnapshotReader(one), nsA)
				g.Done()
			}()
			cancel1()

			g.Wait()
			require.NotNil(t, d2)
			require.Equal(t, nsA, d2.GetName())

			dsMock.AssertExpectations(t)
		})
	}
}

func TestMixedCaching(t *testing.T) {
	for _, tester := range testers {
		tester := tester
		t.Run(tester.name, func(t *testing.T) {
			dsMock := &proxy_test.MockDatastore{}

			defA := tester.createDef(nsA)
			defB := tester.createDef(nsB)

			reader := &proxy_test.MockReader{}
			reader.On(tester.readSingleFunctionName, nsA).Return(defA, old, nil).Once()
			reader.On(tester.lookupFunctionName, []string{nsB}).Return(tester.wrapRevisioned(defB), nil).Once()

			dsMock.On("SnapshotReader", one).Return(reader)

			require := require.New(t)
			ds := NewCachingDatastoreProxy(dsMock, DatastoreProxyTestCache(t), 1*time.Hour, JustInTimeCaching, 100*time.Millisecond)

			dsReader := ds.SnapshotReader(one)

			// Lookup name A
			_, _, err := tester.readSingleFunc(context.Background(), dsReader, nsA)
			require.NoError(err)

			// Lookup A and B, which should only lookup B and use A from cache.
			found, err := tester.lookupFunc(context.Background(), dsReader, []string{nsA, nsB})
			require.NoError(err)
			require.Equal(2, len(found))

			names := mapz.NewSet[string]()
			for _, d := range found {
				names.Add(d.GetName())
			}

			require.True(names.Has(nsA))
			require.True(names.Has(nsB))

			// Lookup A and B, which should use both from cache.
			foundAgain, err := tester.lookupFunc(context.Background(), dsReader, []string{nsA, nsB})
			require.NoError(err)
			require.Equal(2, len(foundAgain))

			namesAgain := mapz.NewSet[string]()
			for _, d := range foundAgain {
				namesAgain.Add(d.GetName())
			}

			require.True(namesAgain.Has(nsA))
			require.True(namesAgain.Has(nsB))

			dsMock.AssertExpectations(t)
			reader.AssertExpectations(t)
		})
	}
}

// NOTE: This uses a full memdb datastore because we want to exercise
// the cache behavior without mocking it.
func TestInvalidNamespaceInCache(t *testing.T) {
	invalidNamespace := "invalid_namespace"

	require := require.New(t)

	ctx := context.Background()

	memoryDatastore, err := memdb.NewMemdbDatastore(0, 1*time.Hour, 1*time.Hour)
	require.NoError(err)
	ds := NewCachingDatastoreProxy(memoryDatastore, DatastoreProxyTestCache(t), 1*time.Hour, JustInTimeCaching, 100*time.Millisecond)

	headRevision, err := ds.HeadRevision(ctx)
	require.NoError(err)
	dsReader := ds.SnapshotReader(headRevision)

	namespace, _, err := dsReader.ReadNamespaceByName(ctx, invalidNamespace)
	require.Nil(namespace)
	// NOTE: we're expecting this to error, because the namespace doesn't exist.
	// However, the act of calling it sets the cache value to nil, which means that
	// subsequent calls to the cache return that nil value. That's what needed to
	// be filtered out of the list call.
	require.Error(err)

	// Look it up again - in the bug that this captures,
	// it was populated into the cache and came back out.
	found, err := dsReader.LookupNamespacesWithNames(ctx, []string{invalidNamespace})
	require.Empty(found)
	require.NoError(err)
}

func TestMixedInvalidNamespacesInCache(t *testing.T) {
	invalidNamespace := "invalid_namespace"
	validNamespace := "valid_namespace"

	require := require.New(t)

	ctx := context.Background()

	memoryDatastore, err := memdb.NewMemdbDatastore(0, 1*time.Hour, 1*time.Hour)
	require.NoError(err)
	ds := NewCachingDatastoreProxy(memoryDatastore, DatastoreProxyTestCache(t), 1*time.Hour, JustInTimeCaching, 100*time.Millisecond)

	require.NoError(err)

	// Write in the valid namespace
	revision, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		writeErr := rwt.WriteNamespaces(ctx, &core.NamespaceDefinition{
			Name: validNamespace,
		})
		return writeErr
	})
	require.NoError(err)

	dsReader := ds.SnapshotReader(revision)

	namespace, _, err := dsReader.ReadNamespaceByName(ctx, invalidNamespace)
	require.Nil(namespace)
	// NOTE: we're expecting this to error, because the namespace doesn't exist.
	// However, the act of calling it sets the cache value to nil, which means that
	// subsequent calls to the cache return that nil value. That's what needed to
	// be filtered out of the list call.
	require.Error(err)

	// We're asserting that we find the thing we're looking for and don't receive a notfound value
	found, err := dsReader.LookupNamespacesWithNames(ctx, []string{invalidNamespace, validNamespace})
	require.Len(found, 1)
	require.Equal(validNamespace, found[0].Definition.Name)
	require.NoError(err)
}
