package schemacaching

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
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
	nsA     = "namespace_a"
	nsB     = "namespace_b"
	caveatA = "caveat_a"
	caveatB = "caveat_b"
)

// TestNilUnmarshal asserts that if we get a nil NamespaceDefinition from a
// datastore implementation, the process of inserting it into the cache and
// back does not break anything.
func TestNilUnmarshal(t *testing.T) {
	nsDef := (*core.NamespaceDefinition)(nil)
	marshalled, err := nsDef.MarshalVT()
	require.NoError(t, err)

	var newDef *core.NamespaceDefinition
	err = nsDef.UnmarshalVT(marshalled)
	require.NoError(t, err)
	require.Equal(t, nsDef, newDef)
}

type oldTesterDef struct {
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

var oldtesters = []oldTesterDef{
	{
		"namespace",

		"LegacyReadNamespaceByName",
		func(ctx context.Context, reader datastore.Reader, name string) (datastore.SchemaDefinition, datastore.Revision, error) {
			return reader.LegacyReadNamespaceByName(ctx, name)
		},

		"LegacyLookupNamespacesWithNames",
		func(ctx context.Context, reader datastore.Reader, names []string) ([]datastore.SchemaDefinition, error) {
			defs, err := reader.LegacyLookupNamespacesWithNames(ctx, names)
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

		"LegacyWriteNamespaces",
		func(rwt datastore.ReadWriteTransaction, def datastore.SchemaDefinition) error {
			return rwt.LegacyWriteNamespaces(context.Background(), def.(*core.NamespaceDefinition))
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
		"LegacyReadCaveatByName",
		func(ctx context.Context, reader datastore.Reader, name string) (datastore.SchemaDefinition, datastore.Revision, error) {
			return reader.LegacyReadCaveatByName(ctx, name)
		},

		"LegacyLookupCaveatsWithNames",
		func(ctx context.Context, reader datastore.Reader, names []string) ([]datastore.SchemaDefinition, error) {
			defs, err := reader.LegacyLookupCaveatsWithNames(ctx, names)
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

		"LegacyWriteCaveats",
		func(rwt datastore.ReadWriteTransaction, def datastore.SchemaDefinition) error {
			return rwt.LegacyWriteCaveats(context.Background(), []*core.CaveatDefinition{def.(*core.CaveatDefinition)})
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

func TestOldSnapshotCaching(t *testing.T) {
	for _, tester := range oldtesters {
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

			dptc := DatastoreProxyTestCache(t)
			t.Cleanup(func() {
				dptc.Close()
			})

			require := require.New(t)
			ds := NewCachingDatastoreProxy(dsMock, dptc, 1*time.Hour, JustInTimeCaching, 100*time.Millisecond)

			_, updatedOneA, err := tester.readSingleFunc(t.Context(), ds.SnapshotReader(one, datastore.NoSchemaHashForTesting), nsA)
			require.NoError(err)
			require.True(old.Equal(updatedOneA))

			_, updatedOneAAgain, err := tester.readSingleFunc(t.Context(), ds.SnapshotReader(one, datastore.NoSchemaHashForTesting), nsA)
			require.NoError(err)
			require.True(old.Equal(updatedOneAAgain))

			_, updatedOneB, err := tester.readSingleFunc(t.Context(), ds.SnapshotReader(one, datastore.NoSchemaHashForTesting), nsB)
			require.NoError(err)
			require.True(zero.Equal(updatedOneB))

			_, updatedOneBAgain, err := tester.readSingleFunc(t.Context(), ds.SnapshotReader(one, datastore.NoSchemaHashForTesting), nsB)
			require.NoError(err)
			require.True(zero.Equal(updatedOneBAgain))

			_, updatedTwoA, err := tester.readSingleFunc(t.Context(), ds.SnapshotReader(two, datastore.NoSchemaHashForTesting), nsA)
			require.NoError(err)
			require.True(zero.Equal(updatedTwoA))

			_, updatedTwoAAgain, err := tester.readSingleFunc(t.Context(), ds.SnapshotReader(two, datastore.NoSchemaHashForTesting), nsA)
			require.NoError(err)
			require.True(zero.Equal(updatedTwoAAgain))

			_, updatedTwoB, err := tester.readSingleFunc(t.Context(), ds.SnapshotReader(two, datastore.NoSchemaHashForTesting), nsB)
			require.NoError(err)
			require.True(one.Equal(updatedTwoB))

			_, updatedTwoBAgain, err := tester.readSingleFunc(t.Context(), ds.SnapshotReader(two, datastore.NoSchemaHashForTesting), nsB)
			require.NoError(err)
			require.True(one.Equal(updatedTwoBAgain))

			dsMock.AssertExpectations(t)
			oneReader.AssertExpectations(t)
			twoReader.AssertExpectations(t)
		})
	}
}

func TestSnapshotCaching(t *testing.T) {
	dsMock := &proxy_test.MockDatastore{}

	oneReader := &proxy_test.MockReader{}
	dsMock.On("SnapshotReader", one).Return(oneReader)
	oneReader.On("LegacyReadNamespaceByName", nsA).Return(nil, old, nil).Once()
	oneReader.On("LegacyReadNamespaceByName", nsB).Return(nil, zero, nil).Once()
	oneReader.On("LegacyReadCaveatByName", caveatA).Return(nil, old, nil).Once()
	oneReader.On("LegacyReadCaveatByName", caveatB).Return(nil, zero, nil).Once()

	twoReader := &proxy_test.MockReader{}
	dsMock.On("SnapshotReader", two).Return(twoReader)
	twoReader.On("LegacyReadNamespaceByName", nsA).Return(nil, zero, nil).Once()
	twoReader.On("LegacyReadNamespaceByName", nsB).Return(nil, one, nil).Once()
	twoReader.On("LegacyReadCaveatByName", caveatA).Return(nil, zero, nil).Once()
	twoReader.On("LegacyReadCaveatByName", caveatB).Return(nil, one, nil).Once()

	dptc := DatastoreProxyTestCache(t)
	t.Cleanup(func() {
		dptc.Close()
	})

	require := require.New(t)
	ds := NewCachingDatastoreProxy(dsMock, dptc, 1*time.Hour, JustInTimeCaching, 100*time.Millisecond)

	// Get a handle on the reader for A
	dsForA := ds.SnapshotReader(one, datastore.NoSchemaHashForTesting)
	schemaReaderForA, err := dsForA.SchemaReader()
	require.NoError(err)

	// Check that revisions match after one read for A
	// Namespace
	revNsDef, found, err := schemaReaderForA.LookupTypeDefByName(t.Context(), nsA)
	require.NoError(err)
	require.True(found, "could not find expected definition")
	require.True(old.Equal(revNsDef.LastWrittenRevision))

	// Caveat
	revCaveatDef, found, err := schemaReaderForA.LookupCaveatDefByName(t.Context(), caveatA)
	require.NoError(err)
	require.True(found, "could not find expected definition")
	require.True(old.Equal(revCaveatDef.LastWrittenRevision))

	// Check that revisions match after another read on A
	// Namespace
	revNsDef, found, err = schemaReaderForA.LookupTypeDefByName(t.Context(), nsA)
	require.NoError(err)
	require.True(found, "could not find expected definition")
	require.True(old.Equal(revNsDef.LastWrittenRevision))

	// Caveat
	revCaveatDef, found, err = schemaReaderForA.LookupCaveatDefByName(t.Context(), caveatA)
	require.NoError(err)
	require.True(found, "could not find expected definition")
	require.True(old.Equal(revCaveatDef.LastWrittenRevision))

	// Get a handle on the reader for B
	dsForB := ds.SnapshotReader(one, datastore.NoSchemaHashForTesting)
	schemaReaderForB, err := dsForB.SchemaReader()
	require.NoError(err)

	// Check that revisions match after one read for B
	// Namespace
	revNsDef, found, err = schemaReaderForB.LookupTypeDefByName(t.Context(), nsB)
	require.NoError(err)
	require.True(found, "could not find expected definition")
	require.True(zero.Equal(revNsDef.LastWrittenRevision))

	// Caveat
	revCaveatDef, found, err = schemaReaderForB.LookupCaveatDefByName(t.Context(), caveatB)
	require.NoError(err)
	require.True(found, "could not find expected definition")
	require.True(zero.Equal(revCaveatDef.LastWrittenRevision))

	// And again
	// Namespace
	revNsDef, found, err = schemaReaderForB.LookupTypeDefByName(t.Context(), nsB)
	require.NoError(err)
	require.True(found, "could not find expected definition")
	require.True(zero.Equal(revNsDef.LastWrittenRevision))

	// Caveat
	revCaveatDef, found, err = schemaReaderForB.LookupCaveatDefByName(t.Context(), caveatB)
	require.NoError(err)
	require.True(found, "could not find expected definition")
	require.True(zero.Equal(revCaveatDef.LastWrittenRevision))

	// Get a handle on the second reader for A
	dsForA = ds.SnapshotReader(two, datastore.NoSchemaHashForTesting)
	schemaReaderForA, err = dsForA.SchemaReader()
	require.NoError(err)

	// Check that revisions match after one read for A on the zero revision
	// Namespace
	revNsDef, found, err = schemaReaderForA.LookupTypeDefByName(t.Context(), nsA)
	require.NoError(err)
	require.True(found, "could not find expected definition")
	require.True(zero.Equal(revNsDef.LastWrittenRevision))

	// Caveat
	revCaveatDef, found, err = schemaReaderForA.LookupCaveatDefByName(t.Context(), caveatA)
	require.NoError(err)
	require.True(found, "could not find expected definition")
	require.True(zero.Equal(revCaveatDef.LastWrittenRevision))

	// Check that revisions match after another read for A on the zero revision
	// Namespace
	revNsDef, found, err = schemaReaderForA.LookupTypeDefByName(t.Context(), nsA)
	require.NoError(err)
	require.True(found, "could not find expected definition")
	require.True(zero.Equal(revNsDef.LastWrittenRevision))

	// Caveat
	revCaveatDef, found, err = schemaReaderForA.LookupCaveatDefByName(t.Context(), caveatA)
	require.NoError(err)
	require.True(found, "could not find expected definition")
	require.True(zero.Equal(revCaveatDef.LastWrittenRevision))

	// Get a handle on the second reader for B
	dsForB = ds.SnapshotReader(two, datastore.NoSchemaHashForTesting)
	schemaReaderForB, err = dsForB.SchemaReader()
	require.NoError(err)

	// Check that revisions match after one read for B on the zero revision
	// Namespace
	revNsDef, found, err = schemaReaderForB.LookupTypeDefByName(t.Context(), nsB)
	require.NoError(err)
	require.True(found, "could not find expected definition")
	require.True(one.Equal(revNsDef.LastWrittenRevision))

	// Caveat
	revCaveatDef, found, err = schemaReaderForB.LookupCaveatDefByName(t.Context(), caveatB)
	require.NoError(err)
	require.True(found, "could not find expected definition")
	require.True(one.Equal(revCaveatDef.LastWrittenRevision))

	// Check that revisions match after another read for B on the zero revision
	// Namespace
	revNsDef, found, err = schemaReaderForB.LookupTypeDefByName(t.Context(), nsB)
	require.NoError(err)
	require.True(found, "could not find expected definition")
	require.True(one.Equal(revNsDef.LastWrittenRevision))

	// Caveat
	revCaveatDef, found, err = schemaReaderForB.LookupCaveatDefByName(t.Context(), caveatB)
	require.NoError(err)
	require.True(found, "could not find expected definition")
	require.True(one.Equal(revCaveatDef.LastWrittenRevision))

	dsMock.AssertExpectations(t)
	oneReader.AssertExpectations(t)
	twoReader.AssertExpectations(t)
}

func TestOldRWTCaching(t *testing.T) {
	for _, tester := range oldtesters {
		tester := tester
		t.Run(tester.name, func(t *testing.T) {
			dsMock := &proxy_test.MockDatastore{}
			rwtMock := &proxy_test.MockReadWriteTransaction{}

			require := require.New(t)

			dsMock.On("ReadWriteTx", nilOpts).Return(rwtMock, one, nil).Once()
			rwtMock.On(tester.readSingleFunctionName, nsA).Return(nil, zero, nil).Once()

			ctx := t.Context()

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

func TestRWTCaching(t *testing.T) {
	dsMock := &proxy_test.MockDatastore{}
	rwtMock := &proxy_test.MockReadWriteTransaction{}

	require := require.New(t)

	dsMock.On("ReadWriteTx", nilOpts).Return(rwtMock, one, nil).Once()
	rwtMock.On("LegacyReadNamespaceByName", nsA).Return(nil, zero, nil).Once()
	rwtMock.On("LegacyReadCaveatByName", caveatA).Return(nil, zero, nil).Once()

	ctx := t.Context()

	ds := NewCachingDatastoreProxy(dsMock, nil, 1*time.Hour, JustInTimeCaching, 100*time.Millisecond)

	rev, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		// get a handle on the reader
		reader, err := rwt.SchemaReader()
		require.NoError(err)

		// read the namespace
		nsRevDef, found, err := reader.LookupTypeDefByName(ctx, nsA)
		require.NoError(err)
		require.True(found)
		require.True(zero.Equal(nsRevDef.LastWrittenRevision))

		// read the caveat
		caveatRevDef, found, err := reader.LookupCaveatDefByName(ctx, caveatA)
		require.NoError(err)
		require.True(found)
		require.True(zero.Equal(caveatRevDef.LastWrittenRevision))

		// This will not call out the mock RWT again, the mock will panic if it does.
		// read the namespace
		nsRevDef, found, err = reader.LookupTypeDefByName(ctx, nsA)
		require.NoError(err)
		require.True(found)
		require.True(zero.Equal(nsRevDef.LastWrittenRevision))

		// read the caveat
		caveatRevDef, found, err = reader.LookupCaveatDefByName(ctx, caveatA)
		require.NoError(err)
		require.True(found)
		require.True(zero.Equal(caveatRevDef.LastWrittenRevision))

		return nil
	})
	require.True(one.Equal(rev))
	require.NoError(err)

	dsMock.AssertExpectations(t)
	rwtMock.AssertExpectations(t)
}

func TestOldRWTCacheWithWrites(t *testing.T) {
	for _, tester := range oldtesters {
		tester := tester
		t.Run(tester.name, func(t *testing.T) {
			dsMock := &proxy_test.MockDatastore{}
			rwtMock := &proxy_test.MockReadWriteTransaction{}

			require := require.New(t)

			dsMock.On("ReadWriteTx", nilOpts).Return(rwtMock, one, nil).Once()
			rwtMock.On(tester.readSingleFunctionName, nsA).Return(nil, zero, tester.notFoundErr).Once()

			ctx := t.Context()

			ds := NewCachingDatastoreProxy(dsMock, nil, 1*time.Hour, JustInTimeCaching, 100*time.Millisecond)

			rev, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
				// Cache the 404
				_, _, err := tester.readSingleFunc(ctx, rwt, nsA)
				require.ErrorIs(err, tester.notFoundErr)

				// This will not call out the mock RWT again, the mock will panic if it does.
				_, _, err = tester.readSingleFunc(ctx, rwt, nsA)
				require.ErrorIs(err, tester.notFoundErr)

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

func TestOldSingleFlight(t *testing.T) {
	for _, tester := range oldtesters {
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
				_, updatedAt, err := tester.readSingleFunc(t.Context(), ds.SnapshotReader(one, datastore.NoSchemaHashForTesting), nsA)
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

func TestSingleFlight(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		dsMock := &proxy_test.MockDatastore{}

		oneReader := &proxy_test.MockReader{}
		dsMock.On("SnapshotReader", one).Return(oneReader)
		oneReader.
			On("LegacyReadNamespaceByName", nsA).
			WaitUntil(time.After(50*time.Millisecond)).
			Return(nil, old, nil).
			Once()
		oneReader.
			On("LegacyReadCaveatByName", caveatA).
			WaitUntil(time.After(50*time.Millisecond)).
			Return(nil, old, nil).
			Once()

		assert := assert.New(t)

		ds := NewCachingDatastoreProxy(dsMock, nil, 1*time.Hour, JustInTimeCaching, 100*time.Millisecond)
		snapshotReader := ds.SnapshotReader(one, datastore.NoSchemaHashForTesting)
		schemaReader, err := snapshotReader.SchemaReader()
		if !assert.NoError(err) { //nolint:testifylint  // you can't use require within a goroutine; the linter is wrong.
			return
		}

		readNamespace := func() {
			nsRevDef, found, err := schemaReader.LookupTypeDefByName(t.Context(), nsA)
			if !assert.NoError(err) { //nolint:testifylint  // you can't use require within a goroutine; the linter is wrong.
				return
			}
			assert.True(found)
			assert.True(old.Equal(nsRevDef.LastWrittenRevision))
		}

		readCaveat := func() {
			caveatRevDef, found, err := schemaReader.LookupCaveatDefByName(t.Context(), caveatA)
			if !assert.NoError(err) { //nolint:testifylint  // you can't use require within a goroutine; the linter is wrong.
				return
			}
			assert.True(found)
			assert.True(old.Equal(caveatRevDef.LastWrittenRevision))
		}

		// NOTE: if the singleflight isn't working, the second call to land will invoke
		// the mock a second time and panic.
		go readNamespace()
		go readNamespace()
		go readCaveat()
		go readCaveat()

		// Let the timers elapse
		time.Sleep(100 * time.Millisecond)

		// Clean up
		synctest.Wait()
		dsMock.AssertExpectations(t)
		oneReader.AssertExpectations(t)
	})
}

func TestOldSnapshotCachingRealDatastore(t *testing.T) {
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
			ns.MustCaveatDefinition(caveats.MustEnvForVariablesWithDefaultTypeSet(
				map[string]caveattypes.VariableType{
					"somevar": caveattypes.Default.IntType,
				},
			), "somecaveat", "somevar < 42"),
			"somecaveat",
		},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, 0, memdb.DisableGC)
			require.NoError(t, err)

			ctx := t.Context()
			ds := NewCachingDatastoreProxy(rawDS, nil, 1*time.Hour, JustInTimeCaching, 100*time.Millisecond)

			if tc.nsDef != nil {
				_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
					err := rwt.LegacyWriteNamespaces(ctx, tc.nsDef)
					if err != nil {
						return err
					}

					return rwt.LegacyWriteCaveats(ctx, []*core.CaveatDefinition{tc.caveatDef})
				})
				require.NoError(t, err)
			}

			headRev, _, err := ds.HeadRevision(ctx)
			require.NoError(t, err)

			reader := ds.SnapshotReader(headRev, datastore.NoSchemaHashForTesting)
			ns, _, _ := reader.LegacyReadNamespaceByName(ctx, tc.namespaceName)
			testutil.RequireProtoEqual(t, tc.nsDef, ns, "found different namespaces")

			ns2, _, _ := reader.LegacyReadNamespaceByName(ctx, tc.namespaceName)
			testutil.RequireProtoEqual(t, tc.nsDef, ns2, "found different namespaces")

			c1, _, _ := reader.LegacyReadCaveatByName(ctx, tc.caveatName)
			testutil.RequireProtoEqual(t, tc.caveatDef, c1, "found different caveats")

			c2, _, _ := reader.LegacyReadCaveatByName(ctx, tc.caveatName)
			testutil.RequireProtoEqual(t, tc.caveatDef, c2, "found different caveats")
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
			ns.MustCaveatDefinition(caveats.MustEnvForVariablesWithDefaultTypeSet(
				map[string]caveattypes.VariableType{
					"somevar": caveattypes.Default.IntType,
				},
			), "somecaveat", "somevar < 42"),
			"somecaveat",
		},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, 0, memdb.DisableGC)
			require.NoError(t, err)

			ctx := t.Context()
			ds := NewCachingDatastoreProxy(rawDS, nil, 1*time.Hour, JustInTimeCaching, 100*time.Millisecond)

			if tc.nsDef != nil {
				_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
					schemaWriter, err := rwt.SchemaWriter()
					if err != nil {
						return err
					}
					return schemaWriter.WriteSchema(ctx, []datastore.SchemaDefinition{tc.nsDef, tc.caveatDef}, "", caveattypes.Default.TypeSet)
				})
				require.NoError(t, err)
			}

			headRev, _, err := ds.HeadRevision(ctx)
			require.NoError(t, err)

			reader := ds.SnapshotReader(headRev, datastore.NoSchemaHashForTesting)
			schemaReader, err := reader.SchemaReader()
			require.NoError(t, err)

			nsRevDef, _, err := schemaReader.LookupTypeDefByName(ctx, tc.namespaceName)
			require.NoError(t, err)
			testutil.RequireProtoEqual(t, tc.nsDef, nsRevDef.Definition, "found different namespaces")

			nsRevDef2, _, err := schemaReader.LookupTypeDefByName(ctx, tc.namespaceName)
			require.NoError(t, err)
			testutil.RequireProtoEqual(t, tc.nsDef, nsRevDef2.Definition, "found different namespaces")

			caveatRevDef, _, err := schemaReader.LookupCaveatDefByName(ctx, tc.caveatName)
			require.NoError(t, err)
			testutil.RequireProtoEqual(t, tc.caveatDef, caveatRevDef.Definition, "found different caveats")

			caveatRevDef2, _, err := schemaReader.LookupCaveatDefByName(ctx, tc.caveatName)
			require.NoError(t, err)
			testutil.RequireProtoEqual(t, tc.caveatDef, caveatRevDef2.Definition, "found different caveats")
		})
	}
}

// singleflightReader is used to test singleflight context cancellation
// behavior.
type singleflightReader struct {
	proxy_test.MockReader
}

func (r *singleflightReader) LegacyReadNamespaceByName(ctx context.Context, namespace string) (ns *core.NamespaceDefinition, lastWritten datastore.Revision, err error) {
	// NOTE: the sleep is here to ensure that the context can be cancelled before this executes.
	time.Sleep(10 * time.Millisecond)
	if errors.Is(ctx.Err(), context.Canceled) {
		return nil, old, fmt.Errorf("error")
	}
	return &core.NamespaceDefinition{Name: namespace}, old, nil
}

func (r *singleflightReader) LegacyReadCaveatByName(ctx context.Context, name string) (*core.CaveatDefinition, datastore.Revision, error) {
	// NOTE: the sleep is here to ensure that the context can be cancelled before this executes.
	time.Sleep(10 * time.Millisecond)
	if errors.Is(ctx.Err(), context.Canceled) {
		return nil, old, fmt.Errorf("error")
	}
	return &core.CaveatDefinition{Name: name}, old, nil
}

func (r *singleflightReader) LookupCaveatDefByName(ctx context.Context, name string) (datastore.RevisionedCaveat, error) {
	time.Sleep(10 * time.Millisecond)
	if errors.Is(ctx.Err(), context.Canceled) {
		return datastore.RevisionedCaveat{}, fmt.Errorf("error")
	}
	return datastore.RevisionedCaveat{
		Definition:          &core.CaveatDefinition{Name: name},
		LastWrittenRevision: old,
	}, nil
}

func (r *singleflightReader) LookupTypeDefByName(ctx context.Context, name string) (datastore.RevisionedTypeDefinition, error) {
	time.Sleep(10 * time.Millisecond)
	if errors.Is(ctx.Err(), context.Canceled) {
		return datastore.RevisionedTypeDefinition{}, fmt.Errorf("error")
	}
	return datastore.RevisionedTypeDefinition{
		Definition:          &core.NamespaceDefinition{Name: name},
		LastWrittenRevision: old,
	}, nil
}

func TestOldSingleFlightCancelled(t *testing.T) {
	for _, tester := range oldtesters {
		tester := tester
		t.Run(tester.name, func(t *testing.T) {
			dsMock := &proxy_test.MockDatastore{}
			ctx1, cancel1 := context.WithCancel(t.Context())
			ctx2, cancel2 := context.WithCancel(t.Context())
			defer cancel2()
			defer cancel1()

			dsMock.On("SnapshotReader", one).Return(&singleflightReader{MockReader: proxy_test.MockReader{}})

			ds := NewCachingDatastoreProxy(dsMock, nil, 1*time.Hour, JustInTimeCaching, 100*time.Millisecond)

			g := sync.WaitGroup{}
			var d2 datastore.SchemaDefinition
			g.Add(2)
			go func() {
				_, _, _ = tester.readSingleFunc(ctx1, ds.SnapshotReader(one, datastore.NoSchemaHashForTesting), nsA)
				g.Done()
			}()
			go func() {
				time.Sleep(5 * time.Millisecond)
				d2, _, _ = tester.readSingleFunc(ctx2, ds.SnapshotReader(one, datastore.NoSchemaHashForTesting), nsA)
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

func TestSingleFlightCancelled(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		dsMock := &proxy_test.MockDatastore{}
		ctx1, cancel1 := context.WithCancel(t.Context())
		defer cancel1()
		// Note that ctx2 will not be cancelled
		ctx2 := t.Context()

		dsMock.On("SnapshotReader", one).Return(&singleflightReader{MockReader: proxy_test.MockReader{}})

		ds := NewCachingDatastoreProxy(dsMock, nil, 1*time.Hour, JustInTimeCaching, 100*time.Millisecond)
		snapshotReader := ds.SnapshotReader(one, datastore.NoSchemaHashForTesting)
		schemaReader, err := snapshotReader.SchemaReader()
		if !assert.NoError(t, err) { //nolint:testifylint  // you can't use require within a goroutine; the linter is wrong.
			return
		}

		var nsRevDef datastore.RevisionedTypeDefinition
		var caveatRevDef datastore.RevisionedCaveat
		go func() {
			_, _, _ = schemaReader.LookupTypeDefByName(ctx1, nsA)
			_, _, _ = schemaReader.LookupCaveatDefByName(ctx1, caveatA)
		}()
		go func() {
			time.Sleep(5 * time.Millisecond)
			nsRevDef, _, _ = schemaReader.LookupTypeDefByName(ctx2, nsA)
			caveatRevDef, _, _ = schemaReader.LookupCaveatDefByName(ctx2, caveatA)
		}()

		// Cancel the context immediately after spinning the goroutines
		cancel1()
		// Sleep long enough to allow other goroutines to complete
		time.Sleep(50 * time.Millisecond)
		// Clean up
		synctest.Wait()

		// Assert that the second request made in the singleflight window
		// still succeeds
		assert.NotNil(t, nsRevDef)
		assert.NotNil(t, caveatRevDef)
		assert.Equal(t, nsA, nsRevDef.Definition.GetName())
		assert.Equal(t, caveatA, caveatRevDef.Definition.GetName())

		dsMock.AssertExpectations(t)
	})
}

func TestOldMixedCaching(t *testing.T) {
	for _, tester := range oldtesters {
		tester := tester
		t.Run(tester.name, func(t *testing.T) {
			dsMock := &proxy_test.MockDatastore{}

			defA := tester.createDef(nsA)
			defB := tester.createDef(nsB)

			reader := &proxy_test.MockReader{}
			reader.On(tester.readSingleFunctionName, nsA).Return(defA, old, nil).Once()
			reader.On(tester.lookupFunctionName, []string{nsB}).Return(tester.wrapRevisioned(defB), nil).Once()

			dsMock.On("SnapshotReader", one).Return(reader)

			dptc := DatastoreProxyTestCache(t)
			t.Cleanup(func() {
				dptc.Close()
			})

			require := require.New(t)
			ds := NewCachingDatastoreProxy(dsMock, dptc, 1*time.Hour, JustInTimeCaching, 100*time.Millisecond)

			dsReader := ds.SnapshotReader(one, datastore.NoSchemaHashForTesting)

			// Lookup name A
			_, _, err := tester.readSingleFunc(t.Context(), dsReader, nsA)
			require.NoError(err)

			// Lookup A and B, which should only lookup B and use A from cache.
			found, err := tester.lookupFunc(t.Context(), dsReader, []string{nsA, nsB})
			require.NoError(err)
			require.Len(found, 2)

			names := mapz.NewSet[string]()
			for _, d := range found {
				names.Add(d.GetName())
			}

			require.True(names.Has(nsA))
			require.True(names.Has(nsB))

			// Lookup A and B, which should use both from cache.
			foundAgain, err := tester.lookupFunc(t.Context(), dsReader, []string{nsA, nsB})
			require.NoError(err)
			require.Len(foundAgain, 2)

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

func TestMixedCaching(t *testing.T) {
	dsMock := &proxy_test.MockDatastore{}

	nsDefA := &core.NamespaceDefinition{
		Name: nsA,
	}
	nsDefB := &core.NamespaceDefinition{
		Name: nsB,
	}

	caveatDefA := &core.CaveatDefinition{
		Name: caveatA,
	}
	caveatDefB := &core.CaveatDefinition{
		Name: caveatB,
	}

	reader := &proxy_test.MockReader{}
	reader.Test(t)
	reader.On("LegacyReadNamespaceByName", nsA).Return(nsDefA, old, nil).Once()
	reader.On("LegacyReadCaveatByName", caveatA).Return(caveatDefA, old, nil).Once()
	// NOTE: the mocks here only expect the Bs because the caching layer is going to
	// grab entries from the cache before going to the reader, which means we don't
	// expect to see requests for the already-cached values hit this layer.
	reader.On("LegacyLookupNamespacesWithNames", []string{nsB}).Return([]datastore.RevisionedTypeDefinition{
		{
			Definition:          nsDefB,
			LastWrittenRevision: old,
		},
	}, nil).Once()
	reader.On("LegacyLookupCaveatsWithNames", []string{caveatB}).Return([]datastore.RevisionedCaveat{
		{
			Definition:          caveatDefB,
			LastWrittenRevision: old,
		},
	}, nil).Once()

	dsMock.On("SnapshotReader", one).Return(reader)

	dptc := DatastoreProxyTestCache(t)
	t.Cleanup(func() {
		dptc.Close()
	})

	require := require.New(t)
	ds := NewCachingDatastoreProxy(dsMock, dptc, 1*time.Hour, JustInTimeCaching, 100*time.Millisecond)

	dsReader := ds.SnapshotReader(one, datastore.NoSchemaHashForTesting)
	schemaReader, err := dsReader.SchemaReader()
	require.NoError(err)

	// Lookup name A and caveat A, which should populate the cache
	_, _, err = schemaReader.LookupTypeDefByName(t.Context(), nsA)
	require.NoError(err)
	_, _, err = schemaReader.LookupCaveatDefByName(t.Context(), caveatA)
	require.NoError(err)

	// Lookup As and Bs, which should only lookup Bs and use As from cache.
	typeDefMap, err := schemaReader.LookupTypeDefinitionsByNames(t.Context(), []string{nsA, nsB})
	require.NoError(err)
	require.Len(typeDefMap, 2)
	require.Contains(typeDefMap, nsA)
	require.Contains(typeDefMap, nsB)

	caveatDefMap, err := schemaReader.LookupCaveatDefinitionsByNames(t.Context(), []string{caveatA, caveatB})
	require.NoError(err)
	require.Len(caveatDefMap, 2)
	require.Contains(caveatDefMap, caveatA)
	require.Contains(caveatDefMap, caveatB)

	// Lookup As and Bs, which should use both from cache.
	typeDefMapAgain, err := schemaReader.LookupTypeDefinitionsByNames(t.Context(), []string{nsA, nsB})
	require.NoError(err)
	require.Len(typeDefMapAgain, 2)
	require.Contains(typeDefMapAgain, nsA)
	require.Contains(typeDefMapAgain, nsB)

	caveatDefMapAgain, err := schemaReader.LookupCaveatDefinitionsByNames(t.Context(), []string{caveatA, caveatB})
	require.NoError(err)
	require.Len(caveatDefMapAgain, 2)
	require.Contains(caveatDefMapAgain, caveatA)
	require.Contains(caveatDefMapAgain, caveatB)

	dsMock.AssertExpectations(t)
	reader.AssertExpectations(t)
}

// NOTE: This uses a full memdb datastore because we want to exercise
// the cache behavior without mocking it.
func TestOldInvalidNamespaceInCache(t *testing.T) {
	invalidNamespace := "invalid_namespace"

	require := require.New(t)

	ctx := t.Context()

	memoryDatastore, err := memdb.NewMemdbDatastore(0, 1*time.Hour, 1*time.Hour)
	require.NoError(err)
	ds := NewCachingDatastoreProxy(memoryDatastore, DatastoreProxyTestCache(t), 1*time.Hour, JustInTimeCaching, 100*time.Millisecond)
	t.Cleanup(func() {
		ds.Close()
	})

	headRevision, _, err := ds.HeadRevision(ctx)
	require.NoError(err)
	dsReader := ds.SnapshotReader(headRevision, datastore.NoSchemaHashForTesting)

	namespace, _, err := dsReader.LegacyReadNamespaceByName(ctx, invalidNamespace)
	require.Nil(namespace)
	// NOTE: we're expecting this to error, because the namespace doesn't exist.
	// However, the act of calling it sets the cache value to nil, which means that
	// subsequent calls to the cache return that nil value. That's what needed to
	// be filtered out of the list call.
	require.Error(err)

	// Look it up again - in the bug that this captures,
	// it was populated into the cache and came back out.
	found, err := dsReader.LegacyLookupNamespacesWithNames(ctx, []string{invalidNamespace})
	require.Empty(found)
	require.NoError(err)
}

// NOTE: This uses a full memdb datastore because we want to exercise
// the cache behavior without mocking it.
func TestInvalidNamespaceInCache(t *testing.T) {
	invalidNamespace := "invalid_namespace"

	require := require.New(t)

	ctx := t.Context()

	memoryDatastore, err := memdb.NewMemdbDatastore(0, 1*time.Hour, 1*time.Hour)
	require.NoError(err)
	ds := NewCachingDatastoreProxy(memoryDatastore, DatastoreProxyTestCache(t), 1*time.Hour, JustInTimeCaching, 100*time.Millisecond)
	t.Cleanup(func() {
		ds.Close()
	})

	headRevision, _, err := ds.HeadRevision(ctx)
	require.NoError(err)
	dsReader := ds.SnapshotReader(headRevision, datastore.NoSchemaHashForTesting)
	schemaReader, err := dsReader.SchemaReader()
	require.NoError(err)

	namespace, found, err := schemaReader.LookupTypeDefByName(ctx, invalidNamespace)
	require.Zero(namespace)
	// NOTE: we're expecting this to be false, because the namespace doesn't exist.
	// However, the act of calling it sets the cache value to nil, which means that
	// subsequent calls to the cache return that nil value. That's what needed to
	// be filtered out of the list call.
	require.False(found)
	require.NoError(err)

	// Look it up again - in the bug that this captures,
	// it was populated into the cache and came back out.
	defMap, err := schemaReader.LookupSchemaDefinitionsByNames(ctx, []string{invalidNamespace})
	require.Empty(defMap)
	require.NoError(err)
}

func TestOldMixedInvalidNamespacesInCache(t *testing.T) {
	invalidNamespace := "invalid_namespace"
	validNamespace := "valid_namespace"

	require := require.New(t)

	ctx := t.Context()

	memoryDatastore, err := memdb.NewMemdbDatastore(0, 1*time.Hour, 1*time.Hour)
	require.NoError(err)
	ds := NewCachingDatastoreProxy(memoryDatastore, DatastoreProxyTestCache(t), 1*time.Hour, JustInTimeCaching, 100*time.Millisecond)
	t.Cleanup(func() {
		ds.Close()
	})

	require.NoError(err)

	// Write in the valid namespace
	revision, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		writeErr := rwt.LegacyWriteNamespaces(ctx, &core.NamespaceDefinition{
			Name: validNamespace,
		})
		return writeErr
	})
	require.NoError(err)

	dsReader := ds.SnapshotReader(revision, datastore.NoSchemaHashForTesting)

	namespace, _, err := dsReader.LegacyReadNamespaceByName(ctx, invalidNamespace)
	require.Nil(namespace)
	// NOTE: we're expecting this to error, because the namespace doesn't exist.
	// However, the act of calling it sets the cache value to nil, which means that
	// subsequent calls to the cache return that nil value. That's what needed to
	// be filtered out of the list call.
	require.Error(err)

	// We're asserting that we find the thing we're looking for and don't receive a notfound value
	found, err := dsReader.LegacyLookupNamespacesWithNames(ctx, []string{invalidNamespace, validNamespace})
	require.Len(found, 1)
	require.Equal(validNamespace, found[0].Definition.Name)
	require.NoError(err)
}

func TestMixedInvalidNamespacesInCache(t *testing.T) {
	invalidNamespace := "invalid_namespace"
	validNamespace := "valid_namespace"

	require := require.New(t)

	ctx := t.Context()

	memoryDatastore, err := memdb.NewMemdbDatastore(0, 1*time.Hour, 1*time.Hour)
	require.NoError(err)
	ds := NewCachingDatastoreProxy(memoryDatastore, DatastoreProxyTestCache(t), 1*time.Hour, JustInTimeCaching, 100*time.Millisecond)
	t.Cleanup(func() {
		ds.Close()
	})

	require.NoError(err)

	// Write in the valid namespace
	revision, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		schemaWriter, err := rwt.SchemaWriter()
		require.NoError(err)
		writeErr := schemaWriter.WriteSchema(ctx, []datastore.SchemaDefinition{
			&core.NamespaceDefinition{
				Name: validNamespace,
			},
		}, "", caveattypes.Default.TypeSet)
		return writeErr
	})
	require.NoError(err)

	dsReader := ds.SnapshotReader(revision, datastore.NoSchemaHashForTesting)
	schemaReader, err := dsReader.SchemaReader()
	require.NoError(err)

	namespace, found, err := schemaReader.LookupTypeDefByName(ctx, invalidNamespace)
	require.Zero(namespace)
	// NOTE: we're expecting this to be false, because the namespace doesn't exist.
	// However, the act of calling it sets the cache value to nil, which means that
	// subsequent calls to the cache return that nil value. That's what needed to
	// be filtered out of the list call.
	require.False(found)
	require.NoError(err)

	// We're asserting that we find the thing we're looking for and don't receive a notfound value
	defMap, err := schemaReader.LookupSchemaDefinitionsByNames(ctx, []string{invalidNamespace, validNamespace})
	require.Len(defMap, 1)
	require.Contains(defMap, validNamespace)
	require.NoError(err)
}
