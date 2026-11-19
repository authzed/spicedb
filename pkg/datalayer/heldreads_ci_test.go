//go:build ci

package datalayer

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/datastore/queryshape"
	"github.com/authzed/spicedb/pkg/tuple"
)

func newHeldReadsTestReader(t *testing.T) RevisionedReader {
	t.Helper()

	ds := newTestDatastore(t)
	_, err := WriteStoredSchemaForTest(t.Context(), ds, `definition user {}
definition document {
	relation viewer: user
}`)
	require.NoError(t, err)

	rev, err := ds.ReadWriteTx(t.Context(), func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{
			tuple.Create(tuple.MustParse("document:first#viewer@user:tom")),
			tuple.Create(tuple.MustParse("document:second#viewer@user:tom")),
		})
	})
	require.NoError(t, err)

	return NewDataLayer(ds, WithSchemaMode(SchemaModeReadNewWriteNew)).SnapshotReader(rev, NoSchemaHashForTesting)
}

func queryDocuments(t *testing.T, reader RevisionedReader) datastore.RelationshipIterator {
	t.Helper()
	it, err := reader.QueryRelationships(t.Context(), datastore.RelationshipsFilter{OptionalResourceType: "document"}, options.WithQueryShape(queryshape.FindResourceOfType))
	require.NoError(t, err)
	return it
}

func TestReadWhileIteratingPanics(t *testing.T) {
	reads := map[string]func(t *testing.T, reader RevisionedReader){
		"ReadSchema": func(t *testing.T, reader RevisionedReader) {
			_, _ = reader.ReadSchema(t.Context())
		},
		"QueryRelationships": func(t *testing.T, reader RevisionedReader) {
			for range queryDocuments(t, reader) {
				break
			}
		},
		"CountRelationships": func(t *testing.T, reader RevisionedReader) {
			_, _ = reader.CountRelationships(t.Context(), "somecounter")
		},
		"LookupCounters": func(t *testing.T, reader RevisionedReader) {
			_, _ = reader.LookupCounters(t.Context())
		},
	}

	for name, read := range reads {
		t.Run(name, func(t *testing.T) {
			reader := newHeldReadsTestReader(t)
			require.Panics(t, func() {
				for _, err := range queryDocuments(t, reader) {
					require.NoError(t, err)
					read(t, reader)
				}
			})

			// The held iterator is released once its iteration unwinds, even by a panic.
			read(t, reader)
		})
	}
}

func TestReadsAroundIterationDoNotPanic(t *testing.T) {
	reader := newHeldReadsTestReader(t)

	// Read before iterating.
	_, err := reader.ReadSchema(t.Context())
	require.NoError(t, err)

	// Collect, then read.
	rels, err := datastore.IteratorToSlice(queryDocuments(t, reader))
	require.NoError(t, err)
	require.Len(t, rels, 2)
	_, err = reader.ReadSchema(t.Context())
	require.NoError(t, err)

	// Break out early, then read.
	for range queryDocuments(t, reader) {
		break
	}
	_, err = reader.ReadSchema(t.Context())
	require.NoError(t, err)

	// Reads on another goroutine while this one iterates do not hold this goroutine's iterator.
	for _, err := range queryDocuments(t, reader) {
		require.NoError(t, err)
		done := make(chan error)
		go func() {
			_, err := reader.ReadSchema(t.Context())
			done <- err
		}()
		require.NoError(t, <-done)
	}
}
