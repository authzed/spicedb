//go:build ci
// +build ci

package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/datastore/test"
	"github.com/authzed/spicedb/internal/testfixtures"
	testdatastore "github.com/authzed/spicedb/internal/testserver/datastore"
)

func TestCRDBPGDatastore(t *testing.T) {
	b := testdatastore.NewCRDBPGBuilder(t)
	test.All(t, test.DatastoreTesterFunc(func(revisionFuzzingTimedelta, gcWindow time.Duration, watchBufferLength uint16) (datastore.Datastore, error) {
		ds := b.NewDatastore(t, func(engine, uri string) datastore.Datastore {
			ds, err := NewPostgresDatastore(uri,
				RevisionFuzzingTimedelta(revisionFuzzingTimedelta),
				GCWindow(gcWindow),
				WatchBufferLength(watchBufferLength),
			)
			require.NoError(t, err)
			return ds
		})
		return ds, nil
	}))
}

func BenchmarkCRDBQuery(b *testing.B) {
	req := require.New(b)

	ds := testdatastore.NewCRDBBuilder(b).NewDatastore(b, func(engine, uri string) datastore.Datastore {
		ds, err := NewPostgresDatastore(uri,
			RevisionFuzzingTimedelta(0),
			GCWindow(24*time.Hour),
			WatchBufferLength(1),
		)
		require.NoError(b, err)
		return ds
	})

	_, revision := testfixtures.StandardDatastoreWithData(ds, req)

	b.Run("benchmark checks", func(b *testing.B) {
		require := require.New(b)

		for i := 0; i < b.N; i++ {
			iter, err := ds.QueryTuples(context.Background(), &v1.RelationshipFilter{
				ResourceType: testfixtures.DocumentNS.Name,
			}, revision)
			require.NoError(err)

			defer iter.Close()

			for tpl := iter.Next(); tpl != nil; tpl = iter.Next() {
				require.Equal(testfixtures.DocumentNS.Name, tpl.ObjectAndRelation.Namespace)
			}
			require.NoError(iter.Err())
		}
	})
}
