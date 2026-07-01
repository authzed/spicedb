//go:build datastore

package crdb

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"

	testdatastore "github.com/authzed/spicedb/internal/testserver/datastore"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/namespace"
	"github.com/authzed/spicedb/pkg/tuple"
)

// BenchmarkWriteRelationshipsMixed measures a ReadWriteTx whose WriteRelationships
// call contains CREATE + TOUCH + DELETE operations (three batchable statements)
// plus transaction metadata (metadata insert + overlap key = two more batchable
// statements). On the batched branch these collapse from ~5 statements into 2
// network flushes; on main each is its own round-trip.
//
// Run against the branch, then `git stash` the implementation and re-run to A/B.
func BenchmarkWriteRelationshipsMixed(b *testing.B) {
	engine := testdatastore.RunCRDBForTesting(b, "", crdbTestVersion())
	ctx := context.Background()

	ds := engine.NewDatastore(b, func(_, uri string) datastore.Datastore {
		d, err := NewCRDBDatastore(ctx, uri, OverlapStrategy(overlapStrategyPrefix), WithAcquireTimeout(5*time.Second))
		require.NoError(b, err)
		b.Cleanup(func() { _ = d.Close() })
		return d
	})

	resourceNS := namespace.Namespace("resource", namespace.MustRelation("reader", nil))
	userNS := namespace.Namespace("user")

	_, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.LegacyWriteNamespaces(ctx, resourceNS, userNS)
	})
	require.NoError(b, err)

	md, err := structpb.NewStruct(map[string]any{"source": "benchmark"})
	require.NoError(b, err)

	const ops = 10

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		updates := make([]tuple.RelationshipUpdate, 0, ops*3)
		for k := 0; k < ops; k++ {
			updates = append(updates,
				tuple.Create(tuple.MustParse(fmt.Sprintf("resource:c%d_%d#reader@user:u%d", i, k, k))),
				tuple.Touch(tuple.MustParse(fmt.Sprintf("resource:t%d_%d#reader@user:u%d", i, k, k))),
				// Deletes of non-existent rels are no-ops but still emit the DELETE statement.
				tuple.Delete(tuple.MustParse(fmt.Sprintf("resource:d%d_%d#reader@user:u%d", i, k, k))),
			)
		}

		_, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
			return rwt.WriteRelationships(ctx, updates)
		}, options.WithMetadata(md))
		require.NoError(b, err)
	}
	b.StopTimer()
}
