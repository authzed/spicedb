//go:build datastore && postgres

package postgres

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	testdatastore "github.com/authzed/spicedb/internal/testserver/datastore"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/tuple"
)

func TestRelationshipsCreatedAfter(t *testing.T) {
	ctx := t.Context()
	b := testdatastore.RunPostgresForTesting(t, postgresConfig.pgVersion, postgresConfig.pgbouncer)
	ds := b.NewDatastore(t, func(engine, uri string) datastore.Datastore {
		ds, err := NewPostgresDatastore(ctx, uri)
		require.NoError(t, err)
		return ds
	})

	write := func(updates ...tuple.RelationshipUpdate) {
		t.Helper()
		_, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
			return rwt.WriteRelationships(ctx, updates)
		})
		require.NoError(t, err)
	}

	write(tuple.Create(tuple.MustParse("document:before#viewer@user:tom")))

	head, err := ds.HeadRevision(ctx)
	require.NoError(t, err)

	write(
		tuple.Create(tuple.MustParse("document:after#viewer@user:tom")),
		tuple.Create(tuple.MustParse("document:after#editor@user:sarah")),
		tuple.Create(tuple.MustParse("document:caveated#viewer@user:tom[somecaveat:{\"a\":1}]")),
		tuple.Create(tuple.MustParse("document:expired#viewer@user:tom[expiration:2000-01-01T00:00:00Z]")),
		tuple.Create(tuple.MustParse("document:deleted#viewer@user:tom")),
		tuple.Create(tuple.MustParse("folder:after#parent@document:after#viewer")),
	)
	write(tuple.Delete(tuple.MustParse("document:deleted#viewer@user:tom")))

	reader := datastore.UnwrapAs[datastore.CreatedRelationshipsReader](ds)
	require.NotNil(t, reader)
	querier, err := reader.RelationshipsCreatedAfter(head.Revision)
	require.NoError(t, err)

	collect := func(it datastore.RelationshipIterator, err error) []string {
		t.Helper()
		require.NoError(t, err)
		var rels []string
		for rel, err := range it {
			require.NoError(t, err)
			rels = append(rels, tuple.MustString(rel))
		}
		return rels
	}

	t.Run("resource type", func(t *testing.T) {
		// Not "before", which the revision includes, nor the expired or deleted relationships.
		require.ElementsMatch(t, []string{
			"document:after#viewer@user:tom",
			"document:after#editor@user:sarah",
			"document:caveated#viewer@user:tom[somecaveat:{\"a\":1}]",
		}, collect(querier.QueryRelationships(ctx, datastore.RelationshipsFilter{OptionalResourceType: "document"})))
	})

	t.Run("relation and subject", func(t *testing.T) {
		require.ElementsMatch(t, []string{"document:after#editor@user:sarah"}, collect(querier.QueryRelationships(ctx, datastore.RelationshipsFilter{
			OptionalResourceType:     "document",
			OptionalResourceRelation: "editor",
			OptionalSubjectsSelectors: []datastore.SubjectsSelector{{
				OptionalSubjectType: "user",
				RelationFilter:      datastore.SubjectRelationFilter{}.WithEllipsisRelation(),
			}},
		})))
	})

	t.Run("caveat", func(t *testing.T) {
		require.ElementsMatch(t, []string{"document:caveated#viewer@user:tom[somecaveat:{\"a\":1}]"}, collect(querier.QueryRelationships(ctx, datastore.RelationshipsFilter{
			OptionalResourceType:     "document",
			OptionalCaveatNameFilter: datastore.WithCaveatName("somecaveat"),
		})))
	})

	t.Run("limit", func(t *testing.T) {
		require.Len(t, collect(querier.QueryRelationships(ctx, datastore.RelationshipsFilter{OptionalResourceType: "document"},
			options.WithLimit(options.LimitOne))), 1)
	})

	t.Run("reverse", func(t *testing.T) {
		require.ElementsMatch(t, []string{"folder:after#parent@document:after#viewer"}, collect(querier.ReverseQueryRelationships(ctx, datastore.SubjectsFilter{
			SubjectType:    "document",
			RelationFilter: datastore.SubjectRelationFilter{}.WithRelation("viewer"),
		})))
	})

	t.Run("nothing created after head", func(t *testing.T) {
		latest, err := ds.HeadRevision(ctx)
		require.NoError(t, err)
		querier, err := reader.RelationshipsCreatedAfter(latest.Revision)
		require.NoError(t, err)
		require.Empty(t, collect(querier.QueryRelationships(ctx, datastore.RelationshipsFilter{OptionalResourceType: "document"})))
	})
}
