package benchmark

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb/internal/datastore/postgres"
	"github.com/authzed/spicedb/internal/testfixtures"
	testdatastore "github.com/authzed/spicedb/internal/testserver/datastore"
	"github.com/authzed/spicedb/internal/testserver/datastore/config"
	dsconfig "github.com/authzed/spicedb/pkg/cmd/datastore"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/tuple"
)

// The seeded data mimics the statistical shape of a large production
// relation_tuple table rather than its absolute size, since the shape is what
// determines the query plans Postgres chooses:
//
//   - A large background population with a modest number of distinct subjects
//     (production tables commonly have on the order of 10k) spread evenly over
//     many (namespace, relation) pairs. This gives the columns of relation_tuple
//     production-like selectivity estimates, and gives the subject-leading
//     indexes the same size ratio to the primary key as on a large table.
//   - Small per-scenario namespaces that differ only in subject fan-in: the
//     number of relationships sharing one subject on one relation. Each scenario
//     is kept to a small fraction of the table so that its own subjects do not
//     dominate the column statistics, and the plan stays the same across
//     scenarios while the fan-in varies.
const (
	// Background population: evenly spread subjects, as on a production table.
	pgDeleteBenchBackgroundRows       = 2_000_000
	pgDeleteBenchBackgroundNamespaces = 20
	pgDeleteBenchBackgroundRelations  = 50
	pgDeleteBenchBackgroundSubjects   = 13_000

	// Per scenario: one namespace with this many relations and rows per relation.
	// The rows per relation is also the fan-in of the shared and wildcard scenarios,
	// and must exceed the largest delete limit below.
	pgDeleteBenchScenarioRelations       = 2
	pgDeleteBenchScenarioRowsPerRelation = 21_000
)

var pgDeleteBenchLimits = []uint64{100, 1000}

// errRollbackBenchmarkTx is returned from the ReadWriteTx callback to roll back
// the deletion, so every iteration operates on the same set of live rows.
var errRollbackBenchmarkTx = errors.New("rollback benchmark transaction")

// pgDeleteBenchScenario describes a set of relationships living in their own
// namespace that share a subject fan-in pattern.
type pgDeleteBenchScenario struct {
	name      string
	namespace string
	// subjectIDSQL is a SQL expression yielding the subject object ID for the
	// row numbered g in generate_series; r is the row's relation index.
	subjectIDSQL string
}

var pgDeleteBenchScenarios = []pgDeleteBenchScenario{
	{
		// Every relationship has a subject unique to its resource (fan-in of 1).
		name:         "UniqueSubject",
		namespace:    "unique",
		subjectIDSQL: "'unique-' || g",
	},
	{
		// Every relationship on a relation shares one subject, so each subject has
		// a fan-in of pgDeleteBenchScenarioRowsPerRelation on that relation.
		name:         "SharedSubject",
		namespace:    "shared",
		subjectIDSQL: "'shared-' || r",
	},
	{
		// Every relationship is granted to the wildcard subject.
		name:         "WildcardSubject",
		namespace:    "wild",
		subjectIDSQL: "'" + tuple.PublicWildcard + "'",
	},
}

// BenchmarkPostgresDeleteWithLimit benchmarks DeleteRelationships with a delete
// limit against Postgres, on a table shaped like a large production deployment,
// for subjects with differing fan-in. Requires Docker.
//
// Run with:
//
//	go test ./internal/datastore/benchmark/ -bench BenchmarkPostgresDeleteWithLimit \
//	    -benchmem -run '^$' -timeout 60m
func BenchmarkPostgresDeleteWithLimit(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping postgres delete benchmarks in -short mode")
	}

	b.StopTimer()
	ctx := b.Context()

	engine := testdatastore.RunDatastoreEngine(b, postgres.Engine)

	var dsURI string
	initFunc := config.DatastoreConfigInitFunc(
		b,
		dsconfig.WithRevisionQuantization(5*time.Second),
		dsconfig.WithGCWindow(2*time.Hour),
		dsconfig.WithGCInterval(1*time.Hour),
		dsconfig.WithWatchBufferLength(1000),
		dsconfig.WithWriteAcquisitionTimeout(5*time.Second),
	)
	ds := engine.NewDatastore(b, func(engine, uri string) datastore.Datastore {
		dsURI = uri
		return initFunc(engine, uri)
	})
	b.Cleanup(func() { _ = ds.Close() })

	ds, _ = testfixtures.StandardDatastoreWithSchema(b, ds)

	// Write one relationship through the datastore so a transaction exists whose
	// xid the bulk-seeded rows can be created under.
	_, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{
			tuple.Create(tuple.MustParse("document:seed#viewer@user:seed")),
		})
	})
	require.NoError(b, err)

	// Maintenance connection, used for bulk seeding and, off the timer, to keep
	// the table's statistics and dead-tuple count representative between iterations.
	maintConn, err := pgx.Connect(ctx, dsURI)
	require.NoError(b, err)
	b.Cleanup(func() { _ = maintConn.Close(context.Background()) })

	// Seed in bulk with SQL: writing millions of rows through WriteRelationships
	// takes minutes per million, while generate_series takes seconds.
	const insertRows = `INSERT INTO relation_tuple (namespace, object_id, relation, userset_namespace, userset_object_id, userset_relation, created_xid)
		SELECT %s, 'obj-' || g, 'rel' || r, 'user', %s, '...', (SELECT max(xid) FROM relation_tuple_transaction)
		FROM generate_series(0, %d) g, LATERAL (SELECT %s AS r) rel`

	_, err = maintConn.Exec(ctx, fmt.Sprintf(insertRows,
		fmt.Sprintf("'bg-' || (g %% %d)", pgDeleteBenchBackgroundNamespaces),
		fmt.Sprintf("'user-' || (g %% %d)", pgDeleteBenchBackgroundSubjects),
		pgDeleteBenchBackgroundRows-1,
		fmt.Sprintf("(g / %d) %% %d", pgDeleteBenchBackgroundNamespaces, pgDeleteBenchBackgroundRelations),
	))
	require.NoError(b, err, "seeding background rows")

	for _, scenario := range pgDeleteBenchScenarios {
		_, err := maintConn.Exec(ctx, fmt.Sprintf(insertRows,
			"'"+scenario.namespace+"'",
			scenario.subjectIDSQL,
			pgDeleteBenchScenarioRelations*pgDeleteBenchScenarioRowsPerRelation-1,
			fmt.Sprintf("g %% %d", pgDeleteBenchScenarioRelations),
		))
		require.NoError(b, err, "seeding scenario %s", scenario.name)
	}

	vacuumAnalyze := func(b *testing.B) {
		_, err := maintConn.Exec(ctx, "VACUUM ANALYZE relation_tuple")
		require.NoError(b, err)
	}
	vacuumAnalyze(b)

	for _, scenario := range pgDeleteBenchScenarios {
		b.Run(scenario.name, func(b *testing.B) {
			for _, limit := range pgDeleteBenchLimits {
				b.Run(fmt.Sprintf("limit=%d", limit), func(b *testing.B) {
					filter := &v1.RelationshipFilter{
						ResourceType:     scenario.namespace,
						OptionalRelation: "rel0",
						OptionalSubjectFilter: &v1.SubjectFilter{
							SubjectType: "user",
						},
					}

					b.ResetTimer()
					for range b.N {
						b.StopTimer()
						// Each rolled-back iteration leaves behind a dead tuple version per
						// updated row; vacuum them away so later iterations are not penalized.
						vacuumAnalyze(b)
						b.StartTimer()

						_, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
							numDeleted, limitReached, err := rwt.DeleteRelationships(ctx, filter, options.WithDeleteLimit(&limit))
							if err != nil {
								return err
							}
							if numDeleted != limit || !limitReached {
								return fmt.Errorf("expected to delete %d rows and hit the limit, deleted %d (limit reached: %v)", limit, numDeleted, limitReached)
							}
							return errRollbackBenchmarkTx
						}, options.WithDisableRetries(true))
						require.ErrorIs(b, err, errRollbackBenchmarkTx)
					}
				})
			}
		})
	}
}
