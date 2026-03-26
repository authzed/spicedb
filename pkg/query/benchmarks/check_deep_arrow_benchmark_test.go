package benchmarks

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/query"
	"github.com/authzed/spicedb/pkg/schema/v2"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
	"github.com/authzed/spicedb/pkg/tuple"
)

// BenchmarkCheckDeepArrow benchmarks permission checking through a deep recursive chain.
// This recreates the testharness scenario with:
// - A 30+ level deep parent chain: document:target -> document:1 -> ... -> document:30
// - document:29#view@user:slow
// - Checking if user:slow has viewer permission on document:target
//
// The permission viewer = view + parent->viewer creates a recursive traversal through
// all 30+ levels to find the view relationship at the end of the chain.
//
// Four sub-benchmarks are run:
//   - plain:         compile the outline directly and run Check each iteration
//   - advised:       seed a CountAdvisor from a single warm-up run, apply it to the
//     canonical outline, compile once, then run Check each iteration
//   - plain_delay:   same as plain, but with a delay reader simulating network latency
//   - advised_delay: same as advised, but with a delay reader simulating network latency
func BenchmarkCheckDeepArrow(b *testing.B) {
	// ---- shared setup ----

	rawDS, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
	require.NoError(b, err)

	ctx := context.Background()

	schemaText := `
		definition user {}

		definition document {
			relation parent: document
			relation view: user
			permission viewer = view + parent->viewer
		}
	`

	compiled, err := compiler.Compile(compiler.InputSchema{
		Source:       input.Source("benchmark"),
		SchemaString: schemaText,
	}, compiler.AllowUnprefixedObjectType())
	require.NoError(b, err)

	_, err = rawDS.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.LegacyWriteNamespaces(ctx, compiled.ObjectDefinitions...)
	})
	require.NoError(b, err)

	// Build relationships for the deep arrow scenario.
	// Chain: document:target -> document:1 -> document:2 -> ... -> document:30
	// Plus: document:29#view@user:slow
	relationships := make([]tuple.Relationship, 0, 33)
	relationships = append(relationships, tuple.MustParse("document:target#parent@document:1"))
	for i := 1; i <= 30; i++ {
		rel := fmt.Sprintf("document:%d#parent@document:%d", i, i+1)
		relationships = append(relationships, tuple.MustParse(rel))
	}
	relationships = append(relationships, tuple.MustParse("document:29#view@user:slow"))

	revision, err := common.WriteRelationships(ctx, rawDS, tuple.UpdateOperationCreate, relationships...)
	require.NoError(b, err)

	dsSchema, err := schema.BuildSchemaFromDefinitions(compiled.ObjectDefinitions, nil)
	require.NoError(b, err)

	// Build the canonical outline once; all sub-benchmarks derive from it.
	canonicalOutline, err := query.BuildOutlineFromSchema(dsSchema, "document", "viewer")
	require.NoError(b, err)

	// The resource and subject are the same for all sub-benchmarks.
	resources := query.NewObjects("document", "target")
	subject := query.NewObject("user", "slow").WithEllipses()

	// Base reader (no simulated latency).
	reader := query.NewQueryDatastoreReader(datalayer.NewDataLayer(rawDS).SnapshotReader(revision, datalayer.NoSchemaHashForTesting))

	// Delay reader wrapping the base reader with simulated network latency.
	delayReader := query.NewDelayReader(networkDelay, reader)

	// buildAdvisedIterator seeds a CountAdvisor from a single warm-up run using the
	// provided reader and returns the compiled advised iterator.
	buildAdvisedIterator := func(b *testing.B, r query.QueryDatastoreReader) query.Iterator {
		b.Helper()
		obs := query.NewCountObserver()
		warmIt, err := canonicalOutline.Compile()
		require.NoError(b, err)
		warmCtx := query.NewLocalContext(ctx,
			query.WithReader(r),
			query.WithObserver(obs),
			query.WithMaxRecursionDepth(50),
		)
		seq, err := warmCtx.Check(warmIt, resources, subject)
		require.NoError(b, err)
		_, err = query.CollectAll(seq)
		require.NoError(b, err)

		advisor := query.NewCountAdvisor(obs.GetStats())
		advisedCO, err := query.ApplyAdvisor(canonicalOutline, advisor)
		require.NoError(b, err)
		advisedIt, err := advisedCO.Compile()
		require.NoError(b, err)
		return advisedIt
	}

	// ---- plain sub-benchmark ----

	b.Run("plain", func(b *testing.B) {
		it, err := canonicalOutline.Compile()
		require.NoError(b, err)

		b.Log("plain explain:\n", it.Explain())

		queryCtx := query.NewLocalContext(ctx,
			query.WithReader(reader),
			query.WithMaxRecursionDepth(50),
		)

		b.ResetTimer()
		for b.Loop() {
			seq, err := queryCtx.Check(it, resources, subject)
			require.NoError(b, err)
			paths, err := query.CollectAll(seq)
			require.NoError(b, err)
			require.Len(b, paths, 1)
			require.Equal(b, "slow", paths[0].Subject.ObjectID)
		}
	})

	// ---- advised sub-benchmark ----

	b.Run("advised", func(b *testing.B) {
		advisedIt := buildAdvisedIterator(b, reader)

		b.Log("advised explain:\n", advisedIt.Explain())

		queryCtx := query.NewLocalContext(ctx,
			query.WithReader(reader),
			query.WithMaxRecursionDepth(50),
		)

		b.ResetTimer()
		for b.Loop() {
			seq, err := queryCtx.Check(advisedIt, resources, subject)
			require.NoError(b, err)
			paths, err := query.CollectAll(seq)
			require.NoError(b, err)
			require.Len(b, paths, 1)
			require.Equal(b, "slow", paths[0].Subject.ObjectID)
		}
	})

	// ---- plain_delay sub-benchmark ----

	b.Run("plain_delay", func(b *testing.B) {
		it, err := canonicalOutline.Compile()
		require.NoError(b, err)

		queryCtx := query.NewLocalContext(ctx,
			query.WithReader(delayReader),
			query.WithMaxRecursionDepth(50),
		)

		b.ResetTimer()
		for b.Loop() {
			seq, err := queryCtx.Check(it, resources, subject)
			require.NoError(b, err)
			paths, err := query.CollectAll(seq)
			require.NoError(b, err)
			require.Len(b, paths, 1)
			require.Equal(b, "slow", paths[0].Subject.ObjectID)
		}
	})

	// ---- advised_delay sub-benchmark ----

	b.Run("advised_delay", func(b *testing.B) {
		advisedIt := buildAdvisedIterator(b, delayReader)
		queryCtx := query.NewLocalContext(ctx,
			query.WithReader(delayReader),
			query.WithMaxRecursionDepth(50),
		)

		b.ResetTimer()
		for b.Loop() {
			seq, err := queryCtx.Check(advisedIt, resources, subject)
			require.NoError(b, err)
			paths, err := query.CollectAll(seq)
			require.NoError(b, err)
			require.Len(b, paths, 1)
			require.Equal(b, "slow", paths[0].Subject.ObjectID)
		}
	})
}
