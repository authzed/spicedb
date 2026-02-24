package benchmarks

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	bm "github.com/authzed/spicedb/pkg/benchmarks"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/query"
)

func BenchmarkLookupResources(b *testing.B) {
	for _, benchmark := range directBenchmarks() {
		b.Run(benchmark.Name, func(b *testing.B) {
			ctx := b.Context()

			rawDS, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
			require.NoError(b, err)

			queries, err := benchmark.Setup(ctx, rawDS)
			require.NoError(b, err)

			if len(queries.IterResources) == 0 {
				b.Skip("no IterResources queries defined")
			}

			lrQuery := queries.IterResources[0]

			revision, err := rawDS.HeadRevision(ctx)
			require.NoError(b, err)

			dsSchema, err := bm.ReadSchema(ctx, rawDS, revision.Revision)
			require.NoError(b, err)

			canonicalOutline, err := query.BuildOutlineFromSchema(dsSchema, lrQuery.FilterResourceType, lrQuery.Permission)
			require.NoError(b, err)

			subject := query.NewObject(lrQuery.SubjectType, lrQuery.SubjectID).WithEllipses()
			filterResourceType := query.NoObjectFilter()

			qReader := query.NewQueryDatastoreReader(datalayer.NewDataLayer(rawDS).SnapshotReader(revision.Revision, datalayer.SchemaHash(revision.SchemaHash)))
			delayReader := query.NewDelayReader(networkDelay, qReader)

			var ctxOpts []query.ContextOption
			if queries.MaxRecursionDepth > 0 {
				ctxOpts = append(ctxOpts, query.WithMaxRecursionDepth(queries.MaxRecursionDepth))
			}

			warmUpIterations := defaultWarmupIterations
			if n, ok := advisorWarmUp[benchmark.Name]; ok {
				warmUpIterations = n
			}

			buildAdvisedIterator := func(b *testing.B, r query.QueryDatastoreReader) query.Iterator {
				b.Helper()
				obs := query.NewCountObserver()
				warmIt, err := canonicalOutline.Compile()
				require.NoError(b, err)

				opts := append([]query.ContextOption{
					query.WithReader(r),
					query.WithObserver(obs),
				}, ctxOpts...)
				warmCtx := query.NewLocalContext(ctx, opts...)

				for range warmUpIterations {
					_, err = warmCtx.IterResources(warmIt, subject, filterResourceType)
					require.NoError(b, err)
				}

				advisor := query.NewCountAdvisor(obs.GetStats())
				advisedCO, err := query.ApplyAdvisor(canonicalOutline, advisor)
				require.NoError(b, err)
				advisedIt, err := advisedCO.Compile()
				require.NoError(b, err)
				return advisedIt
			}

			if *includePlain {
				b.Run("plain", func(b *testing.B) {
					it, err := canonicalOutline.Compile()
					require.NoError(b, err)

					b.Log("plain explain:\n", it.Explain())

					opts := append([]query.ContextOption{query.WithReader(qReader)}, ctxOpts...)
					queryCtx := query.NewLocalContext(ctx, opts...)

					b.ResetTimer()
					for b.Loop() {
						paths, err := queryCtx.IterResources(it, subject, filterResourceType)
						require.NoError(b, err)
						results, err := query.CollectAll(paths)
						require.NoError(b, err)
						require.Len(b, results, len(lrQuery.ExpectedResourceIDs))
					}
				})
			}

			b.Run("advised", func(b *testing.B) {
				advisedIt := buildAdvisedIterator(b, qReader)

				b.Log("advised explain:\n", advisedIt.Explain())

				opts := append([]query.ContextOption{query.WithReader(qReader)}, ctxOpts...)
				queryCtx := query.NewLocalContext(ctx, opts...)

				b.ResetTimer()
				for b.Loop() {
					paths, err := queryCtx.IterResources(advisedIt, subject, filterResourceType)
					require.NoError(b, err)
					results, err := query.CollectAll(paths)
					require.NoError(b, err)
					require.Len(b, results, len(lrQuery.ExpectedResourceIDs))
				}
			})

			if *includeDelay {
				if *includePlain {
					b.Run("plain_delay", func(b *testing.B) {
						it, err := canonicalOutline.Compile()
						require.NoError(b, err)

						opts := append([]query.ContextOption{query.WithReader(delayReader)}, ctxOpts...)
						queryCtx := query.NewLocalContext(ctx, opts...)

						b.ResetTimer()
						for b.Loop() {
							paths, err := queryCtx.IterResources(it, subject, filterResourceType)
							require.NoError(b, err)
							results, err := query.CollectAll(paths)
							require.NoError(b, err)
							require.Len(b, results, len(lrQuery.ExpectedResourceIDs))
						}
					})
				}

				b.Run("advised_delay", func(b *testing.B) {
					advisedIt := buildAdvisedIterator(b, delayReader)

					opts := append([]query.ContextOption{query.WithReader(delayReader)}, ctxOpts...)
					queryCtx := query.NewLocalContext(ctx, opts...)

					b.ResetTimer()
					for b.Loop() {
						paths, err := queryCtx.IterResources(advisedIt, subject, filterResourceType)
						require.NoError(b, err)
						results, err := query.CollectAll(paths)
						require.NoError(b, err)
						require.Len(b, results, len(lrQuery.ExpectedResourceIDs))
					}
				})
			}
		})
	}
}
