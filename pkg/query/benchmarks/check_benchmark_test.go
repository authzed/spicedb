package benchmarks

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	bm "github.com/authzed/spicedb/pkg/benchmarks"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/query"
)

// networkDelay is the simulated per-call round-trip latency used by the delay
// sub-benchmarks. Adjust this to model different network environments.
const networkDelay = 100 * time.Microsecond

// advisorWarmUp holds per-benchmark overrides for the number of
// warm-up iterations used to seed the CountAdvisor. Benchmarks not listed
// here default to 1.
var advisorWarmUp = map[string]int{
	"DoubleWideArrow": 10,
}

const defaultWarmupIterations = 1

func BenchmarkCheck(b *testing.B) {
	for _, benchmark := range directBenchmarks() {
		b.Run(benchmark.Name, func(b *testing.B) {
			ctx := b.Context()

			rawDS, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
			require.NoError(b, err)
			b.Cleanup(func() {
				_ = rawDS.Close()
			})

			queries, err := benchmark.Setup(ctx, rawDS)
			require.NoError(b, err)
			require.NotEmpty(b, queries.Checks)

			check := queries.Checks[0]

			revision, err := rawDS.HeadRevision(ctx)
			require.NoError(b, err)

			dsSchema, err := bm.ReadSchema(ctx, rawDS, revision.Revision)
			require.NoError(b, err)

			canonicalOutline, err := query.BuildOutlineFromSchema(dsSchema, check.ResourceType, check.Permission)
			require.NoError(b, err)

			resource := query.NewObject(check.ResourceType, check.ResourceID)
			subject := query.NewObject(check.SubjectType, check.SubjectID).WithEllipses()

			qReader := query.NewQueryDatastoreReader(datalayer.NewDataLayer(rawDS).SnapshotReader(revision.Revision, datalayer.SchemaHash(revision.SchemaHash)))
			delayReader := query.NewDelayReader(networkDelay, qReader)

			var ctxOpts []query.ContextOption
			if queries.MaxRecursionDepth > 0 {
				ctxOpts = append(ctxOpts, query.WithMaxRecursionDepth(queries.MaxRecursionDepth))
			}

			warmUpIterations := 1
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
					_, err = warmCtx.Check(warmIt, resource, subject)
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
						path, err := queryCtx.Check(it, resource, subject)
						require.NoError(b, err)
						require.NotNil(b, path)
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
					path, err := queryCtx.Check(advisedIt, resource, subject)
					require.NoError(b, err)
					require.NotNil(b, path)
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
							path, err := queryCtx.Check(it, resource, subject)
							require.NoError(b, err)
							require.NotNil(b, path)
						}
					})
				}

				b.Run("advised_delay", func(b *testing.B) {
					advisedIt := buildAdvisedIterator(b, delayReader)

					opts := append([]query.ContextOption{query.WithReader(delayReader)}, ctxOpts...)
					queryCtx := query.NewLocalContext(ctx, opts...)

					b.ResetTimer()
					for b.Loop() {
						path, err := queryCtx.Check(advisedIt, resource, subject)
						require.NoError(b, err)
						require.NotNil(b, path)
					}
				})
			}
		})
	}
}
