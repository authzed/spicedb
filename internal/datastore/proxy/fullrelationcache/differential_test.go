package fullrelationcache_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/datastore/proxy/fullrelationcache"
	"github.com/authzed/spicedb/pkg/cache"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/tuple"
)

// differentialRels are the relationships that both the proxy and memdb serve in the differential tests.
// document:foo#owner has no relationships, so queries for it cover the empty set.
// Each expiration is far in the past or the future, so the result never depends on the current time.
var differentialRels = []string{
	"document:foo#viewer@user:evan",
	"document:foo#viewer@user:tanner",
	"document:foo#viewer@user:*",
	"document:foo#viewer@group:eng#member",
	// An ellipsis userset edge.
	"document:foo#viewer@group:sales#...",
	"document:foo#editor@user:evan[cav1]",
	"document:foo#editor@user:jake[cav1:{\"tier\":1}]",
	"document:foo#editor@user:noah",
	"document:foo#commenter@user:evan[expiration:2999-01-01T00:00:00Z]",
	// This relationship is already expired.
	"document:foo#commenter@user:tanner[expiration:2000-01-01T00:00:00Z]",
	"document:foo#commenter@user:noah",
	"document:bar#viewer@user:evan",
	"document:bar#commenter@user:jake[expiration:2999-01-01T00:00:00Z]",
	"document:empty#viewer@user:nobody",
	"folder:root#parent@folder:child",
}

// subjectSelectorSets returns the sets of subject selectors that the differential tests query with.
// Each element is one query's OptionalSubjectsSelectors. The nil element queries without a subject filter.
func subjectSelectorSets() [][]datastore.SubjectsSelector {
	wild := datastore.SubjectsSelector{
		OptionalSubjectType: "user",
		OptionalSubjectIds:  []string{tuple.PublicWildcard},
		RelationFilter:      datastore.SubjectRelationFilter{}.WithEllipsisRelation(),
	}
	evan := datastore.SubjectsSelector{
		OptionalSubjectType: "user",
		OptionalSubjectIds:  []string{"evan"},
		RelationFilter:      datastore.SubjectRelationFilter{}.WithEllipsisRelation(),
	}
	return [][]datastore.SubjectsSelector{
		// No selector, as in Expand, LookupSubjects and arrows.
		nil,
		{evan},
		// The subject and wildcard shape that check.go uses.
		{evan, wild},
		{{
			OptionalSubjectType: "user",
			OptionalSubjectIds:  []string{"missing"},
			RelationFilter:      datastore.SubjectRelationFilter{}.WithEllipsisRelation(),
		}},
		// The non-ellipsis shape that check.go uses.
		{{RelationFilter: datastore.SubjectRelationFilter{}.WithOnlyNonEllipsisRelations()}},
		{{
			OptionalSubjectType: "group",
			RelationFilter:      datastore.SubjectRelationFilter{}.WithNonEllipsisRelation("member"),
		}},
		{wild},
	}
}

func canonical(t *testing.T, r datastore.Reader, f datastore.RelationshipsFilter,
	qOpts []options.QueryOptionsOption,
) []string {
	t.Helper()
	it, err := r.QueryRelationships(t.Context(), f, qOpts...)
	require.NoError(t, err)
	rels, err := datastore.IteratorToSlice(it)
	require.NoError(t, err)
	out := make([]string, 0, len(rels))
	for _, rel := range rels {
		out = append(out, tuple.MustString(rel))
	}
	return out
}

func TestDifferentialAgainstMemdb(t *testing.T) {
	// 1<<20 lets every set fit. 1 makes most sets too big.
	for _, maxSize := range []uint64{1 << 20, 1} {
		t.Run(fmt.Sprintf("cap=%d", maxSize), func(t *testing.T) {
			raw, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
			require.NoError(t, err)
			t.Cleanup(func() { _ = raw.Close() })
			rev := writeRels(t, raw, differentialRels...)

			counting := &countingDatastore{Datastore: raw}
			c, err := cache.NewStandardCache[fullrelationcache.SetKey, *fullrelationcache.CachedSet](&cache.Config{MaxCost: 1 << 26})
			require.NoError(t, err)
			t.Cleanup(c.Close)
			est, err := fullrelationcache.NewAccessCounter(1<<20, time.Minute)
			require.NoError(t, err)
			t.Cleanup(est.Close)
			// Threshold 1 materializes a set on the first query.
			proxied := fullrelationcache.NewProxy(counting, c, est, fullrelationcache.Options{
				MaterializeThreshold: 1, MaximumSetSize: maxSize,
			})

			caveatFilters := []datastore.CaveatNameFilter{
				{}, datastore.WithCaveatName("cav1"), datastore.WithNoCaveat(),
			}
			expOptions := []datastore.ExpirationFilterOption{
				datastore.ExpirationFilterOptionNone,
				datastore.ExpirationFilterOptionHasExpiration,
				datastore.ExpirationFilterOptionNoExpiration,
			}

			// For SkipCaveats and SkipExpiration, the proxy nulls the field and keeps the row, as SQL does.
			// memdb drops such rows instead.
			// Production uses these options only when the schema forbids caveats and expiration on the relation.
			// Thus skip=true runs only on relations whose fixture tuples have neither.
			relations := []struct {
				name     string
				skipSafe bool
			}{
				{"viewer", true},
				{"editor", false},
				{"commenter", false},
				{"owner", true},
			}

			var servedQueries, totalQueries, rowsCompared int
			for _, objIDs := range [][]string{{"foo"}, {"bar"}, {"foo", "bar", "unknown"}} {
				for _, rl := range relations {
					for _, sels := range subjectSelectorSets() {
						for _, cf := range caveatFilters {
							for _, eo := range expOptions {
								skips := []bool{false}
								if rl.skipSafe {
									skips = append(skips, true)
								}
								for _, skip := range skips {
									filter := datastore.RelationshipsFilter{
										OptionalResourceType:      "document",
										OptionalResourceIds:       objIDs,
										OptionalResourceRelation:  rl.name,
										OptionalSubjectsSelectors: sels,
										OptionalCaveatNameFilter:  cf,
										OptionalExpirationOption:  eo,
									}
									var qOpts []options.QueryOptionsOption
									if skip {
										qOpts = append(qOpts,
											options.WithSkipCaveats(true),
											options.WithSkipExpiration(true))
									}
									msg := fmt.Sprintf("divergence: filter=%+v skip=%v", filter, skip)
									want := canonical(t, raw.SnapshotReader(rev), filter, qOpts)
									// Warm the cache.
									_ = canonical(t, proxied.SnapshotReader(rev), filter, qOpts)
									counting.reset()
									got := canonical(t, proxied.SnapshotReader(rev), filter, qOpts)
									require.ElementsMatch(t, want, got, msg)

									totalQueries++
									rowsCompared += len(got)
									if counting.queries.Load() == 0 {
										servedQueries++
									}
									if maxSize == 1<<20 {
										// Every set fits, so every proxied read must come from sets.
										require.Zero(t, counting.queries.Load(), "expected set-served read: %s", msg)
									}
								}
							}
						}
					}
				}
			}

			// Guard against vacuous comparisons.
			require.Greater(t, rowsCompared, 200)
			for _, rl := range []string{"viewer", "editor", "commenter"} {
				require.NotEmpty(t, canonical(t, proxied.SnapshotReader(rev), datastore.RelationshipsFilter{
					OptionalResourceType: "document", OptionalResourceIds: []string{"foo"},
					OptionalResourceRelation: rl,
				}, nil))
			}

			if maxSize != 1<<20 {
				// document:foo#viewer, #editor and #commenter exceed the cap of 1 and go to the delegate.
				// foo#owner and bar#viewer fit.
				require.Positive(t, servedQueries)
				require.Less(t, servedQueries, totalQueries)
				counting.reset()
				_ = canonical(t, proxied.SnapshotReader(rev), datastore.RelationshipsFilter{
					OptionalResourceType: "document", OptionalResourceIds: []string{"foo"},
					OptionalResourceRelation: "viewer",
				}, nil)
				require.Positive(t, counting.queries.Load())
				counting.reset()
				_ = canonical(t, proxied.SnapshotReader(rev), datastore.RelationshipsFilter{
					OptionalResourceType: "document", OptionalResourceIds: []string{"bar"},
					OptionalResourceRelation: "viewer",
				}, nil)
				require.Zero(t, counting.queries.Load())
			}
		})
	}
}

// TestDifferentialServeTimeExpiry materializes a set while a tuple is live and waits for the tuple to expire.
// It checks that the proxy filters the tuple at serve time as memdb does, without a delegate query.
func TestDifferentialServeTimeExpiry(t *testing.T) {
	raw, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
	require.NoError(t, err)
	t.Cleanup(func() { _ = raw.Close() })
	expires := time.Now().Add(1500 * time.Millisecond).UTC().Format(time.RFC3339Nano)
	rev := writeRels(t, raw,
		"document:foo#commenter@user:soon[expiration:"+expires+"]",
		"document:foo#commenter@user:forever",
	)

	counting := &countingDatastore{Datastore: raw}
	c, err := cache.NewStandardCache[fullrelationcache.SetKey, *fullrelationcache.CachedSet](&cache.Config{MaxCost: 1 << 24})
	require.NoError(t, err)
	t.Cleanup(c.Close)
	est, err := fullrelationcache.NewAccessCounter(1<<20, time.Minute)
	require.NoError(t, err)
	t.Cleanup(est.Close)
	proxied := fullrelationcache.NewProxy(counting, c, est, fullrelationcache.Options{MaterializeThreshold: 1, MaximumSetSize: 1 << 20})

	user := func(id string) []datastore.SubjectsSelector {
		return []datastore.SubjectsSelector{{
			OptionalSubjectType: "user", OptionalSubjectIds: []string{id},
			RelationFilter: datastore.SubjectRelationFilter{}.WithEllipsisRelation(),
		}}
	}
	selectorSets := [][]datastore.SubjectsSelector{nil, user("soon"), user("forever")}
	expirationOptions := []datastore.ExpirationFilterOption{
		datastore.ExpirationFilterOptionNone, datastore.ExpirationFilterOptionHasExpiration,
	}
	filters := make([]datastore.RelationshipsFilter, 0, len(selectorSets)*len(expirationOptions))
	for _, sels := range selectorSets {
		for _, eo := range expirationOptions {
			filters = append(filters, datastore.RelationshipsFilter{
				OptionalResourceType: "document", OptionalResourceIds: []string{"foo"},
				OptionalResourceRelation: "commenter", OptionalSubjectsSelectors: sels,
				OptionalExpirationOption: eo,
			})
		}
	}

	// Materialize while both tuples are live.
	live := canonical(t, proxied.SnapshotReader(rev), filters[0], nil)
	require.Len(t, live, 2)

	time.Sleep(1800 * time.Millisecond)

	for _, f := range filters {
		want := canonical(t, raw.SnapshotReader(rev), f, nil)
		counting.reset()
		got := canonical(t, proxied.SnapshotReader(rev), f, nil)
		require.Zero(t, counting.queries.Load(), "must be served from the cached set: %+v", f)
		require.ElementsMatch(t, want, got, "filter=%+v", f)
		for _, s := range got {
			require.NotContains(t, s, "user:soon")
		}
	}
	all := canonical(t, proxied.SnapshotReader(rev), filters[0], nil)
	require.Len(t, all, 1)
}
