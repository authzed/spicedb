package setcache

import (
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/tuple"
)

func rel(t *testing.T, s string) tuple.Relationship {
	t.Helper()
	return tuple.MustParse(s)
}

func TestServeAppliesSubjectSelectors(t *testing.T) {
	set := &CachedSet{Complete: true, Rels: []tuple.Relationship{
		rel(t, "document:foo#viewer@user:evan"),
		rel(t, "document:foo#viewer@user:tanner"),
		rel(t, "document:foo#viewer@user:*"),
		rel(t, "document:foo#viewer@group:eng#member"),
	}}
	filter := datastore.RelationshipsFilter{
		OptionalResourceType:     "document",
		OptionalResourceIds:      []string{"foo"},
		OptionalResourceRelation: "viewer",
		OptionalSubjectsSelectors: []datastore.SubjectsSelector{{
			OptionalSubjectType: "user",
			OptionalSubjectIds:  []string{"evan"},
			RelationFilter:      datastore.SubjectRelationFilter{}.WithEllipsisRelation(),
		}},
	}
	got := serveFromSet(set, filter, options.QueryOptions{}, time.Now())
	// The wildcard tuple must not match a concrete-ID selector.
	require.Len(t, got, 1)
	require.Equal(t, "evan", got[0].Subject.ObjectID)

	// A wildcard selector, as check.go builds it, matches the wildcard tuple.
	filter.OptionalSubjectsSelectors = []datastore.SubjectsSelector{{
		OptionalSubjectType: "user",
		OptionalSubjectIds:  []string{tuple.PublicWildcard},
		RelationFilter:      datastore.SubjectRelationFilter{}.WithEllipsisRelation(),
	}}
	got = serveFromSet(set, filter, options.QueryOptions{}, time.Now())
	require.Len(t, got, 1)
	require.Equal(t, tuple.PublicWildcard, got[0].Subject.ObjectID)

	// A userset-only selector, as in the indirect shape of check.go.
	filter.OptionalSubjectsSelectors = []datastore.SubjectsSelector{{
		RelationFilter: datastore.SubjectRelationFilter{}.WithOnlyNonEllipsisRelations(),
	}}
	got = serveFromSet(set, filter, options.QueryOptions{}, time.Now())
	require.Len(t, got, 1)
	require.Equal(t, "group", got[0].Subject.ObjectType)
}

func TestServeFiltersExpiredTuples(t *testing.T) {
	now := time.Now()
	past, future := now.Add(-time.Minute), now.Add(time.Hour)
	expired := rel(t, "document:foo#viewer@user:old")
	expired.OptionalExpiration = &past
	live := rel(t, "document:foo#viewer@user:new")
	live.OptionalExpiration = &future
	set := &CachedSet{Complete: true, Rels: []tuple.Relationship{expired, live}}

	filter := datastore.RelationshipsFilter{
		OptionalResourceType: "document", OptionalResourceIds: []string{"foo"},
		OptionalResourceRelation: "viewer",
	}
	got := serveFromSet(set, filter, options.QueryOptions{}, now)
	require.Len(t, got, 1)
	require.Equal(t, "new", got[0].Subject.ObjectID)
}

func TestServeHonorsSkipOptions(t *testing.T) {
	future := time.Now().Add(time.Hour)
	r := rel(t, "document:foo#viewer@user:evan[somecaveat]")
	r.OptionalExpiration = &future
	set := &CachedSet{Complete: true, Rels: []tuple.Relationship{r}}
	filter := datastore.RelationshipsFilter{
		OptionalResourceType: "document", OptionalResourceIds: []string{"foo"},
		OptionalResourceRelation: "viewer",
	}
	got := serveFromSet(set, filter,
		options.QueryOptions{SkipCaveats: true, SkipExpiration: true}, time.Now())
	require.Len(t, got, 1)
	require.Nil(t, got[0].OptionalCaveat)
	require.Nil(t, got[0].OptionalExpiration)
	// The cached copy must not change.
	require.NotNil(t, set.Rels[0].OptionalCaveat)
}

func TestServeAppliesCaveatAndExpirationFilters(t *testing.T) {
	future := time.Now().Add(time.Hour)
	caveated := rel(t, "document:foo#viewer@user:a[somecaveat]")
	plain := rel(t, "document:foo#viewer@user:b")
	expiring := rel(t, "document:foo#viewer@user:c")
	expiring.OptionalExpiration = &future
	set := &CachedSet{Complete: true, Rels: []tuple.Relationship{caveated, plain, expiring}}
	base := datastore.RelationshipsFilter{
		OptionalResourceType: "document", OptionalResourceIds: []string{"foo"},
		OptionalResourceRelation: "viewer",
	}

	withName := base
	withName.OptionalCaveatNameFilter = datastore.WithCaveatName("somecaveat")
	require.Len(t, serveFromSet(set, withName, options.QueryOptions{}, time.Now()), 1)

	noCaveat := base
	noCaveat.OptionalCaveatNameFilter = datastore.WithNoCaveat()
	require.Len(t, serveFromSet(set, noCaveat, options.QueryOptions{}, time.Now()), 2)

	hasExp := base
	hasExp.OptionalExpirationOption = datastore.ExpirationFilterOptionHasExpiration
	require.Len(t, serveFromSet(set, hasExp, options.QueryOptions{}, time.Now()), 1)
}

// RelationshipsFilter.Test skips the caveat and expiration filters when subject selectors are present.
// serveFromSet must still apply them.
func TestServeAppliesCaveatAndExpirationFiltersWithSelectors(t *testing.T) {
	future := time.Now().Add(time.Hour)
	caveated := rel(t, "document:foo#viewer@user:a[somecaveat]")
	plain := rel(t, "document:foo#viewer@user:b")
	expiring := rel(t, "document:foo#viewer@user:c")
	expiring.OptionalExpiration = &future
	set := &CachedSet{Complete: true, Rels: []tuple.Relationship{caveated, plain, expiring}}
	base := datastore.RelationshipsFilter{
		OptionalResourceType: "document", OptionalResourceIds: []string{"foo"},
		OptionalResourceRelation: "viewer",
		OptionalSubjectsSelectors: []datastore.SubjectsSelector{{
			OptionalSubjectType: "user",
			RelationFilter:      datastore.SubjectRelationFilter{}.WithEllipsisRelation(),
		}},
	}

	withName := base
	withName.OptionalCaveatNameFilter = datastore.WithCaveatName("somecaveat")
	require.Len(t, serveFromSet(set, withName, options.QueryOptions{}, time.Now()), 1)

	noCaveat := base
	noCaveat.OptionalCaveatNameFilter = datastore.WithNoCaveat()
	require.Len(t, serveFromSet(set, noCaveat, options.QueryOptions{}, time.Now()), 2)

	hasExp := base
	hasExp.OptionalExpirationOption = datastore.ExpirationFilterOptionHasExpiration
	require.Len(t, serveFromSet(set, hasExp, options.QueryOptions{}, time.Now()), 1)

	noExp := base
	noExp.OptionalExpirationOption = datastore.ExpirationFilterOptionNoExpiration
	require.Len(t, serveFromSet(set, noExp, options.QueryOptions{}, time.Now()), 2)
}

// With SkipExpiration, the SQL builder omits the expiry predicate and returns expired rows.
// See queryBuilderWithMaybeExpirationFilter in common/sql.go.
func TestServeSkipExpirationReturnsExpired(t *testing.T) {
	now := time.Now()
	past := now.Add(-time.Minute)
	expired := rel(t, "document:foo#viewer@user:old")
	expired.OptionalExpiration = &past
	set := &CachedSet{Complete: true, Rels: []tuple.Relationship{expired}}
	filter := datastore.RelationshipsFilter{OptionalResourceType: "document"}

	require.Empty(t, serveFromSet(set, filter, options.QueryOptions{}, now))
	got := serveFromSet(set, filter, options.QueryOptions{SkipExpiration: true}, now)
	require.Len(t, got, 1)
	require.Nil(t, got[0].OptionalExpiration)
}

func TestSetKeyAndCost(t *testing.T) {
	require.Equal(t, "document:foo#viewer@1", NewSetKey("document", "foo", "viewer", "1").KeyString())
	empty := &CachedSet{}
	full := &CachedSet{Rels: []tuple.Relationship{rel(t, "document:foo#viewer@user:evan")}}
	require.Greater(t, full.Cost(), empty.Cost())
}

// TestServableCoversAllQueryFields pins the query-type fields that servable and serveFromSet must handle.
// A new field may change the result of a query.
// If the cache serves that field without handling it, the results are wrong.
func TestServableCoversAllQueryFields(t *testing.T) {
	fieldNames := func(v any) []string {
		typ := reflect.TypeOf(v)
		names := make([]string, 0, typ.NumField())
		for i := range typ.NumField() {
			names = append(names, typ.Field(i).Name)
		}
		return names
	}
	const msg = "%s gained or lost a field: update servable() (to force pass-through) or serveFromSet() " +
		"(to apply it in memory) in internal/datastore/proxy/setcache, then update this list"

	require.Equal(t, []string{
		"OptionalResourceType",
		"OptionalResourceIds",
		"OptionalResourceIDPrefix",
		"OptionalResourceRelation",
		"OptionalSubjectsSelectors",
		"OptionalCaveatNameFilter",
		"OptionalExpirationOption",
	}, fieldNames(datastore.RelationshipsFilter{}), msg, "datastore.RelationshipsFilter")

	require.Equal(t, []string{
		"Limit",
		"Sort",
		"After",
		"BeforeOrEqual",
		"SkipCaveats",
		"SkipExpiration",
		"SQLCheckAssertionForTest",
		"SQLExplainCallbackForTest",
		"QueryShape",
		"UseTupleComparison",
	}, fieldNames(options.QueryOptions{}), msg, "options.QueryOptions")
}
