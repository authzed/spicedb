package fullrelationcache

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/genutil/mapz"
	"github.com/authzed/spicedb/pkg/tuple"
)

func rel(t *testing.T, s string) tuple.Relationship {
	t.Helper()
	return tuple.MustParse(s)
}

func TestServeFromSetFilterShapes(t *testing.T) {
	set := &CachedSet{Complete: true}
	for _, r := range FilterShapeRels {
		parsed := rel(t, r)
		if parsed.Resource.ObjectID == "foo" && parsed.Resource.Relation == "viewer" {
			set.Rels = append(set.Rels, parsed)
		}
	}

	for _, tc := range FilterShapeCases() {
		t.Run(tc.Name, func(t *testing.T) {
			// The set key scopes the object, so the other resource IDs in the filter change nothing.
			for _, ids := range [][]string{{"foo"}, {"bar", "foo", "missing"}} {
				got := serveFromSet(set, tc.Filter(ids...), options.QueryOptions{}, time.Now())
				subjects := make([]string, 0, len(got))
				for _, r := range got {
					subjects = append(subjects, tuple.StringONR(r.Subject))
				}
				slices.Sort(subjects)
				require.Equal(t, tc.Want, nilIfEmpty(subjects), "resource IDs %v", ids)
			}
		})
	}
}

// nilIfEmpty returns nil for an empty slice, so that it compares equal to a missing Want.
func nilIfEmpty(s []string) []string {
	if len(s) == 0 {
		return nil
	}
	return s
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

func TestTooBigMemoKeyNeverParsesOrCollides(t *testing.T) {
	require.Equal(t, SetKey("!toobig::document:foo#viewer"), NewTooBigMemoKey("document", "foo", "viewer"))

	type onr struct{ resourceType, objectID, relation string }
	onrs := []onr{
		{"document", "foo", "viewer"},
		{"org/document", "a=b|c+d/e_f-g", "viewer"},
		{"document", "*", "viewer"},
		{"!toobig", "foo", "viewer"},
		{"!toobig:", "document", "viewer"},
		{"", "", ""},
		{"document", ":foo", "viewer"},
		{"document", "foo#viewer@1", "viewer"},
		{"document", "foo", "viewer@1"},
		{"!toobig", ":document:foo", "viewer"},
	}
	revisions := []string{"1", "1700000000000000000.0000000001", ""}

	valid := func(o onr) bool {
		_, err := tuple.ParseONR(o.resourceType + ":" + o.objectID + "#" + o.relation)
		return err == nil
	}
	require.True(t, valid(onrs[0]))
	require.True(t, valid(onrs[1]))

	setKeys := map[SetKey]onr{}
	for _, o := range onrs {
		for _, rev := range revisions {
			setKeys[NewSetKey(o.resourceType, o.objectID, o.relation, rev)] = o
		}
	}

	var collisions int
	for _, o := range onrs {
		memoKey := NewTooBigMemoKey(o.resourceType, o.objectID, o.relation)
		s := string(memoKey)

		_, err := tuple.ParseONR(s)
		require.Error(t, err, s)
		_, err = tuple.ParseSubjectONR(s)
		require.Error(t, err, s)
		_, err = tuple.Parse(s)
		require.Error(t, err, s)
		_, err = tuple.Parse(s + "@user:someone")
		require.Error(t, err, s)
		_, err = tuple.ParseV1Rel(s + "@user:someone")
		require.Error(t, err, s)

		// A memo key can equal a set key only if both object#relations are invalid.
		if other, ok := setKeys[memoKey]; ok {
			collisions++
			require.False(t, valid(o), "memo key %q collides for a valid object#relation", s)
			require.False(t, valid(other), "memo key %q collides with the set key of a valid object#relation", s)
		}
	}
	require.Positive(t, collisions, "the adversarial inputs must include a collision of invalid object#relations")
}

// uniqueIDsWithSet is a mapz.Set version of uniqueIDs, kept for comparison in BenchmarkUniqueIDs.
func uniqueIDsWithSet(ids []string) []string {
	seen := mapz.NewSet[string]()
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		if seen.Add(id) {
			out = append(out, id)
		}
	}
	return out
}

// BenchmarkUniqueIDs compares the deduplication of 100 IDs, of which 20 are duplicates.
func BenchmarkUniqueIDs(b *testing.B) {
	ids := make([]string, 0, 100)
	for i := range 80 {
		ids = append(ids, fmt.Sprintf("doc-%d", i))
	}
	for i := range 20 {
		ids = append(ids, fmt.Sprintf("doc-%d", i*3))
	}
	require.Equal(b, uniqueIDs(ids), uniqueIDsWithSet(ids))

	b.Run("presized map", func(b *testing.B) {
		for b.Loop() {
			_ = uniqueIDs(ids)
		}
	})
	b.Run("mapz.Set", func(b *testing.B) {
		for b.Loop() {
			_ = uniqueIDsWithSet(ids)
		}
	})
}
