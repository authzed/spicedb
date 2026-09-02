package query

import (
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/dsfortesting"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/tuple"
)

const selfEdgeSchema = `
definition user {}

definition group {
	relation member: user | group#member
	relation banned: user | group#member
	permission active = member - banned
}
`

// iterSubjectsWithCounts runs a LookupSubjects against the given relationships
// and returns the subjects found plus the datastore queries it took.
func iterSubjectsWithCounts(
	t *testing.T,
	relationships []string,
	resourceID, permission string,
	target ObjectType,
) ([]string, ReaderCounts) {
	t.Helper()
	require := require.New(t)

	rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, 0, memdb.DisableGC)
	require.NoError(err)

	parsed := make([]tuple.Relationship, 0, len(relationships))
	for _, rel := range relationships {
		parsed = append(parsed, tuple.MustParse(rel))
	}
	ds, revision := testfixtures.DatastoreFromSchemaAndTestRelationships(t, rawDS, selfEdgeSchema, parsed)

	dsSchema, err := ReadSchema(t.Context(), ds, revision)
	require.NoError(err)
	canonicalOutline, err := BuildOutlineFromSchema(dsSchema, "group", permission)
	require.NoError(err)
	it, err := canonicalOutline.Compile()
	require.NoError(err)

	reader := NewCountingReader(
		NewQueryDatastoreReader(datalayer.NewDataLayer(ds).SnapshotReader(revision, datalayer.NoSchemaHashForTesting)),
	)
	ctx := NewLocalContext(t.Context(), WithReader(reader))

	pathSeq, err := ctx.IterSubjects(it, NewObject("group", resourceID), target)
	require.NoError(err)
	paths, err := CollectAll(pathSeq)
	require.NoError(err)

	found := make([]string, 0, len(paths))
	for _, path := range paths {
		found = append(found, fmt.Sprintf("%s:%s#%s", path.Subject.ObjectType, path.Subject.ObjectID, path.Subject.Relation))
	}
	sort.Strings(found)
	return found, reader.Counts()
}

// TestAliasSelfEdge covers the reflexive identity subject: enumerating the
// subjects of `group:a#member` includes `group:a#member` itself.
//
// The expected values are the classic dispatcher's, captured by running the
// same queries through internal/dispatch/graph. Classic decides this with a
// comparison (lookupsubjects.go, `req.SubjectRelation` against
// `req.ResourceRelation`) and never queries for it, which is the property the
// first case pins: asking for the identity costs no extra round-trips.
func TestAliasSelfEdge(t *testing.T) {
	deepHierarchy := []string{
		"group:a#member@group:b#member",
		"group:b#member@group:c#member",
		"group:c#member@user:alice",
	}
	exclusion := []string{
		"group:a#member@group:b#member",
		"group:a#banned@group:b#member",
		"group:b#member@user:alice",
	}

	t.Run("identity is included when the target relation matches", func(t *testing.T) {
		require := require.New(t)

		found, counts := iterSubjectsWithCounts(t, deepHierarchy, "a", "member", NewType("group", "member"))

		require.Equal([]string{"group:a#member", "group:b#member", "group:c#member"}, found,
			"group:a is reflexively one of its own members")

		// The same traversal with a target that admits no identity must cost the
		// same: whether the identity applies is a comparison, not a lookup.
		_, withoutIdentity := iterSubjectsWithCounts(t, deepHierarchy, "a", "member", NewType("user"))
		require.Equal(withoutIdentity.Total(), counts.Total(),
			"deciding the identity must not cost a datastore query")
	})

	t.Run("identity is excluded when the target is a different type", func(t *testing.T) {
		require := require.New(t)

		found, _ := iterSubjectsWithCounts(t, deepHierarchy, "a", "member", NewType("user"))

		require.Equal([]string{"user:alice#..."}, found,
			"a group is not a user, so no identity subject applies")
	})

	t.Run("identity is per relation, so exclusion cancels asymmetrically", func(t *testing.T) {
		require := require.New(t)

		// `active = member - banned`. The identity applies to `member`, whose
		// relation is the target, but not to `banned`, whose relation is not —
		// so group:a survives the exclusion while group:b does not.
		found, _ := iterSubjectsWithCounts(t, exclusion, "a", "active", NewType("group", "member"))

		require.Equal([]string{"group:a#member"}, found)
	})

	t.Run("identity does not leak into a differently typed target", func(t *testing.T) {
		require := require.New(t)

		found, _ := iterSubjectsWithCounts(t, exclusion, "a", "active", NewType("user"))

		require.Empty(found, "alice is both a member and banned, so nothing survives")
	})
}
