package membership

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"

	"github.com/authzed/spicedb/pkg/graph"
	"github.com/authzed/spicedb/pkg/testutil"
	"github.com/authzed/spicedb/pkg/tuple"
)

var (
	ONR      = tuple.ObjectAndRelation
	Ellipsis = "..."
)

var (
	_this *core.ObjectAndRelation

	companyOwner = graph.Leaf(ONR("folder", "company", "owner"),
		tuple.User(ONR("user", "owner", Ellipsis)),
	)
	companyEditor = graph.Union(ONR("folder", "company", "editor"),
		graph.Leaf(_this, tuple.User(ONR("user", "writer", Ellipsis))),
		companyOwner,
	)

	auditorsOwner = graph.Leaf(ONR("folder", "auditors", "owner"))

	auditorsEditor = graph.Union(ONR("folder", "auditors", "editor"),
		graph.Leaf(_this),
		auditorsOwner,
	)

	auditorsViewerRecursive = graph.Union(ONR("folder", "auditors", "viewer"),
		graph.Leaf(_this,
			tuple.User(ONR("user", "auditor", "...")),
		),
		auditorsEditor,
		graph.Union(ONR("folder", "auditors", "viewer")),
	)

	companyViewerRecursive = graph.Union(ONR("folder", "company", "viewer"),
		graph.Union(ONR("folder", "company", "viewer"),
			auditorsViewerRecursive,
			graph.Leaf(_this,
				tuple.User(ONR("user", "legal", "...")),
				tuple.User(ONR("folder", "auditors", "viewer")),
			),
		),
		companyEditor,
		graph.Union(ONR("folder", "company", "viewer")),
	)
)

func TestMembershipSetBasic(t *testing.T) {
	require := require.New(t)
	ms := NewMembershipSet()

	// Add some expansion trees.
	fso, ok, err := ms.AddExpansion(ONR("folder", "company", "owner"), companyOwner)
	require.True(ok)
	require.NoError(err)
	verifySubjects(t, require, fso, "user:owner")

	fse, ok, err := ms.AddExpansion(ONR("folder", "company", "editor"), companyEditor)
	require.True(ok)
	require.NoError(err)
	verifySubjects(t, require, fse, "user:owner", "user:writer")

	fsv, ok, err := ms.AddExpansion(ONR("folder", "company", "viewer"), companyViewerRecursive)
	require.True(ok)
	require.NoError(err)
	verifySubjects(t, require, fsv, "folder:auditors#viewer", "user:auditor", "user:legal", "user:owner", "user:writer")
}

func TestMembershipSetIntersectionBasic(t *testing.T) {
	require := require.New(t)
	ms := NewMembershipSet()

	intersection := graph.Intersection(ONR("folder", "company", "viewer"),
		graph.Leaf(_this,
			tuple.User(ONR("user", "legal", "...")),
		),
		graph.Leaf(_this,
			tuple.User(ONR("user", "owner", "...")),
			tuple.User(ONR("user", "legal", "...")),
		),
	)

	fso, ok, err := ms.AddExpansion(ONR("folder", "company", "viewer"), intersection)
	require.True(ok)
	require.NoError(err)
	verifySubjects(t, require, fso, "user:legal")
}

func TestMembershipSetExclusion(t *testing.T) {
	require := require.New(t)
	ms := NewMembershipSet()

	exclusion := graph.Exclusion(ONR("folder", "company", "viewer"),
		graph.Leaf(_this,
			tuple.User(ONR("user", "owner", "...")),
			tuple.User(ONR("user", "legal", "...")),
		),
		graph.Leaf(_this,
			tuple.User(ONR("user", "legal", "...")),
		),
	)

	fso, ok, err := ms.AddExpansion(ONR("folder", "company", "viewer"), exclusion)
	require.True(ok)
	require.NoError(err)
	verifySubjects(t, require, fso, "user:owner")
}

func TestMembershipSetExclusionMultiple(t *testing.T) {
	require := require.New(t)
	ms := NewMembershipSet()

	exclusion := graph.Exclusion(ONR("folder", "company", "viewer"),
		graph.Leaf(_this,
			tuple.User(ONR("user", "owner", "...")),
			tuple.User(ONR("user", "legal", "...")),
			tuple.User(ONR("user", "third", "...")),
		),
		graph.Leaf(_this,
			tuple.User(ONR("user", "legal", "...")),
		),
		graph.Leaf(_this,
			tuple.User(ONR("user", "owner", "...")),
		),
	)

	fso, ok, err := ms.AddExpansion(ONR("folder", "company", "viewer"), exclusion)
	require.True(ok)
	require.NoError(err)
	verifySubjects(t, require, fso, "user:third")
}

func TestMembershipSetExclusionMultipleWithWildcard(t *testing.T) {
	require := require.New(t)
	ms := NewMembershipSet()

	exclusion := graph.Exclusion(ONR("folder", "company", "viewer"),
		graph.Leaf(_this,
			tuple.User(ONR("user", "owner", "...")),
			tuple.User(ONR("user", "legal", "...")),
		),
		graph.Leaf(_this,
			tuple.User(ONR("user", "legal", "...")),
		),
		graph.Leaf(_this,
			tuple.User(ONR("user", "*", "...")),
		),
	)

	fso, ok, err := ms.AddExpansion(ONR("folder", "company", "viewer"), exclusion)
	require.True(ok)
	require.NoError(err)
	verifySubjects(t, require, fso)
}

func TestMembershipSetExclusionMultipleMiddle(t *testing.T) {
	require := require.New(t)
	ms := NewMembershipSet()

	exclusion := graph.Exclusion(ONR("folder", "company", "viewer"),
		graph.Leaf(_this,
			tuple.User(ONR("user", "owner", "...")),
			tuple.User(ONR("user", "legal", "...")),
			tuple.User(ONR("user", "third", "...")),
		),
		graph.Leaf(_this,
			tuple.User(ONR("user", "another", "...")),
		),
		graph.Leaf(_this,
			tuple.User(ONR("user", "owner", "...")),
		),
	)

	fso, ok, err := ms.AddExpansion(ONR("folder", "company", "viewer"), exclusion)
	require.True(ok)
	require.NoError(err)
	verifySubjects(t, require, fso, "user:third", "user:legal")
}

func TestMembershipSetIntersectionWithOneWildcard(t *testing.T) {
	require := require.New(t)
	ms := NewMembershipSet()

	intersection := graph.Intersection(ONR("folder", "company", "viewer"),
		graph.Leaf(_this,
			tuple.User(ONR("user", "owner", "...")),
			tuple.User(ONR("user", "*", "...")),
		),
		graph.Leaf(_this,
			tuple.User(ONR("user", "legal", "...")),
		),
	)

	fso, ok, err := ms.AddExpansion(ONR("folder", "company", "viewer"), intersection)
	require.True(ok)
	require.NoError(err)
	verifySubjects(t, require, fso, "user:legal")
}

func TestMembershipSetIntersectionWithAllWildcardLeft(t *testing.T) {
	require := require.New(t)
	ms := NewMembershipSet()

	intersection := graph.Intersection(ONR("folder", "company", "viewer"),
		graph.Leaf(_this,
			tuple.User(ONR("user", "owner", "...")),
			tuple.User(ONR("user", "*", "...")),
		),
		graph.Leaf(_this,
			tuple.User(ONR("user", "*", "...")),
		),
	)

	fso, ok, err := ms.AddExpansion(ONR("folder", "company", "viewer"), intersection)
	require.True(ok)
	require.NoError(err)
	verifySubjects(t, require, fso, "user:*", "user:owner")
}

func TestMembershipSetIntersectionWithAllWildcardRight(t *testing.T) {
	require := require.New(t)
	ms := NewMembershipSet()

	intersection := graph.Intersection(ONR("folder", "company", "viewer"),
		graph.Leaf(_this,
			tuple.User(ONR("user", "*", "...")),
		),
		graph.Leaf(_this,
			tuple.User(ONR("user", "owner", "...")),
			tuple.User(ONR("user", "*", "...")),
		),
	)

	fso, ok, err := ms.AddExpansion(ONR("folder", "company", "viewer"), intersection)
	require.True(ok)
	require.NoError(err)
	verifySubjects(t, require, fso, "user:*", "user:owner")
}

func TestMembershipSetExclusionWithLeftWildcard(t *testing.T) {
	require := require.New(t)
	ms := NewMembershipSet()

	exclusion := graph.Exclusion(ONR("folder", "company", "viewer"),
		graph.Leaf(_this,
			tuple.User(ONR("user", "owner", "...")),
			tuple.User(ONR("user", "*", "...")),
		),
		graph.Leaf(_this,
			tuple.User(ONR("user", "legal", "...")),
		),
	)

	fso, ok, err := ms.AddExpansion(ONR("folder", "company", "viewer"), exclusion)
	require.True(ok)
	require.NoError(err)
	verifySubjects(t, require, fso, "user:*", "user:owner")
}

func TestMembershipSetExclusionWithRightWildcard(t *testing.T) {
	require := require.New(t)
	ms := NewMembershipSet()

	exclusion := graph.Exclusion(ONR("folder", "company", "viewer"),
		graph.Leaf(_this,
			tuple.User(ONR("user", "owner", "...")),
			tuple.User(ONR("user", "legal", "...")),
		),
		graph.Leaf(_this,
			tuple.User(ONR("user", "*", "...")),
		),
	)

	fso, ok, err := ms.AddExpansion(ONR("folder", "company", "viewer"), exclusion)
	require.True(ok)
	require.NoError(err)
	verifySubjects(t, require, fso)
}

func TestMembershipSetIntersectionWithThreeWildcards(t *testing.T) {
	require := require.New(t)
	ms := NewMembershipSet()

	intersection := graph.Intersection(ONR("folder", "company", "viewer"),
		graph.Leaf(_this,
			tuple.User(ONR("user", "owner", "...")),
			tuple.User(ONR("user", "legal", "...")),
		),
		graph.Leaf(_this,
			tuple.User(ONR("user", "*", "...")),
		),
		graph.Leaf(_this,
			tuple.User(ONR("user", "*", "...")),
		),
	)

	fso, ok, err := ms.AddExpansion(ONR("folder", "company", "viewer"), intersection)
	require.True(ok)
	require.NoError(err)
	verifySubjects(t, require, fso, "user:owner", "user:legal")
}

func TestMembershipSetIntersectionWithOneBranchMissingWildcards(t *testing.T) {
	require := require.New(t)
	ms := NewMembershipSet()

	intersection := graph.Intersection(ONR("folder", "company", "viewer"),
		graph.Leaf(_this,
			tuple.User(ONR("user", "owner", "...")),
			tuple.User(ONR("user", "legal", "...")),
			tuple.User(ONR("user", "*", "...")),
		),
		graph.Leaf(_this,
			tuple.User(ONR("user", "owner", "...")),
		),
		graph.Leaf(_this,
			tuple.User(ONR("user", "*", "...")),
		),
	)

	fso, ok, err := ms.AddExpansion(ONR("folder", "company", "viewer"), intersection)
	require.True(ok)
	require.NoError(err)
	verifySubjects(t, require, fso, "user:owner")
}

func TestMembershipSetIntersectionWithTwoBranchesMissingWildcards(t *testing.T) {
	require := require.New(t)
	ms := NewMembershipSet()

	intersection := graph.Intersection(ONR("folder", "company", "viewer"),
		graph.Leaf(_this,
			tuple.User(ONR("user", "owner", "...")),
			tuple.User(ONR("user", "legal", "...")),
		),
		graph.Leaf(_this,
			tuple.User(ONR("user", "another", "...")),
		),
		graph.Leaf(_this,
			tuple.User(ONR("user", "*", "...")),
		),
	)

	fso, ok, err := ms.AddExpansion(ONR("folder", "company", "viewer"), intersection)
	require.True(ok)
	require.NoError(err)
	verifySubjects(t, require, fso)
}

func verifySubjects(t *testing.T, require *require.Assertions, fs FoundSubjects, expected ...string) {
	foundSubjects := []*core.ObjectAndRelation{}
	for _, found := range fs.ListFound() {
		foundSubjects = append(foundSubjects, found.Subject())

		_, ok := fs.LookupSubject(found.Subject())
		require.True(ok, "Could not find expected subject %s", found.Subject())
	}

	found := tuple.StringsONRs(foundSubjects)
	sort.Strings(expected)
	sort.Strings(found)

	testutil.RequireEqualEmptyNil(t, expected, found)
}
