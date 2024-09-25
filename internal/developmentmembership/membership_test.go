package developmentmembership

import (
	"sort"
	"testing"

	"github.com/authzed/spicedb/internal/caveats"

	"github.com/stretchr/testify/require"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"

	"github.com/authzed/spicedb/pkg/graph"
	"github.com/authzed/spicedb/pkg/testutil"
	"github.com/authzed/spicedb/pkg/tuple"
)

var (
	ONR      = tuple.CoreONR
	Ellipsis = "..."
)

func DS(objectType string, objectID string, objectRelation string) *core.DirectSubject {
	return &core.DirectSubject{
		Subject: ONR(objectType, objectID, objectRelation),
	}
}

func CaveatedDS(objectType string, objectID string, objectRelation string, caveatName string) *core.DirectSubject {
	return &core.DirectSubject{
		Subject:          ONR(objectType, objectID, objectRelation),
		CaveatExpression: caveats.CaveatExprForTesting(caveatName),
	}
}

var (
	_this    *tuple.ObjectAndRelation
	ownerONR = tuple.ONR("folder", "company", "owner")

	companyOwner = graph.Leaf(&ownerONR,
		(DS("user", "owner", Ellipsis)),
	)
	companyEditor = graph.Union(tuple.ONRRef("folder", "company", "editor"),
		graph.Leaf(_this, (DS("user", "writer", Ellipsis))),
		companyOwner,
	)

	auditorONR    = tuple.ONR("folder", "auditors", "owner")
	auditorsOwner = graph.Leaf(&auditorONR)

	auditorsEditor = graph.Union(tuple.ONRRef("folder", "auditors", "editor"),
		graph.Leaf(_this),
		auditorsOwner,
	)

	auditorsViewerRecursive = graph.Union(tuple.ONRRef("folder", "auditors", "viewer"),
		graph.Leaf(_this,
			(DS("user", "auditor", "...")),
		),
		auditorsEditor,
		graph.Union(tuple.ONRRef("folder", "auditors", "viewer")),
	)

	companyViewerRecursive = graph.Union(tuple.ONRRef("folder", "company", "viewer"),
		graph.Union(tuple.ONRRef("folder", "company", "viewer"),
			auditorsViewerRecursive,
			graph.Leaf(_this,
				(DS("user", "legal", "...")),
				(DS("folder", "auditors", "viewer")),
			),
		),
		companyEditor,
		graph.Union(tuple.ONRRef("folder", "company", "viewer")),
	)
)

func TestMembershipSetBasic(t *testing.T) {
	require := require.New(t)
	ms := NewMembershipSet()

	// Add some expansion trees.
	fso, ok, err := ms.AddExpansion(tuple.ONR("folder", "company", "owner"), companyOwner)
	require.True(ok)
	require.NoError(err)
	verifySubjects(t, require, fso, "user:owner")

	fse, ok, err := ms.AddExpansion(tuple.ONR("folder", "company", "editor"), companyEditor)
	require.True(ok)
	require.NoError(err)
	verifySubjects(t, require, fse, "user:owner", "user:writer")

	fsv, ok, err := ms.AddExpansion(tuple.ONR("folder", "company", "viewer"), companyViewerRecursive)
	require.True(ok)
	require.NoError(err)
	verifySubjects(t, require, fsv, "folder:auditors#viewer", "user:auditor", "user:legal", "user:owner", "user:writer")
}

func TestMembershipSetIntersectionBasic(t *testing.T) {
	require := require.New(t)
	ms := NewMembershipSet()

	intersection := graph.Intersection(tuple.ONRRef("folder", "company", "viewer"),
		graph.Leaf(_this,
			(DS("user", "legal", "...")),
		),
		graph.Leaf(_this,
			(DS("user", "owner", "...")),
			(DS("user", "legal", "...")),
		),
	)

	fso, ok, err := ms.AddExpansion(tuple.ONR("folder", "company", "viewer"), intersection)
	require.True(ok)
	require.NoError(err)
	verifySubjects(t, require, fso, "user:legal")
}

func TestMembershipSetIntersectionWithDifferentTypesOneMissingLeft(t *testing.T) {
	require := require.New(t)
	ms := NewMembershipSet()

	intersection := graph.Intersection(tuple.ONRRef("folder", "company", "viewer"),
		graph.Leaf(_this,
			(DS("user", "legal", "...")),
			(DS("folder", "foobar", "...")),
		),
		graph.Leaf(_this,
			(DS("user", "owner", "...")),
			(DS("user", "legal", "...")),
		),
	)

	fso, ok, err := ms.AddExpansion(tuple.ONR("folder", "company", "viewer"), intersection)
	require.True(ok)
	require.NoError(err)
	verifySubjects(t, require, fso, "user:legal")
}

func TestMembershipSetIntersectionWithDifferentTypesOneMissingRight(t *testing.T) {
	require := require.New(t)
	ms := NewMembershipSet()

	intersection := graph.Intersection(tuple.ONRRef("folder", "company", "viewer"),
		graph.Leaf(_this,
			(DS("user", "legal", "...")),
		),
		graph.Leaf(_this,
			(DS("user", "owner", "...")),
			(DS("user", "legal", "...")),
			(DS("folder", "foobar", "...")),
		),
	)

	fso, ok, err := ms.AddExpansion(tuple.ONR("folder", "company", "viewer"), intersection)
	require.True(ok)
	require.NoError(err)
	verifySubjects(t, require, fso, "user:legal")
}

func TestMembershipSetIntersectionWithDifferentTypes(t *testing.T) {
	require := require.New(t)
	ms := NewMembershipSet()

	intersection := graph.Intersection(tuple.ONRRef("folder", "company", "viewer"),
		graph.Leaf(_this,
			(DS("user", "legal", "...")),
			(DS("folder", "foobar", "...")),
			(DS("folder", "barbaz", "...")),
		),
		graph.Leaf(_this,
			(DS("user", "owner", "...")),
			(DS("user", "legal", "...")),
			(DS("folder", "barbaz", "...")),
		),
	)

	fso, ok, err := ms.AddExpansion(tuple.ONR("folder", "company", "viewer"), intersection)
	require.True(ok)
	require.NoError(err)
	verifySubjects(t, require, fso, "folder:barbaz", "user:legal")
}

func TestMembershipSetExclusion(t *testing.T) {
	require := require.New(t)
	ms := NewMembershipSet()

	exclusion := graph.Exclusion(tuple.ONRRef("folder", "company", "viewer"),
		graph.Leaf(_this,
			(DS("user", "owner", "...")),
			(DS("user", "legal", "...")),
		),
		graph.Leaf(_this,
			(DS("user", "legal", "...")),
		),
	)

	fso, ok, err := ms.AddExpansion(tuple.ONR("folder", "company", "viewer"), exclusion)
	require.True(ok)
	require.NoError(err)
	verifySubjects(t, require, fso, "user:owner")
}

func TestMembershipSetExclusionMultiple(t *testing.T) {
	require := require.New(t)
	ms := NewMembershipSet()

	exclusion := graph.Exclusion(tuple.ONRRef("folder", "company", "viewer"),
		graph.Leaf(_this,
			(DS("user", "owner", "...")),
			(DS("user", "legal", "...")),
			(DS("user", "third", "...")),
		),
		graph.Leaf(_this,
			(DS("user", "legal", "...")),
		),
		graph.Leaf(_this,
			(DS("user", "owner", "...")),
		),
	)

	fso, ok, err := ms.AddExpansion(tuple.ONR("folder", "company", "viewer"), exclusion)
	require.True(ok)
	require.NoError(err)
	verifySubjects(t, require, fso, "user:third")
}

func TestMembershipSetExclusionMultipleWithWildcard(t *testing.T) {
	require := require.New(t)
	ms := NewMembershipSet()

	exclusion := graph.Exclusion(tuple.ONRRef("folder", "company", "viewer"),
		graph.Leaf(_this,
			(DS("user", "owner", "...")),
			(DS("user", "legal", "...")),
		),
		graph.Leaf(_this,
			(DS("user", "legal", "...")),
		),
		graph.Leaf(_this,
			(DS("user", "*", "...")),
		),
	)

	fso, ok, err := ms.AddExpansion(tuple.ONR("folder", "company", "viewer"), exclusion)
	require.True(ok)
	require.NoError(err)
	verifySubjects(t, require, fso)
}

func TestMembershipSetExclusionMultipleMiddle(t *testing.T) {
	require := require.New(t)
	ms := NewMembershipSet()

	exclusion := graph.Exclusion(tuple.ONRRef("folder", "company", "viewer"),
		graph.Leaf(_this,
			(DS("user", "owner", "...")),
			(DS("user", "legal", "...")),
			(DS("user", "third", "...")),
		),
		graph.Leaf(_this,
			(DS("user", "another", "...")),
		),
		graph.Leaf(_this,
			(DS("user", "owner", "...")),
		),
	)

	fso, ok, err := ms.AddExpansion(tuple.ONR("folder", "company", "viewer"), exclusion)
	require.True(ok)
	require.NoError(err)
	verifySubjects(t, require, fso, "user:third", "user:legal")
}

func TestMembershipSetIntersectionWithOneWildcard(t *testing.T) {
	require := require.New(t)
	ms := NewMembershipSet()

	intersection := graph.Intersection(tuple.ONRRef("folder", "company", "viewer"),
		graph.Leaf(_this,
			(DS("user", "owner", "...")),
			(DS("user", "*", "...")),
		),
		graph.Leaf(_this,
			(DS("user", "legal", "...")),
		),
	)

	fso, ok, err := ms.AddExpansion(tuple.ONR("folder", "company", "viewer"), intersection)
	require.True(ok)
	require.NoError(err)
	verifySubjects(t, require, fso, "user:legal")
}

func TestMembershipSetIntersectionWithAllWildcardLeft(t *testing.T) {
	require := require.New(t)
	ms := NewMembershipSet()

	intersection := graph.Intersection(tuple.ONRRef("folder", "company", "viewer"),
		graph.Leaf(_this,
			(DS("user", "owner", "...")),
			(DS("user", "*", "...")),
		),
		graph.Leaf(_this,
			(DS("user", "*", "...")),
		),
	)

	fso, ok, err := ms.AddExpansion(tuple.ONR("folder", "company", "viewer"), intersection)
	require.True(ok)
	require.NoError(err)
	verifySubjects(t, require, fso, "user:*", "user:owner")
}

func TestMembershipSetIntersectionWithAllWildcardRight(t *testing.T) {
	require := require.New(t)
	ms := NewMembershipSet()

	intersection := graph.Intersection(tuple.ONRRef("folder", "company", "viewer"),
		graph.Leaf(_this,
			(DS("user", "*", "...")),
		),
		graph.Leaf(_this,
			(DS("user", "owner", "...")),
			(DS("user", "*", "...")),
		),
	)

	fso, ok, err := ms.AddExpansion(tuple.ONR("folder", "company", "viewer"), intersection)
	require.True(ok)
	require.NoError(err)
	verifySubjects(t, require, fso, "user:*", "user:owner")
}

func TestMembershipSetExclusionWithLeftWildcard(t *testing.T) {
	require := require.New(t)
	ms := NewMembershipSet()

	exclusion := graph.Exclusion(tuple.ONRRef("folder", "company", "viewer"),
		graph.Leaf(_this,
			(DS("user", "owner", "...")),
			(DS("user", "*", "...")),
		),
		graph.Leaf(_this,
			(DS("user", "legal", "...")),
		),
	)

	fso, ok, err := ms.AddExpansion(tuple.ONR("folder", "company", "viewer"), exclusion)
	require.True(ok)
	require.NoError(err)
	verifySubjects(t, require, fso, "user:*", "user:owner")
}

func TestMembershipSetExclusionWithRightWildcard(t *testing.T) {
	require := require.New(t)
	ms := NewMembershipSet()

	exclusion := graph.Exclusion(tuple.ONRRef("folder", "company", "viewer"),
		graph.Leaf(_this,
			(DS("user", "owner", "...")),
			(DS("user", "legal", "...")),
		),
		graph.Leaf(_this,
			(DS("user", "*", "...")),
		),
	)

	fso, ok, err := ms.AddExpansion(tuple.ONR("folder", "company", "viewer"), exclusion)
	require.True(ok)
	require.NoError(err)
	verifySubjects(t, require, fso)
}

func TestMembershipSetIntersectionWithThreeWildcards(t *testing.T) {
	require := require.New(t)
	ms := NewMembershipSet()

	intersection := graph.Intersection(tuple.ONRRef("folder", "company", "viewer"),
		graph.Leaf(_this,
			(DS("user", "owner", "...")),
			(DS("user", "legal", "...")),
		),
		graph.Leaf(_this,
			(DS("user", "*", "...")),
		),
		graph.Leaf(_this,
			(DS("user", "*", "...")),
		),
	)

	fso, ok, err := ms.AddExpansion(tuple.ONR("folder", "company", "viewer"), intersection)
	require.True(ok)
	require.NoError(err)
	verifySubjects(t, require, fso, "user:owner", "user:legal")
}

func TestMembershipSetIntersectionWithOneBranchMissingWildcards(t *testing.T) {
	require := require.New(t)
	ms := NewMembershipSet()

	intersection := graph.Intersection(tuple.ONRRef("folder", "company", "viewer"),
		graph.Leaf(_this,
			(DS("user", "owner", "...")),
			(DS("user", "legal", "...")),
			(DS("user", "*", "...")),
		),
		graph.Leaf(_this,
			(DS("user", "owner", "...")),
		),
		graph.Leaf(_this,
			(DS("user", "*", "...")),
		),
	)

	fso, ok, err := ms.AddExpansion(tuple.ONR("folder", "company", "viewer"), intersection)
	require.True(ok)
	require.NoError(err)
	verifySubjects(t, require, fso, "user:owner")
}

func TestMembershipSetIntersectionWithTwoBranchesMissingWildcards(t *testing.T) {
	require := require.New(t)
	ms := NewMembershipSet()

	intersection := graph.Intersection(tuple.ONRRef("folder", "company", "viewer"),
		graph.Leaf(_this,
			(DS("user", "owner", "...")),
			(DS("user", "legal", "...")),
		),
		graph.Leaf(_this,
			(DS("user", "another", "...")),
		),
		graph.Leaf(_this,
			(DS("user", "*", "...")),
		),
	)

	fso, ok, err := ms.AddExpansion(tuple.ONR("folder", "company", "viewer"), intersection)
	require.True(ok)
	require.NoError(err)
	verifySubjects(t, require, fso)
}

func TestMembershipSetWithCaveats(t *testing.T) {
	require := require.New(t)
	ms := NewMembershipSet()

	intersection := graph.Intersection(tuple.ONRRef("folder", "company", "viewer"),
		graph.Leaf(_this,
			(DS("user", "owner", "...")),
			(DS("user", "legal", "...")),
			(DS("user", "*", "...")),
		),
		graph.Leaf(_this,
			(CaveatedDS("user", "owner", "...", "somecaveat")),
		),
		graph.Leaf(_this,
			(DS("user", "*", "...")),
		),
	)
	intersection.CaveatExpression = caveats.CaveatExprForTesting("anothercaveat")

	fso, ok, err := ms.AddExpansion(tuple.ONR("folder", "company", "viewer"), intersection)
	require.True(ok)
	require.NoError(err)
	verifySubjects(t, require, fso, "user:owner")

	// Verify the caveat on the user:owner.
	subject, ok := fso.LookupSubject(tuple.ONR("user", "owner", "..."))
	require.True(ok)

	testutil.RequireProtoEqual(t, subject.GetCaveatExpression(), caveats.And(
		caveats.CaveatExprForTesting("anothercaveat"),
		caveats.CaveatExprForTesting("somecaveat"),
	), "found invalid caveat expr for subject")
}

func verifySubjects(t *testing.T, require *require.Assertions, fs FoundSubjects, expected ...string) {
	foundSubjects := []tuple.ObjectAndRelation{}
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
