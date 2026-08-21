package query

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/caveats"
	"github.com/authzed/spicedb/internal/datastore/dsfortesting"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/testfixtures"
	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/tuple"
)

// compileRecursive writes the schema and relationships to a fresh datastore and
// returns a query context (with a caveat runner) plus the compiled iterator for
// the given definition and relation.
func compileRecursive(t *testing.T, schemaText, def, relation string, rels []tuple.Relationship) (*Context, Iterator) {
	t.Helper()

	rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, 0, memdb.DisableGC)
	require.NoError(t, err)

	ds, revision := testfixtures.DatastoreFromSchemaAndTestRelationships(t, rawDS, schemaText, rels)

	dsSchema, err := ReadSchema(t.Context(), ds, revision)
	require.NoError(t, err)

	outline, err := BuildOutlineFromSchema(dsSchema, def, relation)
	require.NoError(t, err)
	it, err := outline.Compile()
	require.NoError(t, err)

	reader := NewQueryDatastoreReader(datalayer.NewDataLayer(ds).SnapshotReader(revision, datalayer.NoSchemaHashForTesting))
	ctx := NewLocalContext(t.Context(),
		WithReader(reader),
		WithCaveatRunner(caveats.NewCaveatRunner(caveattypes.Default.TypeSet)),
		WithMaxRecursionDepth(defaultMaxRecursionDepth),
	)
	return ctx, it
}

// groupMemberSchema is the canonical directly-cyclic userset relation: a group's
// members are users plus the members of nested groups.
const groupMemberSchema = `
definition user {}
definition group {
	relation member: user | group#member
}
`

// buildGroupChain constructs `depth` nested groups g0..g(depth-1) where
// g0#member@user:tom and g(i)#member@g(i-1)#member. Thus user:tom is transitively
// a member of every group, reached only by walking the full chain.
func buildGroupChain(t *testing.T, depth int) (*Context, Iterator) {
	t.Helper()

	rels := make([]tuple.Relationship, 0, depth)
	rels = append(rels, tuple.MustParse("group:g0#member@user:tom"))
	for i := 1; i < depth; i++ {
		rels = append(rels, tuple.MustParse(fmt.Sprintf("group:g%d#member@group:g%d#member", i, i-1)))
	}
	return compileRecursive(t, groupMemberSchema, "group", "member", rels)
}

// TestDeepChainCheckErrorsRatherThanSilentlyDenying verifies that a Check whose
// answer lies beyond MaxRecursionDepth returns an error, matching the legacy
// engine's MaxDepthExceeded, rather than silently returning NOT_MEMBER.
func TestDeepChainCheckErrorsRatherThanSilentlyDenying(t *testing.T) {
	// The chain is longer than defaultMaxRecursionDepth (50), so the membership of
	// user:tom in group:g59 cannot be determined within the depth budget.
	ctx, it := buildGroupChain(t, 60)

	_, err := ctx.Check(it, NewObject("group", "g59"), NewObject("user", "tom").WithEllipses())
	require.Error(t, err, "a check beyond max recursion depth must error, not silently deny")
	require.ErrorAs(t, err, &MaxRecursionDepthError{})
}

// TestShallowChainCheckSucceeds is the control: a chain within the depth budget
// resolves normally, so the depth error is not spuriously raised.
func TestShallowChainCheckSucceeds(t *testing.T) {
	ctx, it := buildGroupChain(t, 10)

	path, err := ctx.Check(it, NewObject("group", "g9"), NewObject("user", "tom").WithEllipses())
	require.NoError(t, err)
	require.NotNil(t, path, "user:tom is a member of group:g9 via the chain")
}

// TestDeepChainLookupResourcesErrorsRatherThanTruncating verifies that a
// LookupResources whose full result set lies beyond MaxRecursionDepth errors
// rather than silently returning a truncated set.
func TestDeepChainLookupResourcesErrorsRatherThanTruncating(t *testing.T) {
	ctx, it := buildGroupChain(t, 60)

	paths, err := ctx.IterResources(it, NewObject("user", "tom").WithEllipses(), NoObjectFilter())
	require.NoError(t, err)
	_, err = CollectAll(paths)
	require.Error(t, err, "an LR beyond max recursion depth must error, not silently truncate")
	require.ErrorAs(t, err, &MaxRecursionDepthError{})
}

// TestShallowChainLookupResourcesSucceeds is the control for the LR path.
func TestShallowChainLookupResourcesSucceeds(t *testing.T) {
	ctx, it := buildGroupChain(t, 10)

	paths, err := ctx.IterResources(it, NewObject("user", "tom").WithEllipses(), NoObjectFilter())
	require.NoError(t, err)
	results, err := CollectAll(paths)
	require.NoError(t, err)
	// user:tom is a member of all 10 groups g0..g9.
	require.Len(t, results, 10)
}

// caveatedDiamondSchema allows a group's members to be reached via a caveated or
// an uncaveated nested-group edge.
const caveatedDiamondSchema = `
definition user {}
caveat cav1(v bool) { v }
definition group {
	relation member: user | group#member | group#member with cav1
}
`

// caveatedDiamondRels forms a diamond a→{b,c}→...→tom where the short edge a→b is
// caveated but the longer path a→c→b is not. user:tom is therefore an
// UNCONDITIONAL member of group:a (via a→c→b→tom), so a correct Check must return
// a nil caveat. The pre-fix implementation contaminates every descendant of b
// with cav1, because b is first reached via the caveated edge and never
// re-expanded once reached uncaveated.
var caveatedDiamondRels = []tuple.Relationship{
	tuple.MustParse("group:a#member@group:b#member[cav1]"),
	tuple.MustParse("group:a#member@group:c#member"),
	tuple.MustParse("group:c#member@group:b#member"),
	tuple.MustParse("group:b#member@user:tom"),
}

// TestCaveatedDiamondCheckIsUnconditional is the core B1 soundness test: a subject
// reachable by an uncaveated path must not be reported as conditionally reachable.
func TestCaveatedDiamondCheckIsUnconditional(t *testing.T) {
	ctx, it := compileRecursive(t, caveatedDiamondSchema, "group", "member", caveatedDiamondRels)

	path, err := ctx.Check(it, NewObject("group", "a"), NewObject("user", "tom").WithEllipses())
	require.NoError(t, err)
	require.NotNil(t, path, "user:tom is a member of group:a")
	require.Nil(t, path.Caveat, "membership is unconditional via a→c→b→tom; got caveat %v", path.Caveat)
}

// TestCaveatedDiamondIterSubjectsIsUnconditional checks the same soundness property
// on the IterSubjects path: every descendant reachable uncaveated must be nil.
func TestCaveatedDiamondIterSubjectsIsUnconditional(t *testing.T) {
	ctx, it := compileRecursive(t, caveatedDiamondSchema, "group", "member", caveatedDiamondRels)

	paths, err := ctx.IterSubjects(it, NewObject("group", "a"), NewType("user"))
	require.NoError(t, err)
	results, err := CollectAll(paths)
	require.NoError(t, err)

	require.Len(t, results, 1)
	require.Equal(t, "tom", results[0].Subject.ObjectID)
	require.Nil(t, results[0].Caveat, "user:tom is an unconditional member of group:a; got caveat %v", results[0].Caveat)
}

// TestCaveatedDiamondIterResourcesIsUnconditional checks the soundness property on
// the IterResources (LookupResources) path: group:a is an unconditional resource
// for user:tom via a→c→b→tom, so its path must carry a nil caveat.
func TestCaveatedDiamondIterResourcesIsUnconditional(t *testing.T) {
	ctx, it := compileRecursive(t, caveatedDiamondSchema, "group", "member", caveatedDiamondRels)

	paths, err := ctx.IterResources(it, NewObject("user", "tom").WithEllipses(), NoObjectFilter())
	require.NoError(t, err)
	results, err := CollectAll(paths)
	require.NoError(t, err)

	byResource := make(map[string]*Path, len(results))
	for _, p := range results {
		byResource[p.Resource.ObjectID] = p
	}
	require.Contains(t, byResource, "a", "group:a must be a resource for user:tom")
	require.Nil(t, byResource["a"].Caveat, "user:tom's membership in group:a is unconditional; got caveat %v", byResource["a"].Caveat)
}

// TestCaveatedCycleTerminates verifies the fixpoint converges on cyclic data with a
// caveated edge — the canonical condition prevents unbounded caveat growth — and
// still resolves the unconditional membership correctly.
func TestCaveatedCycleTerminates(t *testing.T) {
	rels := []tuple.Relationship{
		tuple.MustParse("group:a#member@group:b#member[cav1]"),
		tuple.MustParse("group:b#member@group:a#member"),
		tuple.MustParse("group:a#member@user:tom"),
	}
	ctx, it := compileRecursive(t, caveatedDiamondSchema, "group", "member", rels)

	path, err := ctx.Check(it, NewObject("group", "a"), NewObject("user", "tom").WithEllipses())
	require.NoError(t, err)
	require.NotNil(t, path, "user:tom is a direct member of group:a")
	require.Nil(t, path.Caveat, "direct membership is unconditional; got caveat %v", path.Caveat)
}
