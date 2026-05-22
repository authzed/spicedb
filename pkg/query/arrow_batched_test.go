package query

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/genutil/slicez"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

// runArrowCheckInBothModes runs the same Check assertion against an arrow with
// BatchedArrows = false and BatchedArrows = true. The two modes must produce
// equivalent results for any input — that is the contract that lets the
// dispatch executor enable batching unconditionally.
func runArrowCheckInBothModes(t *testing.T, name string, arrowFactory func() *ArrowIterator, resource Object, subject ObjectAndRelation, expectMatch bool, expectedSubjectID string) {
	t.Helper()
	for _, batched := range []bool{false, true} {
		mode := "unbatched"
		if batched {
			mode = "batched"
		}
		t.Run(name+"/"+mode, func(t *testing.T) {
			req := require.New(t)
			ctx := NewTestContext(t)
			ctx.BatchedArrows = batched

			path, err := ctx.Check(arrowFactory(), resource, subject)
			req.NoError(err)
			if !expectMatch {
				req.Nil(path, "expected no match")
				return
			}
			req.NotNil(path, "expected match in %s mode", mode)
			req.Equal(resource.ObjectID, path.Resource.ObjectID)
			if expectedSubjectID != "" {
				req.Equal(expectedSubjectID, path.Subject.ObjectID)
			}
		})
	}
}

func TestArrow_BatchedVsUnbatched_LTR(t *testing.T) {
	factory := func() *ArrowIterator {
		left := NewFolderHierarchyFixedIterator()
		right := NewFolderHierarchyFixedIterator()
		a := NewArrowIterator(left, right)
		a.direction = leftToRight
		return a
	}

	runArrowCheckInBothModes(t, "spec1_alice_match", factory,
		NewObject("document", "spec1"),
		NewObject("user", "alice").WithEllipses(),
		true, "alice")

	runArrowCheckInBothModes(t, "spec2_alice_no_match", factory,
		NewObject("document", "spec2"),
		NewObject("user", "alice").WithEllipses(),
		false, "")

	runArrowCheckInBothModes(t, "nonexistent_resource", factory,
		NewObject("document", "nonexistent"),
		NewObject("user", "alice").WithEllipses(),
		false, "")

	runArrowCheckInBothModes(t, "nonexistent_subject", factory,
		NewObject("document", "spec1"),
		NewObject("user", "nobody").WithEllipses(),
		false, "")
}

func TestArrow_BatchedVsUnbatched_RTL(t *testing.T) {
	factory := func() *ArrowIterator {
		left := NewFolderHierarchyFixedIterator()
		right := NewFolderHierarchyFixedIterator()
		a := NewArrowIterator(left, right)
		a.direction = rightToLeft
		return a
	}

	runArrowCheckInBothModes(t, "spec1_alice_match", factory,
		NewObject("document", "spec1"),
		NewObject("user", "alice").WithEllipses(),
		true, "alice")

	runArrowCheckInBothModes(t, "spec2_alice_no_match", factory,
		NewObject("document", "spec2"),
		NewObject("user", "alice").WithEllipses(),
		false, "")
}

// countingExecutor wraps LocalExecutor to count Check / CheckMany invocations
// so tests can assert that the batched path actually batches.
type countingExecutor struct {
	LocalExecutor
	checks            int
	checkManyCalls    int
	checkManyTotalLen int
}

func (c *countingExecutor) Check(ctx *Context, it Iterator, resource Object, subject ObjectAndRelation) (*Path, error) {
	c.checks++
	return c.LocalExecutor.Check(ctx, it, resource, subject)
}

func (c *countingExecutor) CheckManySubjects(ctx *Context, it Iterator, resource Object, subjects []ObjectAndRelation) ([]*Path, error) {
	c.checkManyCalls++
	c.checkManyTotalLen += len(subjects)
	return c.LocalExecutor.CheckManySubjects(ctx, it, resource, subjects)
}

func (c *countingExecutor) CheckManyResources(ctx *Context, it Iterator, resources []Object, subject ObjectAndRelation) ([]*Path, error) {
	c.checkManyCalls++
	c.checkManyTotalLen += len(resources)
	return c.LocalExecutor.CheckManyResources(ctx, it, resources, subject)
}

func TestArrow_BatchedDispatchesOneCheckMany_LTR(t *testing.T) {
	req := require.New(t)
	left := NewFolderHierarchyFixedIterator()
	right := NewFolderHierarchyFixedIterator()
	arrow := NewArrowIterator(left, right)
	arrow.direction = leftToRight

	exec := &countingExecutor{}
	ctx := &Context{
		Context:       t.Context(),
		Executor:      exec,
		BatchedArrows: true,
	}

	_, err := ctx.Check(arrow, NewObject("document", "spec1"), NewObject("user", "alice").WithEllipses())
	req.NoError(err)
	// Exactly one CheckMany call for the right side (after collecting the left subjects).
	// Per-element ctx.Check on the right side must not be invoked when batched mode is on.
	req.Equal(1, exec.checkManyCalls, "expected exactly one CheckMany call for the batched right side")
	// The single CheckMany call must contain at least one input.
	req.GreaterOrEqual(exec.checkManyTotalLen, 1)
}

func TestArrow_UnbatchedDoesNotCallCheckMany_LTR(t *testing.T) {
	req := require.New(t)
	left := NewFolderHierarchyFixedIterator()
	right := NewFolderHierarchyFixedIterator()
	arrow := NewArrowIterator(left, right)
	arrow.direction = leftToRight

	exec := &countingExecutor{}
	ctx := &Context{
		Context:       t.Context(),
		Executor:      exec,
		BatchedArrows: false,
	}

	_, err := ctx.Check(arrow, NewObject("document", "spec1"), NewObject("user", "alice").WithEllipses())
	req.NoError(err)
	req.Equal(0, exec.checkManyCalls, "unbatched mode must not invoke CheckMany")
}

func TestArrow_BatchedDispatchesOneCheckMany_RTL(t *testing.T) {
	req := require.New(t)
	left := NewFolderHierarchyFixedIterator()
	right := NewFolderHierarchyFixedIterator()
	arrow := NewArrowIterator(left, right)
	arrow.direction = rightToLeft

	exec := &countingExecutor{}
	ctx := &Context{
		Context:       t.Context(),
		Executor:      exec,
		BatchedArrows: true,
	}

	_, err := ctx.Check(arrow, NewObject("document", "spec1"), NewObject("user", "alice").WithEllipses())
	req.NoError(err)
	req.Equal(1, exec.checkManyCalls, "expected exactly one CheckMany call for the batched left side")
}

func TestLocalExecutor_CheckManyResources(t *testing.T) {
	req := require.New(t)
	rels := NewDocumentAccessFixedIterator()
	ctx := NewTestContext(t)

	resources := []Object{
		NewObject("document", "doc1"),
		NewObject("document", "doc2"),
		NewObject("document", "missing"),
	}
	out, err := ctx.CheckManyResources(rels, resources, NewObject("user", "alice").WithEllipses())
	req.NoError(err)
	req.Len(out, 3)
	req.NotNil(out[0], "alice -> doc1 expected")
	req.NotNil(out[1], "alice -> doc2 expected")
	req.Nil(out[2], "alice -> missing expected to be nil")
}

func TestLocalExecutor_CheckManySubjects(t *testing.T) {
	req := require.New(t)
	rels := NewDocumentAccessFixedIterator()
	ctx := NewTestContext(t)

	subjects := []ObjectAndRelation{
		NewObject("user", "alice").WithEllipses(),
		NewObject("user", "nobody").WithEllipses(),
		NewObject("user", "bob").WithEllipses(),
	}
	out, err := ctx.CheckManySubjects(rels, NewObject("document", "doc1"), subjects)
	req.NoError(err)
	req.Len(out, 3)
	req.NotNil(out[0], "alice -> doc1 expected")
	req.Nil(out[1], "nobody -> doc1 expected to be nil")
	req.NotNil(out[2], "bob -> doc1 expected")
}

func TestLocalExecutor_CheckMany_EmptyInputs(t *testing.T) {
	req := require.New(t)
	rels := NewDocumentAccessFixedIterator()
	ctx := NewTestContext(t)

	out, err := ctx.CheckManyResources(rels, nil, NewObject("user", "alice").WithEllipses())
	req.NoError(err)
	req.Nil(out)

	out2, err := ctx.CheckManySubjects(rels, NewObject("document", "doc1"), nil)
	req.NoError(err)
	req.Nil(out2)
}

// --- Wildcard tests for ArrowIterator (batched path, lines 242-260 in arrow.go) ---
// Note: wildcardPath() is defined in wildcard_intersection_test.go (same package).

// wildcardPathWithCaveat creates a wildcard path with a caveat attached.
func wildcardPathWithCaveat(resourceType, resourceID, relation, subjectType, caveatName string) *Path {
	p := wildcardPath(resourceType, resourceID, relation, subjectType)
	p.Caveat = &core.CaveatExpression{
		OperationOrCaveat: &core.CaveatExpression_Caveat{
			Caveat: &core.ContextualizedCaveat{
				CaveatName: caveatName,
			},
		},
	}
	return p
}

// rightPathForWildcard creates a path suitable for the right side of an arrow when the
// left side produced a wildcard. The path's Resource must match the wildcard's object type,
// and the Subject must be the target subject — so that IterResources(subject, wildcardType)
// finds it.
func rightPathForWildcard(wildcardSubjectType, wildcardSubjectID, relation string, subject ObjectAndRelation) *Path {
	return &Path{
		Resource: Object{
			ObjectType: wildcardSubjectType,
			ObjectID:   wildcardSubjectID,
		},
		Relation: relation,
		Subject:  subject,
	}
}

// rightPathForWildcardWithCaveat is like rightPathForWildcard but with a caveat.
func rightPathForWildcardWithCaveat(wildcardSubjectType, wildcardSubjectID, relation string, subject ObjectAndRelation, caveatName string) *Path {
	p := rightPathForWildcard(wildcardSubjectType, wildcardSubjectID, relation, subject)
	p.Caveat = &core.CaveatExpression{
		OperationOrCaveat: &core.CaveatExpression_Caveat{
			Caveat: &core.ContextualizedCaveat{
				CaveatName: caveatName,
			},
		},
	}
	return p
}

// TestArrow_BatchedWildcard_LeftOnly exercises the wildcard branch in
// checkLeftToRightBatch (arrow.go lines 242-260). The left side returns only a
// wildcard path, so the batched path must invert to IterResources.
func TestArrow_BatchedWildcard_LeftOnly(t *testing.T) {
	factory := func() *ArrowIterator {
		// Left side: document:doc1 has a wildcard viewer (user:*)
		left := NewFixedIterator(*wildcardPath("document", "doc1", "viewer", "user"))

		// Right side: a path where user:alice is the subject, resource is of type "user".
		// IterResources(user:alice#..., ObjectType{Type: "user"}) will find this path.
		subject := NewObject("user", "alice").WithEllipses()
		right := NewFixedIterator(*rightPathForWildcard("user", "alice", "viewer", subject))

		a := NewArrowIterator(left, right)
		a.direction = leftToRight
		return a
	}

	resource := NewObject("document", "doc1")
	subject := NewObject("user", "alice").WithEllipses()

	// Both modes should produce a match. The combined path's subject comes from
	// the right path (combineArrowPaths uses rightPath.Subject), so the subject
	// is the concrete "alice" from the right-side IterResources result.
	runArrowCheckInBothModes(t, "wildcard_left_match", factory,
		resource, subject, true, "alice")
}

// TestArrow_BatchedWildcard_LeftOnly_NoMatch verifies that when the left side
// returns a wildcard but IterResources on the right finds nothing, there is no match.
func TestArrow_BatchedWildcard_LeftOnly_NoMatch(t *testing.T) {
	factory := func() *ArrowIterator {
		// Left side: document:doc1 has a wildcard viewer (user:*)
		left := NewFixedIterator(*wildcardPath("document", "doc1", "viewer", "user"))

		// Right side: a path for bob, not alice.
		// IterResources(user:alice#..., ObjectType{Type: "user"}) will NOT find this.
		bobSubject := NewObject("user", "bob").WithEllipses()
		right := NewFixedIterator(*rightPathForWildcard("user", "bob", "viewer", bobSubject))

		a := NewArrowIterator(left, right)
		a.direction = leftToRight
		return a
	}

	resource := NewObject("document", "doc1")
	subject := NewObject("user", "alice").WithEllipses()

	runArrowCheckInBothModes(t, "wildcard_left_no_match", factory,
		resource, subject, false, "")
}

// TestArrow_BatchedWildcard_MixedConcreteAndWildcard verifies the branch where the
// left side returns BOTH concrete subjects AND a wildcard. In batched mode, concrete
// paths go through CheckManyResources while the wildcard goes through IterResources.
func TestArrow_BatchedWildcard_MixedConcreteAndWildcard(t *testing.T) {
	factory := func() *ArrowIterator {
		// Left side: document:doc1 has a concrete viewer (bob) AND a wildcard (user:*)
		concretePath := MustPathFromString("document:doc1#viewer@user:bob")
		wildcard := wildcardPath("document", "doc1", "viewer", "user")
		left := NewFixedIterator(*concretePath, *wildcard)

		// Right side:
		// - user:bob has a viewer path (for the concrete branch)
		// - user:alice has a viewer path (for the wildcard branch via IterResources)
		bobSubject := NewObject("user", "bob").WithEllipses()
		aliceSubject := NewObject("user", "alice").WithEllipses()
		right := NewFixedIterator(
			*rightPathForWildcard("user", "bob", "viewer", bobSubject),
			*rightPathForWildcard("user", "alice", "viewer", aliceSubject),
		)

		a := NewArrowIterator(left, right)
		a.direction = leftToRight
		return a
	}

	resource := NewObject("document", "doc1")

	// alice matches via the wildcard branch (IterResources finds alice on right).
	// The combined path's subject is "alice" from the right path.
	runArrowCheckInBothModes(t, "mixed_alice_via_wildcard", factory,
		resource, NewObject("user", "alice").WithEllipses(), true, "alice")

	// bob matches via the concrete branch (CheckManyResources)
	runArrowCheckInBothModes(t, "mixed_bob_via_concrete", factory,
		resource, NewObject("user", "bob").WithEllipses(), true, "bob")

	// charlie matches via the wildcard branch (IterResources finds user:charlie path)
	// But we don't have a charlie path on the right, so no match.
	runArrowCheckInBothModes(t, "mixed_charlie_no_right_path", factory,
		resource, NewObject("user", "charlie").WithEllipses(), false, "")
}

// TestArrow_BatchedWildcard_WithCaveat exercises the MergeOr branch (lines 255-258)
// where the combined path has a caveat, so it accumulates via MergeOr instead of
// returning early.
func TestArrow_BatchedWildcard_WithCaveat(t *testing.T) {
	factory := func() *ArrowIterator {
		// Left side: wildcard with a caveat
		left := NewFixedIterator(*wildcardPathWithCaveat("document", "doc1", "viewer", "user", "left_caveat"))

		// Right side: matching path with a caveat
		subject := NewObject("user", "alice").WithEllipses()
		right := NewFixedIterator(*rightPathForWildcardWithCaveat("user", "alice", "viewer", subject, "right_caveat"))

		a := NewArrowIterator(left, right)
		a.direction = leftToRight
		return a
	}

	resource := NewObject("document", "doc1")
	subject := NewObject("user", "alice").WithEllipses()

	// Both modes should match. Subject comes from the right path ("alice").
	runArrowCheckInBothModes(t, "wildcard_with_caveat_match", factory,
		resource, subject, true, "alice")
}

// TestArrow_BatchedWildcard_MultipleRightPaths tests the case where IterResources
// returns multiple matching right paths for a single wildcard left path.
func TestArrow_BatchedWildcard_MultipleRightPaths(t *testing.T) {
	factory := func() *ArrowIterator {
		// Left side: single wildcard
		left := NewFixedIterator(*wildcardPath("document", "doc1", "viewer", "user"))

		// Right side: multiple users
		aliceSubject := NewObject("user", "alice").WithEllipses()
		bobSubject := NewObject("user", "bob").WithEllipses()
		right := NewFixedIterator(
			*rightPathForWildcard("user", "alice", "viewer", aliceSubject),
			*rightPathForWildcard("user", "bob", "viewer", bobSubject),
		)

		a := NewArrowIterator(left, right)
		a.direction = leftToRight
		return a
	}

	resource := NewObject("document", "doc1")

	// alice matches (subject from right path)
	runArrowCheckInBothModes(t, "multi_right_alice", factory,
		resource, NewObject("user", "alice").WithEllipses(), true, "alice")

	// bob matches (subject from right path)
	runArrowCheckInBothModes(t, "multi_right_bob", factory,
		resource, NewObject("user", "bob").WithEllipses(), true, "bob")

	// charlie doesn't match (no right path for charlie)
	runArrowCheckInBothModes(t, "multi_right_charlie_no_match", factory,
		resource, NewObject("user", "charlie").WithEllipses(), false, "")
}

// TestArrow_BatchedWildcard_UseIterResourcesNotCheckMany verifies that when the left
// side returns ONLY wildcards, the batched path uses IterResources (not CheckManyResources)
// to follow the arrow.
func TestArrow_BatchedWildcard_UseIterResourcesNotCheckMany(t *testing.T) {
	req := require.New(t)

	// Left side: only a wildcard path
	left := NewFixedIterator(*wildcardPath("document", "doc1", "viewer", "user"))

	// Right side: matching path for alice
	subject := NewObject("user", "alice").WithEllipses()
	right := NewFixedIterator(*rightPathForWildcard("user", "alice", "viewer", subject))

	arrow := NewArrowIterator(left, right)
	arrow.direction = leftToRight

	exec := &countingExecutor{}
	ctx := &Context{
		Context:       t.Context(),
		Executor:      exec,
		BatchedArrows: true,
	}

	path, err := ctx.Check(arrow, NewObject("document", "doc1"), subject)
	req.NoError(err)
	req.NotNil(path, "expected match via wildcard")
	// The combined path's subject comes from the right path (alice).
	req.Equal("alice", path.Subject.ObjectID, "subject should be from right path")

	// Since the left side is ONLY wildcards, CheckManyResources must NOT be called
	// (there are no concrete intermediates to batch). The wildcard branch uses
	// IterResources directly.
	req.Equal(0, exec.checkManyCalls, "wildcard-only left side must not call CheckManyResources")
}

// TestArrow_BatchedWildcard_MixedDispatch verifies that when the left side has both
// concrete and wildcard paths, CheckManyResources is called for the concrete paths.
// We check for a subject that does NOT match the wildcard branch, forcing the
// concrete CheckManyResources path to be exercised.
func TestArrow_BatchedWildcard_MixedDispatch(t *testing.T) {
	req := require.New(t)

	// Left side: concrete + wildcard
	concretePath := MustPathFromString("document:doc1#viewer@user:alice")
	wildcard := wildcardPath("document", "doc1", "viewer", "user")
	left := NewFixedIterator(*concretePath, *wildcard)

	// Right side: only bob has a right-side path.
	// When we check for alice:
	//   - Wildcard branch: IterResources(user:alice#..., {Type: "user"}) finds nothing (no alice path on right)
	//   - Concrete branch: CheckManyResources with [user:alice] — alice matches because right has alice path
	bobSubject := NewObject("user", "bob").WithEllipses()
	right := NewFixedIterator(*rightPathForWildcard("user", "bob", "viewer", bobSubject))

	arrow := NewArrowIterator(left, right)
	arrow.direction = leftToRight

	exec := &countingExecutor{}
	ctx := &Context{
		Context:       t.Context(),
		Executor:      exec,
		BatchedArrows: true,
	}

	// Check for alice: wildcard branch finds nothing (no alice on right).
	// Concrete branch: CheckManyResources checks user:alice against right — no match either.
	// Result: no match, but CheckManyResources WAS called.
	_, err := ctx.Check(arrow, NewObject("document", "doc1"), NewObject("user", "alice").WithEllipses())
	req.NoError(err)

	// CheckManyResources WAS called for the concrete path.
	req.Equal(1, exec.checkManyCalls, "mixed left side must call CheckManyResources for concrete paths")
}

// TestArrow_BatchedWildcard_RTL verifies that wildcards work correctly in the
// right-to-left direction. In RTL mode, the right side is queried first via
// IterResources, then the left side is checked via CheckManySubjects. Wildcards
// on the right side are handled naturally through IterResources (which filters
// by the target subject).
func TestArrow_BatchedWildcard_RTL(t *testing.T) {
	factory := func() *ArrowIterator {
		// Left side: folder:project1 has parent folder:root
		leftPath := MustPathFromString("folder:project1#parent@folder:root")
		left := NewFixedIterator(*leftPath)

		// Right side: folder:root has a concrete viewer (user:alice).
		// RTL: IterResources(right, user:alice#..., ...) finds folder:root.
		// Then CheckManySubjects(left, folder:project1, [folder:root#...]) finds the parent path.
		rightPath := MustPathFromString("folder:root#viewer@user:alice")
		right := NewFixedIterator(*rightPath)

		a := NewArrowIterator(left, right)
		a.direction = rightToLeft
		return a
	}

	// Check: does folder:project1 have user:alice as a viewer?
	// Right: user:alice views folder:root (IterResources finds this)
	// Left: folder:project1 has parent folder:root (CheckManySubjects finds this)
	// Result: match via folder:root
	runArrowCheckInBothModes(t, "rtl_concrete_viewer", factory,
		NewObject("folder", "project1"),
		NewObject("user", "alice").WithEllipses(),
		true, "alice")
}

// TestArrow_BatchedWildcard_IterSubjects verifies that IterSubjects through an
// arrow skips wildcard left paths (arrow.go line 462: wildcards are skipped in
// IterSubjects because they can't be followed through arrows without a store-wide
// query). Only concrete left paths are followed.
func TestArrow_BatchedWildcard_IterSubjects(t *testing.T) {
	req := require.New(t)

	// Left side: document:doc1 has both a concrete viewer and a wildcard
	concretePath := MustPathFromString("document:doc1#viewer@user:bob")
	wildcard := wildcardPath("document", "doc1", "viewer", "user")
	left := NewFixedIterator(*concretePath, *wildcard)

	// Right side: user:bob has viewer access (so bob -> bob chain works)
	bobSubject := NewObject("user", "bob").WithEllipses()
	right := NewFixedIterator(
		*rightPathForWildcard("user", "bob", "viewer", bobSubject),
	)

	arrow := NewArrowIterator(left, right)
	arrow.direction = leftToRight

	ctx := NewTestContext(t)

	// IterSubjects follows concrete left paths but SKIPS wildcard left paths
	// (see arrow.go:462 "cannot follow arrow through wildcard").
	// Only bob (via concrete path) should appear.
	seq, err := ctx.IterSubjects(arrow, NewObject("document", "doc1"), NoObjectFilter())
	req.NoError(err)

	paths, err := CollectAll(seq)
	req.NoError(err)

	subjectIDs := slicez.Map(paths, func(p *Path) string { return p.Subject.ObjectID })
	req.Contains(subjectIDs, "bob", "expected concrete subject bob")
	req.NotContains(subjectIDs, tuple.PublicWildcard, "wildcards are skipped in IterSubjects through arrows")
}

// TestArrow_BatchedWildcard_CombinedCaveat verifies that when both left (wildcard)
// and right paths have caveats, they are combined with AND logic via combineArrowPaths.
func TestArrow_BatchedWildcard_CombinedCaveat(t *testing.T) {
	req := require.New(t)

	// Left: wildcard with caveat
	left := NewFixedIterator(*wildcardPathWithCaveat("document", "doc1", "viewer", "user", "left_caveat"))

	// Right: matching path with caveat
	subject := NewObject("user", "alice").WithEllipses()
	right := NewFixedIterator(*rightPathForWildcardWithCaveat("user", "alice", "viewer", subject, "right_caveat"))

	arrow := NewArrowIterator(left, right)
	arrow.direction = leftToRight

	ctx := NewTestContext(t)
	path, err := ctx.Check(arrow, NewObject("document", "doc1"), subject)
	req.NoError(err)
	req.NotNil(path)

	// The combined caveat should be an AND of left and right caveats.
	req.NotNil(path.Caveat, "expected combined caveat")

	// combineArrowPaths uses caveats.And() which produces a CaveatOperation with AND.
	op := path.Caveat.GetOperation()
	req.NotNil(op, "expected caveat operation (AND)")
	req.Equal(core.CaveatOperation_AND, op.Op, "combined caveat should use AND")
	req.Len(op.Children, 2, "AND should have 2 children")
}
