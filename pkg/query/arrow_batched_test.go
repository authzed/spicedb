package query

import (
	"testing"

	"github.com/stretchr/testify/require"
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
