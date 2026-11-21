package testing

import (
	"slices"
	"strconv"

	"pgregory.net/rapid"
)

// Shape describes a permission and the relationships that support it. Shapes are
// immutable and can be composed or reused across scenarios. Reusing a Shape in
// the same definition shares its relation/permission; reusing an arrow's or
// userset's child shares its backing definition and objects.
//
// Shapes describe acyclic schemas. Recursion, wildcards, caveats, and expiration
// are deliberately not part of this generator's current vocabulary.
type Shape struct {
	kind     shapeKind
	children []*Shape
}

type shapeKind int

const (
	directShape shapeKind = iota
	emptyShape
	aliasShape
	usersetShape
	arrowShape
	allArrowShape
	unionShape
	intersectionShape
	exclusionShape
)

func mustNewShape(kind shapeKind, children ...*Shape) *Shape {
	if len(children) == 0 {
		panic("a composed shape requires at least one child")
	}
	for _, child := range children {
		if child == nil {
			panic("shape children must not be nil")
		}
	}
	return &Shape{kind: kind, children: slices.Clone(children)}
}

// DrawShape draws a bounded composition using Rapid, so failures can shrink both
// the schema structure and its data. maxDepth bounds nesting; zero draws only
// Direct or Empty. Use explicit compositions when a test must retain a shape
// (such as an intersection or an all arrow) throughout shrinking.
func DrawShape(t *rapid.T, maxDepth int) *Shape {
	if maxDepth < 0 {
		t.Fatal("shape depth must be nonnegative")
	}
	return drawShape(t, maxDepth, "shape")
}

func drawShape(t *rapid.T, depth int, label string) *Shape {
	maxChoice := int(exclusionShape)
	if depth == 0 {
		maxChoice = int(emptyShape)
	}
	kind := shapeKind(rapid.IntRange(0, maxChoice).Draw(t, label))
	if kind == directShape {
		return Direct()
	}
	if kind == emptyShape {
		return Empty()
	}
	numChildren := 1
	if kind == unionShape || kind == intersectionShape || kind == exclusionShape {
		numChildren = 2
	}
	children := make([]*Shape, numChildren)
	for i := range children {
		if i > 0 && rapid.Bool().Draw(t, label+"/share") {
			children[i] = children[0]
		} else {
			children[i] = drawShape(t, depth-1, label+"/"+strconv.Itoa(i))
		}
	}
	return mustNewShape(kind, children...)
}
