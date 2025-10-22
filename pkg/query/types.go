package query

import (
	"fmt"
	"strings"
)

// Plan is the external-facing notion of a query plan. These follow the general API for
// querying anything in the database as well as describing the plan.
type Plan interface {
	// CheckImpl tests if, for the underlying set of relationships (which may be a full expression or a basic lookup, depending on the iterator)
	// any of the `resourceIDs` are connected to `subjectID`.
	// Returns the sequence of matching paths, if they exist, at most `len(resourceIDs)`.
	CheckImpl(ctx *Context, resources []Object, subject ObjectAndRelation) (PathSeq, error)

	// IterSubjectsImpl returns a sequence of all the paths in this set that match the given resourceID.
	IterSubjectsImpl(ctx *Context, resource Object) (PathSeq, error)

	// IterResourcesImpl returns a sequence of all the paths in this set that match the given subjectID.
	IterResourcesImpl(ctx *Context, subject ObjectAndRelation) (PathSeq, error)

	// Explain generates a human-readable tree that describes each iterator and its state.
	Explain() Explain
}

// Iterator is a Plan that forms a tree structure through its Subiterators,
// where the tree represents the query execution plan that can be traversed and
// optimized. While Plan provides a read-only query interface, Iterator adds
// methods for cloning, inspecting, and rebuilding iterator trees. This enables
// query optimization by rewriting the tree.
//
// Implementations should form a composite tree structure where leaf nodes
// (e.g., datastore scans) have no subiterators, and composite nodes (e.g.,
// unions, intersections) combine multiple subiterators.
//
// Most tree transformations should use the Walk helper function rather than
// manually calling Subiterators and ReplaceSubiterators.
type Iterator interface {
	Plan

	// Clone does a deep-copy to duplicate the iterator tree at this point.
	Clone() Iterator

	// Subiterators returns the child iterators of this iterator, if any.
	// Returns nil or empty slice for leaf iterators.
	Subiterators() []Iterator

	// ReplaceSubiterators returns a new iterator with the given subiterators replacing the current ones.
	// This method always returns a new iterator instance.
	// For leaf iterators (those with no subiterators), this returns an error.
	// For composite iterators, the length of newSubs should match the length of Subiterators().
	// Returns an error if the replacement fails or if the length of newSubs doesn't match expectations.
	ReplaceSubiterators(newSubs []Iterator) (Iterator, error)
}

// Explain describes the state of an iterator tree, in a human-readable fashion, with an Info line at
// each node.
//
// TODO: This can be extended with other interesting stats about the tree.
type Explain struct {
	Name       string // Short name for tracing (e.g., "Arrow", "Union")
	Info       string // Full info for display
	SubExplain []Explain
}

func (e Explain) String() string {
	return e.IndentString(0)
}

// IndentString pretty-prints an Explain tree with a given indentation.
func (e Explain) IndentString(depth int) string {
	var sb strings.Builder
	for _, sub := range e.SubExplain {
		sb.WriteString(sub.IndentString(depth + 1))
	}
	return fmt.Sprintf("%s%s\n%s", strings.Repeat("\t", depth), e.Info, sb.String())
}
