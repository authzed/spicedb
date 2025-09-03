package query

import (
	"fmt"
	"iter"
	"strings"

	"github.com/authzed/spicedb/pkg/tuple"
)

type (
	// Relation is the basic unit of connection
	Relation = tuple.Relationship
	// RelationSeq is the intermediate iter closure that any of the planning calls return.
	RelationSeq iter.Seq2[Relation, error]
)

// Plan is the external-facing notion of a query plan. These follow the general API for
// querying anything in the database as well as describing the plan.
type Plan interface {
	// CheckImpl tests if, for the underlying set of relationships (which may be a full expression or a basic lookup, depending on the iterator)
	// any of the `resourceIDs` are connected to `subjectID`.
	// Returns the sequence of matching relations, if they exist, at most `len(resourceIDs)`.
	CheckImpl(ctx *Context, resourceIDs []string, subjectID string) (RelationSeq, error)

	// IterSubjectsImpl returns a sequence of all the relations in this set that match the given resourceID.
	IterSubjectsImpl(ctx *Context, resourceID string) (RelationSeq, error)

	// IterResourcesImpl returns a sequence of all the relations in this set that match the given subjectID.
	IterResourcesImpl(ctx *Context, subjectID string) (RelationSeq, error)

	// Explain generates a human-readable tree that describes each iterator and its state.
	Explain() Explain
}

// Iterator is an interface for manipulating iterators to create more query plans.
//
// A Plan is abstract and more limited, and easier to pass around.
//
// An Iterator is what Iterators (even future datastore-specific ones) implement so that optimizations
// and reorderings and rebalancing can take place.
type Iterator interface {
	Plan

	// Clone does a deep-copy to duplicate the iterator tree at this point.
	Clone() Iterator
}

// Explain describes the state of an iterator tree, in a human-readable fashion, with an Info line at
// each node.
//
// TODO: This can be extended with other interesting stats about the tree.
type Explain struct {
	Info       string
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

// CollectAll is a helper function to build read a complete RelationSeq and turn it into a fully realized slice of Relations.
func CollectAll(seq RelationSeq) ([]Relation, error) {
	out := make([]Relation, 0) // `prealloc` is overly aggressive. This should be `var out []Relation`
	for x, err := range seq {
		if err != nil {
			return nil, err
		}
		out = append(out, x)
	}
	return out, nil
}
