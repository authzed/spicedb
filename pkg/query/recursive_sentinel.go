package query

import (
	"fmt"

	"github.com/authzed/spicedb/pkg/spiceerrors"
)

var _ Iterator = &RecursiveSentinel{}

// RecursiveSentinel is a sentinel iterator that marks recursion points during iterator tree construction.
// It acts as a placeholder that will be replaced during execution by RecursiveIterator.
type RecursiveSentinel struct {
	definitionName   string
	relationName     string
	withSubRelations bool
	id               string // Cached identifier: "def#rel:bool"
}

// NewRecursiveSentinel creates a new sentinel marking a recursion point
func NewRecursiveSentinel(definitionName, relationName string, withSubRelations bool) *RecursiveSentinel {
	return &RecursiveSentinel{
		definitionName:   definitionName,
		relationName:     relationName,
		withSubRelations: withSubRelations,
		id:               fmt.Sprintf("%s#%s:%v", definitionName, relationName, withSubRelations),
	}
}

// ID returns the unique identifier for this sentinel
func (r *RecursiveSentinel) ID() string {
	return r.id
}

// DefinitionName returns the definition name this sentinel represents
func (r *RecursiveSentinel) DefinitionName() string {
	return r.definitionName
}

// RelationName returns the relation name this sentinel represents
func (r *RecursiveSentinel) RelationName() string {
	return r.relationName
}

// WithSubRelations returns whether subrelations should be included
func (r *RecursiveSentinel) WithSubRelations() bool {
	return r.withSubRelations
}

// CheckImpl returns an empty PathSeq since sentinels don't execute during construction
func (r *RecursiveSentinel) CheckImpl(ctx *Context, resources []Object, subject ObjectAndRelation) (PathSeq, error) {
	return EmptyPathSeq(), nil
}

// IterSubjectsImpl returns an empty PathSeq since sentinels don't execute during construction
func (r *RecursiveSentinel) IterSubjectsImpl(ctx *Context, resource Object) (PathSeq, error) {
	return EmptyPathSeq(), nil
}

// IterResourcesImpl returns an empty PathSeq since sentinels don't execute during construction
func (r *RecursiveSentinel) IterResourcesImpl(ctx *Context, subject ObjectAndRelation) (PathSeq, error) {
	return EmptyPathSeq(), nil
}

// Clone returns a shallow copy of the sentinel
func (r *RecursiveSentinel) Clone() Iterator {
	return &RecursiveSentinel{
		definitionName:   r.definitionName,
		relationName:     r.relationName,
		withSubRelations: r.withSubRelations,
		id:               r.id,
	}
}

// Explain returns a description of this sentinel for debugging
func (r *RecursiveSentinel) Explain() Explain {
	return Explain{
		Name: "RecursiveSentinel",
		Info: fmt.Sprintf("%s#%s (withSubRelations=%v)", r.definitionName, r.relationName, r.withSubRelations),
	}
}

func (r *RecursiveSentinel) Subiterators() []Iterator {
	return nil
}

func (r *RecursiveSentinel) ReplaceSubiterators(newSubs []Iterator) (Iterator, error) {
	return nil, spiceerrors.MustBugf("Trying to replace a leaf RecursiveSentinel's subiterators")
}
