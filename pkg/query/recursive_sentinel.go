package query

import (
	"fmt"

	"github.com/google/uuid"

	"github.com/authzed/spicedb/pkg/spiceerrors"
)

var _ Iterator = &RecursiveSentinel{}

// RecursiveSentinel is a sentinel iterator that marks recursion points during iterator tree construction.
// It acts as a placeholder that will be replaced during execution by RecursiveIterator.
type RecursiveSentinel struct {
	id               string
	definitionName   string
	relationName     string
	withSubRelations bool
}

// NewRecursiveSentinel creates a new sentinel marking a recursion point
func NewRecursiveSentinel(definitionName, relationName string, withSubRelations bool) *RecursiveSentinel {
	return &RecursiveSentinel{
		id:               uuid.NewString(),
		definitionName:   definitionName,
		relationName:     relationName,
		withSubRelations: withSubRelations,
	}
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
		id:               uuid.NewString(),
		definitionName:   r.definitionName,
		relationName:     r.relationName,
		withSubRelations: r.withSubRelations,
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

func (r *RecursiveSentinel) ID() string {
	return r.id
}

func (r *RecursiveSentinel) ResourceType() (ObjectType, error) {
	return ObjectType{
		Type:        r.definitionName,
		Subrelation: r.relationName,
	}, nil
}

func (r *RecursiveSentinel) SubjectTypes() ([]ObjectType, error) {
	// Sentinels don't know their subject types until expanded
	// Return the recursive type as a placeholder
	subrel := r.relationName
	if r.withSubRelations {
		subrel = "" // Unknown during construction
	}
	return []ObjectType{{
		Type:        r.definitionName,
		Subrelation: subrel,
	}}, nil
}
