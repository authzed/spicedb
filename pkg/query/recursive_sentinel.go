package query

import (
	"fmt"

	"github.com/google/uuid"

	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/tuple"
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

// CheckImpl returns an empty PathSeq. If collection mode is enabled, it collects
// the queried resources to the frontier collection instead of returning paths.
func (r *RecursiveSentinel) CheckImpl(ctx *Context, resources []Object, subject ObjectAndRelation) (PathSeq, error) {
	// Check if collection mode is enabled for this sentinel
	if ctx.IsCollectingFrontier(r.id) {
		// Collection mode: append resources to frontier, return empty
		return func(yield func(Path, error) bool) {
			for _, resource := range resources {
				// Only collect if it matches our recursion type
				if resource.ObjectType == r.definitionName {
					ctx.CollectFrontierObject(r.id, resource)
					ctx.TraceStep(r, "Collected frontier: %s:%s", resource.ObjectType, resource.ObjectID)
				}
			}
			// Return empty (collection doesn't yield paths)
		}, nil
	}

	// Normal mode: return empty (standard sentinel behavior)
	return EmptyPathSeq(), nil
}

// IterSubjectsImpl returns an empty PathSeq. If collection mode is enabled, it collects
// the queried resource to the frontier collection instead of returning paths.
func (r *RecursiveSentinel) IterSubjectsImpl(ctx *Context, resource Object, filterSubjectType ObjectType) (PathSeq, error) {
	// Check if collection mode is enabled for this sentinel
	if ctx.IsCollectingFrontier(r.id) {
		// Collection mode: append resource to frontier, return empty
		return func(yield func(Path, error) bool) {
			// Only collect if it matches our recursion type
			if resource.ObjectType == r.definitionName {
				ctx.CollectFrontierObject(r.id, resource)
				ctx.TraceStep(r, "Collected frontier: %s:%s", resource.ObjectType, resource.ObjectID)
			}
			// Return empty (collection doesn't yield paths)
		}, nil
	}

	// Normal mode: return empty (standard sentinel behavior)
	return EmptyPathSeq(), nil
}

// IterResourcesImpl returns an empty PathSeq since sentinels don't execute during construction
func (r *RecursiveSentinel) IterResourcesImpl(ctx *Context, subject ObjectAndRelation, filterResourceType ObjectType) (PathSeq, error) {
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
		Info: fmt.Sprintf("RecursiveSentinel(%s#%s withSubRelations=%v)", r.definitionName, r.relationName, r.withSubRelations),
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

func (r *RecursiveSentinel) ResourceType() ([]ObjectType, error) {
	return []ObjectType{{
		Type:        r.definitionName,
		Subrelation: tuple.Ellipsis,
	}}, nil
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
