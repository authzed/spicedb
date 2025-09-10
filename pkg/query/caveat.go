package query

import (
	"fmt"

	"github.com/authzed/spicedb/internal/caveats"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

// CaveatIterator wraps another iterator and applies caveat evaluation to its results.
// It checks caveat conditions on relationships during iteration and only yields
// relationships that satisfy the caveat constraints.
type CaveatIterator struct {
	subiterator Iterator
	caveat      *core.ContextualizedCaveat
}

var _ Iterator = &CaveatIterator{}

// NewCaveatIterator creates a new caveat iterator that wraps the given subiterator
// and applies the specified caveat conditions.
func NewCaveatIterator(subiterator Iterator, caveat *core.ContextualizedCaveat) *CaveatIterator {
	return &CaveatIterator{
		subiterator: subiterator,
		caveat:      caveat,
	}
}

func (c *CaveatIterator) CheckImpl(ctx *Context, resources []Object, subject ObjectAndRelation) (RelationSeq, error) {
	subSeq, err := c.subiterator.CheckImpl(ctx, resources, subject)
	if err != nil {
		return nil, err
	}

	return func(yield func(Relation, error) bool) {
		for rel, err := range subSeq {
			if err != nil {
				if !yield(rel, err) {
					return
				}
				continue
			}

			// Apply caveat evaluation to the relation
			passed, err := c.evaluateCaveat(ctx, rel)
			if err != nil {
				if !yield(rel, err) {
					return
				}
				continue
			}
			
			if passed {
				if !yield(rel, nil) {
					return
				}
			}
		}
	}, nil
}

func (c *CaveatIterator) IterSubjectsImpl(ctx *Context, resource Object) (RelationSeq, error) {
	subSeq, err := c.subiterator.IterSubjectsImpl(ctx, resource)
	if err != nil {
		return nil, err
	}

	return func(yield func(Relation, error) bool) {
		for rel, err := range subSeq {
			if err != nil {
				if !yield(rel, err) {
					return
				}
				continue
			}

			// Apply caveat evaluation to the relation
			passed, err := c.evaluateCaveat(ctx, rel)
			if err != nil {
				if !yield(rel, err) {
					return
				}
				continue
			}
			
			if passed {
				if !yield(rel, nil) {
					return
				}
			}
		}
	}, nil
}

func (c *CaveatIterator) IterResourcesImpl(ctx *Context, subject ObjectAndRelation) (RelationSeq, error) {
	subSeq, err := c.subiterator.IterResourcesImpl(ctx, subject)
	if err != nil {
		return nil, err
	}

	return func(yield func(Relation, error) bool) {
		for rel, err := range subSeq {
			if err != nil {
				if !yield(rel, err) {
					return
				}
				continue
			}

			// Apply caveat evaluation to the relation
			passed, err := c.evaluateCaveat(ctx, rel)
			if err != nil {
				if !yield(rel, err) {
					return
				}
				continue
			}
			
			if passed {
				if !yield(rel, nil) {
					return
				}
			}
		}
	}, nil
}

// evaluateCaveat determines if the given relation satisfies the caveat conditions.
func (c *CaveatIterator) evaluateCaveat(ctx *Context, rel Relation) (bool, error) {
	// If no caveat is specified, allow all relations
	if c.caveat == nil {
		return true, nil
	}

	// If the relation has no caveat, check if we expect one
	if rel.OptionalCaveat == nil {
		// No caveat on the relation - only allow if our caveat iterator expects no caveat
		return false, nil
	}

	// Check if the caveat names match
	if rel.OptionalCaveat.CaveatName != c.caveat.CaveatName {
		return false, nil
	}

	// Get the caveat context from the query context
	caveatContext, exists := ctx.CaveatContext[c.caveat.CaveatName]
	if !exists {
		// No context provided for this caveat - this means it cannot be evaluated
		return false, nil
	}

	// Build the caveat expression for evaluation
	caveatExpr := &core.CaveatExpression{
		OperationOrCaveat: &core.CaveatExpression_Caveat{
			Caveat: caveatContext,
		},
	}

	// Use the CaveatRunner from the context if available
	if ctx.CaveatRunner == nil {
		// No caveat runner available - cannot evaluate caveats
		return false, fmt.Errorf("no caveat runner available for caveat evaluation")
	}

	// Get a snapshot reader which should implement CaveatReader
	reader := ctx.Datastore.SnapshotReader(ctx.Revision)
	
	// Use the caveat runner to evaluate the expression
	result, err := ctx.CaveatRunner.RunCaveatExpression(
		ctx,
		caveatExpr,
		nil, // No additional context beyond what's in the caveat
		reader, // Caveat reader
		caveats.RunCaveatExpressionNoDebugging,
	)
	if err != nil {
		// If evaluation fails, return the error
		return false, err
	}

	// Return true only if the caveat evaluated to true and is not partial
	return result.Value() && !result.IsPartial(), nil
}

func (c *CaveatIterator) Clone() Iterator {
	return &CaveatIterator{
		subiterator: c.subiterator.Clone(),
		caveat:      c.caveat, // ContextualizedCaveat is immutable, safe to share
	}
}

func (c *CaveatIterator) Explain() Explain {
	caveatName := ""
	if c.caveat != nil {
		caveatName = c.caveat.CaveatName
	}
	
	return Explain{
		Info:       fmt.Sprintf("Caveat(%s)", caveatName),
		SubExplain: []Explain{c.subiterator.Explain()},
	}
}