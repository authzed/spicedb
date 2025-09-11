package query

import (
	"errors"
	"fmt"

	"github.com/authzed/spicedb/internal/caveats"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

// CaveatEvaluation represents the result of evaluating a caveat
type CaveatEvaluation int

const (
	// CaveatFalse means the caveat evaluated to false - don't yield the relation
	CaveatFalse CaveatEvaluation = iota
	// CaveatTrue means the caveat evaluated to true - yield the relation without caveat
	CaveatTrue
	// CaveatPartial means the caveat is partial/conditional - yield the relation with caveat
	CaveatPartial
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
			evaluation, err := c.evaluateCaveat(ctx, rel)
			if err != nil {
				if !yield(rel, err) {
					return
				}
				continue
			}
			
			switch evaluation {
			case CaveatTrue:
				// Caveat evaluated to true - yield relation without caveat
				modifiedRel := rel
				modifiedRel.OptionalCaveat = nil
				if !yield(modifiedRel, nil) {
					return
				}
			case CaveatPartial:
				// Caveat is partial - yield relation with caveat
				if !yield(rel, nil) {
					return
				}
			case CaveatFalse:
				// Caveat evaluated to false - don't yield the relation
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
			evaluation, err := c.evaluateCaveat(ctx, rel)
			if err != nil {
				if !yield(rel, err) {
					return
				}
				continue
			}
			
			switch evaluation {
			case CaveatTrue:
				// Caveat evaluated to true - yield relation without caveat
				modifiedRel := rel
				modifiedRel.OptionalCaveat = nil
				if !yield(modifiedRel, nil) {
					return
				}
			case CaveatPartial:
				// Caveat is partial - yield relation with caveat
				if !yield(rel, nil) {
					return
				}
			case CaveatFalse:
				// Caveat evaluated to false - don't yield the relation
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
			evaluation, err := c.evaluateCaveat(ctx, rel)
			if err != nil {
				if !yield(rel, err) {
					return
				}
				continue
			}
			
			switch evaluation {
			case CaveatTrue:
				// Caveat evaluated to true - yield relation without caveat
				modifiedRel := rel
				modifiedRel.OptionalCaveat = nil
				if !yield(modifiedRel, nil) {
					return
				}
			case CaveatPartial:
				// Caveat is partial - yield relation with caveat
				if !yield(rel, nil) {
					return
				}
			case CaveatFalse:
				// Caveat evaluated to false - don't yield the relation
			}
		}
	}, nil
}

// evaluateCaveat determines if the given relation satisfies the caveat conditions.
func (c *CaveatIterator) evaluateCaveat(ctx *Context, rel Relation) (CaveatEvaluation, error) {
	
	// If no caveat is specified, allow all relations
	if c.caveat == nil {
		return CaveatTrue, nil
	}

	// If the relation has no caveat, check if we expect one
	if rel.OptionalCaveat == nil {
		// No caveat on the relation - only allow if our caveat iterator expects no caveat
		return CaveatFalse, nil
	}

	// Check if the caveat names match
	if rel.OptionalCaveat.CaveatName != c.caveat.CaveatName {
		return CaveatFalse, nil
	}

	// Build the caveat expression for evaluation using the relationship's caveat
	caveatExpr := caveats.CaveatAsExpr(rel.OptionalCaveat)

	// Use the CaveatRunner from the context if available
	if ctx.CaveatRunner == nil {
		// No caveat runner available - cannot evaluate caveats
		return CaveatFalse, fmt.Errorf("no caveat runner available for caveat evaluation")
	}

	// Get a snapshot reader which should implement CaveatReader
	reader := ctx.Datastore.SnapshotReader(ctx.Revision)
	
	// Build the combined context map
	contextMap := c.buildCaveatContext(ctx, rel.OptionalCaveat)
	
	// Use the caveat runner to evaluate the expression
	result, err := ctx.CaveatRunner.RunCaveatExpression(
		ctx,
		caveatExpr,
		contextMap,
		reader, // Caveat reader
		caveats.RunCaveatExpressionNoDebugging,
	)
	if err != nil {
		// Check if this is a specific caveat evaluation error and wrap it appropriately
		var evalErr caveats.EvaluationError
		var paramErr caveats.ParameterTypeError
		
		if errors.As(err, &evalErr) {
			return CaveatFalse, fmt.Errorf("caveat evaluation failed for caveat %s: %w", rel.OptionalCaveat.CaveatName, evalErr)
		}
		if errors.As(err, &paramErr) {
			return CaveatFalse, fmt.Errorf("caveat parameter error for caveat %s: %w", rel.OptionalCaveat.CaveatName, paramErr)
		}
		
		// For other errors, provide context about which caveat failed
		return CaveatFalse, fmt.Errorf("failed to evaluate caveat %s: %w", rel.OptionalCaveat.CaveatName, err)
	}

	// Handle the caveat evaluation result
	if result.IsPartial() {
		return CaveatPartial, nil
	}
	
	if result.Value() {
		return CaveatTrue, nil
	} else {
		return CaveatFalse, nil
	}
}

// buildCaveatContext combines the relationship's caveat context with query-time context
func (c *CaveatIterator) buildCaveatContext(ctx *Context, relationCaveat *core.ContextualizedCaveat) map[string]any {
	contextMap := make(map[string]any)
	
	// Start with the relationship's context if available
	if relationCaveat != nil && relationCaveat.Context != nil {
		contextMap = relationCaveat.Context.AsMap()
	}
	
	// Overlay query-time context if available
	// Now ctx.CaveatContext is map[string]any, so we can directly use it
	if ctx.CaveatContext != nil {
		// Merge the global query-time context, with query context taking precedence over relationship context
		for k, v := range ctx.CaveatContext {
			contextMap[k] = v
		}
	}
	
	return contextMap
}

func (c *CaveatIterator) Clone() Iterator {
	return &CaveatIterator{
		subiterator: c.subiterator.Clone(),
		caveat:      c.caveat, // ContextualizedCaveat is immutable, safe to share
	}
}

func (c *CaveatIterator) Explain() Explain {
	caveatInfo := c.buildExplainInfo()
	
	return Explain{
		Info:       caveatInfo,
		SubExplain: []Explain{c.subiterator.Explain()},
	}
}

// buildExplainInfo creates detailed explanation information for the caveat iterator
func (c *CaveatIterator) buildExplainInfo() string {
	if c.caveat == nil {
		return "Caveat(none)"
	}
	
	// Build basic caveat information
	info := fmt.Sprintf("Caveat(%s", c.caveat.CaveatName)
	
	// Add context information if available
	if c.caveat.Context != nil && len(c.caveat.Context.GetFields()) > 0 {
		contextInfo := make([]string, 0, len(c.caveat.Context.GetFields()))
		for key := range c.caveat.Context.GetFields() {
			contextInfo = append(contextInfo, key)
		}
		info += fmt.Sprintf(", context: [%v]", contextInfo)
	}
	
	info += ")"
	return info
}