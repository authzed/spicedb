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

func (c *CaveatIterator) CheckImpl(ctx *Context, resources []Object, subject ObjectAndRelation) (PathSeq, error) {
	subSeq, err := c.subiterator.CheckImpl(ctx, resources, subject)
	if err != nil {
		return nil, err
	}

	return func(yield func(*Path, error) bool) {
		for path, err := range subSeq {
			if err != nil {
				if !yield(path, err) {
					return
				}
				continue
			}

			// Apply caveat evaluation to the path
			evaluation, err := c.evaluateCaveat(ctx, path)
			if err != nil {
				if !yield(path, err) {
					return
				}
				continue
			}

			switch evaluation {
			case CaveatTrue:
				// Caveat evaluated to true - yield path without caveat
				modifiedPath := *path  // Copy the path
				modifiedPath.Caveat = nil
				if !yield(&modifiedPath, nil) {
					return
				}
			case CaveatPartial:
				// Caveat is partial - yield path with caveat
				if !yield(path, nil) {
					return
				}
			case CaveatFalse:
				// Caveat evaluated to false - don't yield the path
			}
		}
	}, nil
}

func (c *CaveatIterator) IterSubjectsImpl(ctx *Context, resource Object) (PathSeq, error) {
	subSeq, err := c.subiterator.IterSubjectsImpl(ctx, resource)
	if err != nil {
		return nil, err
	}

	return func(yield func(*Path, error) bool) {
		for path, err := range subSeq {
			if err != nil {
				if !yield(path, err) {
					return
				}
				continue
			}

			// Apply caveat evaluation to the path
			evaluation, err := c.evaluateCaveat(ctx, path)
			if err != nil {
				if !yield(path, err) {
					return
				}
				continue
			}

			switch evaluation {
			case CaveatTrue:
				// Caveat evaluated to true - yield path without caveat
				modifiedPath := *path  // Copy the path
				modifiedPath.Caveat = nil
				if !yield(&modifiedPath, nil) {
					return
				}
			case CaveatPartial:
				// Caveat is partial - yield path with caveat
				if !yield(path, nil) {
					return
				}
			case CaveatFalse:
				// Caveat evaluated to false - don't yield the path
			}
		}
	}, nil
}

func (c *CaveatIterator) IterResourcesImpl(ctx *Context, subject ObjectAndRelation) (PathSeq, error) {
	subSeq, err := c.subiterator.IterResourcesImpl(ctx, subject)
	if err != nil {
		return nil, err
	}

	return func(yield func(*Path, error) bool) {
		for path, err := range subSeq {
			if err != nil {
				if !yield(path, err) {
					return
				}
				continue
			}

			// Apply caveat evaluation to the path
			evaluation, err := c.evaluateCaveat(ctx, path)
			if err != nil {
				if !yield(path, err) {
					return
				}
				continue
			}

			switch evaluation {
			case CaveatTrue:
				// Caveat evaluated to true - yield path without caveat
				modifiedPath := *path  // Copy the path
				modifiedPath.Caveat = nil
				if !yield(&modifiedPath, nil) {
					return
				}
			case CaveatPartial:
				// Caveat is partial - yield path with caveat
				if !yield(path, nil) {
					return
				}
			case CaveatFalse:
				// Caveat evaluated to false - don't yield the path
			}
		}
	}, nil
}

// evaluateCaveat determines if the given path satisfies the caveat conditions.
func (c *CaveatIterator) evaluateCaveat(ctx *Context, path *Path) (CaveatEvaluation, error) {
	
	// If no caveat is specified, allow all relations
	if c.caveat == nil {
		return CaveatTrue, nil
	}

	// If the path has no caveat, check if we expect one
	if path.Caveat == nil {
		// No caveat on the path - only allow if our caveat iterator expects no caveat
		return CaveatFalse, nil
	}

	// Build the caveat expression for evaluation using the path's caveat
	caveatExpr := path.Caveat

	// Use the CaveatRunner from the context if available
	if ctx.CaveatRunner == nil {
		// No caveat runner available - cannot evaluate caveats
		return CaveatFalse, fmt.Errorf("no caveat runner available for caveat evaluation")
	}

	// Get a snapshot reader which should implement CaveatReader
	reader := ctx.Datastore.SnapshotReader(ctx.Revision)
	
	// Build the combined context map - pass the caveat from path since we're using the Path's Caveat field directly
	contextMap := c.buildCaveatContext(ctx, nil) // We'll use the context from the CaveatIterator instead
	
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
			return CaveatFalse, fmt.Errorf("caveat evaluation failed: %w", evalErr)
		}
		if errors.As(err, &paramErr) {
			return CaveatFalse, fmt.Errorf("caveat parameter error: %w", paramErr)
		}

		// For other errors, provide context about caveat failure
		return CaveatFalse, fmt.Errorf("failed to evaluate caveat: %w", err)
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