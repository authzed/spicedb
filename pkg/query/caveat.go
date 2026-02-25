package query

import (
	"errors"
	"fmt"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

// CaveatIterator wraps another iterator and applies caveat evaluation to its results.
// It checks caveat conditions on relationships during iteration and only yields
// relationships that satisfy the caveat constraints.
type CaveatIterator struct {
	subiterator  Iterator
	caveat       *core.ContextualizedCaveat
	canonicalKey CanonicalKey
}

var _ Iterator = &CaveatIterator{}

// NewCaveatIterator creates a new caveat iterator that wraps the given subiterator
// and applies the specified caveat conditions.
func NewCaveatIterator(subiterator Iterator, caveat *core.ContextualizedCaveat) *CaveatIterator {
	return &CaveatIterator{
		subiterator: subiterator,
		caveat:      caveat.CloneVT(),
	}
}

func (c *CaveatIterator) CheckImpl(ctx *Context, resources []Object, subject ObjectAndRelation) (PathSeq, error) {
	subSeq, err := ctx.Check(c.subiterator, resources, subject)
	if err != nil {
		return nil, err
	}
	return c.runCaveats(ctx, subSeq)
}

func (c *CaveatIterator) IterSubjectsImpl(ctx *Context, resource Object, filterSubjectType ObjectType) (PathSeq, error) {
	subSeq, err := ctx.IterSubjects(c.subiterator, resource, filterSubjectType)
	if err != nil {
		return nil, err
	}
	return c.runCaveats(ctx, subSeq)
}

func (c *CaveatIterator) IterResourcesImpl(ctx *Context, subject ObjectAndRelation, filterResourceType ObjectType) (PathSeq, error) {
	subSeq, err := ctx.IterResources(c.subiterator, subject, filterResourceType)
	if err != nil {
		return nil, err
	}

	return c.runCaveats(ctx, subSeq)
}

func (c *CaveatIterator) runCaveats(ctx *Context, subSeq PathSeq) (PathSeq, error) {
	if c.caveat == nil {
		return subSeq, nil
	}

	return func(yield func(Path, error) bool) {
		ctx.TraceStep(c, "applying caveat '%s' to sub-iterator results", c.caveat.CaveatName)

		processedCount := 0
		passedCount := 0

		for path, err := range subSeq {
			if err != nil {
				if !yield(path, err) {
					return
				}
			}

			processedCount++

			// Apply caveat simplification to the path
			simplified, passes, err := c.simplifyCaveat(ctx, path)
			if err != nil {
				ctx.TraceStep(c, "caveat evaluation failed for path: %v", err)
				if !yield(path, err) {
					return
				}
			}

			if !passes {
				// Caveat evaluated to false - don't yield the path
				ctx.TraceStep(c, "path failed caveat evaluation")
				continue
			}

			passedCount++

			path.Caveat = simplified
			if !yield(path, nil) {
				return
			}
		}

		ctx.TraceStep(c, "processed %d paths, %d passed caveat evaluation", processedCount, passedCount)
	}, nil
}

// simplifyCaveat simplifies the caveat on the given path using AND/OR logic.
// Returns: (simplified_expression, passes, error)
func (c *CaveatIterator) simplifyCaveat(ctx *Context, path Path) (*core.CaveatExpression, bool, error) {
	// If no caveat is specified on the iterator, allow all paths
	if c.caveat == nil {
		return path.Caveat, true, nil
	}

	// If the path has no caveat, it means unconditional access
	if path.Caveat == nil {
		// No caveat on the path - this means unconditional access (always true)
		return nil, true, nil
	}

	// For complex caveat expressions, check if any leaf caveat matches the expected name
	if !c.containsExpectedCaveat(path.Caveat) {
		// If the path doesn't contain the expected caveat, the path doesn't depend on this caveat.
		// Pass it through unchanged - this caveat check doesn't apply to this path.
		return path.Caveat, true, nil
	}

	// Use the CaveatRunner from the context if available
	if ctx.CaveatRunner == nil {
		// No caveat runner available - cannot evaluate caveats
		return nil, false, errors.New("no caveat runner available for caveat evaluation")
	}

	// Use the SimplifyCaveatExpression function to properly handle AND/OR logic
	sr, err := ctx.Reader.ReadSchema()
	if err != nil {
		return nil, false, fmt.Errorf("failed to get schema reader: %w", err)
	}
	simplified, passes, err := SimplifyCaveatExpression(
		ctx,
		ctx.CaveatRunner,
		path.Caveat,
		ctx.CaveatContext,
		sr,
	)
	if err != nil {
		return nil, false, fmt.Errorf("failed to simplify caveat: %w", err)
	}

	// Return the simplification result directly
	return simplified, passes, nil
}

// containsExpectedCaveat checks if a caveat expression contains the expected caveat name
// This works for both simple caveats and complex expressions (AND/OR)
func (c *CaveatIterator) containsExpectedCaveat(expr *core.CaveatExpression) bool {
	if c.caveat == nil {
		return true // No expected caveat, so any expression passes
	}

	return c.containsCaveatName(expr, c.caveat.CaveatName)
}

// containsCaveatName recursively checks if a caveat expression contains a specific caveat name
func (c *CaveatIterator) containsCaveatName(expr *core.CaveatExpression, expectedName string) bool {
	if expr == nil {
		return false
	}

	// Check if this is a leaf caveat
	if expr.GetCaveat() != nil {
		return expr.GetCaveat().CaveatName == expectedName
	}

	// Check if this is an operation with children
	if expr.GetOperation() != nil {
		for _, child := range expr.GetOperation().Children {
			if c.containsCaveatName(child, expectedName) {
				return true
			}
		}
	}

	return false
}

func (c *CaveatIterator) Clone() Iterator {
	return &CaveatIterator{
		canonicalKey: c.canonicalKey,
		subiterator:  c.subiterator.Clone(),
		caveat:       c.caveat.CloneVT(),
	}
}

func (c *CaveatIterator) Explain() Explain {
	caveatInfo := c.buildExplainInfo()

	return Explain{
		Name:       "Caveat",
		Info:       caveatInfo,
		SubExplain: []Explain{c.subiterator.Explain()},
	}
}

func (c *CaveatIterator) Subiterators() []Iterator {
	return []Iterator{c.subiterator}
}

func (c *CaveatIterator) ReplaceSubiterators(newSubs []Iterator) (Iterator, error) {
	return &CaveatIterator{canonicalKey: c.canonicalKey, subiterator: newSubs[0], caveat: c.caveat}, nil
}

func (c *CaveatIterator) CanonicalKey() CanonicalKey {
	return c.canonicalKey
}

func (c *CaveatIterator) ResourceType() ([]ObjectType, error) {
	// Delegate to the wrapped iterator
	return c.subiterator.ResourceType()
}

func (c *CaveatIterator) SubjectTypes() ([]ObjectType, error) {
	// Delegate to the wrapped iterator
	return c.subiterator.SubjectTypes()
}

// buildExplainInfo creates detailed explanation information for the caveat iterator
func (c *CaveatIterator) buildExplainInfo() string {
	if c.caveat == nil {
		return "Caveat(none)"
	}

	// Build basic caveat information
	info := "Caveat(" + c.caveat.CaveatName

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
