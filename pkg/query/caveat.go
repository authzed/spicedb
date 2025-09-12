package query

import (
	"errors"
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

func (c *CaveatIterator) CheckImpl(ctx *Context, resources []Object, subject ObjectAndRelation) (PathSeq, error) {
	subSeq, err := ctx.Check(c.subiterator, resources, subject)
	if err != nil {
		return nil, err
	}

	return func(yield func(*Path, error) bool) {
		caveatName := "none"
		if c.caveat != nil {
			caveatName = c.caveat.CaveatName
		}
		ctx.TraceStep(c, "applying caveat '%s' to sub-iterator results", caveatName)

		processedCount := 0
		passedCount := 0

		for path, err := range subSeq {
			if err != nil {
				if !yield(path, err) {
					return
				}
				continue
			}

			processedCount++

			// Apply caveat simplification to the path
			simplified, passes, err := c.simplifyCaveat(ctx, path)
			if err != nil {
				ctx.TraceStep(c, "caveat evaluation failed for path: %v", err)
				if !yield(path, err) {
					return
				}
				continue
			}

			if !passes {
				// Caveat evaluated to false - don't yield the path
				ctx.TraceStep(c, "path failed caveat evaluation")
				continue
			}

			passedCount++

			// Create modified path with simplified caveat
			modifiedPath := *path // Copy the path
			if simplified == nil {
				// Caveat simplified to unconditionally true - remove caveat
				modifiedPath.Caveat = nil
				ctx.TraceStep(c, "caveat simplified to unconditionally true")
			} else {
				// Update path with simplified caveat
				modifiedPath.Caveat = simplified
				ctx.TraceStep(c, "caveat simplified but still conditional")
			}

			if !yield(&modifiedPath, nil) {
				return
			}
		}

		ctx.TraceStep(c, "processed %d paths, %d passed caveat evaluation", processedCount, passedCount)
	}, nil
}

func (c *CaveatIterator) IterSubjectsImpl(ctx *Context, resource Object) (PathSeq, error) {
	subSeq, err := ctx.IterSubjects(c.subiterator, resource)
	if err != nil {
		return nil, err
	}

	return func(yield func(*Path, error) bool) {
		defer func() {
		}()

		caveatName := "none"
		if c.caveat != nil {
			caveatName = c.caveat.CaveatName
		}
		ctx.TraceStep(c, "applying caveat '%s' to subjects for resource %s:%s", caveatName, resource.ObjectType, resource.ObjectID)

		processedCount := 0
		passedCount := 0

		for path, err := range subSeq {
			if err != nil {
				if !yield(path, err) {
					return
				}
				continue
			}

			processedCount++

			// Apply caveat simplification to the path
			simplified, passes, err := c.simplifyCaveat(ctx, path)
			if err != nil {
				ctx.TraceStep(c, "caveat evaluation failed for path: %v", err)
				if !yield(path, err) {
					return
				}
				continue
			}

			if !passes {
				// Caveat evaluated to false - don't yield the path
				ctx.TraceStep(c, "path failed caveat evaluation")
				continue
			}

			passedCount++

			// Create modified path with simplified caveat
			modifiedPath := *path // Copy the path
			if simplified == nil {
				// Caveat simplified to unconditionally true - remove caveat
				modifiedPath.Caveat = nil
			} else {
				// Update path with simplified caveat
				modifiedPath.Caveat = simplified
			}

			if !yield(&modifiedPath, nil) {
				return
			}
		}

		ctx.TraceStep(c, "processed %d subjects, %d passed caveat evaluation", processedCount, passedCount)
	}, nil
}

func (c *CaveatIterator) IterResourcesImpl(ctx *Context, subject ObjectAndRelation) (PathSeq, error) {
	subSeq, err := ctx.IterResources(c.subiterator, subject)
	if err != nil {
		return nil, err
	}

	return func(yield func(*Path, error) bool) {
		defer func() {
		}()

		caveatName := "none"
		if c.caveat != nil {
			caveatName = c.caveat.CaveatName
		}
		ctx.TraceStep(c, "applying caveat '%s' to resources for subject %s:%s", caveatName, subject.ObjectType, subject.ObjectID)

		processedCount := 0
		passedCount := 0

		for path, err := range subSeq {
			if err != nil {
				if !yield(path, err) {
					return
				}
				continue
			}

			processedCount++

			// Apply caveat simplification to the path
			simplified, passes, err := c.simplifyCaveat(ctx, path)
			if err != nil {
				ctx.TraceStep(c, "caveat evaluation failed for path: %v", err)
				if !yield(path, err) {
					return
				}
				continue
			}

			if !passes {
				// Caveat evaluated to false - don't yield the path
				ctx.TraceStep(c, "path failed caveat evaluation")
				continue
			}

			passedCount++

			// Create modified path with simplified caveat
			modifiedPath := *path // Copy the path
			if simplified == nil {
				// Caveat simplified to unconditionally true - remove caveat
				modifiedPath.Caveat = nil
			} else {
				// Update path with simplified caveat
				modifiedPath.Caveat = simplified
			}

			if !yield(&modifiedPath, nil) {
				return
			}
		}

		ctx.TraceStep(c, "processed %d resources, %d passed caveat evaluation", processedCount, passedCount)
	}, nil
}

// simplifyCaveat simplifies the caveat on the given path using AND/OR logic.
// Returns: (simplified_expression, passes, error)
func (c *CaveatIterator) simplifyCaveat(ctx *Context, path *Path) (*core.CaveatExpression, bool, error) {
	// If no caveat is specified on the iterator, allow all paths
	if c.caveat == nil {
		return path.Caveat, true, nil
	}

	// If the path has no caveat, it means unconditional access
	if path.Caveat == nil {
		// No caveat on the path - this means unconditional access (always true)
		return nil, true, nil
	}

	// Check if the path's caveat name matches the iterator's caveat name
	pathCaveatName := ""
	if path.Caveat.GetCaveat() != nil {
		pathCaveatName = path.Caveat.GetCaveat().CaveatName
	}

	// If caveat names don't match, filter out the path
	if pathCaveatName != c.caveat.CaveatName {
		return nil, false, nil // Path doesn't match caveat name, so it doesn't pass
	}

	// Use the CaveatRunner from the context if available
	if ctx.CaveatRunner == nil {
		// No caveat runner available - cannot evaluate caveats
		return nil, false, fmt.Errorf("no caveat runner available for caveat evaluation")
	}

	// Get a snapshot reader which should implement CaveatReader
	reader := ctx.Datastore.SnapshotReader(ctx.Revision)

	// Build the combined context map
	contextMap := c.buildCaveatContext(ctx, path.Caveat)

	// Use the SimplifyCaveatExpression function to properly handle AND/OR logic
	simplified, passes, err := SimplifyCaveatExpression(
		ctx,
		ctx.CaveatRunner,
		path.Caveat,
		contextMap,
		reader,
	)
	if err != nil {
		// Check if this is a specific caveat evaluation error and wrap it appropriately
		var evalErr *caveats.EvaluationError
		var paramErr *caveats.ParameterTypeError

		if errors.As(err, &evalErr) {
			return nil, false, fmt.Errorf("caveat evaluation failed: %w", evalErr)
		}
		if errors.As(err, &paramErr) {
			return nil, false, fmt.Errorf("caveat parameter error: %w", paramErr)
		}

		// For other errors, provide context about caveat failure
		return nil, false, fmt.Errorf("failed to evaluate caveat: %w", err)
	}

	// Return the simplification result directly
	return simplified, passes, nil
}

// buildCaveatContext combines the path's caveat context with query-time context
func (c *CaveatIterator) buildCaveatContext(ctx *Context, pathCaveat *core.CaveatExpression) map[string]any {
	contextMap := make(map[string]any)

	// Start with the path's caveat context if available
	if pathCaveat != nil && pathCaveat.GetCaveat() != nil && pathCaveat.GetCaveat().Context != nil {
		contextMap = pathCaveat.GetCaveat().Context.AsMap()
	}

	// Overlay query-time context if available
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
		Name:       "Caveat",
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
