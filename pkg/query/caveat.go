package query

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

func init() {
	MustRegisterIterator(IteratorSpec{
		Type: CaveatIteratorType,
		Name: "Caveat",
		ConstructWithArgs: func(args *IteratorArgs, subs []Iterator, key CanonicalKey) (Iterator, error) {
			if len(subs) != 1 {
				return nil, fmt.Errorf("CaveatIterator requires exactly 1 subiterator, got %d", len(subs))
			}
			if args == nil || args.Caveat == nil {
				return nil, errors.New("CaveatIterator requires Caveat in Args")
			}
			caveat := NewCaveatIterator(subs[0], args.Caveat)
			caveat.canonicalKey = key
			return caveat, nil
		},
		Deserialize: deserializeCaveat,
	})
}

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

func (c *CaveatIterator) CheckImpl(ctx *Context, resource Object, subject ObjectAndRelation) (*Path, error) {
	path, err := ctx.Check(c.subiterator, resource, subject)
	if err != nil {
		return nil, err
	}
	if path == nil {
		return nil, nil
	}
	return c.runCaveatOnPath(ctx, path)
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

// runCaveatOnPath applies caveat evaluation to a single path (used by CheckImpl).
// Returns nil if the path fails caveat evaluation, or the (possibly annotated) path if it passes.
func (c *CaveatIterator) runCaveatOnPath(ctx *Context, path *Path) (*Path, error) {
	if c.caveat == nil {
		return path, nil
	}
	simplified, passes, err := c.simplifyCaveat(ctx, path)
	if err != nil {
		return nil, err
	}
	if !passes {
		return nil, nil
	}
	path.Caveat = simplified
	return path, nil
}

func (c *CaveatIterator) runCaveats(ctx *Context, subSeq PathSeq) (PathSeq, error) {
	if c.caveat == nil {
		return subSeq, nil
	}

	return func(yield func(*Path, error) bool) {
		if ctx.shouldTrace() {
			ctx.TraceStep(c, "applying caveat '%s' to sub-iterator results", c.caveat.CaveatName)
		}

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
				if ctx.shouldTrace() {
					ctx.TraceStep(c, "caveat evaluation failed for path: %v", err)
				}
				if !yield(path, err) {
					return
				}
			}

			if !passes {
				// Caveat evaluated to false - don't yield the path
				if ctx.shouldTrace() {
					ctx.TraceStep(c, "path failed caveat evaluation")
				}
				continue
			}

			passedCount++

			path.Caveat = simplified
			if !yield(path, nil) {
				return
			}
		}

		if ctx.shouldTrace() {
			ctx.TraceStep(c, "processed %d paths, %d passed caveat evaluation", processedCount, passedCount)
		}
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
	simplified, passes, err := SimplifyCaveatExpression(
		ctx,
		ctx.CaveatRunner,
		path.Caveat,
		ctx.CaveatContext,
		caveatDefinitionLookupAdapter{ctx.Reader},
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

// caveatDefinitionLookupAdapter wraps a QueryDatastoreReader to satisfy
// caveats.CaveatDefinitionLookup (which takes a bulk name slice) by calling
// LookupCaveatDefinition individually for each name.
type caveatDefinitionLookupAdapter struct{ r QueryDatastoreReader }

func (a caveatDefinitionLookupAdapter) LookupCaveatDefinitionsByNames(
	ctx context.Context,
	names []string,
) (map[string]datastore.CaveatDefinition, error) {
	out := make(map[string]datastore.CaveatDefinition, len(names))
	for _, name := range names {
		def, err := a.r.LookupCaveatDefinition(ctx, name)
		if err != nil {
			return nil, err
		}
		out[name] = def
	}
	return out, nil
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

const caveatFlagHasCaveat = 0

func (c *CaveatIterator) Serialize(w io.Writer) error {
	return serializeWithHeader(w, CaveatIteratorType, c.canonicalKey, func(buf io.Writer) error {
		var flags uint64
		setFlag(&flags, caveatFlagHasCaveat, c.caveat != nil)
		if err := writeUvarint(buf, flags); err != nil {
			return err
		}
		if hasFlag(flags, caveatFlagHasCaveat) {
			if err := writeProto(buf, c.caveat); err != nil {
				return fmt.Errorf("caveat proto: %w", err)
			}
		}
		return c.subiterator.Serialize(buf)
	})
}

func deserializeCaveat(body io.Reader, key CanonicalKey, dctx *DeserializeContext) (Iterator, error) {
	br := asByteReader(body)
	flags, err := readUvarint(br)
	if err != nil {
		return nil, fmt.Errorf("caveat flags: %w", err)
	}
	var caveat *core.ContextualizedCaveat
	if hasFlag(flags, caveatFlagHasCaveat) {
		c := &core.ContextualizedCaveat{}
		if err := readProto(br, c); err != nil {
			return nil, fmt.Errorf("caveat proto: %w", err)
		}
		caveat = c
	}
	sub, err := Deserialize(br, dctx)
	if err != nil {
		return nil, fmt.Errorf("caveat sub: %w", err)
	}
	if caveat == nil {
		return nil, errors.New("CaveatIterator requires non-nil caveat")
	}
	ci := NewCaveatIterator(sub, caveat)
	ci.canonicalKey = key
	return ci, nil
}
