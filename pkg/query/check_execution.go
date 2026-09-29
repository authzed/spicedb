package query

import (
	"slices"

	"github.com/authzed/spicedb/pkg/schema/v2"
	"github.com/authzed/spicedb/pkg/tuple"
)

// CheckExecutionOptions selects alternative local check strategies. Defaults
// preserve the existing execution behavior. These controls support work audits.
type CheckExecutionOptions struct {
	TargetedRecursion            bool
	BaseFirstExclusion           bool
	StrictSubjectMatching        bool
	ExhaustiveIntersectionArrows bool
	StopAfterDirectMatch         bool
	CoalesceDirectWildcards      bool
	UnfilteredSingleTypeReads    bool
	// BroadSingleUsersetReads uses the classic non-ellipsis scan when exactly
	// one indirect type/relation is allowed. Applies only to unpaged local reads.
	BroadSingleUsersetReads bool
}

func WithCheckExecution(options CheckExecutionOptions) ContextOption {
	return func(ctx *Context) { ctx.checkExecution = options }
}

func (r *RecursiveIterator) targetedCheck(ctx *Context, resource Object, subject ObjectAndRelation) (*Path, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	limit := ctx.MaxRecursionDepth
	if limit == 0 {
		limit = defaultMaxRecursionDepth
	}
	if ctx.targetedDepth >= limit {
		return nil, MaxRecursionDepthError{Depth: limit}
	}
	if ctx.targetedRecursions == nil {
		ctx.targetedRecursions = map[string]*RecursiveIterator{}
	}
	key := r.definitionName + "#" + r.relationName
	previous := ctx.targetedRecursions[key]
	ctx.targetedRecursions[key] = r
	ctx.targetedDepth++
	defer func() {
		ctx.targetedDepth--
		if previous == nil {
			delete(ctx.targetedRecursions, key)
		} else {
			ctx.targetedRecursions[key] = previous
		}
	}()
	return ctx.Check(r.templateTree, resource, subject)
}

// BuildOption configures outline construction independently of execution context.
type BuildOption func(*outlineBuilder)

// WithSchemaExecutionOrder retains schema branch order for local comparison.
// Canonical keys are still computed from the canonical form of each subtree.
func WithSchemaExecutionOrder() BuildOption {
	return func(b *outlineBuilder) { b.preserveExecutionOrder = true }
}

func (b *outlineBuilder) orderedBaseRelations(r *schema.Relation) []*schema.BaseRelation {
	brs := r.BaseRelations()
	if b.deferCaveats {
		unique := make([]*schema.BaseRelation, 0, len(brs))
		seen := map[string]bool{}
		for _, br := range brs {
			key := br.Type() + "#" + br.Subrelation()
			if br.Wildcard() {
				key += "*"
			}
			if !seen[key] {
				unique = append(unique, br)
				seen[key] = true
			}
		}
		brs = unique
	}
	if !b.preserveExecutionOrder {
		return brs
	}
	brs = slices.Clone(brs)
	slices.SortStableFunc(brs, func(a, b *schema.BaseRelation) int {
		direct := func(r *schema.BaseRelation) bool { return r.Subrelation() == tuple.Ellipsis || r.Wildcard() }
		if direct(a) == direct(b) {
			return 0
		}
		if direct(a) {
			return -1
		}
		return 1
	})
	return brs
}

func canonicalizeForExecutionOrder(outline Outline) (CanonicalOutline, error) {
	// Keep the actual tree, including its composite nesting. Derive keys from
	// canonical copies so changing evaluation order does not change logical identity.
	keys := map[OutlineNodeID]CanonicalKey{}
	root := assignNodeIDs(outline, keys)
	_, err := WalkOutlineBottomUp(root, func(node Outline) (Outline, error) {
		canonical, err := CanonicalizeOutline(node)
		if err != nil {
			return Outline{}, err
		}
		keys[node.ID] = canonical.CanonicalKeys[canonical.Root.ID]
		return node, nil
	})
	return CanonicalOutline{Root: root, CanonicalKeys: keys}, err
}

// WithDeferredCaveats leaves caveat expressions on paths for the caller to
// evaluate after Check, matching graph dispatch. Trait variants sharing one
// physical relation query are coalesced; reads retain all relation traits.
// Callers MUST evaluate the returned expression before reporting a decision.
func WithDeferredCaveats() BuildOption { return func(b *outlineBuilder) { b.deferCaveats = true } }
