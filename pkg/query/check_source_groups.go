package query

import (
	"github.com/authzed/spicedb/pkg/query/planhint"
	"github.com/authzed/spicedb/pkg/schema/v2"
	"github.com/authzed/spicedb/pkg/tuple"
)

// namedSourceRelation unwraps named dispatch boundaries to verify the computed
// relation's owning type. Arbitrary wrappers and unnamed branches are rejected.
func namedSourceRelation(it Iterator) (definition, relation string, ok bool) {
	for {
		switch node := it.(type) {
		case *AliasIterator:
			return node.definitionName, node.effectiveRelation(), node.definitionName != ""
		case *RecursiveIterator:
			return node.definitionName, node.relationName, node.definitionName != ""
		default:
			named, namedOK := it.(NamedCheckBranch)
			subs := it.Subiterators()
			if !namedOK || !named.IsNamedCheckBranch() || len(subs) != 1 {
				return "", "", false
			}

			it = subs[0]
		}
	}
}

// sourceGroupCandidate avoids allocating a scalar batch for ordinary permission
// sets and single-form relations, which cannot use typed source grouping.
func sourceGroupCandidate(children []Iterator) bool {
	if len(children) < 2 {
		return false
	}

	switch children[0].(type) {
	case *DatastoreIterator:
		prefix := 0

		for _, child := range children {
			leaf, ok := child.(*DatastoreIterator)
			if !ok || (leaf.base.Subrelation() != tuple.Ellipsis && !leaf.base.Wildcard()) {
				break
			}

			prefix++
		}

		return len(children)-prefix >= 2
	case *UnionIterator, *ArrowIterator, *FixedIterator:
		return true
	default:
		return false
	}
}

// arrowLeftSource checks the compiled source rather than iterator pointer identity:
// compiling the same outline in two positions creates separate leaf instances.
func arrowLeftSource(arrow *ArrowIterator, ok bool) (*DatastoreIterator, bool) {
	if !ok {
		return nil, false
	}

	left, isSource := arrow.left.(*DatastoreIterator)
	return left, isSource
}

// sourceGroups recognizes the schema's native grouping boundaries, rather than
// speculating on arbitrary set children. Arrows share one tupleset/computed name;
// usersets retain their direct prefix and group their indirect type/relations.
func sourceGroups(children []Iterator) (direct, grouped []Iterator, source *DatastoreIterator, ok bool) {
	if len(children) < 2 {
		return
	}

	switch children[0].(type) {
	case *DatastoreIterator, *UnionIterator:
		prefix := 0

		for _, child := range children {
			leaf, leafOK := child.(*DatastoreIterator)
			if !leafOK || (leaf.base.Subrelation() != tuple.Ellipsis && !leaf.base.Wildcard()) {
				break
			}

			prefix++
		}

		if len(children)-prefix < 2 {
			return
		}

		for _, child := range children[prefix:] {
			forms, formsOK := child.(*UnionIterator)
			if !formsOK || len(forms.subIts) != 2 {
				return
			}

			leaf, leafOK := forms.subIts[0].(*DatastoreIterator)
			arrow, arrowOK := forms.subIts[1].(*ArrowIterator)
			left, leftOK := arrowLeftSource(arrow, arrowOK)
			if !leafOK || !leftOK || left.base.Parent() != leaf.base.Parent() || !left.base.Equal(leaf.base) || leaf.base.Wildcard() ||
				leaf.base.Subrelation() == "" || leaf.base.Subrelation() == tuple.Ellipsis {
				return
			}

			definition, relation, named := namedSourceRelation(arrow.right)
			if !named || definition != leaf.base.Type() || relation != leaf.base.Subrelation() {
				return
			}

			if source == nil {
				source = leaf
			}

			if leaf.base.Parent() != source.base.Parent() {
				return
			}
		}

		if _, isRelation := source.base.Parent().(*schema.Relation); !isRelation {
			return
		}

		for _, child := range children[:prefix] {
			if child.(*DatastoreIterator).base.Parent() != source.base.Parent() {
				return
			}
		}

		return children[:prefix], children[prefix:], source, true
	case *ArrowIterator, *FixedIterator:
		seenTypes := map[string]bool{}
		computed := ""

		for _, child := range children {
			if empty, isFixed := child.(*FixedIterator); isFixed && len(empty.paths) == 0 {
				// Missing computed relations lower to an empty alternative.
				continue
			}

			arrow, arrowOK := child.(*ArrowIterator)
			if !arrowOK {
				return
			}

			leaf, leafOK := arrow.left.(*DatastoreIterator)
			if !leafOK || leaf.base.Wildcard() || leaf.base.Subrelation() != tuple.Ellipsis {
				return
			}

			definition, relation, named := namedSourceRelation(arrow.right)
			if !named || definition != leaf.base.Type() || seenTypes[definition] {
				return
			}

			if source == nil {
				source, computed = leaf, relation
			}

			if leaf.base.Parent() != source.base.Parent() || relation != computed {
				return
			}

			seenTypes[definition] = true
			grouped = append(grouped, child)
		}

		if source == nil || len(grouped) < 2 {
			return
		}

		if _, isRelation := source.base.Parent().(*schema.Relation); !isRelation {
			return
		}

		return nil, grouped, source, true
	default:
		return
	}
}

// checkSourceGroups matches native's phase order: direct membership can end the
// check; otherwise enumerate the common source once and run its typed targets
// concurrently. Preloading before forks avoids one broad scan per worker. The
// original iterators still execute, retaining canonical IDs and observations.
func checkSourceGroups(ctx *Context, union *UnionIterator, batch checkBatch) ([]*Path, bool, error) {
	if !ctx.checkExecution.BatchChecks || !ctx.checkExecution.StrictSubjectMatching ||
		min(union.checkConcurrency, ctx.checkConcurrency) <= 1 ||
		ctx.PaginationLimit != nil || ctx.TraceLogger != nil || len(ctx.observers) > 0 ||
		len(ctx.recursiveFrontierCollectors) > 0 {
		return nil, false, nil
	}

	if _, readerOK := ctx.Reader.(*datalayerQueryDatastoreReader); !readerOK {
		return nil, false, nil
	}

	// Literal userset requests can match an indirect form's raw edge. Keep those
	// on their existing ordered path so identity can avoid all indirect work.
	for _, subject := range batch.subjects {
		if subject.Relation != tuple.Ellipsis {
			return nil, false, nil
		}
	}

	direct, grouped, source, eligible := sourceGroups(union.subIts)
	if !eligible {
		return nil, false, nil
	}

	// Shared scans require concrete resources from this source definition.
	for _, resource := range batch.resources {
		if resource.ObjectType != source.base.DefinitionName() || resource.ObjectID == "" {
			return nil, false, nil
		}
	}

	indirect := source.base.Subrelation() != tuple.Ellipsis
	enabled := ctx.checkExecution.UnfilteredSingleTypeReads

	if indirect {
		enabled = ctx.checkExecution.BroadSingleUsersetReads
	}

	if !enabled || !source.hasMultipleScanForms(indirect) {
		return nil, false, nil
	}

	paths, err := checkSetBatch(ctx, batch, direct, false)
	if err != nil {
		return nil, true, err
	}

	pending := make([]int, 0, len(paths))

	for i, path := range paths {
		if path == nil || path.Caveat != nil {
			pending = append(pending, i)
		}
	}

	if len(pending) == 0 || (batch.mode == AllowSingleResult && hasUnconditionalGrant(paths)) {
		// No indirect targets need execution. Explain why the broad concurrency
		// recommendation was declined even though this source can have many types.
		err := ctx.accountOrderedCheckConcurrency(
			union.checkConcurrency,
			union.concurrencyHint,
			"direct-membership",
			planhint.RelationChildOrder,
		)
		return paths, true, err
	}

	remaining := batch.subset(pending)

	if _, _, err := sharedTypedSubjects(ctx, source, remaining.resources); err != nil {
		return nil, true, err
	}

	limit, err := ctx.setCheckConcurrency(union.checkConcurrency, union.concurrencyHint)
	if err != nil {
		return nil, true, err
	}

	results, err := checkSetBatchParallel(ctx, remaining, grouped, limit, false)
	if err != nil {
		return nil, true, err
	}

	// Retain conditional direct paths while OR-ing each pending target's grouped
	// result. A concrete grant elsewhere in an exhaustive batch remains intact.
	for i, original := range pending {
		paths[original], err = paths[original].MergeOr(results[i])
		if err != nil {
			return nil, true, err
		}
	}

	return paths, true, nil
}
