package query

import (
	"slices"

	"github.com/authzed/spicedb/pkg/spiceerrors"
)

// checkBatch retains the varying dimension of a multi-check. Keeping it through
// reducers lets each child issue one compatible datastore/dispatch batch instead
// of re-entering the entire iterator tree once per target. The constant endpoint
// occupies one slot rather than a repeated vector proportional to fanout.
type checkBatch struct {
	resources     []Object
	subjects      []ObjectAndRelation
	resourcesVary bool
	mode          CheckResultMode
}

// cloneTargets snapshots endpoints before a worker can outlive the foreground
// call. Copy only at asynchronous launch boundaries; serial child traversal can
// keep borrowing its caller's vectors until it returns.
func (b checkBatch) cloneTargets() checkBatch {
	b.resources = slices.Clone(b.resources)
	b.subjects = slices.Clone(b.subjects)
	return b
}

func resourceBatch(resources []Object, subject ObjectAndRelation) checkBatch {
	return checkBatch{
		resources:     resources,
		subjects:      []ObjectAndRelation{subject},
		resourcesVary: true,
	}
}

func subjectBatch(resource Object, subjects []ObjectAndRelation) checkBatch {
	return checkBatch{
		resources: []Object{resource},
		subjects:  subjects,
	}
}

// size counts targets using the varying dimension. The other dimension retains
// one constant endpoint, including when the caller supplied an empty vector.
func (b checkBatch) size() int {
	if b.resourcesVary {
		return len(b.resources)
	}

	return len(b.subjects)
}

// resourceAt resolves a target slot through either a varying or constant vector.
func (b checkBatch) resourceAt(index int) Object {
	if len(b.resources) == 1 {
		return b.resources[0]
	}

	return b.resources[index]
}

// subjectAt resolves a target slot through either a varying or constant vector.
func (b checkBatch) subjectAt(index int) ObjectAndRelation {
	if len(b.subjects) == 1 {
		return b.subjects[0]
	}

	return b.subjects[index]
}

func (b checkBatch) subset(indices []int) checkBatch {
	out := checkBatch{resourcesVary: b.resourcesVary, mode: b.mode}

	if len(indices) == 0 {
		return out
	}

	// Reducers repeatedly select unresolved targets from large fanout batches.
	// Allocate each owned vector once, keeping the constant endpoint as one slot
	// rather than copying it for every selected target. Varying slots preserve
	// input order and duplicates independently of the parent's vectors.
	resourceCount, subjectCount := len(indices), len(indices)

	if !b.resourcesVary && len(b.resources) == 1 {
		resourceCount = 1
	}

	if b.resourcesVary && len(b.subjects) == 1 {
		subjectCount = 1
	}

	out.resources = make([]Object, resourceCount)
	out.subjects = make([]ObjectAndRelation, subjectCount)

	for i := range out.resources {
		out.resources[i] = b.resourceAt(indices[i])
	}

	for i := range out.subjects {
		out.subjects[i] = b.subjectAt(indices[i])
	}

	return out
}

func (b checkBatch) child(ctx *Context, it Iterator) ([]*Path, error) {
	if b.size() == 0 {
		return nil, nil
	}

	if b.resourcesVary {
		return ctx.CheckManyResources(it, b.resources, b.subjects[0], b.mode)
	}

	return ctx.CheckManySubjects(it, b.resources[0], b.subjects, b.mode)
}

// CheckManyResourcesImpl executes a resource batch locally at an already crossed
// dispatch boundary. Results retain input order; the optional mode defaults to
// AllCheckResults. AllowSingleResult can leave nil slots unvisited after a grant.
func CheckManyResourcesImpl(
	ctx *Context,
	it Iterator,
	resources []Object,
	subject ObjectAndRelation,
	modes ...CheckResultMode,
) ([]*Path, error) {
	batch := resourceBatch(resources, subject)
	batch.mode = ctx.checkBatchMode(modes)
	return checkBatchChunks(ctx, it, batch)
}

// CheckManySubjectsImpl executes a subject batch locally at an already crossed
// dispatch boundary. Results retain input order; the optional mode defaults to
// AllCheckResults. AllowSingleResult can leave nil slots unvisited after a grant.
func CheckManySubjectsImpl(
	ctx *Context,
	it Iterator,
	resource Object,
	subjects []ObjectAndRelation,
	modes ...CheckResultMode,
) ([]*Path, error) {
	batch := subjectBatch(resource, subjects)
	batch.mode = ctx.checkBatchMode(modes)
	return checkBatchChunks(ctx, it, batch)
}

func checkBatchImpl(ctx *Context, it Iterator, batch checkBatch) ([]*Path, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if batch.size() == 0 {
		return nil, nil
	}

	// Batch-capable nodes preserve the request shape through their children.
	// Other nodes use the scalar implementation, retaining its ownership rules
	// for frontiers, observers and specialized iterator semantics.
	if ctx.checkExecution.BatchChecks {
		switch node := it.(type) {
		case *AliasIterator:
			// A heterogeneous right-hand union sends each named branch only the
			// resources owned by that definition. Preserve original result slots,
			// including duplicates, while keeping foreign targets out of its body.
			scoped := batch
			var indices []int

			for _, resource := range batch.resources {
				if node.definitionName != "" && resource.ObjectType != node.definitionName {
					indices = make([]int, 0, batch.size())

					for i, candidate := range batch.resources {
						if candidate.ObjectType == node.definitionName {
							indices = append(indices, i)
						}
					}

					scoped = batch.subset(indices)
					break
				}
			}

			paths, err := scoped.child(ctx, node.subIt)
			if err != nil {
				return nil, err
			}

			for i, path := range paths {
				resource := scoped.resourceAt(i)
				subject := scoped.subjectAt(i)

				if path != nil {
					path.Relation = node.effectiveRelation()
				} else if resource.ObjectType == subject.ObjectType &&
					resource.ObjectID == subject.ObjectID &&
					node.matchesSelfEdgeRelation(subject.Relation) {
					paths[i] = &Path{
						Resource: resource,
						Relation: node.effectiveRelation(),
						Subject:  subject,
						Metadata: map[string]any{},
					}
				}
			}

			// Homogeneous batches already belong to this named branch. Keep their
			// existing vectors/results instead of allocating an identical partition.
			if indices == nil {
				return paths, nil
			}

			out := make([]*Path, batch.size())

			for i, original := range indices {
				out[original] = paths[i]
			}

			return out, nil
		case *UnionIterator:
			// Retain the same direct-before-grouped phase order for the whole vector.
			// Targets already granted directly are omitted from the indirect scan.
			if paths, handled, err := checkSourceGroups(ctx, node, batch); handled {
				return paths, err
			}

			// Permission operands are independent even when arrows or nested sets
			// hide their named boundaries. Relation forms still preserve child order
			// so direct grants can avoid indirect userset work.
			if node.independentCheckBranches || namedBatchBranches(node.subIts) {
				limit, err := ctx.setCheckConcurrency(node.checkConcurrency, node.concurrencyHint)
				if err != nil {
					return nil, err
				}

				if limit > 1 {
					return checkSetBatchParallel(ctx, batch, node.subIts, limit, false)
				}
			} else if err := ctx.accountBatchConcurrency(node.checkConcurrency, node.concurrencyHint); err != nil {
				return nil, err
			}

			return checkSetBatch(ctx, batch, node.subIts, false)
		case *IntersectionIterator:
			// Permission operands are independent even when arrows or nested sets
			// hide their named boundaries. Relation forms still preserve child order
			// so direct grants can avoid indirect userset work.
			if node.independentCheckBranches || namedBatchBranches(node.subIts) {
				limit, err := ctx.setCheckConcurrency(node.checkConcurrency, node.concurrencyHint)
				if err != nil {
					return nil, err
				}

				if limit > 1 {
					return checkSetBatchParallel(ctx, batch, node.subIts, limit, true)
				}
			} else if err := ctx.accountBatchConcurrency(node.checkConcurrency, node.concurrencyHint); err != nil {
				return nil, err
			}

			return checkSetBatch(ctx, batch, node.subIts, true)
		case *CaveatIterator:
			// A grant below a caveat is insufficient until the caveat is applied.
			childBatch := batch
			childBatch.mode = AllCheckResults
			paths, err := childBatch.child(ctx, node.subiterator)
			if err != nil {
				return nil, err
			}

			for i, path := range paths {
				if path != nil {
					paths[i], err = node.runCaveatOnPath(ctx, path)
					if err != nil {
						return nil, err
					}
				}
			}

			return paths, nil
		case *ExclusionIterator:
			return node.checkExclusionBatch(ctx, batch)
		case *RecursiveIterator:
			if ctx.checkExecution.TargetedRecursion {
				return node.targetedBatchCheck(ctx, batch)
			}
		case *RecursiveSentinelIterator:
			if ctx.checkExecution.TargetedRecursion {
				target := ctx.targetedRecursions[node.definitionName+"#"+node.relationName]
				if target == nil {
					return nil, spiceerrors.MustBugf("unbound recursive batch sentinel %s#%s", node.definitionName, node.relationName)
				}

				return target.targetedBatchCheck(ctx, batch)
			}
		case *DatastoreIterator:
			if reader, ok := ctx.Reader.(*datalayerQueryDatastoreReader); ok {
				return node.checkDatastoreBatch(ctx, reader, batch)
			}
		case *ArrowIterator:
			if batch.resourcesVary && node.direction == leftToRight {
				return node.checkArrowResourceBatch(ctx, batch)
			}
		}
	}

	return scalarCheckBatch(ctx, it, batch)
}

func scalarCheckBatch(ctx *Context, it Iterator, batch checkBatch) ([]*Path, error) {
	paths := make([]*Path, batch.size())

	for i := range batch.size() {
		path, err := it.CheckImpl(ctx, batch.resourceAt(i), batch.subjectAt(i))
		if err != nil {
			return nil, err
		}

		paths[i] = path

		if batch.mode == AllowSingleResult && path != nil && path.Caveat == nil {
			break
		}
	}

	return paths, nil
}

// checkSetBatch retains per-target short circuits: a union skips an unconditional
// grant, while an intersection skips a denial. Conditional paths must continue
// through remaining children so their caveat expressions are combined correctly.
// Child order also keeps native relation reads direct-before-indirect; this is
// distinct from speculative scalar permission reducers.
func checkSetBatch(ctx *Context, batch checkBatch, children []Iterator, intersection bool) ([]*Path, error) {
	out := make([]*Path, batch.size())
	active := make([]int, len(out))

	for i := range active {
		active[i] = i
	}

	for childIndex, child := range children {
		if len(active) == 0 {
			break
		}

		childBatch := batch.subset(active)

		// Intersection children must resolve every candidate: a grant in one
		// child can be denied by another child for that same target.
		if intersection {
			childBatch.mode = AllCheckResults
		}

		paths, err := childBatch.child(ctx, child)
		if err != nil {
			return nil, err
		}

		remaining := active[:0]

		for i, index := range active {
			path := paths[i]

			if intersection {
				if path == nil {
					out[index] = nil
					continue
				}

				if childIndex == 0 {
					out[index] = path
				} else {
					out[index], err = out[index].MergeAnd(path)
				}

				remaining = append(remaining, index)
			} else {
				if path == nil {
					remaining = append(remaining, index)
					continue
				}

				out[index], err = out[index].MergeOr(path)

				if out[index] == nil || out[index].Caveat != nil {
					remaining = append(remaining, index)
				}
			}

			if err != nil {
				return nil, err
			}
		}

		active = remaining

		if !intersection && batch.mode == AllowSingleResult && hasUnconditionalGrant(out) {
			return out, nil
		}
	}

	return out, nil
}

func (r *RecursiveIterator) targetedBatchCheck(ctx *Context, batch checkBatch) ([]*Path, error) {
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
	return batch.child(ctx, r.templateTree)
}

// checkBatchChunks applies the same target ceiling to local reads and transport.
// Nested fanout can exceed an incoming dispatch batch, so enforce it at every
// shared multi-check entry point rather than relying on the original sender.
func checkBatchChunks(ctx *Context, it Iterator, batch checkBatch) ([]*Path, error) {
	limit := int(ctx.checkExecution.BatchSize)
	if limit == 0 {
		limit = 100
	}

	if batch.size() <= limit {
		return checkBatchImpl(ctx, it, batch)
	}

	out := make([]*Path, batch.size())

	for start := 0; start < batch.size(); start += limit {
		end := min(start+limit, batch.size())

		// Slice only vectors with multiple slots. A constant endpoint belongs to
		// every chunk and must remain available even after the first chunk.
		chunk := batch

		if len(batch.resources) > 1 {
			chunk.resources = batch.resources[start:end]
		}

		if len(batch.subjects) > 1 {
			chunk.subjects = batch.subjects[start:end]
		}

		paths, err := checkBatchImpl(ctx, it, chunk)
		if err != nil {
			return nil, err
		}

		copy(out[start:end], paths)

		if batch.mode == AllowSingleResult && hasUnconditionalGrant(paths) {
			break
		}
	}

	return out, nil
}
