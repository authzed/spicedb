package query

import (
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/datastore/queryshape"
)

// subjectsForResources batches the datastore tupleset scan used by an arrow.
// Other iterator shapes retain their enumeration path, including dispatch and
// frontier ownership. Only a leaf's compatible resource IDs are combined here.
func subjectsForResources(ctx *Context, it Iterator, resources []Object) ([][]*Path, error) {
	out := make([][]*Path, len(resources))
	leaf, leafOK := it.(*DatastoreIterator)
	reader, readerOK := ctx.Reader.(*datalayerQueryDatastoreReader)

	if leafOK {
		if paths, handled, err := sharedTypedSubjects(ctx, leaf, resources); handled {
			return paths, err
		}
	}

	if leafOK && readerOK && !leaf.base.Wildcard() && ctx.PaginationLimit == nil {
		ids := []string{}
		indices := map[Object][]int{}

		for i, resource := range resources {
			if resource.ObjectType != leaf.base.DefinitionName() {
				continue
			}

			if len(indices[resource]) == 0 {
				ids = append(ids, resource.ObjectID)
			}

			indices[resource] = append(indices[resource], i)
		}

		if len(ids) == 0 {
			return out, nil
		}

		selector := datastore.SubjectsSelector{
			OptionalSubjectType: leaf.base.Type(),
			RelationFilter:      buildSubjectRelationFilter(leaf.base.Subrelation()),
		}
		// A filtered tupleset has subject columns without a subject ID. Use the
		// same flexible shape as QuerySubjects so CRDB does not force a PK span
		// that requires a complete prefix through those columns.
		shape := queryshape.Varying

		if eligible, _, _ := leaf.singleUsersetForm(); eligible && ctx.checkExecution.BroadSingleUsersetReads {
			selector = datastore.SubjectsSelector{RelationFilter: datastore.SubjectRelationFilter{}.WithOnlyNonEllipsisRelations()}
			shape = queryshape.CheckPermissionSelectIndirectSubjects
		}

		seq, err := reader.inner.QueryRelationships(ctx, datastore.RelationshipsFilter{
			OptionalResourceType:      leaf.base.DefinitionName(),
			OptionalResourceIds:       ids,
			OptionalResourceRelation:  leaf.base.RelationName(),
			OptionalSubjectsSelectors: []datastore.SubjectsSelector{selector},
		}, options.WithSkipCaveats(
			!leaf.relationHasCaveats(),
		), options.WithSkipExpiration(
			!leaf.relationHasExpiration(),
		), options.WithQueryShape(
			shape,
		))
		if err != nil {
			return nil, err
		}

		for rel, err := range seq {
			if err != nil {
				return nil, err
			}

			path := FromRelationship(rel)
			// Broad native scans can encounter stale schema rows. Retain this leaf's
			// type/relation semantics rather than expanding an unrelated userset.
			if path.Subject.ObjectType != leaf.base.Type() || path.Subject.Relation != leaf.base.Subrelation() {
				continue
			}

			for _, index := range indices[path.Resource] {
				out[index] = append(out[index], ownedPath(path))
			}
		}

		return out, nil
	}

	for i, resource := range resources {
		seq, err := ctx.IterSubjects(it, resource, NoObjectFilter())
		if err != nil {
			return nil, err
		}

		out[i], err = CollectAll(seq)
		if err != nil {
			return nil, err
		}
	}

	return out, nil
}

func (a *ArrowIterator) checkArrowResourceBatch(ctx *Context, batch checkBatch) ([]*Path, error) {
	// Wildcard tuplesets require inversion. Preserve the scalar arrow path for
	// those shapes rather than treating '*' as a concrete intermediate resource.
	left, err := subjectsForResources(ctx, a.left, batch.resources)
	if err != nil {
		return nil, err
	}

	out := make([]*Path, len(left))
	targets := []Object{}
	targetIndices := map[Object]int{}

	for _, paths := range left {
		for _, path := range paths {
			if path.Subject.ObjectID == WildcardObjectID {
				for i, resource := range batch.resources {
					out[i], err = a.CheckImpl(ctx, resource, batch.subjectAt(i))
					if err != nil {
						return nil, err
					}
				}

				return out, nil
			}

			target := GetObject(path.Subject)

			if _, ok := targetIndices[target]; !ok {
				targetIndices[target] = len(targets)
				targets = append(targets, target)
			}
		}
	}

	// One child batch serves all incoming edges. Deduplication saves work while
	// each original edge remains available for caveat/expiration provenance.
	right, err := ctx.CheckManyResources(a.right, targets, batch.subjects[0])
	if err != nil {
		return nil, err
	}

	for i, paths := range left {
		for _, path := range paths {
			matched := right[targetIndices[GetObject(path.Subject)]]
			if matched == nil {
				continue
			}

			combined := combineArrowPaths(path, ownedPath(matched))
			out[i], err = out[i].MergeOr(combined)
			if err != nil {
				return nil, err
			}

			if out[i].Caveat == nil {
				break
			}
		}
	}

	return out, nil
}
