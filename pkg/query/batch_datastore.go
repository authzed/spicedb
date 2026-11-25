package query

import (
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/datastore/queryshape"
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/tuple"
)

func (r *DatastoreIterator) checkDatastoreBatch(ctx *Context, reader *datalayerQueryDatastoreReader, batch checkBatch) ([]*Path, error) {
	out := make([]*Path, batch.size())
	resourceIDs := []string{}
	selectors := []datastore.SubjectsSelector{}
	seenResources := map[string]bool{}
	type selectorKey struct{ namespace, relation string }
	selectorIndices := map[selectorKey]int{}
	seenSubjects := map[ObjectAndRelation]bool{}

	// Group only compatible reads. A leaf already fixes resource type/relation;
	// selectors keep subject types and relations separate. Duplicate targets stay
	// in the result vector but do not duplicate datastore filter entries.
	for i := range batch.size() {
		resource := batch.resourceAt(i)
		subject := batch.subjectAt(i)

		if resource.ObjectType != r.base.DefinitionName() {
			return scalarCheckBatch(ctx, r, batch)
		}

		if ctx.checkExecution.StrictSubjectMatching && !r.base.Wildcard() &&
			(subject.ObjectType != r.base.Type() || subject.Relation != r.base.Subrelation()) {
			continue
		}

		if subject.ObjectType != r.base.Type() &&
			(r.base.Subrelation() == "" || r.base.Subrelation() == tuple.Ellipsis || r.base.Wildcard()) {
			continue
		}

		if ctx.checkExecution.CoalesceDirectWildcards && r.base.Wildcard() && r.hasPairedDirectForm(false) {
			continue
		}

		if r.base.Wildcard() && subject.Relation != tuple.Ellipsis {
			return nil, spiceerrors.MustBugf("wildcard batch check requires an ellipsis subject relation, got %q", subject.Relation)
		}

		if !seenResources[resource.ObjectID] {
			resourceIDs = append(resourceIDs, resource.ObjectID)
			seenResources[resource.ObjectID] = true
		}

		if seenSubjects[subject] {
			continue
		}

		seenSubjects[subject] = true
		key := selectorKey{subject.ObjectType, subject.Relation}
		index, ok := selectorIndices[key]
		if !ok {
			index = len(selectors)
			selectorIndices[key] = index
			selectors = append(
				selectors,
				datastore.SubjectsSelector{
					OptionalSubjectType: subject.ObjectType,
					RelationFilter:      buildSubjectRelationFilter(subject.Relation),
				},
			)
		}

		id := subject.ObjectID

		if r.base.Wildcard() {
			id = tuple.PublicWildcard
		}

		selectors[index].OptionalSubjectIds = append(selectors[index].OptionalSubjectIds, id)
	}

	if len(resourceIDs) == 0 {
		return out, nil
	}

	if ctx.checkExecution.CoalesceDirectWildcards && !r.base.Wildcard() && r.hasPairedDirectForm(true) {
		for i := range selectors {
			if selectors[i].RelationFilter.IncludeEllipsisRelation {
				selectors[i].OptionalSubjectIds = append(selectors[i].OptionalSubjectIds, tuple.PublicWildcard)
			}
		}
	}

	seq, err := reader.inner.QueryRelationships(ctx, datastore.RelationshipsFilter{
		OptionalResourceType:      r.base.DefinitionName(),
		OptionalResourceIds:       resourceIDs,
		OptionalResourceRelation:  r.base.RelationName(),
		OptionalSubjectsSelectors: selectors,
	}, options.WithSkipCaveats(
		!r.relationHasCaveats(),
	), options.WithSkipExpiration(
		!r.relationHasExpiration(),
	), options.WithQueryShape(
		queryshape.CheckPermissionSelectDirectSubjects,
	))
	if err != nil {
		return nil, err
	}

	// Map rows back to exact requested endpoints, never a selector cross product.
	// Every slot owns its mutable Path, including duplicate requests. Wildcards
	// project onto the requested concrete subject just as scalar Check does.
	for rel, err := range seq {
		if err != nil {
			return nil, err
		}

		path := FromRelationship(rel)

		for i := range batch.size() {
			resource := batch.resourceAt(i)
			subject := batch.subjectAt(i)
			if path.Resource != resource || path.Subject.ObjectType != subject.ObjectType || path.Subject.Relation != subject.Relation {
				continue
			}

			if path.Subject.ObjectID != subject.ObjectID && path.Subject.ObjectID != tuple.PublicWildcard {
				continue
			}

			if out[i] != nil && out[i].Caveat == nil {
				continue
			}

			projected := ownedPath(path)
			projected.Subject = subject
			out[i], err = out[i].MergeOr(projected)
			if err != nil {
				return nil, err
			}
		}
	}

	return out, nil
}
