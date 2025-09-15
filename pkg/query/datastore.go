package query

import (
	"fmt"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/datastore/queryshape"
	"github.com/authzed/spicedb/pkg/schema/v2"
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/tuple"
)

// RelationIterator is a common leaf iterator. It represents the set of all
// relationships of the given schema.BaseRelation, ie, relations that have a
// known resource and subject type and may contain caveats or expiration.
//
// The RelationIterator, being the leaf, generates this set by calling the datastore.
type RelationIterator struct {
	base *schema.BaseRelation
}

var _ Iterator = &RelationIterator{}

func NewRelationIterator(base *schema.BaseRelation) *RelationIterator {
	return &RelationIterator{
		base: base,
	}
}

func (r *RelationIterator) buildSubjectRelationFilter() datastore.SubjectRelationFilter {
	if r.base.Subrelation == tuple.Ellipsis {
		return datastore.SubjectRelationFilter{}.WithEllipsisRelation()
	}
	return datastore.SubjectRelationFilter{}.WithNonEllipsisRelation(r.base.Subrelation)
}

func (r *RelationIterator) CheckImpl(ctx *Context, resources []Object, subject ObjectAndRelation) (RelationSeq, error) {
	// If the subject type doesn't match the base relation type, return no results
	if subject.ObjectType != r.base.Type {
		return func(yield func(Relation, error) bool) {
			// Empty sequence
		}, nil
	}

	if r.base.Wildcard {
		return r.checkWildcardImpl(ctx, resources, subject)
	}
	return r.checkNormalImpl(ctx, resources, subject)
}

func (r *RelationIterator) checkNormalImpl(ctx *Context, resources []Object, subject ObjectAndRelation) (RelationSeq, error) {
	resourceIDs := make([]string, len(resources))
	for i, res := range resources {
		resourceIDs[i] = res.ObjectID
	}

	filter := datastore.RelationshipsFilter{
		OptionalResourceType:     r.base.DefinitionName(),
		OptionalResourceIds:      resourceIDs,
		OptionalResourceRelation: r.base.RelationName(),
		OptionalSubjectsSelectors: []datastore.SubjectsSelector{
			{
				OptionalSubjectType: r.base.Type,
				OptionalSubjectIds:  []string{subject.ObjectID},
				RelationFilter:      r.buildSubjectRelationFilter(),
			},
		},
	}

	reader := ctx.Datastore.SnapshotReader(ctx.Revision)

	relIter, err := reader.QueryRelationships(ctx, filter,
		options.WithSkipCaveats(r.base.Caveat == ""),
		options.WithSkipExpiration(!r.base.Expiration),
		options.WithQueryShape(queryshape.CheckPermissionSelectDirectSubjects),
	)
	if err != nil {
		return nil, err
	}

	return RelationSeq(relIter), nil
}

func (r *RelationIterator) checkWildcardImpl(ctx *Context, resources []Object, subject ObjectAndRelation) (RelationSeq, error) {
	// Query the datastore for wildcard relationships (subject ObjectID = "*")
	resourceIDs := make([]string, len(resources))
	for i, res := range resources {
		resourceIDs[i] = res.ObjectID
	}

	filter := datastore.RelationshipsFilter{
		OptionalResourceType:     r.base.DefinitionName(),
		OptionalResourceIds:      resourceIDs,
		OptionalResourceRelation: r.base.RelationName(),
		OptionalSubjectsSelectors: []datastore.SubjectsSelector{
			{
				OptionalSubjectType: r.base.Type,
				OptionalSubjectIds:  []string{tuple.PublicWildcard}, // Look for "*" subjects
				RelationFilter:      r.buildSubjectRelationFilter(),
			},
		},
	}

	reader := ctx.Datastore.SnapshotReader(ctx.Revision)

	relIter, err := reader.QueryRelationships(ctx, filter,
		options.WithSkipCaveats(r.base.Caveat == ""),
		options.WithSkipExpiration(!r.base.Expiration),
		options.WithQueryShape(queryshape.CheckPermissionSelectDirectSubjects),
	)
	if err != nil {
		return nil, err
	}

	// Transform the wildcard relationships to use the concrete subject
	return func(yield func(Relation, error) bool) {
		for rel, err := range relIter {
			if err != nil {
				if !yield(rel, err) {
					return
				}
				continue
			}

			// Replace the wildcard subject with the concrete subject
			concreteRel := rel
			concreteRel.Subject = subject

			if !yield(concreteRel, nil) {
				return
			}
		}
	}, nil
}

func (r *RelationIterator) IterSubjectsImpl(ctx *Context, resource Object) (RelationSeq, error) {
	if r.base.Wildcard {
		return r.iterSubjectsWildcardImpl(ctx, resource)
	}
	return r.iterSubjectsNormalImpl(ctx, resource)
}

func (r *RelationIterator) iterSubjectsNormalImpl(ctx *Context, resource Object) (RelationSeq, error) {
	filter := datastore.RelationshipsFilter{
		OptionalResourceType:     r.base.DefinitionName(),
		OptionalResourceIds:      []string{resource.ObjectID},
		OptionalResourceRelation: r.base.RelationName(),
		OptionalSubjectsSelectors: []datastore.SubjectsSelector{
			{
				OptionalSubjectType: r.base.Type,
				RelationFilter:      r.buildSubjectRelationFilter(),
			},
		},
	}

	reader := ctx.Datastore.SnapshotReader(ctx.Revision)

	relIter, err := reader.QueryRelationships(ctx, filter,
		options.WithSkipCaveats(r.base.Caveat == ""),
		options.WithSkipExpiration(!r.base.Expiration),
		options.WithQueryShape(queryshape.AllSubjectsForResources),
	)
	if err != nil {
		return nil, err
	}

	return RelationSeq(relIter), nil
}

func (r *RelationIterator) iterSubjectsWildcardImpl(ctx *Context, resource Object) (RelationSeq, error) {
	filter := datastore.RelationshipsFilter{
		OptionalResourceType:     r.base.DefinitionName(),
		OptionalResourceIds:      []string{resource.ObjectID},
		OptionalResourceRelation: r.base.RelationName(),
		OptionalSubjectsSelectors: []datastore.SubjectsSelector{
			{
				OptionalSubjectType: r.base.Type,
				OptionalSubjectIds:  []string{tuple.PublicWildcard}, // Look for "*" subjects
				RelationFilter:      r.buildSubjectRelationFilter(),
			},
		},
	}

	reader := ctx.Datastore.SnapshotReader(ctx.Revision)

	relIter, err := reader.QueryRelationships(ctx, filter,
		options.WithSkipCaveats(r.base.Caveat == ""),
		options.WithSkipExpiration(!r.base.Expiration),
		options.WithQueryShape(queryshape.AllSubjectsForResources),
	)
	if err != nil {
		return nil, err
	}

	return RelationSeq(relIter), nil
}

func (r *RelationIterator) IterResourcesImpl(ctx *Context, subject ObjectAndRelation) (RelationSeq, error) {
	return nil, spiceerrors.MustBugf("unimplemented")
}

func (r *RelationIterator) Clone() Iterator {
	return &RelationIterator{
		base: r.base,
	}
}

func (r *RelationIterator) Explain() Explain {
	relationName := r.base.Subrelation
	if r.base.Wildcard {
		relationName = "*"
	}
	return Explain{
		Info: fmt.Sprintf("Relation(%s:%s -> %s:%s, caveat: %v, expiration: %v)",
			r.base.DefinitionName(), r.base.RelationName(), r.base.Type, relationName,
			r.base.Caveat != "", r.base.Expiration),
	}
}
