package query

import (
	"fmt"
	"iter"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/datastore/queryshape"
	"github.com/authzed/spicedb/pkg/schema/v2"
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/tuple"
)

// convertRelationSeqToPathSeq converts an iter.Seq2[tuple.Relationship, error] from the datastore
// into a PathSeq by transforming each tuple.Relationship into a Path using FromRelationship.
func convertRelationSeqToPathSeq(relSeq iter.Seq2[tuple.Relationship, error]) PathSeq {
	return func(yield func(Path, error) bool) {
		for rel, err := range relSeq {
			if err != nil {
				if !yield(Path{}, err) {
					return
				}
				continue
			}

			path := FromRelationship(rel)
			if !yield(path, nil) {
				return
			}
		}
	}
}

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
	if r.base.Subrelation() == tuple.Ellipsis {
		return datastore.SubjectRelationFilter{}.WithEllipsisRelation()
	}
	return datastore.SubjectRelationFilter{}.WithNonEllipsisRelation(r.base.Subrelation())
}

func (r *RelationIterator) CheckImpl(ctx *Context, resources []Object, subject ObjectAndRelation) (PathSeq, error) {
	// For subrelations, we need to allow type mismatches because the subrelation might bridge different types
	// For example, group:member -> group:member should find group:everyone#member@group:engineering#member
	// and then that relationship should be used by the Arrow to check group:engineering#member for user subjects
	// However, wildcard relations and ellipsis relations should always enforce strict type checking
	// Ellipsis (...) means "any relation on the same type", not "bridging to a different type"
	if subject.ObjectType != r.base.Type() && r.base.Subrelation() != "" && r.base.Subrelation() != tuple.Ellipsis && !r.base.Wildcard() {
		// For non-wildcard, non-ellipsis subrelations, we proceed with the query even if types don't match
		// This allows finding intermediate relationships that bridge type gaps
		ctx.TraceStep(r, "subject type %s doesn't match base type %s, but proceeding due to subrelation %s",
			subject.ObjectType, r.base.Type(), r.base.Subrelation())
	} else if subject.ObjectType != r.base.Type() {
		// For non-subrelations, ellipsis, and all wildcard relations, strict type checking applies
		ctx.TraceStep(r, "subject type %s doesn't match base type %s, returning empty", subject.ObjectType, r.base.Type())
		return EmptyPathSeq(), nil
	}

	if r.base.Wildcard() {
		return r.checkWildcardImpl(ctx, resources, subject)
	}
	return r.checkNormalImpl(ctx, resources, subject)
}

func (r *RelationIterator) checkNormalImpl(ctx *Context, resources []Object, subject ObjectAndRelation) (PathSeq, error) {
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
				OptionalSubjectType: r.base.Type(),
				OptionalSubjectIds:  []string{subject.ObjectID},
				RelationFilter:      r.buildSubjectRelationFilter(),
			},
		},
	}

	ctx.TraceStep(r, "querying datastore for %s:%s with resources=%v", r.base.Type(), r.base.RelationName(), resourceIDs)

	relIter, err := ctx.Reader.QueryRelationships(ctx, filter,
		options.WithSkipCaveats(r.base.Caveat() == ""),
		options.WithSkipExpiration(!r.base.Expiration()),
		options.WithQueryShape(queryshape.CheckPermissionSelectDirectSubjects),
	)
	if err != nil {
		return nil, err
	}

	return convertRelationSeqToPathSeq(iter.Seq2[tuple.Relationship, error](relIter)), nil
}

func (r *RelationIterator) checkWildcardImpl(ctx *Context, resources []Object, subject ObjectAndRelation) (PathSeq, error) {
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
				OptionalSubjectType: r.base.Type(),
				OptionalSubjectIds:  []string{tuple.PublicWildcard}, // Look for "*" subjects
				RelationFilter:      r.buildSubjectRelationFilter(),
			},
		},
	}

	relIter, err := ctx.Reader.QueryRelationships(ctx, filter,
		options.WithSkipCaveats(r.base.Caveat() == ""),
		options.WithSkipExpiration(!r.base.Expiration()),
		options.WithQueryShape(queryshape.CheckPermissionSelectDirectSubjects),
	)
	if err != nil {
		return nil, err
	}

	// Transform the wildcard relationships to use the concrete subject
	return func(yield func(Path, error) bool) {
		for rel, err := range relIter {
			if err != nil {
				if !yield(Path{}, err) {
					return
				}
				continue
			}

			// Replace the wildcard subject with the concrete subject
			concreteRel := rel
			concreteRel.Subject = subject

			// Convert to Path
			path := FromRelationship(concreteRel)
			if !yield(path, nil) {
				return
			}
		}
	}, nil
}

func (r *RelationIterator) IterSubjectsImpl(ctx *Context, resource Object) (PathSeq, error) {
	if r.base.Wildcard() {
		return r.iterSubjectsWildcardImpl(ctx, resource)
	}
	return r.iterSubjectsNormalImpl(ctx, resource)
}

func (r *RelationIterator) iterSubjectsNormalImpl(ctx *Context, resource Object) (PathSeq, error) {
	filter := datastore.RelationshipsFilter{
		OptionalResourceType:     r.base.DefinitionName(),
		OptionalResourceIds:      []string{resource.ObjectID},
		OptionalResourceRelation: r.base.RelationName(),
		OptionalSubjectsSelectors: []datastore.SubjectsSelector{
			{
				OptionalSubjectType: r.base.Type(),
				RelationFilter:      r.buildSubjectRelationFilter(),
			},
		},
	}

	relIter, err := ctx.Reader.QueryRelationships(ctx, filter,
		options.WithSkipCaveats(r.base.Caveat() == ""),
		options.WithSkipExpiration(!r.base.Expiration()),
		options.WithQueryShape(queryshape.AllSubjectsForResources),
	)
	if err != nil {
		return nil, err
	}

	return convertRelationSeqToPathSeq(iter.Seq2[tuple.Relationship, error](relIter)), nil
}

func (r *RelationIterator) iterSubjectsWildcardImpl(ctx *Context, resource Object) (PathSeq, error) {
	filter := datastore.RelationshipsFilter{
		OptionalResourceType:     r.base.DefinitionName(),
		OptionalResourceIds:      []string{resource.ObjectID},
		OptionalResourceRelation: r.base.RelationName(),
		OptionalSubjectsSelectors: []datastore.SubjectsSelector{
			{
				OptionalSubjectType: r.base.Type(),
				OptionalSubjectIds:  []string{tuple.PublicWildcard}, // Look for "*" subjects
				RelationFilter:      r.buildSubjectRelationFilter(),
			},
		},
	}

	relIter, err := ctx.Reader.QueryRelationships(ctx, filter,
		options.WithSkipCaveats(r.base.Caveat() == ""),
		options.WithSkipExpiration(!r.base.Expiration()),
		options.WithQueryShape(queryshape.AllSubjectsForResources),
	)
	if err != nil {
		return nil, err
	}

	return convertRelationSeqToPathSeq(iter.Seq2[tuple.Relationship, error](relIter)), nil
}

func (r *RelationIterator) IterResourcesImpl(ctx *Context, subject ObjectAndRelation) (PathSeq, error) {
	return nil, spiceerrors.MustBugf("unimplemented")
}

func (r *RelationIterator) Clone() Iterator {
	return &RelationIterator{
		base: r.base,
	}
}

func (r *RelationIterator) Explain() Explain {
	relationName := r.base.Subrelation()
	if r.base.Wildcard() {
		relationName = "*"
	}
	return Explain{
		Info: fmt.Sprintf("Relation(%s:%s -> %s:%s, caveat: %v, expiration: %v)",
			r.base.DefinitionName(), r.base.RelationName(), r.base.Type(), relationName,
			r.base.Caveat() != "", r.base.Expiration()),
	}
}

func (r *RelationIterator) Subiterators() []Iterator {
	return nil
}

func (r *RelationIterator) ReplaceSubiterators(newSubs []Iterator) (Iterator, error) {
	return nil, spiceerrors.MustBugf("Trying to replace a leaf RelationIterator's subiterators")
}
