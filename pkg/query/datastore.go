package query

import (
	"fmt"
	"iter"

	"github.com/google/uuid"

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
	id   string
	base *schema.BaseRelation
}

var _ Iterator = &RelationIterator{}

func NewRelationIterator(base *schema.BaseRelation) *RelationIterator {
	return &RelationIterator{
		id:   uuid.NewString(),
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
	// We rewrite the subject to the concrete subject before returning the paths
	return RewriteSubject(convertRelationSeqToPathSeq(iter.Seq2[tuple.Relationship, error](relIter)), subject), nil
}

func (r *RelationIterator) IterSubjectsImpl(ctx *Context, resource Object, filterSubjectType ObjectType) (PathSeq, error) {
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

	// Filter out wildcard subjects to match the behavior of LookupSubjects. Wildcards are not
	// concrete enumerable subjects. They will be expanded to concrete subjects by the wildcard branch.
	return FilterWildcardSubjects(convertRelationSeqToPathSeq(iter.Seq2[tuple.Relationship, error](relIter))), nil
}

func (r *RelationIterator) iterSubjectsWildcardImpl(ctx *Context, resource Object) (PathSeq, error) {
	// When a relation contains a wildcard (e.g., user:*), it means "all subjects of that type"
	// that have ANY relationship with this resource. We enumerate concrete subjects by:
	// 1. First checking if a wildcard relationship actually exists for this resource
	// 2. If yes, querying for all concrete subjects with relationships to this resource
	//
	// This avoids doing a full subject enumeration when no wildcard exists (the common case).
	// When wildcards do exist, we do 2 queries in this branch, but that's the correct semantic
	// behavior - we only enumerate when there's actually a wildcard to expand.

	// First, check if there's actually a wildcard relationship for this resource
	wildcardFilter := datastore.RelationshipsFilter{
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

	wildcardIter, err := ctx.Reader.QueryRelationships(ctx, wildcardFilter,
		options.WithSkipCaveats(r.base.Caveat() == ""),
		options.WithSkipExpiration(!r.base.Expiration()),
		options.WithQueryShape(queryshape.AllSubjectsForResources),
		options.WithLimit(options.LimitOne), // We only need to know if one exists
	)
	if err != nil {
		return nil, err
	}

	// Check if any wildcard relationship exists
	hasWildcard := false
	for _, err := range wildcardIter {
		if err != nil {
			return nil, err
		}
		hasWildcard = true
		break
	}

	// If no wildcard relationship exists, return empty - nothing to enumerate
	if !hasWildcard {
		return EmptyPathSeq(), nil
	}

	// Wildcard exists, so enumerate all concrete subjects of the appropriate type.
	// A wildcard (e.g., user:*) means "all subjects of that type", so we need to enumerate
	// all defined subjects of that type in the datastore. This may return some of the same
	// subjects as the non-wildcard branch (when both wildcard and concrete relationships exist),
	// but the Union will deduplicate them.
	//
	// Note: We query for all subjects of the appropriate type, not just those with a relationship
	// to this specific resource. This matches the semantics of wildcards, which grant access to
	// ALL subjects of the type, regardless of whether they have other relationships.
	allSubjectsFilter := datastore.RelationshipsFilter{
		// Note: We intentionally omit OptionalResourceType and OptionalResourceIds to find
		// all subjects of the appropriate type across all resources
		OptionalSubjectsSelectors: []datastore.SubjectsSelector{
			{
				OptionalSubjectType: r.base.Type(),
				RelationFilter:      r.buildSubjectRelationFilter(),
			},
		},
	}

	relIter, err := ctx.Reader.QueryRelationships(ctx, allSubjectsFilter,
		options.WithSkipCaveats(r.base.Caveat() == ""),
		options.WithSkipExpiration(!r.base.Expiration()),
		options.WithQueryShape(queryshape.AllSubjectsForResources),
	)
	if err != nil {
		return nil, err
	}

	// Filter out wildcard subjects from the results - we only want concrete subjects
	return FilterWildcardSubjects(convertRelationSeqToPathSeq(iter.Seq2[tuple.Relationship, error](relIter))), nil
}

func (r *RelationIterator) IterResourcesImpl(ctx *Context, subject ObjectAndRelation, filterResourceType ObjectType) (PathSeq, error) {
	// If the types don't match, we don't even have to go to the datastore.
	if subject.ObjectType != r.base.Type() {
		return EmptyPathSeq(), nil
	}

	// Handle wildcards first - they don't have subrelations and match any query relation
	if r.base.Wildcard() {
		return r.iterResourcesWildcardImpl(ctx, subject)
	}

	// Check if subject relation matches what this iterator expects.
	// Both the schema's expected subrelation and the query's subject relation must match exactly.
	// Ellipsis is a specific relation value, not a wildcard.
	if r.base.Subrelation() != subject.Relation {
		return EmptyPathSeq(), nil
	}

	filter := datastore.RelationshipsFilter{
		OptionalResourceType:     r.base.DefinitionName(),
		OptionalResourceRelation: r.base.RelationName(),
		OptionalSubjectsSelectors: []datastore.SubjectsSelector{
			{
				OptionalSubjectType: subject.ObjectType,
				OptionalSubjectIds:  []string{subject.ObjectID},
				RelationFilter:      r.buildSubjectRelationFilter(),
			},
		},
	}

	relIter, err := ctx.Reader.QueryRelationships(ctx, filter,
		options.WithSkipCaveats(r.base.Caveat() == ""),
		options.WithSkipExpiration(!r.base.Expiration()),
		options.WithQueryShape(queryshape.MatchingResourcesForSubject),
	)
	if err != nil {
		return nil, err
	}

	return convertRelationSeqToPathSeq(iter.Seq2[tuple.Relationship, error](relIter)), nil
}

func (r *RelationIterator) iterResourcesWildcardImpl(ctx *Context, subject ObjectAndRelation) (PathSeq, error) {
	filter := datastore.RelationshipsFilter{
		OptionalResourceType:     r.base.DefinitionName(),
		OptionalResourceRelation: r.base.RelationName(),
		OptionalSubjectsSelectors: []datastore.SubjectsSelector{
			{
				OptionalSubjectType: subject.ObjectType,
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

	// We rewrite the subject to the concrete subject before returning the paths
	return RewriteSubject(convertRelationSeqToPathSeq(iter.Seq2[tuple.Relationship, error](relIter)), subject), nil
}

func (r *RelationIterator) Clone() Iterator {
	return &RelationIterator{
		id:   uuid.NewString(),
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

func (r *RelationIterator) ID() string {
	return r.id
}

func (r *RelationIterator) ResourceType() ([]ObjectType, error) {
	return []ObjectType{{
		Type:        r.base.DefinitionName(),
		Subrelation: tuple.Ellipsis,
	}}, nil
}

func (r *RelationIterator) SubjectTypes() ([]ObjectType, error) {
	// For wildcards, return the base type with no subrelation
	if r.base.Wildcard() {
		return []ObjectType{{
			Type:        r.base.Type(),
			Subrelation: "",
		}}, nil
	}

	// For ellipsis, return the base type with empty subrelation
	// Ellipsis means "any relation on this type"
	if r.base.Subrelation() == tuple.Ellipsis {
		return []ObjectType{{
			Type:        r.base.Type(),
			Subrelation: "",
		}}, nil
	}

	// For regular subrelations, return the specific type and subrelation
	return []ObjectType{{
		Type:        r.base.Type(),
		Subrelation: r.base.Subrelation(),
	}}, nil
}
