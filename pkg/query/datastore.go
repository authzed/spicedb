package query

import (
	"fmt"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/datastore/queryshape"
	"github.com/authzed/spicedb/pkg/schema/v2"
)

const NoSubRel = ""

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
	if r.base.Subrelation == "" {
		return datastore.SubjectRelationFilter{}.WithEllipsisRelation()
	}
	return datastore.SubjectRelationFilter{}.WithNonEllipsisRelation(r.base.Subrelation)
}

func (r *RelationIterator) Check(ctx *Context, resourceIDs []string, subjectID string) (RelationSeq, error) {
	filter := datastore.RelationshipsFilter{
		OptionalResourceType:     r.base.DefinitionName(),
		OptionalResourceIds:      resourceIDs,
		OptionalResourceRelation: r.base.RelationName(),
		OptionalSubjectsSelectors: []datastore.SubjectsSelector{
			{
				OptionalSubjectType: r.base.Type,
				OptionalSubjectIds:  []string{subjectID},
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

func (r *RelationIterator) LookupSubjects(ctx *Context, resourceID string) (RelationSeq, error) {
	filter := datastore.RelationshipsFilter{
		OptionalResourceType:     r.base.DefinitionName(),
		OptionalResourceIds:      []string{resourceID},
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

func (r *RelationIterator) LookupResources(ctx *Context, subjectID string) (RelationSeq, error) {
	return nil, ErrUnimplemented
}

func (r *RelationIterator) Clone() Iterator {
	return &RelationIterator{
		base: r.base,
	}
}

func (r *RelationIterator) Explain() Explain {
	return Explain{
		Info: fmt.Sprintf("Relation(%s:%s, \"%s\", caveat: %v, expiration: %v)", r.base.DefinitionName(), r.base.RelationName(), r.base.Subrelation, r.base.Caveat != "", r.base.Expiration),
	}
}
