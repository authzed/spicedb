package query

import (
	"fmt"
	"iter"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/datastore/queryshape"
	"github.com/authzed/spicedb/pkg/schema/v2"
)

const NoSubRel = ""

type RelationIterator struct {
	relationName string
	base         *schema.BaseRelation
}

var _ Iterator = &RelationIterator{}

func NewRelationIterator(base *schema.BaseRelation) *RelationIterator {
	return &RelationIterator{
		relationName: base.Parent.Name,
		base:         base,
	}
}

func (r *RelationIterator) Check(ctx *Context, resourceIds []string, subjectId string) ([]Relation, error) {
	var relFilter datastore.SubjectRelationFilter
	if r.base.Subrelation == "" {
		relFilter = datastore.SubjectRelationFilter{}.WithEllipsisRelation()
	} else {
		relFilter = datastore.SubjectRelationFilter{}.WithNonEllipsisRelation(r.base.Subrelation)
	}
	reader := ctx.Datastore.SnapshotReader(ctx.Revision)

	filter := datastore.RelationshipsFilter{
		OptionalResourceType:     r.base.Parent.Parent.Name,
		OptionalResourceIds:      resourceIds,
		OptionalResourceRelation: r.relationName,
		OptionalSubjectsSelectors: []datastore.SubjectsSelector{
			{
				OptionalSubjectType: r.base.Type,
				OptionalSubjectIds:  []string{subjectId},
				RelationFilter:      relFilter,
			},
		},
	}

	// TODO: Handle caveats and such correctly
	relIter, err := reader.QueryRelationships(ctx, filter,
		options.WithSkipCaveats(r.base.Caveat == ""),
		options.WithSkipExpiration(!r.base.Expiration),
		options.WithQueryShape(queryshape.CheckPermissionSelectDirectSubjects),
	)
	if err != nil {
		return nil, err
	}
	var out []Relation
	for x, err := range relIter {
		if err != nil {
			return nil, err
		}
		out = append(out, x)
	}
	return out, nil
}

func (r *RelationIterator) LookupSubjects(ctx *Context, resourceId string) (iter.Seq2[Relation, error], error) {
	panic("not implemented") // TODO: Implement
}

func (r *RelationIterator) LookupResources(ctx *Context, subjectId string) (iter.Seq2[Relation, error], error) {
	panic("not implemented") // TODO: Implement
}

func (r *RelationIterator) Explain() Explain {
	return Explain{
		Info: fmt.Sprintf("Relation(%s:%s, \"%s\")", r.base.Parent.Parent.Name, r.relationName, r.base.Subrelation),
	}
}
