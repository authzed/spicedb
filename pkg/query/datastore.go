package query

import (
	"fmt"
	"iter"
)

const NoSubRel = ""

type RelationIterator struct {
	relation string
	subrel   string
}

var _ Iterator = &RelationIterator{}

func NewRelationIterator(relation, subrel string) *RelationIterator {
	return &RelationIterator{
		relation: relation,
		subrel:   subrel,
	}
}

func (r *RelationIterator) Check(ctx Context, resource_ids []string, subject_id string) ([]Relation, error) {
	//reader := ctx.Datastore.SnapshotReader(ctx.Revision)
	panic("not implemented") // TODO: Implement
}

func (r *RelationIterator) LookupSubjects(ctx Context, resource_id string) (iter.Seq2[Relation, error], error) {
	panic("not implemented") // TODO: Implement
}

func (r *RelationIterator) LookupResources(ctx Context, subject_id string) (iter.Seq2[Relation, error], error) {
	panic("not implemented") // TODO: Implement
}

func (r *RelationIterator) Explain() Explain {
	return Explain{
		Info: fmt.Sprintf("Relation(%s, %s)", r.relation, r.subrel),
	}
}
