package query

import (
	"context"
	"iter"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/tuple"
)

type (
	Relation    = tuple.Relationship
	RelationSeq iter.Seq2[Relation, error]
)

type Plan interface {
	Check(ctx *Context, resourceIds []string, subjectId string) (RelationSeq, error)
	LookupSubjects(ctx *Context, resourceId string) (RelationSeq, error)
	LookupResources(ctx *Context, subjectId string) (RelationSeq, error)
	Explain() Explain
}

type Iterator interface {
	Plan
}

type Explain struct {
	Info       string
	SubExplain []Explain
}

type Context struct {
	context.Context
	Datastore datastore.ReadOnlyDatastore
	Revision  datastore.Revision
}

func CollectAll(seq RelationSeq) ([]Relation, error) {
	var out []Relation
	for x, err := range seq {
		if err != nil {
			return nil, err
		}
		out = append(out, x)
	}
	return out, nil
}
