package query

import (
	"context"
	"iter"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/tuple"
)

type (
	Relation = tuple.Relationship
)

type Plan interface {
	Check(ctx *Context, resourceIds []string, subjectId string) ([]Relation, error)
	LookupSubjects(ctx *Context, resourceId string) (iter.Seq2[Relation, error], error)
	LookupResources(ctx *Context, subjectId string) (iter.Seq2[Relation, error], error)
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
