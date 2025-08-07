package query

import (
	"context"
	"iter"

	"github.com/authzed/spicedb/pkg/datastore"
)

type Relation struct {
	ResourceID string
	SubjectID  string
	Relation   string
}

type Plan interface {
	Check(ctx *Context, resource_ids []string, subject_id string) ([]Relation, error)
	LookupSubjects(ctx *Context, resource_id string) (iter.Seq2[Relation, error], error)
	LookupResources(ctx *Context, subject_id string) (iter.Seq2[Relation, error], error)
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
