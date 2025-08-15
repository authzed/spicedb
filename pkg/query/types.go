package query

import (
	"context"
	"fmt"
	"iter"
	"strings"

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
	Clone() Iterator
}

type Explain struct {
	Info       string
	SubExplain []Explain
}

func (e Explain) String() string {
	return e.IndentString(0)
}

func (e Explain) IndentString(depth int) string {
	var sb strings.Builder
	for _, sub := range e.SubExplain {
		sb.WriteString(sub.IndentString(depth + 1))
	}
	return fmt.Sprintf("%s%s\n%s", strings.Repeat("\t", depth), e.Info, sb.String())
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
