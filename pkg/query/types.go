package query

import (
	"context"
	"errors"
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

var ErrUnimplemented = errors.New("feature not yet implemented")

type Plan interface {
	Check(ctx *Context, resourceIDs []string, subjectID string) (RelationSeq, error)
	LookupSubjects(ctx *Context, resourceID string) (RelationSeq, error)
	LookupResources(ctx *Context, subjectID string) (RelationSeq, error)
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
	out := make([]Relation, 0) // `prealloc` is overly aggressive. This should be `var out []Relation`
	for x, err := range seq {
		if err != nil {
			return nil, err
		}
		out = append(out, x)
	}
	return out, nil
}
