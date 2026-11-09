package dispatch

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/query"
)

type localBatchRecorder struct {
	query.Executor
	alias        query.Iterator
	aliasScalars int
	vectors      []int
	modes        []query.CheckResultMode
}

// Check records scalar execution at the protected alias.
func (r *localBatchRecorder) Check(
	ctx *query.Context,
	it query.Iterator,
	resource query.Object,
	subject query.ObjectAndRelation,
) (*query.Path, error) {
	if it == r.alias {
		r.aliasScalars++
	}

	return r.Executor.Check(ctx, it, resource, subject)
}

// CheckManyResources records resource vectors reaching the protected alias.
func (r *localBatchRecorder) CheckManyResources(
	ctx *query.Context,
	it query.Iterator,
	resources []query.Object,
	subject query.ObjectAndRelation,
	modes ...query.CheckResultMode,
) ([]*query.Path, error) {
	if it == r.alias {
		r.vectors = append(r.vectors, len(resources))
		r.modes = append(r.modes, modes...)
	}

	return r.Executor.CheckManyResources(ctx, it, resources, subject, modes...)
}

// CheckManySubjects records subject vectors reaching the protected alias.
func (r *localBatchRecorder) CheckManySubjects(
	ctx *query.Context,
	it query.Iterator,
	resource query.Object,
	subjects []query.ObjectAndRelation,
	modes ...query.CheckResultMode,
) ([]*query.Path, error) {
	if it == r.alias {
		r.vectors = append(r.vectors, len(subjects))
		r.modes = append(r.modes, modes...)
	}

	return r.Executor.CheckManySubjects(ctx, it, resource, subjects, modes...)
}

func TestProtectedDispatchWrapperPreservesLocalBatches(t *testing.T) {
	for _, resourcesVary := range []bool{true, false} {
		for _, mode := range []query.CheckResultMode{query.AllCheckResults, query.AllowSingleResult} {
			t.Run(fmt.Sprintf("resources=%v/mode=%v", resourcesVary, mode), func(t *testing.T) {
				receiver := &testDispatcher{}
				plan := NewPlanContext("revision", "schema", nil, 50, 0)
				plan.InProgressKeys = []string{"document#view"}
				executor := NewDispatchExecutor(receiver, plan, 100)
				resource := query.NewObject("document", "d")
				subject := query.NewObject("user", "u").WithEllipses()
				alias := query.NewAliasIterator(
					"document",
					"view",
					query.NewFixedIterator(query.Path{Resource: resource, Subject: subject}),
				)
				wrapped := NewDispatchIterator(alias)
				record := &localBatchRecorder{Executor: executor, alias: alias}
				ctx := query.NewLocalContext(t.Context(), query.WithCheckExecution(query.CheckExecutionOptions{BatchChecks: true}))
				ctx.Executor = record

				// An in-progress named boundary must stay local while preserving the
				// original target dimension, mode, duplicate slots and result ownership.
				var paths []*query.Path
				var err error

				if resourcesVary {
					paths, err = ctx.CheckManyResources(
						wrapped,
						[]query.Object{query.NewObject("document", "missing"), resource, resource},
						subject,
						mode,
					)
				} else {
					paths, err = ctx.CheckManySubjects(
						wrapped,
						resource,
						[]query.ObjectAndRelation{query.NewObject("user", "missing").WithEllipses(), subject, subject},
						mode,
					)
				}

				require.NoError(t, err)
				require.Len(t, paths, 3)
				require.Nil(t, paths[0])
				require.NotNil(t, paths[1])
				require.Equal(t, "view", paths[1].Relation)
				require.Empty(t, receiver.planCalls)
				require.Zero(t, record.aliasScalars)
				require.Equal(t, []int{3}, record.vectors)
				require.Equal(t, []query.CheckResultMode{mode}, record.modes)

				if mode == query.AllCheckResults {
					require.NotNil(t, paths[2])
					require.NotSame(t, paths[1], paths[2])
				}
			})
		}
	}
}
