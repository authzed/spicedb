package dispatch

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/query"
)

// serializationCountingIterator counts actual subtree serialization while
// preserving the embedded iterator's wire format and execution behavior.
type serializationCountingIterator struct {
	query.Iterator
	calls int
	err   error
}

// Serialize counts attempts and delegates the wire encoding to the real iterator.
func (i *serializationCountingIterator) Serialize(w io.Writer) error {
	i.calls++

	if i.err != nil {
		return i.err
	}

	return i.Iterator.Serialize(w)
}

// TestCheckManySerializesSubtreeOnce checks request-local serialization reuse and chunk ownership.
func TestCheckManySerializesSubtreeOnce(t *testing.T) {
	for _, resourcesVary := range []bool{false, true} {
		name := "subjects"

		if resourcesVary {
			name = "resources"
		}

		t.Run(name, func(t *testing.T) {
			body := &serializationCountingIterator{Iterator: query.NewFixedIterator()}
			alias := query.NewAliasIterator("document", "viewer", body)
			receiver := &chunkingDispatcher{}
			sender := NewDispatchExecutor(receiver, &v1.PlanContext{Revision: "revision"}, 2)
			ctx := newTestContext()
			resources := []query.Object{
				query.NewObject("document", "first"),
				query.NewObject("document", "second"),
				query.NewObject("document", "first"),
			}
			subjects := []query.ObjectAndRelation{
				query.NewObject("user", "first").WithEllipses(),
				query.NewObject("user", "second").WithEllipses(),
				query.NewObject("user", "first").WithEllipses(),
			}

			var out []*query.Path
			var err error

			if resourcesVary {
				out, err = sender.CheckManyResources(ctx, NewDispatchIterator(alias), resources, subjects[0])
			} else {
				out, err = sender.CheckManySubjects(ctx, NewDispatchIterator(alias), resources[0], subjects)
			}

			require.NoError(t, err)
			require.Len(t, out, 3)
			require.Equal(t, 1, body.calls, "chunking must not repeat subtree serialization")
			require.Len(t, receiver.planCalls, 2)

			for _, request := range receiver.planCalls {
				_, err := query.Deserialize(bytes.NewReader(request.Plan), &query.DeserializeContext{})
				require.NoError(t, err)
			}

			// Mutable request context and target slots remain chunk-owned even
			// though the serialized plan can be shared as immutable bytes.
			first, second := receiver.planCalls[0], receiver.planCalls[1]
			require.Equal(t, first.Plan, second.Plan)
			require.Equal(t, []*core.ObjectAndRelation{first.Many[0]}, second.Many)
			first.PlanContext.InProgressKeys[0] = "changed"
			first.Many[0].ObjectId = "changed"
			require.Equal(t, []string{"document#viewer"}, second.PlanContext.InProgressKeys)
			require.Equal(t, "first", second.Many[0].ObjectId)

			// Sharing is limited to this multi-check, so a subsequent request
			// prepares its current subtree rather than retaining an old wire plan.
			if resourcesVary {
				_, err = sender.CheckManyResources(ctx, NewDispatchIterator(alias), resources, subjects[0])
			} else {
				_, err = sender.CheckManySubjects(ctx, NewDispatchIterator(alias), resources[0], subjects)
			}

			require.NoError(t, err)
			require.Equal(t, 2, body.calls)
		})
	}
}

// TestCheckManySerializationFailureAndEmptyInputs checks errors before dispatch and empty input handling.
func TestCheckManySerializationFailureAndEmptyInputs(t *testing.T) {
	serializationError := errors.New("cannot serialize subtree")
	body := &serializationCountingIterator{
		Iterator: query.NewFixedIterator(),
		err:      serializationError,
	}
	alias := NewDispatchIterator(query.NewAliasIterator("document", "viewer", body))
	receiver := &chunkingDispatcher{}
	sender := NewDispatchExecutor(receiver, &v1.PlanContext{Revision: "revision"}, 2)
	ctx := newTestContext()
	resource := query.NewObject("document", "first")
	subject := query.NewObject("user", "first").WithEllipses()

	_, err := sender.CheckManyResources(ctx, alias, nil, subject)
	require.NoError(t, err)
	_, err = sender.CheckManySubjects(ctx, alias, resource, nil)
	require.NoError(t, err)
	require.Zero(t, body.calls, "empty checks do not prepare a dispatch")

	_, err = sender.CheckManyResources(ctx, alias, []query.Object{resource}, subject)
	require.ErrorIs(t, err, serializationError)
	_, err = sender.CheckManySubjects(ctx, alias, resource, []query.ObjectAndRelation{subject})
	require.ErrorIs(t, err, serializationError)
	require.Equal(t, 2, body.calls)
	require.Empty(t, receiver.planCalls, "failed serialization must not launch chunks")
}

// TestCheckManyEndpointAllocationBudget guards the cost and independence of target messages.
func TestCheckManyEndpointAllocationBudget(t *testing.T) {
	ctx := newTestContext()
	sender := NewDispatchExecutor(&chunkingDispatcher{}, &v1.PlanContext{Revision: "revision"}, 100)
	plan := serializedCheckPlan{wire: []byte{1}, key: "document#viewer"}
	planContext := planContextForDispatch(sender.planContext, plan.key, ctx.TopLevelOperation, ctx.TargetSubjectType)
	resources := make([]query.Object, 100)
	subjects := make([]query.ObjectAndRelation, 100)

	for _, resourcesVary := range []bool{false, true} {
		name := "subjects"

		if resourcesVary {
			name = "resources"
		}

		t.Run(name, func(t *testing.T) {
			// A large chunk needs independent target messages, but can store
			// them in one backing array instead of allocating each separately.
			var request *v1.DispatchQueryPlanRequest
			allocations := testing.AllocsPerRun(100, func() {
				if resourcesVary {
					request = sender.buildManyRequest(planContext, v1.PlanOperation_PLAN_OPERATION_CHECK_MANY_RESOURCES,
						plan, query.Object{}, query.ObjectAndRelation{}, resources, nil)
				} else {
					request = sender.buildManyRequest(planContext, v1.PlanOperation_PLAN_OPERATION_CHECK_MANY_SUBJECTS,
						plan, query.Object{}, query.ObjectAndRelation{}, nil, subjects)
				}
			})
			require.Len(t, request.Many, 100)
			require.LessOrEqual(t, allocations, float64(16))
			request.Many[0].ObjectId = "changed"
			require.Empty(t, request.Many[1].ObjectId)
		})
	}
}
