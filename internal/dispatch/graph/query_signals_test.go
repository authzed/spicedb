package graph

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/revisions"
	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/querysignalstest"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/query"
	"github.com/authzed/spicedb/pkg/query/signals"
	"github.com/authzed/spicedb/pkg/query/signals/branchoutcome"
	"github.com/authzed/spicedb/pkg/query/signals/contextfield"
	"github.com/authzed/spicedb/pkg/query/signals/signal"
)

// Embedded interfaces deliberately panic on unexpected datastore operations.
type signalDataLayer struct {
	datalayer.DataLayer
	revision datastore.Revision
	reader   *signalReader
	t        *testing.T
}

func (d *signalDataLayer) RevisionFromString(s string) (datastore.Revision, error) {
	require.Equal(d.t, d.revision.String(), s)
	return d.revision, nil
}

func (d *signalDataLayer) SnapshotReader(r datastore.Revision, h datalayer.SchemaHash) datalayer.RevisionedReader {
	require.Equal(d.t, d.revision, r)
	require.Equal(d.t, datalayer.SchemaHash("a"), h)
	return d.reader
}

type signalReader struct {
	datalayer.RevisionedReader
	schema *signalSchema
	reads  int
}

func (r *signalReader) ReadSchema(context.Context) (datalayer.SchemaReader, error) {
	r.reads++
	return r.schema, nil
}

type signalSchema struct {
	datalayer.SchemaReader
	stored *datastore.ReadOnlyStoredSchema
}

func (s *signalSchema) StoredSchema() *datastore.ReadOnlyStoredSchema { return s.stored }
func (s *signalSchema) ListAllTypeDefinitions(context.Context) ([]datastore.RevisionedTypeDefinition, error) {
	return nil, nil
}

func (s *signalSchema) ListAllCaveatDefinitions(context.Context) ([]datastore.RevisionedCaveat, error) {
	return nil, nil
}

func TestQuerySignalsReceiverReusesLoadedSchema(t *testing.T) {
	owner := signals.NewDefaultOwner()
	params := MustNewDefaultDispatcherParametersForTesting()
	params.QuerySignals = owner
	dispatcher, err := NewLocalOnlyDispatcher(params)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dispatcher.Close()) })
	reader := &signalReader{schema: &signalSchema{stored: querysignalstest.Stored("a")}}
	revision := revisions.NewForTransactionID(7)
	dl := &signalDataLayer{revision: revision, reader: reader, t: t}
	ctx := datalayer.ContextWithDataLayer(t.Context(), dl)
	it := query.NewFixedIterator()
	var plan bytes.Buffer
	require.NoError(t, it.Serialize(&plan))
	request := &v1.DispatchQueryPlanRequest{Plan: plan.Bytes(), PlanContext: dispatch.NewPlanContext(revision.String(), datalayer.SchemaHash("a"), map[string]any{"field": true}, 50, 0), Operation: v1.PlanOperation_PLAN_OPERATION_CHECK, Resource: &core.ObjectAndRelation{Namespace: "doc", ObjectId: "d"}, Subject: &core.ObjectAndRelation{Namespace: "user", ObjectId: "u", Relation: "..."}}

	for range 2 {
		stream := dispatch.NewCollectingDispatchStream[*v1.DispatchQueryPlanResponse](ctx)
		require.NoError(t, dispatcher.DispatchQueryPlan(request, stream))
		require.Empty(t, stream.Results())
	}

	require.Equal(t, 2, reader.reads) // One schema read per receiver invocation, no extra signal acquisition read.
	v, err := owner.Bind(reader.schema.stored, signal.Scope{SchemaHash: "a", Revision: revision})
	require.NoError(t, err)
	descriptors, err := query.ExecutionDescriptors(it)
	require.NoError(t, err)
	d := descriptors[it]
	k := signal.ExecutionKey{Identity: d.Identity, Operation: signal.Check, Filter: "doc|user#...", Mode: "single", Strategy: "default/batched", Role: "body", Dependencies: signal.EncodeFootprint(d.Footprint)}
	e, err := signal.Lookup(v, branchoutcome.Ref, k)
	require.NoError(t, err)
	require.Equal(t, uint64(2), e.Value.Denied)
	field, err := signal.Lookup(v, contextfield.Ref, "field")
	require.NoError(t, err)
	require.Equal(t, signal.Missing, field.Availability)
}

func TestQuerySignalsReceiverKeepsBatchMode(t *testing.T) {
	for _, tc := range []struct {
		operation           v1.PlanOperation
		mode                string
		namespace, relation string
	}{{v1.PlanOperation_PLAN_OPERATION_CHECK_MANY_RESOURCES, "many-resources", "doc", ""}, {v1.PlanOperation_PLAN_OPERATION_CHECK_MANY_SUBJECTS, "many-subjects", "user", "..."}} {
		t.Run(tc.mode, func(t *testing.T) {
			owner := signals.NewDefaultOwner()
			params := MustNewDefaultDispatcherParametersForTesting()
			params.QuerySignals = owner
			dispatcher, err := NewLocalOnlyDispatcher(params)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, dispatcher.Close()) })
			reader := &signalReader{schema: &signalSchema{stored: querysignalstest.Stored("a")}}
			revision := revisions.NewForTransactionID(1)
			ctx := datalayer.ContextWithDataLayer(t.Context(), &signalDataLayer{revision: revision, reader: reader, t: t})
			it := query.NewFixedIterator()
			var plan bytes.Buffer
			require.NoError(t, it.Serialize(&plan))
			req := &v1.DispatchQueryPlanRequest{Plan: plan.Bytes(), PlanContext: dispatch.NewPlanContext(revision.String(), "a", nil, 50, 0), Operation: tc.operation, Resource: &core.ObjectAndRelation{Namespace: "doc", ObjectId: "d"}, Subject: &core.ObjectAndRelation{Namespace: "user", ObjectId: "u", Relation: "..."}, Many: []*core.ObjectAndRelation{{Namespace: tc.namespace, ObjectId: "one", Relation: tc.relation}, {Namespace: tc.namespace, ObjectId: "two", Relation: tc.relation}}}
			require.NoError(t, dispatcher.DispatchQueryPlan(req, dispatch.NewCollectingDispatchStream[*v1.DispatchQueryPlanResponse](ctx)))
			v, err := owner.Bind(reader.schema.stored, signal.Scope{SchemaHash: "a", Revision: revision})
			require.NoError(t, err)
			descriptors, err := query.ExecutionDescriptors(it)
			require.NoError(t, err)
			d := descriptors[it]
			k := signal.ExecutionKey{Identity: d.Identity, Operation: signal.Check, Filter: "doc|user#...", Mode: tc.mode, Strategy: "default/batched", Role: "body", Dependencies: signal.EncodeFootprint(d.Footprint)}
			e, err := signal.Lookup(v, branchoutcome.Ref, k)
			require.NoError(t, err)
			require.Equal(t, uint64(2), e.Value.Denied)
			k.Mode = "single"
			single, err := signal.Lookup(v, branchoutcome.Ref, k)
			require.NoError(t, err)
			require.Equal(t, signal.Missing, single.Availability)
		})
	}
}
