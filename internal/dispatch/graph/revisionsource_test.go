package graph

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/dsfortesting"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/dispatch"
	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/pkg/datalayer"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

// recordingDispatcher records the metadata of every dispatch it receives before
// handing it to the wrapped dispatcher, which redispatches back through it.
type recordingDispatcher struct {
	dispatch.Dispatcher

	lock     sync.Mutex
	recorded map[string][]*v1.ResolverMeta
}

func (rd *recordingDispatcher) record(method string, md *v1.ResolverMeta) {
	rd.lock.Lock()
	defer rd.lock.Unlock()
	rd.recorded[method] = append(rd.recorded[method], md)
}

func (rd *recordingDispatcher) DispatchCheck(ctx context.Context, req *v1.DispatchCheckRequest) (*v1.DispatchCheckResponse, error) {
	rd.record("check", req.Metadata)
	return rd.Dispatcher.DispatchCheck(ctx, req)
}

func (rd *recordingDispatcher) DispatchExpand(ctx context.Context, req *v1.DispatchExpandRequest) (*v1.DispatchExpandResponse, error) {
	rd.record("expand", req.Metadata)
	return rd.Dispatcher.DispatchExpand(ctx, req)
}

func (rd *recordingDispatcher) DispatchLookupResources3(req *v1.DispatchLookupResources3Request, stream dispatch.LookupResources3Stream) error {
	rd.record("lookupresources3", req.Metadata)
	return rd.Dispatcher.DispatchLookupResources3(req, stream)
}

func (rd *recordingDispatcher) DispatchLookupSubjects(req *v1.DispatchLookupSubjectsRequest, stream dispatch.LookupSubjectsStream) error {
	rd.record("lookupsubjects", req.Metadata)
	return rd.Dispatcher.DispatchLookupSubjects(req, stream)
}

func TestRevisionSourcePropagatesToNestedDispatches(t *testing.T) {
	rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, 0, memdb.DisableGC)
	require.NoError(t, err)

	ds, revision := testfixtures.StandardDatastoreWithData(t, rawDS)

	ctx := log.Logger.WithContext(datalayer.ContextWithHandle(t.Context()))
	require.NoError(t, datalayer.SetInContext(ctx, datalayer.NewDataLayer(ds)))

	recorder := &recordingDispatcher{recorded: map[string][]*v1.ResolverMeta{}}
	local, err := NewDispatcher(recorder, MustNewDefaultDispatcherParametersForTesting())
	require.NoError(t, err)
	recorder.Dispatcher = local
	t.Cleanup(func() {
		_ = local.Close()
	})

	const source = v1.RevisionSource_REVISION_SOURCE_HEAD
	rootMeta := func() *v1.ResolverMeta {
		return &v1.ResolverMeta{
			AtRevision:     revision.String(),
			DepthRemaining: 50,
			SchemaHash:     []byte(datalayer.NoSchemaHashForTesting),
			RevisionSource: source,
		}
	}

	checkResp, err := recorder.DispatchCheck(ctx, &v1.DispatchCheckRequest{
		ResourceRelation: RR("document", "view").ToCoreRR(),
		ResourceIds:      []string{"masterplan"},
		ResultsSetting:   v1.DispatchCheckRequest_ALLOW_SINGLE_RESULT,
		Subject:          ONR("user", "eng_lead", "...").ToCoreONR(),
		Metadata:         rootMeta(),
	})
	require.NoError(t, err)
	require.Equal(t, v1.ResourceCheckResult_MEMBER, checkResp.ResultsByResourceId["masterplan"].Membership)

	_, err = recorder.DispatchExpand(ctx, &v1.DispatchExpandRequest{
		ResourceAndRelation: ONR("document", "masterplan", "view").ToCoreONR(),
		Metadata:            rootMeta(),
		ExpansionMode:       v1.DispatchExpandRequest_RECURSIVE,
	})
	require.NoError(t, err)

	lrStream := dispatch.NewCloningCollectingDispatchStream[*v1.DispatchLookupResources3Response](ctx)
	err = recorder.DispatchLookupResources3(&v1.DispatchLookupResources3Request{
		ResourceRelation: RR("document", "view").ToCoreRR(),
		SubjectRelation:  RR("user", "...").ToCoreRR(),
		SubjectIds:       []string{"eng_lead"},
		TerminalSubject:  ONR("user", "eng_lead", "...").ToCoreONR(),
		Metadata:         rootMeta(),
		OptionalLimit:    veryLargeLimit,
	}, lrStream)
	require.NoError(t, err)
	require.NotEmpty(t, lrStream.Results())

	lsStream := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupSubjectsResponse](ctx)
	err = recorder.DispatchLookupSubjects(&v1.DispatchLookupSubjectsRequest{
		ResourceRelation: RR("document", "view").ToCoreRR(),
		ResourceIds:      []string{"masterplan"},
		SubjectRelation:  RR("user", "...").ToCoreRR(),
		Metadata:         rootMeta(),
	}, lsStream)
	require.NoError(t, err)
	require.NotEmpty(t, lsStream.Results())

	recorder.lock.Lock()
	defer recorder.lock.Unlock()

	for _, method := range []string{"check", "expand", "lookupresources3", "lookupsubjects"} {
		recorded := recorder.recorded[method]
		require.Greater(t, len(recorded), 1, "expected nested %s dispatches", method)
		for _, md := range recorded {
			require.Equal(t, source, md.RevisionSource, "%s dispatch at depth %d lost its revision source", method, md.DepthRemaining)
		}
	}
}
