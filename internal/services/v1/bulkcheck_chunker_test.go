package v1

import (
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb/internal/datastore/dsfortesting"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/dispatch/chunkertest"
	"github.com/authzed/spicedb/internal/testfixtures"
	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/genutil"
	"github.com/authzed/spicedb/pkg/middleware/consistency"
)

func documentViewDispatch(firstID string, ids ...string) chunkertest.Dispatch {
	return chunkertest.Dispatch{
		Relation: "document#view",
		IDs:      ids,
		Hint:     chunkertest.HintFor("document", firstID),
		HasHint:  true,
	}
}

func TestCheckBulkPermissionsChunksOnceByOwner(t *testing.T) {
	require := require.New(t)

	uninitialized, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, 0, memdb.DisableGC)
	require.NoError(err)
	ds, _ := testfixtures.StandardDatastoreWithData(t, uninitialized)
	dl := datalayer.NewDataLayer(ds)
	ctx := datalayer.ContextWithDataLayer(consistency.ContextWithHandle(t.Context()), dl)

	documentIDs := []string{"a1", "b1", "a2"}
	items := make([]*v1.CheckBulkPermissionsRequestItem, 0, len(documentIDs))
	for _, id := range documentIDs {
		items = append(items, &v1.CheckBulkPermissionsRequestItem{
			Resource:   &v1.ObjectReference{ObjectType: "document", ObjectId: id},
			Permission: "view",
			Subject:    &v1.SubjectReference{Object: &v1.ObjectReference{ObjectType: "user", ObjectId: "eng_lead"}},
		})
	}
	req := &v1.CheckBulkPermissionsRequest{Items: items}
	require.NoError(consistency.AddRevisionToContext(ctx, req, dl, "", consistency.TreatMismatchingTokensAsError))

	recorder := &chunkertest.Recorder{}
	chunker := &chunkertest.ByFirstByte{}
	bc := &bulkChecker{
		maxAPIDepth:          50,
		maxCaveatContextSize: 4096,
		maxConcurrency:       2,
		caveatTypeSet:        caveattypes.Default.TypeSet,
		dispatch:             recorder,
		dispatchChunkSize:    100,
	}

	resp, err := bc.checkBulkPermissions(dispatch.ContextWithChunker(ctx, chunker), req, NewMetrics(nil))
	require.NoError(err)
	require.Len(resp.Pairs, 3)
	require.ElementsMatch([]chunkertest.Dispatch{
		documentViewDispatch("a1", "a1", "a2"),
		documentViewDispatch("b1", "b1"),
	}, recorder.Dispatches())
	require.Equal([]chunkertest.Call{
		{Namespace: "document", IDs: []string{"a1", "b1", "a2"}, MaxChunkSize: 100},
	}, chunker.Calls(), "bulk check chunks once, so each key is touched once")
}

func TestCheckPermissionUsesContextChunker(t *testing.T) {
	require := require.New(t)

	uninitialized, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, 0, memdb.DisableGC)
	require.NoError(err)
	ds, _ := testfixtures.StandardDatastoreWithData(t, uninitialized)
	dl := datalayer.NewDataLayer(ds)
	ctx := datalayer.ContextWithDataLayer(consistency.ContextWithHandle(t.Context()), dl)

	req := &v1.CheckPermissionRequest{
		Resource:   &v1.ObjectReference{ObjectType: "document", ObjectId: "masterplan"},
		Permission: "view",
		Subject:    &v1.SubjectReference{Object: &v1.ObjectReference{ObjectType: "user", ObjectId: "eng_lead"}},
	}
	require.NoError(consistency.AddRevisionToContext(ctx, req, dl, "", consistency.TreatMismatchingTokensAsError))

	recorder := &chunkertest.Recorder{}
	chunker := &chunkertest.ByFirstByte{}
	ps := NewPermissionsServer(recorder, PermissionsServerConfig{}, genutil.MustNewProtoValidator())

	_, err = ps.CheckPermission(dispatch.ContextWithChunker(ctx, chunker), req)
	require.NoError(err)
	require.Equal([]chunkertest.Dispatch{documentViewDispatch("masterplan", "masterplan")}, recorder.Dispatches())
	require.Equal([]chunkertest.Call{
		{Namespace: "document", IDs: []string{"masterplan"}, MaxChunkSize: 100},
	}, chunker.Calls())
}
