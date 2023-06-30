package graph

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/dispatch/caching"
	"github.com/authzed/spicedb/internal/dispatch/keys"
	"github.com/authzed/spicedb/internal/graph"
	log "github.com/authzed/spicedb/internal/logging"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/genutil/mapz"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

var ONR = tuple.ObjectAndRelation

var goleakIgnores = []goleak.Option{
	goleak.IgnoreTopFunction("github.com/golang/glog.(*loggingT).flushDaemon"),
	goleak.IgnoreTopFunction("github.com/outcaste-io/ristretto.(*lfuPolicy).processItems"),
	goleak.IgnoreTopFunction("github.com/outcaste-io/ristretto.(*Cache).processItems"),
	goleak.IgnoreCurrent(),
}

func TestSimpleCheck(t *testing.T) {
	defer goleak.VerifyNone(t, goleakIgnores...)

	type expected struct {
		relation string
		isMember bool
	}

	type userset struct {
		userset  *core.ObjectAndRelation
		expected []expected
	}

	testCases := []struct {
		namespace string
		objectID  string
		usersets  []userset
	}{
		{"document", "masterplan", []userset{
			{ONR("user", "product_manager", graph.Ellipsis), []expected{{"owner", true}, {"edit", true}, {"view", true}}},
			{ONR("user", "chief_financial_officer", graph.Ellipsis), []expected{{"owner", false}, {"edit", false}, {"view", true}}},
			{ONR("user", "owner", graph.Ellipsis), []expected{{"owner", false}, {"edit", false}, {"view", true}}},
			{ONR("user", "legal", graph.Ellipsis), []expected{{"owner", false}, {"edit", false}, {"view", true}}},
			{ONR("user", "vp_product", graph.Ellipsis), []expected{{"owner", false}, {"edit", false}, {"view", true}}},
			{ONR("user", "eng_lead", graph.Ellipsis), []expected{{"owner", false}, {"edit", false}, {"view", true}}},
			{ONR("user", "auditor", graph.Ellipsis), []expected{{"owner", false}, {"edit", false}, {"view", true}}},
			{ONR("user", "villain", graph.Ellipsis), []expected{{"owner", false}, {"edit", false}, {"view", false}}},
		}},
		{"document", "healthplan", []userset{
			{ONR("user", "product_manager", graph.Ellipsis), []expected{{"owner", false}, {"edit", false}, {"view", false}}},
			{ONR("user", "chief_financial_officer", graph.Ellipsis), []expected{{"owner", false}, {"edit", false}, {"view", true}}},
			{ONR("user", "owner", graph.Ellipsis), []expected{{"owner", false}, {"edit", false}, {"view", false}}},
			{ONR("user", "legal", graph.Ellipsis), []expected{{"owner", false}, {"edit", false}, {"view", false}}},
			{ONR("user", "vp_product", graph.Ellipsis), []expected{{"owner", false}, {"edit", false}, {"view", false}}},
			{ONR("user", "eng_lead", graph.Ellipsis), []expected{{"owner", false}, {"edit", false}, {"view", false}}},
			{ONR("user", "auditor", graph.Ellipsis), []expected{{"owner", false}, {"edit", false}, {"view", false}}},
			{ONR("user", "villain", graph.Ellipsis), []expected{{"owner", false}, {"edit", false}, {"view", false}}},
		}},
		{"folder", "company", []userset{
			{ONR("user", "product_manager", graph.Ellipsis), []expected{{"owner", false}, {"edit", false}, {"view", false}}},
			{ONR("user", "chief_financial_officer", graph.Ellipsis), []expected{{"owner", false}, {"edit", false}, {"view", false}}},
			{ONR("user", "owner", graph.Ellipsis), []expected{{"owner", true}, {"edit", true}, {"view", true}}},
			{ONR("user", "legal", graph.Ellipsis), []expected{{"owner", false}, {"edit", false}, {"view", true}}},
			{ONR("user", "vp_product", graph.Ellipsis), []expected{{"owner", false}, {"edit", false}, {"view", false}}},
			{ONR("user", "eng_lead", graph.Ellipsis), []expected{{"owner", false}, {"edit", false}, {"view", false}}},
			{ONR("user", "auditor", graph.Ellipsis), []expected{{"owner", false}, {"edit", false}, {"view", true}}},
			{ONR("user", "villain", graph.Ellipsis), []expected{{"owner", false}, {"edit", false}, {"view", false}}},
			{ONR("folder", "auditors", "viewer"), []expected{{"view", true}}},
		}},
		{"folder", "strategy", []userset{
			{ONR("user", "product_manager", graph.Ellipsis), []expected{{"owner", false}, {"edit", false}, {"view", false}}},
			{ONR("user", "chief_financial_officer", graph.Ellipsis), []expected{{"owner", false}, {"edit", false}, {"view", false}}},
			{ONR("user", "owner", graph.Ellipsis), []expected{{"owner", false}, {"edit", false}, {"view", true}}},
			{ONR("user", "legal", graph.Ellipsis), []expected{{"owner", false}, {"edit", false}, {"view", true}}},
			{ONR("user", "vp_product", graph.Ellipsis), []expected{{"owner", true}, {"edit", true}, {"view", true}}},
			{ONR("user", "eng_lead", graph.Ellipsis), []expected{{"owner", false}, {"edit", false}, {"view", false}}},
			{ONR("user", "auditor", graph.Ellipsis), []expected{{"owner", false}, {"edit", false}, {"view", true}}},
			{ONR("user", "villain", graph.Ellipsis), []expected{{"owner", false}, {"edit", false}, {"view", false}}},
			{ONR("folder", "company", graph.Ellipsis), []expected{{"parent", true}}},
		}},
		{"folder", "isolated", []userset{
			{ONR("user", "product_manager", graph.Ellipsis), []expected{{"owner", false}, {"edit", false}, {"view", false}}},
			{ONR("user", "chief_financial_officer", graph.Ellipsis), []expected{{"owner", false}, {"edit", false}, {"view", false}}},
			{ONR("user", "owner", graph.Ellipsis), []expected{{"owner", false}, {"edit", false}, {"view", false}}},
			{ONR("user", "legal", graph.Ellipsis), []expected{{"owner", false}, {"edit", false}, {"view", false}}},
			{ONR("user", "vp_product", graph.Ellipsis), []expected{{"owner", false}, {"edit", false}, {"view", false}}},
			{ONR("user", "eng_lead", graph.Ellipsis), []expected{{"owner", false}, {"edit", false}, {"view", false}}},
			{ONR("user", "auditor", graph.Ellipsis), []expected{{"owner", false}, {"edit", false}, {"view", false}}},
			{ONR("user", "villain", graph.Ellipsis), []expected{{"owner", false}, {"edit", false}, {"view", true}}},
		}},
	}

	for _, tc := range testCases {
		for _, userset := range tc.usersets {
			for _, expected := range userset.expected {
				name := fmt.Sprintf(
					"simple::%s:%s#%s@%s:%s#%s=>%t",
					tc.namespace,
					tc.objectID,
					expected.relation,
					userset.userset.Namespace,
					userset.userset.ObjectId,
					userset.userset.Relation,
					expected.isMember,
				)

				tc := tc
				userset := userset
				expected := expected
				t.Run(name, func(t *testing.T) {
					require := require.New(t)

					ctx, dispatch, revision := newLocalDispatcher(t)

					checkResult, err := dispatch.DispatchCheck(ctx, &v1.DispatchCheckRequest{
						ResourceRelation: RR(tc.namespace, expected.relation),
						ResourceIds:      []string{tc.objectID},
						ResultsSetting:   v1.DispatchCheckRequest_ALLOW_SINGLE_RESULT,
						Subject:          userset.userset,
						Metadata: &v1.ResolverMeta{
							AtRevision:     revision.String(),
							DepthRemaining: 50,
						},
					})

					require.NoError(err)

					isMember := false
					if found, ok := checkResult.ResultsByResourceId[tc.objectID]; ok {
						isMember = found.Membership == v1.ResourceCheckResult_MEMBER
					}

					require.Equal(expected.isMember, isMember, "For object %s in %v: ", tc.objectID, checkResult.ResultsByResourceId)
					require.GreaterOrEqual(checkResult.Metadata.DepthRequired, uint32(1))
				})
			}
		}
	}
}

func TestMaxDepth(t *testing.T) {
	require := require.New(t)

	rawDS, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
	require.NoError(err)

	ds, _ := testfixtures.StandardDatastoreWithSchema(rawDS, require)

	mutation := tuple.Create(tuple.Parse("folder:oops#parent@folder:oops"))

	ctx := log.Logger.WithContext(datastoremw.ContextWithHandle(context.Background()))
	require.NoError(datastoremw.SetInContext(ctx, ds))

	revision, err := common.UpdateTuplesInDatastore(ctx, ds, mutation)
	require.NoError(err)
	require.True(revision.GreaterThan(datastore.NoRevision))

	dispatch := NewLocalOnlyDispatcher(10)

	_, err = dispatch.DispatchCheck(ctx, &v1.DispatchCheckRequest{
		ResourceRelation: RR("folder", "view"),
		ResourceIds:      []string{"oops"},
		ResultsSetting:   v1.DispatchCheckRequest_ALLOW_SINGLE_RESULT,
		Subject:          ONR("user", "fake", graph.Ellipsis),
		Metadata: &v1.ResolverMeta{
			AtRevision:     revision.String(),
			DepthRemaining: 50,
		},
	})

	require.Error(err)
}

func TestCheckMetadata(t *testing.T) {
	type expected struct {
		relation              string
		isMember              bool
		expectedDispatchCount int
		expectedDepthRequired int
	}

	type userset struct {
		userset  *core.ObjectAndRelation
		expected []expected
	}

	testCases := []struct {
		namespace string
		objectID  string
		usersets  []userset
	}{
		{"document", "masterplan", []userset{
			{
				ONR("user", "product_manager", graph.Ellipsis),
				[]expected{
					{"owner", true, 1, 1},
					{"edit", true, 3, 2},
					{"view", true, 21, 5},
				},
			},
			{
				ONR("user", "owner", graph.Ellipsis),
				[]expected{
					{"owner", false, 1, 1},
					{"edit", false, 3, 2},
					{"view", true, 21, 5},
				},
			},
		}},
		{"folder", "strategy", []userset{
			{
				ONR("user", "vp_product", graph.Ellipsis),
				[]expected{
					{"owner", true, 1, 1},
					{"edit", true, 3, 2},
					{"view", true, 11, 4},
				},
			},
		}},
		{"folder", "company", []userset{
			{
				ONR("user", "unknown", graph.Ellipsis),
				[]expected{
					{"view", false, 6, 3},
				},
			},
		}},
	}

	for _, tc := range testCases {
		for _, userset := range tc.usersets {
			for _, expected := range userset.expected {
				name := fmt.Sprintf(
					"metadata:%s:%s#%s@%s:%s#%s=>%t",
					tc.namespace,
					tc.objectID,
					expected.relation,
					userset.userset.Namespace,
					userset.userset.ObjectId,
					userset.userset.Relation,
					expected.isMember,
				)

				tc := tc
				userset := userset
				expected := expected
				t.Run(name, func(t *testing.T) {
					require := require.New(t)

					ctx, dispatch, revision := newLocalDispatcher(t)

					checkResult, err := dispatch.DispatchCheck(ctx, &v1.DispatchCheckRequest{
						ResourceRelation: RR(tc.namespace, expected.relation),
						ResourceIds:      []string{tc.objectID},
						ResultsSetting:   v1.DispatchCheckRequest_ALLOW_SINGLE_RESULT,
						Subject:          userset.userset,
						Metadata: &v1.ResolverMeta{
							AtRevision:     revision.String(),
							DepthRemaining: 50,
						},
					})

					require.NoError(err)

					isMember := false
					if found, ok := checkResult.ResultsByResourceId[tc.objectID]; ok {
						isMember = found.Membership == v1.ResourceCheckResult_MEMBER
					}

					require.Equal(expected.isMember, isMember)
					require.GreaterOrEqual(expected.expectedDispatchCount, int(checkResult.Metadata.DispatchCount), "dispatch count mismatch")
					require.GreaterOrEqual(expected.expectedDepthRequired, int(checkResult.Metadata.DepthRequired), "depth required mismatch")
				})
			}
		}
	}
}

func addFrame(trace *v1.CheckDebugTrace, foundFrames *mapz.Set[string]) {
	foundFrames.Add(fmt.Sprintf("%s:%s#%s", trace.Request.ResourceRelation.Namespace, strings.Join(trace.Request.ResourceIds, ","), trace.Request.ResourceRelation.Relation))
	for _, subTrace := range trace.SubProblems {
		addFrame(subTrace, foundFrames)
	}
}

func TestCheckDebugging(t *testing.T) {
	type expectedFrame struct {
		resourceType *core.RelationReference
		resourceIDs  []string
	}

	testCases := []struct {
		namespace      string
		objectID       string
		permission     string
		subject        *core.ObjectAndRelation
		expectedFrames []expectedFrame
	}{
		{
			"document", "masterplan", "view",
			ONR("user", "product_manager", graph.Ellipsis),
			[]expectedFrame{
				{
					RR("document", "view"),
					[]string{"masterplan"},
				},
				{
					RR("document", "edit"),
					[]string{"masterplan"},
				},
				{
					RR("document", "owner"),
					[]string{"masterplan"},
				},
			},
		},
		{
			"document", "masterplan", "view_and_edit",
			ONR("user", "product_manager", graph.Ellipsis),
			[]expectedFrame{
				{
					RR("document", "view_and_edit"),
					[]string{"masterplan"},
				},
			},
		},
		{
			"document", "specialplan", "view_and_edit",
			ONR("user", "multiroleguy", graph.Ellipsis),
			[]expectedFrame{
				{
					RR("document", "view_and_edit"),
					[]string{"specialplan"},
				},
				{
					RR("document", "viewer_and_editor"),
					[]string{"specialplan"},
				},
				{
					RR("document", "edit"),
					[]string{"specialplan"},
				},
				{
					RR("document", "editor"),
					[]string{"specialplan"},
				},
			},
		},
	}

	for _, tc := range testCases {
		name := fmt.Sprintf(
			"debugging::%s:%s#%s@%s:%s#%s",
			tc.namespace,
			tc.objectID,
			tc.permission,
			tc.subject.Namespace,
			tc.subject.ObjectId,
			tc.subject.Relation,
		)

		tc := tc
		t.Run(name, func(t *testing.T) {
			require := require.New(t)

			ctx, dispatch, revision := newLocalDispatcher(t)

			checkResult, err := dispatch.DispatchCheck(ctx, &v1.DispatchCheckRequest{
				ResourceRelation: RR(tc.namespace, tc.permission),
				ResourceIds:      []string{tc.objectID},
				ResultsSetting:   v1.DispatchCheckRequest_ALLOW_SINGLE_RESULT,
				Subject:          tc.subject,
				Metadata: &v1.ResolverMeta{
					AtRevision:     revision.String(),
					DepthRemaining: 50,
				},
				Debug: v1.DispatchCheckRequest_ENABLE_BASIC_DEBUGGING,
			})

			require.NoError(err)
			require.NotNil(checkResult.Metadata.DebugInfo)
			require.NotNil(checkResult.Metadata.DebugInfo.Check)
			require.NotNil(checkResult.Metadata.DebugInfo.Check.Duration)

			expectedFrames := mapz.NewSet[string]()
			for _, expectedFrame := range tc.expectedFrames {
				expectedFrames.Add(fmt.Sprintf("%s:%s#%s", expectedFrame.resourceType.Namespace, strings.Join(expectedFrame.resourceIDs, ","), expectedFrame.resourceType.Relation))
			}

			foundFrames := mapz.NewSet[string]()
			addFrame(checkResult.Metadata.DebugInfo.Check, foundFrames)

			require.Empty(expectedFrames.Subtract(foundFrames).AsSlice(), "missing expected frames: %v", expectedFrames.Subtract(foundFrames).AsSlice())
		})
	}
}

func newLocalDispatcherWithConcurrencyLimit(t testing.TB, concurrencyLimit uint16) (context.Context, dispatch.Dispatcher, datastore.Revision) {
	rawDS, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
	require.NoError(t, err)

	ds, revision := testfixtures.StandardDatastoreWithData(rawDS, require.New(t))

	dispatch := NewLocalOnlyDispatcher(concurrencyLimit)

	cachingDispatcher, err := caching.NewCachingDispatcher(caching.DispatchTestCache(t), false, "", &keys.CanonicalKeyHandler{})
	cachingDispatcher.SetDelegate(dispatch)
	require.NoError(t, err)

	ctx := log.Logger.WithContext(datastoremw.ContextWithHandle(context.Background()))
	require.NoError(t, datastoremw.SetInContext(ctx, ds))

	return ctx, cachingDispatcher, revision
}

func newLocalDispatcher(t testing.TB) (context.Context, dispatch.Dispatcher, datastore.Revision) {
	return newLocalDispatcherWithConcurrencyLimit(t, 10)
}

func newLocalDispatcherWithSchemaAndRels(t testing.TB, schema string, rels []*core.RelationTuple) (context.Context, dispatch.Dispatcher, datastore.Revision) {
	rawDS, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
	require.NoError(t, err)

	ds, revision := testfixtures.DatastoreFromSchemaAndTestRelationships(rawDS, schema, rels, require.New(t))

	dispatch := NewLocalOnlyDispatcher(10)

	cachingDispatcher, err := caching.NewCachingDispatcher(caching.DispatchTestCache(t), false, "", &keys.CanonicalKeyHandler{})
	cachingDispatcher.SetDelegate(dispatch)
	require.NoError(t, err)

	ctx := log.Logger.WithContext(datastoremw.ContextWithHandle(context.Background()))
	require.NoError(t, datastoremw.SetInContext(ctx, ds))

	return ctx, cachingDispatcher, revision
}
