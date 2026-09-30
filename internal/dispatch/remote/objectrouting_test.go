package remote

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/authzed/consistent"
	"github.com/authzed/consistent/hashring"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/dispatch/keys"
	"github.com/authzed/spicedb/internal/frequency"
	corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

type fakeMember string

func (m fakeMember) Key() string { return string(m) }

// fakeRingView is a consistent.RingView that maps object keys to member names.
type fakeRingView struct {
	owners map[string]string
	// memberCount is the number of members that Members returns.
	memberCount int
	err         error
	// If errAfter is more than 0, FindN call N returns err. This simulates membership churn in a chunk.
	errAfter int
	calls    int
}

func (f *fakeRingView) FindN(key []byte, _ uint8) ([]hashring.Member, error) {
	f.calls++
	if f.err != nil && (f.errAfter == 0 || f.calls >= f.errAfter) {
		return nil, f.err
	}
	return []hashring.Member{fakeMember(f.owners[string(key)])}, nil
}

func (f *fakeRingView) Members() []hashring.Member {
	members := make([]hashring.Member, 0, f.memberCount)
	for i := range f.memberCount {
		members = append(members, fakeMember(fmt.Sprintf("n%d", i)))
	}
	return members
}

// fakeCheckClient records each DispatchCheck request and its routing key, and answers MEMBER.
// A request that contains an ID in failOn returns an error.
type fakeCheckClient struct {
	fakeClusterClient

	failOn string

	mu   sync.Mutex
	reqs []*v1.DispatchCheckRequest
	keys [][]byte
}

func (f *fakeCheckClient) DispatchCheck(ctx context.Context,
	req *v1.DispatchCheckRequest, _ ...grpc.CallOption,
) (*v1.DispatchCheckResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reqs = append(f.reqs, req)
	key, _ := ctx.Value(consistent.CtxKey).([]byte)
	f.keys = append(f.keys, key)
	results := make(map[string]*v1.ResourceCheckResult, len(req.ResourceIds))
	for _, id := range req.ResourceIds {
		if f.failOn != "" && id == f.failOn {
			return nil, errors.New("injected failure")
		}
		results[id] = &v1.ResourceCheckResult{Membership: v1.ResourceCheckResult_MEMBER}
	}
	return &v1.DispatchCheckResponse{
		Metadata:            &v1.ResponseMeta{DispatchCount: 1, DepthRequired: uint32(len(req.ResourceIds))}, //nolint:gosec // test sizes are tiny
		ResultsByResourceId: results,
	}, nil
}

// newTestObjectRoutingDispatcher uses the real constructor, which initializes digests and trackers.
func newTestObjectRoutingDispatcher(t *testing.T, client ClusterClient, view consistent.RingView) *clusterDispatcher {
	t.Helper()
	fb := &fakeBuilder{Builder: consistent.NewBuilder(xxhash.Sum64), view: view}
	d, err := NewClusterDispatcher(client, newTestConn(t), ClusterDispatcherConfig{
		KeyHandler:             &keys.DirectKeyHandler{},
		DispatchOverallTimeout: 30 * time.Second,
		ObjectAffinityRouting:  true,
		HashringBuilder:        fb,
	}, nil, nil, 0)
	require.NoError(t, err)
	return d.(*clusterDispatcher)
}

func checkReq(ids ...string) *v1.DispatchCheckRequest {
	return &v1.DispatchCheckRequest{
		ResourceRelation: &corev1.RelationReference{Namespace: "document", Relation: "view"},
		ResourceIds:      ids,
		Subject:          &corev1.ObjectAndRelation{Namespace: "user", ObjectId: "evan", Relation: "..."},
		Metadata:         &v1.ResolverMeta{AtRevision: "1", DepthRemaining: 50},
	}
}

func TestCheckFansOutByOwner(t *testing.T) {
	client := &fakeCheckClient{}
	cd := newTestObjectRoutingDispatcher(t, client, &fakeRingView{owners: map[string]string{
		"document/a": "n1", "document/b": "n2", "document/c": "n1",
	}})
	resp, err := cd.DispatchCheck(t.Context(), checkReq("a", "b", "c"))
	require.NoError(t, err)

	require.Len(t, client.reqs, 2)
	idSets := make([][]string, 0, len(client.reqs))
	for _, r := range client.reqs {
		idSets = append(idSets, r.ResourceIds)
	}
	require.ElementsMatch(t, [][]string{{"a", "c"}, {"b"}}, idSets)
	// The routing key of each sub-request is an object key from its owner group.
	for i, r := range client.reqs {
		require.Equal(t, objectRoutingKey("document", r.ResourceIds[0]), client.keys[i])
	}
	// The merge has all three resources, the sum of the counts and the maximum depth.
	require.Len(t, resp.ResultsByResourceId, 3)
	require.EqualValues(t, 2, resp.Metadata.DispatchCount)
	require.EqualValues(t, 2, resp.Metadata.DepthRequired)
}

func TestSingleOwnerStaysSingleRPC(t *testing.T) {
	client := &fakeCheckClient{}
	cd := newTestObjectRoutingDispatcher(t, client, &fakeRingView{owners: map[string]string{
		"document/a": "n1", "document/b": "n1",
	}})
	_, err := cd.DispatchCheck(t.Context(), checkReq("a", "b"))
	require.NoError(t, err)
	require.Len(t, client.reqs, 1)
	require.Equal(t, []string{"a", "b"}, client.reqs[0].ResourceIds)
	require.Equal(t, objectRoutingKey("document", "a"), client.keys[0])
}

func TestRingErrorMidChunkFallsBack(t *testing.T) {
	client := &fakeCheckClient{}
	cd := newTestObjectRoutingDispatcher(t, client, &fakeRingView{
		owners: map[string]string{"document/a": "n1"},
		// The second lookup fails.
		err: consistent.ErrNoRing, errAfter: 2,
	})
	before := testutil.ToFloat64(ringFallbackTotal)
	resp, err := cd.DispatchCheck(t.Context(), checkReq("a", "b", "c"))
	require.InDelta(t, before+1, testutil.ToFloat64(ringFallbackTotal), 0)
	require.NoError(t, err, "ring unavailability degrades locality, never errors")
	require.Len(t, client.reqs, 1)
	require.Equal(t, []string{"a", "b", "c"}, client.reqs[0].ResourceIds, "no ID may be dropped")
	require.Equal(t, objectRoutingKey("document", "a"), client.keys[0], "fallback routes by first-ID object key")
	require.Len(t, resp.ResultsByResourceId, 3)
}

func TestDebugRequestsBypassFanOut(t *testing.T) {
	client := &fakeCheckClient{}
	cd := newTestObjectRoutingDispatcher(t, client, &fakeRingView{owners: map[string]string{
		"document/a": "n1", "document/b": "n2",
	}})
	req := checkReq("a", "b")
	req.Debug = v1.DispatchCheckRequest_ENABLE_BASIC_DEBUGGING
	_, err := cd.DispatchCheck(t.Context(), req)
	require.NoError(t, err)
	require.Len(t, client.reqs, 1, "debug traces must never be split across RPCs")

	wantKey, err := (&keys.DirectKeyHandler{}).CheckDispatchKey(t.Context(), req)
	require.NoError(t, err)
	require.Equal(t, wantKey, client.keys[0], "debug requests keep the request-hash key")
}

func TestRoutingOffUsesRequestKey(t *testing.T) {
	client := &fakeCheckClient{}
	view := &fakeRingView{owners: map[string]string{"document/a": "n1", "document/b": "n2"}}
	cd := newTestObjectRoutingDispatcher(t, client, view)
	cd.objectRouting = false

	req := checkReq("a", "b")
	_, err := cd.DispatchCheck(t.Context(), req)
	require.NoError(t, err)
	require.Len(t, client.reqs, 1)
	require.Same(t, req, client.reqs[0], "flag-off path sends the request unchanged")
	require.Zero(t, view.calls, "flag-off path never consults the ring")

	wantKey, err := (&keys.DirectKeyHandler{}).CheckDispatchKey(t.Context(), req)
	require.NoError(t, err)
	require.Equal(t, wantKey, client.keys[0])

	// With the flag off, the constructor leaves no ring view. This is not a fallback.
	cd.ringView = nil
	before := testutil.ToFloat64(ringFallbackTotal)
	_, err = cd.DispatchCheck(t.Context(), req)
	require.NoError(t, err)
	require.InDelta(t, before, testutil.ToFloat64(ringFallbackTotal), 0, "flag-off path never counts a ring fallback")
}

func TestNilRingViewFallsBackToObjectKey(t *testing.T) {
	client := &fakeCheckClient{}
	cd := newTestObjectRoutingDispatcher(t, client, &fakeRingView{})
	cd.ringView = nil

	before := testutil.ToFloat64(ringFallbackTotal)
	_, err := cd.DispatchCheck(t.Context(), checkReq("a", "b"))
	require.NoError(t, err)
	require.InDelta(t, before+1, testutil.ToFloat64(ringFallbackTotal), 0, "a missing ring view is a counted fallback")
	require.Len(t, client.reqs, 1)
	require.Equal(t, objectRoutingKey("document", "a"), client.keys[0])
}

func TestFanOutGroupErrorFailsWholeCheck(t *testing.T) {
	client := &fakeCheckClient{failOn: "b"}
	cd := newTestObjectRoutingDispatcher(t, client, &fakeRingView{owners: map[string]string{
		"document/a": "n1", "document/b": "n2",
	}})
	resp, err := cd.DispatchCheck(t.Context(), checkReq("a", "b"))
	require.Error(t, err)
	require.Equal(t, requestFailureMetadata, resp.Metadata)
	require.Empty(t, resp.ResultsByResourceId)
}

func TestObjectRoutingKey(t *testing.T) {
	require.Equal(t, []byte("document/a"), objectRoutingKey("document", "a"))
	require.Equal(t, []byte("/"), objectRoutingKey("", ""))
}

func TestMergeCheckResponses(t *testing.T) {
	merged := mergeCheckResponses([]*v1.DispatchCheckResponse{
		{
			Metadata: &v1.ResponseMeta{DispatchCount: 2, CachedDispatchCount: 1, DepthRequired: 3},
			ResultsByResourceId: map[string]*v1.ResourceCheckResult{
				"a": {Membership: v1.ResourceCheckResult_MEMBER},
			},
		},
		{
			Metadata: &v1.ResponseMeta{DispatchCount: 4, CachedDispatchCount: 2, DepthRequired: 1},
			ResultsByResourceId: map[string]*v1.ResourceCheckResult{
				"b": {Membership: v1.ResourceCheckResult_NOT_MEMBER},
			},
		},
		{ResultsByResourceId: map[string]*v1.ResourceCheckResult{
			"c": {Membership: v1.ResourceCheckResult_CAVEATED_MEMBER},
		}},
		nil,
	})
	require.EqualValues(t, 6, merged.Metadata.DispatchCount)
	require.EqualValues(t, 3, merged.Metadata.CachedDispatchCount)
	require.EqualValues(t, 3, merged.Metadata.DepthRequired)
	require.Nil(t, merged.Metadata.DebugInfo)
	require.Len(t, merged.ResultsByResourceId, 3)
	require.Equal(t, v1.ResourceCheckResult_NOT_MEMBER, merged.ResultsByResourceId["b"].Membership)
}

type fakeBuilder struct {
	// The real builder supplies balancer.Builder and ConfigParser.
	consistent.Builder
	view      consistent.RingView
	calls     int
	sawTarget string
}

func (f *fakeBuilder) RingFor(target string) consistent.RingView {
	f.calls++
	f.sawTarget = target
	return f.view
}

func newTestConn(t *testing.T) *grpc.ClientConn {
	conn, err := grpc.NewClient("passthrough:///unused",
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func TestNewClusterDispatcherAcquiresRingView(t *testing.T) {
	conn := newTestConn(t)

	fb := &fakeBuilder{Builder: consistent.NewBuilder(xxhash.Sum64), view: &fakeRingView{}}
	d, err := NewClusterDispatcher(nil, conn, ClusterDispatcherConfig{
		KeyHandler:            &keys.DirectKeyHandler{},
		ObjectAffinityRouting: true,
		HashringBuilder:       fb,
		SpreadShare:           0.5,
		Spread:                3,
		SpreadLatencyFactor:   1.5,
	}, nil, nil, 0)
	require.NoError(t, err)
	cd := d.(*clusterDispatcher)
	require.NotNil(t, cd.ringView)
	require.True(t, cd.objectRouting)
	require.InDelta(t, 0.5, cd.spreadShare, 0)
	require.Equal(t, uint8(3), cd.spread)
	require.InDelta(t, 1.5, cd.spreadLatencyFactor, 0)
	require.NotNil(t, cd.spreadEstimator)
	require.NotNil(t, cd.ownerLatency)
	require.NoError(t, cd.Close())
	require.Equal(t, conn.CanonicalTarget(), fb.sawTarget,
		"the ring view must be keyed by the conn's canonical target — the same string gRPC passes to Builder.Build")
}

func TestNewClusterDispatcherRoutingOffDoesNotAcquireRingView(t *testing.T) {
	fb := &fakeBuilder{Builder: consistent.NewBuilder(xxhash.Sum64), view: &fakeRingView{}}
	d, err := NewClusterDispatcher(nil, newTestConn(t), ClusterDispatcherConfig{
		KeyHandler:      &keys.DirectKeyHandler{},
		HashringBuilder: fb,
	}, nil, nil, 0)
	require.NoError(t, err)
	require.Nil(t, d.(*clusterDispatcher).ringView)
	require.Zero(t, fb.calls)
}

// fakeLookupSubjectsClient records each DispatchLookupSubjects request and its routing key.
// Its streams yield the subjects for each resource ID and then io.EOF.
// A request that contains failOn returns an error.
// A request that contains blockOn waits for cancellation and returns the ctx error.
type fakeLookupSubjectsClient struct {
	fakeClusterClient

	subjects map[string][]string
	failOn   string
	blockOn  string
	// failErr, if set, is the error for every stream.
	failErr error

	mu   sync.Mutex
	reqs []*v1.DispatchLookupSubjectsRequest
	keys [][]byte
}

func newFakeLookupSubjectsClient(subjects map[string][]string) *fakeLookupSubjectsClient {
	return &fakeLookupSubjectsClient{subjects: subjects}
}

func (f *fakeLookupSubjectsClient) requests() []*v1.DispatchLookupSubjectsRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.reqs)
}

func (f *fakeLookupSubjectsClient) routingKeys() [][]byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.keys)
}

func (f *fakeLookupSubjectsClient) DispatchLookupSubjects(ctx context.Context,
	req *v1.DispatchLookupSubjectsRequest, _ ...grpc.CallOption,
) (v1.DispatchService_DispatchLookupSubjectsClient, error) {
	f.mu.Lock()
	f.reqs = append(f.reqs, req)
	key, _ := ctx.Value(consistent.CtxKey).([]byte)
	f.keys = append(f.keys, key)
	f.mu.Unlock()

	s := &fakeLSStream{ctx: ctx, err: f.failErr}
	for _, id := range req.ResourceIds {
		switch id {
		case f.failOn:
			s.err = errors.New("injected failure")
		case f.blockOn:
			s.block = true
		}
		found := make([]*v1.FoundSubject, 0, len(f.subjects[id]))
		for _, subjectID := range f.subjects[id] {
			found = append(found, &v1.FoundSubject{SubjectId: subjectID})
		}
		s.responses = append(s.responses, &v1.DispatchLookupSubjectsResponse{
			FoundSubjectsByResourceId: map[string]*v1.FoundSubjects{id: {FoundSubjects: found}},
			Metadata:                  &v1.ResponseMeta{DispatchCount: 1},
		})
	}
	return s, nil
}

type fakeLSStream struct {
	grpc.ClientStream

	ctx       context.Context
	responses []*v1.DispatchLookupSubjectsResponse
	err       error
	block     bool
}

func (s *fakeLSStream) Recv() (*v1.DispatchLookupSubjectsResponse, error) {
	if s.err != nil {
		return nil, s.err
	}
	if s.block {
		<-s.ctx.Done()
		return nil, s.ctx.Err()
	}
	if len(s.responses) == 0 {
		return nil, io.EOF
	}
	resp := s.responses[0]
	s.responses = s.responses[1:]
	return resp, nil
}

func lsReq(ids ...string) *v1.DispatchLookupSubjectsRequest {
	return &v1.DispatchLookupSubjectsRequest{
		ResourceRelation: &corev1.RelationReference{Namespace: "document", Relation: "view"},
		ResourceIds:      ids,
		SubjectRelation:  &corev1.RelationReference{Namespace: "user", Relation: "..."},
		Metadata:         &v1.ResolverMeta{AtRevision: "1", DepthRemaining: 50},
	}
}

func foundResourceIDs(results []*v1.DispatchLookupSubjectsResponse) map[string]bool {
	found := map[string]bool{}
	for _, resp := range results {
		for resourceID := range resp.FoundSubjectsByResourceId {
			found[resourceID] = true
		}
	}
	return found
}

func TestLookupSubjectsFansOutByOwner(t *testing.T) {
	client := newFakeLookupSubjectsClient(map[string][]string{
		"a": {"evan"}, "b": {"tanner"}, "c": {"jake"},
	})
	cd := newTestObjectRoutingDispatcher(t, client, &fakeRingView{owners: map[string]string{
		"document/a": "n1", "document/b": "n2", "document/c": "n1",
	}})

	stream := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupSubjectsResponse](t.Context())
	err := cd.DispatchLookupSubjects(lsReq("a", "b", "c"), stream)
	require.NoError(t, err)

	reqs := client.requests()
	// There is one stream for each owner.
	require.Len(t, reqs, 2)
	idSets := make([][]string, 0, len(reqs))
	for _, r := range reqs {
		idSets = append(idSets, r.ResourceIds)
	}
	require.ElementsMatch(t, [][]string{{"a", "c"}, {"b"}}, idSets)
	keys := client.routingKeys()
	for i, r := range reqs {
		require.Equal(t, objectRoutingKey("document", r.ResourceIds[0]), keys[i])
	}

	require.Equal(t, map[string]bool{"a": true, "b": true, "c": true}, foundResourceIDs(stream.Results()))
	require.Len(t, stream.Results(), 3)
}

func TestLookupSubjectsSingleOwnerStaysSingleStream(t *testing.T) {
	client := newFakeLookupSubjectsClient(map[string][]string{"a": {"evan"}, "b": {"tanner"}})
	cd := newTestObjectRoutingDispatcher(t, client, &fakeRingView{owners: map[string]string{
		"document/a": "n1", "document/b": "n1",
	}})

	stream := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupSubjectsResponse](t.Context())
	req := lsReq("a", "b")
	require.NoError(t, cd.DispatchLookupSubjects(req, stream))

	reqs := client.requests()
	require.Len(t, reqs, 1)
	require.Same(t, req, reqs[0], "a single group sends the request unchanged")
	require.Equal(t, objectRoutingKey("document", "a"), client.routingKeys()[0])
	require.Len(t, stream.Results(), 2)
}

func TestLookupSubjectsRingErrorFallsBack(t *testing.T) {
	client := newFakeLookupSubjectsClient(map[string][]string{"a": {"evan"}, "b": {"tanner"}, "c": {"jake"}})
	cd := newTestObjectRoutingDispatcher(t, client, &fakeRingView{
		owners: map[string]string{"document/a": "n1"},
		err:    consistent.ErrNoRing, errAfter: 2,
	})

	stream := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupSubjectsResponse](t.Context())
	before := testutil.ToFloat64(ringFallbackTotal)
	require.NoError(t, cd.DispatchLookupSubjects(lsReq("a", "b", "c"), stream))
	require.InDelta(t, before+1, testutil.ToFloat64(ringFallbackTotal), 0)

	reqs := client.requests()
	require.Len(t, reqs, 1)
	require.Equal(t, []string{"a", "b", "c"}, reqs[0].ResourceIds, "no ID may be dropped")
	require.Equal(t, objectRoutingKey("document", "a"), client.routingKeys()[0])
	require.Equal(t, map[string]bool{"a": true, "b": true, "c": true}, foundResourceIDs(stream.Results()))
}

func TestLookupSubjectsRoutingOffUsesRequestKey(t *testing.T) {
	client := newFakeLookupSubjectsClient(map[string][]string{"a": {"evan"}, "b": {"tanner"}})
	view := &fakeRingView{owners: map[string]string{"document/a": "n1", "document/b": "n2"}}
	cd := newTestObjectRoutingDispatcher(t, client, view)
	cd.objectRouting = false

	stream := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupSubjectsResponse](t.Context())
	req := lsReq("a", "b")
	require.NoError(t, cd.DispatchLookupSubjects(req, stream))

	reqs := client.requests()
	require.Len(t, reqs, 1)
	require.Same(t, req, reqs[0], "flag-off path sends the request unchanged")
	require.Zero(t, view.calls, "flag-off path never consults the ring")

	wantKey, err := (&keys.DirectKeyHandler{}).LookupSubjectsDispatchKey(t.Context(), req)
	require.NoError(t, err)
	require.Equal(t, wantKey, client.routingKeys()[0])
	require.Len(t, stream.Results(), 2)
}

func TestLookupSubjectsGroupErrorFailsWholeCall(t *testing.T) {
	client := newFakeLookupSubjectsClient(map[string][]string{"a": {"evan"}, "b": {"tanner"}})
	client.failOn = "b"
	// The other owner group returns only after the failure cancels it.
	client.blockOn = "a"
	cd := newTestObjectRoutingDispatcher(t, client, &fakeRingView{owners: map[string]string{
		"document/a": "n1", "document/b": "n2",
	}})

	stream := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupSubjectsResponse](t.Context())
	err := cd.DispatchLookupSubjects(lsReq("a", "b"), stream)
	require.ErrorContains(t, err, "injected failure")
	require.NotErrorIs(t, err, context.Canceled, "the sibling's cancellation must not mask the original error")
	require.Len(t, client.requests(), 2)
}

// enableSpread turns on spread with the given share and a fresh estimator.
func enableSpread(t *testing.T, cd *clusterDispatcher, share float64) {
	t.Helper()
	cd.spreadShare = share
	cd.spread = 2
	est, err := frequency.NewEstimator(1<<20, time.Minute)
	require.NoError(t, err)
	t.Cleanup(est.Close)
	cd.spreadEstimator = est
}

// touchShare runs the share test for key in a new spread call.
func touchShare(cd *clusterDispatcher, key string) bool {
	return cd.passesShareTest([]byte(key), &spreadCall{})
}

func TestShareTestUniformTrafficNeverSpreads(t *testing.T) {
	cd := newTestObjectRoutingDispatcher(t, &fakeCheckClient{}, &fakeRingView{memberCount: 3})
	enableSpread(t, cd, 0.5)
	// Each of 50 keys has 2% of the traffic and passes the floor. The cutoff is 0.5/3.
	for round := range 300 {
		for k := range 50 {
			require.False(t, touchShare(cd, fmt.Sprintf("document/k%d", k)), "round %d key %d", round, k)
		}
	}
}

func TestShareTestDominantKeySpreads(t *testing.T) {
	cd := newTestObjectRoutingDispatcher(t, &fakeCheckClient{}, &fakeRingView{memberCount: 3})
	enableSpread(t, cd, 0.5)
	// The hot key has half of the traffic. The cutoff is 0.5/3.
	for i := range 1000 {
		hot := touchShare(cd, "document/hot")
		require.Equal(t, i+1 >= minSpreadCount, hot, "touch %d", i+1)
		require.False(t, touchShare(cd, fmt.Sprintf("document/cold%d", i)))
	}
}

func TestShareTestFloor(t *testing.T) {
	cd := newTestObjectRoutingDispatcher(t, &fakeCheckClient{}, &fakeRingView{memberCount: 2})
	enableSpread(t, cd, 0.5)
	// One key has all of the traffic, but it cannot spread below the floor.
	for i := range minSpreadCount - 1 {
		require.False(t, touchShare(cd, "document/only"), "touch %d", i+1)
	}
	require.True(t, touchShare(cd, "document/only"))
}

func TestShareTestMemberCountChangesCutoff(t *testing.T) {
	// The hot key has 30% of the traffic. With share 1, the cutoff is 1/N.
	run := func(members int) bool {
		cd := newTestObjectRoutingDispatcher(t, &fakeCheckClient{}, &fakeRingView{memberCount: members})
		enableSpread(t, cd, 1)
		var hot bool
		for i := range 1000 {
			if i%10 < 3 {
				hot = touchShare(cd, "document/hot")
			} else {
				touchShare(cd, fmt.Sprintf("document/cold%d", i))
			}
		}
		return hot
	}
	require.False(t, run(2), "30% is below 1/2")
	require.True(t, run(4), "30% is above 1/4")
}

func TestShareTestNoMembersNeverSpreads(t *testing.T) {
	cd := newTestObjectRoutingDispatcher(t, &fakeCheckClient{}, &fakeRingView{})
	enableSpread(t, cd, 0.5)
	for range 2 * minSpreadCount {
		require.False(t, touchShare(cd, "document/only"))
	}
}

func TestSaltKey(t *testing.T) {
	cold := objectRoutingKey("document", "foo")
	salts := map[byte]int{}
	for range 100 {
		key := saltKey(cold, 2)
		require.Len(t, key, len(cold)+2)
		require.Equal(t, cold, key[:len(cold)])
		require.Equal(t, byte('|'), key[len(cold)])
		salt := key[len(cold)+1]
		require.Less(t, salt, byte(2))
		salts[salt]++
	}
	require.Len(t, salts, 2, "traffic must actually spread across both peer positions")
}

func TestSaltKeyDoesNotAliasCallerSlice(t *testing.T) {
	backing := make([]byte, 0, 64)
	backing = append(backing, "document/foo"...)
	orig := slices.Clone(backing)
	got := saltKey(backing, 2)
	require.Equal(t, orig, backing)
	require.Zero(t, backing[:cap(backing)][len(orig)], "caller's spare capacity must be untouched")
	got[len(got)-1] = 0xff
	require.Equal(t, orig, backing)
}

func TestSpreadDisabledByDefault(t *testing.T) {
	cd := newTestObjectRoutingDispatcher(t, &fakeCheckClient{}, &fakeRingView{memberCount: 2})
	require.Nil(t, cd.spreadEstimator)
	require.Nil(t, cd.ownerLatency)
	for range 2 * minSpreadCount {
		require.False(t, touchShare(cd, "document/foo"))
	}
}

func TestSpreadEstimatorLifecycle(t *testing.T) {
	d, err := NewClusterDispatcher(&fakeCheckClient{}, newTestConn(t), ClusterDispatcherConfig{
		ObjectAffinityRouting: true,
		SpreadShare:           0.5,
		Spread:                2,
	}, nil, nil, 0)
	require.NoError(t, err)
	cd := d.(*clusterDispatcher)
	require.NotNil(t, cd.spreadEstimator)
	require.Nil(t, cd.ownerLatency, "a latency factor of 0 disables the gate and its tracking")
	require.NoError(t, cd.Close())

	d, err = NewClusterDispatcher(&fakeCheckClient{}, newTestConn(t), ClusterDispatcherConfig{
		SpreadShare:         0.5,
		Spread:              2,
		SpreadLatencyFactor: 1.5,
	}, nil, nil, 0)
	require.NoError(t, err)
	require.Nil(t, d.(*clusterDispatcher).spreadEstimator, "spread requires object routing")
	require.Nil(t, d.(*clusterDispatcher).ownerLatency, "latency tracking requires object routing")
	require.NoError(t, d.Close())

	d, err = NewClusterDispatcher(&fakeCheckClient{}, newTestConn(t), ClusterDispatcherConfig{
		ObjectAffinityRouting: true,
		Spread:                2,
		SpreadLatencyFactor:   1.5,
	}, nil, nil, 0)
	require.NoError(t, err)
	require.Nil(t, d.(*clusterDispatcher).spreadEstimator, "a share of 0 disables spread")
	require.Nil(t, d.(*clusterDispatcher).ownerLatency, "latency tracking requires spread")
	require.NoError(t, d.Close())
}

// hotOwnerView maps each unsalted key of ids to owner n0, and each salted key to n<salt>.
func hotOwnerView(ids []string) *fakeRingView {
	owners := map[string]string{}
	for _, id := range ids {
		base := string(objectRoutingKey("document", id))
		owners[base] = "n0"
		owners[base+"|\x00"] = "n0"
		owners[base+"|\x01"] = "n1"
	}
	return &fakeRingView{owners: owners, memberCount: 2}
}

func TestOwnerGroupsWithSpreadKeepsRoutingKeyOwner(t *testing.T) {
	// The salt selects the owner, so the routing key of each ID goes to the owner of its owner group.
	ids := []string{"a", "b", "c", "d", "e", "f", "g", "h"}
	cd := newTestObjectRoutingDispatcher(t, &fakeCheckClient{}, hotOwnerView(ids))
	// A share of 0.01 and a warm estimator make every key hot.
	enableSpread(t, cd, 0.01)
	for range minSpreadCount {
		for _, id := range ids {
			touchShare(cd, string(objectRoutingKey("document", id)))
		}
	}

	sawSalted := false
	for range 20 {
		groups, ok := cd.ownerGroups("document", ids)
		require.True(t, ok)
		require.LessOrEqual(t, len(groups), 2)
		seenOwners := map[string]bool{}
		var all []string
		for _, g := range groups {
			members, err := cd.ringView.FindN(g.routingKey, 1)
			require.NoError(t, err)
			owner := members[0].Key()
			require.Equal(t, owner, g.owner, "the group owner is the owner of its routing key")
			require.False(t, seenOwners[owner], "one group per owner")
			seenOwners[owner] = true
			require.Len(t, g.routingKey, len("document/a")+2, "every key is hot, so every routing key has a salt")
			sawSalted = true
			all = append(all, g.ids...)
		}
		slices.Sort(all)
		require.Equal(t, ids, all)
	}
	require.True(t, sawSalted)
}

// recordLatency adds count samples of d for owner.
func recordLatency(cd *clusterDispatcher, owner string, d time.Duration, count int) {
	for range count {
		cd.ownerLatency.record(owner, d)
	}
}

// newGatedDispatcher returns a dispatcher with spread, a latency gate of 1.5, and three owners.
// Owner n0 owns every unsalted key.
func newGatedDispatcher(t *testing.T, ids []string) *clusterDispatcher {
	t.Helper()
	view := hotOwnerView(ids)
	view.memberCount = 3
	cd := newTestObjectRoutingDispatcher(t, &fakeCheckClient{}, view)
	enableSpread(t, cd, 0.01)
	cd.spreadLatencyFactor = 1.5
	cd.ownerLatency = newOwnerLatencies(time.Minute, time.Now)
	return cd
}

// warm makes every key of ids pass the share test.
func warm(cd *clusterDispatcher, ids []string) {
	for range minSpreadCount {
		for _, id := range ids {
			touchShare(cd, string(objectRoutingKey("document", id)))
		}
	}
}

func isSalted(g ownerGroup) bool {
	return len(g.routingKey) == len("document/")+1+2
}

func TestOwnerGroupsLatencyGate(t *testing.T) {
	ids := []string{"a"}
	for _, tc := range []struct {
		name       string
		n0, n1, n2 time.Duration
		warm       bool
		wantSalted bool
	}{
		{"elevated owner allows spread", 10 * time.Millisecond, time.Millisecond, time.Millisecond, true, true},
		{"normal owner blocks spread", time.Millisecond, time.Millisecond, 10 * time.Millisecond, true, false},
		{"gate never spreads a cold key", 10 * time.Millisecond, time.Millisecond, time.Millisecond, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cd := newGatedDispatcher(t, ids)
			recordLatency(cd, "n0", tc.n0, minimumDigestCount)
			recordLatency(cd, "n1", tc.n1, minimumDigestCount)
			recordLatency(cd, "n2", tc.n2, minimumDigestCount)
			if tc.warm {
				warm(cd, ids)
			}
			blockedBefore := testutil.ToFloat64(spreadLatencyBlockedTotal)
			groups, ok := cd.ownerGroups("document", ids)
			require.True(t, ok)
			require.Len(t, groups, 1)
			require.Equal(t, tc.wantSalted, isSalted(groups[0]))
			wantBlocked := tc.warm && !tc.wantSalted
			require.Equal(t, wantBlocked, testutil.ToFloat64(spreadLatencyBlockedTotal) > blockedBefore)
		})
	}
}

func TestOwnerGroupsLatencyGateInsufficientSamplesFallsBack(t *testing.T) {
	ids := []string{"a"}
	cd := newGatedDispatcher(t, ids)
	// Only one owner has enough samples, so the share test alone decides.
	recordLatency(cd, "n0", time.Millisecond, minimumDigestCount)
	recordLatency(cd, "n1", time.Second, minimumDigestCount-1)
	warm(cd, ids)
	groups, ok := cd.ownerGroups("document", ids)
	require.True(t, ok)
	require.True(t, isSalted(groups[0]))
}

func TestOwnerGroupsLatencyFactorZeroDisablesGate(t *testing.T) {
	ids := []string{"a"}
	cd := newGatedDispatcher(t, ids)
	recordLatency(cd, "n0", time.Millisecond, minimumDigestCount)
	recordLatency(cd, "n1", time.Second, minimumDigestCount)
	recordLatency(cd, "n2", time.Second, minimumDigestCount)
	cd.spreadLatencyFactor = 0
	warm(cd, ids)
	groups, ok := cd.ownerGroups("document", ids)
	require.True(t, ok)
	require.True(t, isSalted(groups[0]), "a factor of 0 must not block a key that passes the share test")
}

func TestRoutedRequestsRecordOwnerLatency(t *testing.T) {
	owners := map[string]string{"document/a": "n1", "document/b": "n2", "document/c": "n1"}
	newDispatcher := func(client ClusterClient) *clusterDispatcher {
		cd := newTestObjectRoutingDispatcher(t, client, &fakeRingView{owners: owners, memberCount: 2})
		cd.ownerLatency = newOwnerLatencies(time.Minute, time.Now)
		return cd
	}

	t.Run("check", func(t *testing.T) {
		cd := newDispatcher(&fakeCheckClient{})
		for range 3 {
			_, err := cd.DispatchCheck(t.Context(), checkReq("a", "b", "c"))
			require.NoError(t, err)
		}
		require.Equal(t, map[string]uint64{"n1": 3, "n2": 3}, cd.ownerLatency.counts())

		_, err := cd.DispatchCheck(t.Context(), checkReq("a"))
		require.NoError(t, err)
		require.Equal(t, map[string]uint64{"n1": 4, "n2": 3}, cd.ownerLatency.counts())
	})

	t.Run("failed check records nothing", func(t *testing.T) {
		cd := newDispatcher(&fakeCheckClient{failOn: "b"})
		_, err := cd.DispatchCheck(t.Context(), checkReq("b"))
		require.Error(t, err)
		require.Empty(t, cd.ownerLatency.counts())
	})

	t.Run("lookup subjects", func(t *testing.T) {
		client := newFakeLookupSubjectsClient(map[string][]string{"a": {"evan"}, "b": {"tanner"}, "c": {"jake"}})
		cd := newDispatcher(client)
		stream := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupSubjectsResponse](t.Context())
		require.NoError(t, cd.DispatchLookupSubjects(lsReq("a", "b", "c"), stream))
		require.Equal(t, map[string]uint64{"n1": 1, "n2": 1}, cd.ownerLatency.counts())
	})

	t.Run("routing off records nothing", func(t *testing.T) {
		cd := newDispatcher(&fakeCheckClient{})
		cd.objectRouting = false
		_, err := cd.DispatchCheck(t.Context(), checkReq("a", "b"))
		require.NoError(t, err)
		require.Empty(t, cd.ownerLatency.counts())
	})
}

// errCheckClient fails every DispatchCheck with err.
type errCheckClient struct {
	fakeClusterClient
	err error
}

func (c errCheckClient) DispatchCheck(context.Context, *v1.DispatchCheckRequest, ...grpc.CallOption) (*v1.DispatchCheckResponse, error) {
	return nil, c.err
}

func TestDeadlineExceededRecordsOwnerLatency(t *testing.T) {
	owners := map[string]string{"document/a": "n1"}
	for _, tc := range []struct {
		name   string
		err    error
		record bool
	}{
		{"context deadline", context.DeadlineExceeded, true},
		{"grpc deadline", status.Error(codes.DeadlineExceeded, "deadline"), true},
		{"wrapped context deadline", fmt.Errorf("dispatch: %w", context.DeadlineExceeded), true},
		{"context canceled", context.Canceled, false},
		{"grpc canceled", status.Error(codes.Canceled, "canceled"), false},
		{"other error", errors.New("boom"), false},
	} {
		t.Run("check/"+tc.name, func(t *testing.T) {
			cd := newTestObjectRoutingDispatcher(t, errCheckClient{err: tc.err}, &fakeRingView{owners: owners, memberCount: 1})
			cd.ownerLatency = newOwnerLatencies(time.Minute, time.Now)
			_, err := cd.DispatchCheck(t.Context(), checkReq("a"))
			require.Error(t, err)
			if tc.record {
				require.Equal(t, map[string]uint64{"n1": 1}, cd.ownerLatency.counts())
			} else {
				require.Empty(t, cd.ownerLatency.counts())
			}
		})
		t.Run("lookup subjects/"+tc.name, func(t *testing.T) {
			client := newFakeLookupSubjectsClient(map[string][]string{"a": {"evan"}})
			client.failErr = tc.err
			cd := newTestObjectRoutingDispatcher(t, client, &fakeRingView{owners: owners, memberCount: 1})
			cd.ownerLatency = newOwnerLatencies(time.Minute, time.Now)
			stream := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupSubjectsResponse](t.Context())
			require.Error(t, cd.DispatchLookupSubjects(lsReq("a"), stream))
			if tc.record {
				require.Equal(t, map[string]uint64{"n1": 1}, cd.ownerLatency.counts())
			} else {
				require.Empty(t, cd.ownerLatency.counts())
			}
		})
	}
}

// BenchmarkOwnerGroupsHotKey measures ownerGroups for a chunk of 10 IDs with an active latency gate.
// Every ID passes the share test, and the gate blocks every spread, so each ID consults the gate.
func BenchmarkOwnerGroupsHotKey(b *testing.B) {
	ids := []string{"hot", "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9"}
	owners := map[string]string{}
	for _, id := range ids {
		base := string(objectRoutingKey("document", id))
		owners[base] = "n0"
		owners[base+"|\x00"] = "n0"
		owners[base+"|\x01"] = "n1"
	}
	view := &fakeRingView{owners: owners, memberCount: 3}
	d, err := NewClusterDispatcher(&fakeCheckClient{}, nil, ClusterDispatcherConfig{
		ObjectAffinityRouting: true,
		SpreadShare:           0.1,
		Spread:                2,
		SpreadLatencyFactor:   1.5,
	}, nil, nil, 0)
	require.NoError(b, err)
	cd := d.(*clusterDispatcher)
	b.Cleanup(func() { _ = cd.Close() })
	cd.ringView = view
	for _, owner := range []string{"n0", "n1", "n2"} {
		for range 1000 {
			cd.ownerLatency.record(owner, time.Millisecond)
		}
	}
	for range minSpreadCount {
		cd.ownerGroups("document", ids)
	}
	blockedBefore := testutil.ToFloat64(spreadLatencyBlockedTotal)

	b.ResetTimer()
	for b.Loop() {
		groups, ok := cd.ownerGroups("document", ids)
		if !ok || len(groups) == 0 {
			b.Fatal("no groups")
		}
	}
	b.StopTimer()
	require.Greater(b, testutil.ToFloat64(spreadLatencyBlockedTotal), blockedBefore, "the gate must run")
}
