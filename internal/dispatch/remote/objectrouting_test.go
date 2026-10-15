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

	"github.com/ccoveille/go-safecast/v2"
	"github.com/cespare/xxhash/v2"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

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
	reqs []*v1.DispatchCheckRequest // GUARDED_BY(mu)
	keys [][]byte                   // GUARDED_BY(mu)
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
			return &v1.DispatchCheckResponse{Metadata: &v1.ResponseMeta{}}, errors.New("injected failure")
		}
		results[id] = &v1.ResourceCheckResult{Membership: v1.ResourceCheckResult_MEMBER}
	}
	return &v1.DispatchCheckResponse{
		Metadata:            &v1.ResponseMeta{DispatchCount: 1, DepthRequired: safecast.MustConvert[uint32](len(req.ResourceIds))},
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

// objectRouterOf returns the object router of cd.
func objectRouterOf(t testing.TB, cd *clusterDispatcher) *objectRouter {
	t.Helper()
	r, ok := cd.router.(*objectRouter)
	require.True(t, ok, "the dispatcher must use an object router")
	return r
}

// newTestObjectRouter returns an object router for view with spread off.
// Its fallback is a requestHashRouter with a DirectKeyHandler.
func newTestObjectRouter(view consistent.RingView) *objectRouter {
	return &objectRouter{
		fallback: &requestHashRouter{keyHandler: &keys.DirectKeyHandler{}},
		ringView: view,
	}
}

func checkReq(ids ...string) *v1.DispatchCheckRequest {
	return &v1.DispatchCheckRequest{
		ResourceRelation: &corev1.RelationReference{Namespace: "document", Relation: "view"},
		ResourceIds:      ids,
		Subject:          &corev1.ObjectAndRelation{Namespace: "user", ObjectId: "evan", Relation: "..."},
		Metadata:         &v1.ResolverMeta{AtRevision: "1", DepthRemaining: 50},
	}
}

func TestObjectRoutingKey(t *testing.T) {
	require.Equal(t, dispatch.RoutingKey("document:a"), objectRoutingKey("document", "a"))
	require.Equal(t, dispatch.RoutingKey(":"), objectRoutingKey("", ""))
}

func TestObjectRoutingKeyDistinct(t *testing.T) {
	type object struct{ namespace, id string }
	for _, tc := range []struct {
		name string
		a, b object
	}{
		{"slash in namespace or id", object{"org/document", "a"}, object{"org", "document/a"}},
		{"two slashes", object{"abc/def/ghi", "x"}, object{"abc/def", "ghi/x"}},
		{"pipe in id", object{"document", "a|b"}, object{"document", "a"}},
		{"pipe and slash in id", object{"org/document", "a|b"}, object{"org", "document/a|b"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			keyA := objectRoutingKey(tc.a.namespace, tc.a.id)
			keyB := objectRoutingKey(tc.b.namespace, tc.b.id)
			require.NotEqual(t, keyA, keyB)
			for _, key := range [][]byte{keyA, keyB} {
				for range 10 {
					require.NotEqual(t, keyA, saltKey(key, 2), "a salted key never equals an unsalted key")
					require.NotEqual(t, keyB, saltKey(key, 2), "a salted key never equals an unsalted key")
				}
			}
		})
	}
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
	r := objectRouterOf(t, cd)
	require.NotNil(t, r.ringView)
	require.IsType(t, &requestHashRouter{}, r.fallback)
	require.InDelta(t, 0.5, r.spreadShare, 0)
	require.Equal(t, uint8(3), r.spread)
	require.InDelta(t, 1.5, r.spreadLatencyFactor, 0)
	require.NotNil(t, r.spreadEstimator)
	require.NotNil(t, r.ownerLatency)
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
	require.IsType(t, &requestHashRouter{}, d.(*clusterDispatcher).router)
	require.Zero(t, fb.calls)
}

// fakeLookupSubjectsClient records each DispatchLookupSubjects request and its routing key.
// Its streams yield the subjects for each resource ID and then io.EOF.
// A request that contains failOn returns an error.
type fakeLookupSubjectsClient struct {
	fakeClusterClient

	subjects map[string][]string
	failOn   string

	// failErr, if set, is the error for every stream.
	failErr error

	mu   sync.Mutex
	reqs []*v1.DispatchLookupSubjectsRequest // GUARDED_BY(mu)
	keys [][]byte                            // GUARDED_BY(mu)
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

	s := &fakeLSStream{err: f.failErr}
	for _, id := range req.ResourceIds {
		if id == f.failOn {
			s.err = errors.New("injected failure")
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

	responses []*v1.DispatchLookupSubjectsResponse
	err       error
}

func (s *fakeLSStream) Recv() (*v1.DispatchLookupSubjectsResponse, error) {
	if s.err != nil {
		return nil, s.err
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

// enableSpread enables spread with the given share and a new estimator.
func enableSpread(t *testing.T, r *objectRouter, share float64) {
	t.Helper()
	r.spreadShare = share
	r.spread = 2
	est, err := frequency.NewEstimator(1<<20, time.Minute)
	require.NoError(t, err)
	t.Cleanup(est.Close)
	r.spreadEstimator = est
}

// touchShare runs the share test for key in a new spread call.
func touchShare(r *objectRouter, key string) bool {
	return r.passesShareTest([]byte(key), &spreadCall{})
}

func TestShareTestUniformTrafficNeverSpreads(t *testing.T) {
	r := newTestObjectRouter(&fakeRingView{memberCount: 3})
	enableSpread(t, r, 0.5)
	// Each of 50 keys has 2% of the traffic and passes the floor. The cutoff is 0.5/3.
	for round := range 300 {
		for k := range 50 {
			require.False(t, touchShare(r, fmt.Sprintf("document:k%d", k)), "round %d key %d", round, k)
		}
	}
}

func TestShareTestDominantKeySpreads(t *testing.T) {
	r := newTestObjectRouter(&fakeRingView{memberCount: 3})
	enableSpread(t, r, 0.5)
	// The hot key has half of the traffic. The cutoff is 0.5/3.
	for i := range 1000 {
		hot := touchShare(r, "document:hot")
		require.Equal(t, i+1 >= minSpreadCount, hot, "touch %d", i+1)
		require.False(t, touchShare(r, fmt.Sprintf("document:cold%d", i)))
	}
}

func TestShareTestFloor(t *testing.T) {
	r := newTestObjectRouter(&fakeRingView{memberCount: 2})
	enableSpread(t, r, 0.5)
	// One key has all of the traffic, but it cannot spread below the floor.
	for i := range minSpreadCount - 1 {
		require.False(t, touchShare(r, "document:only"), "touch %d", i+1)
	}
	require.True(t, touchShare(r, "document:only"))
}

func TestShareTestMemberCountChangesCutoff(t *testing.T) {
	// The hot key has 30% of the traffic. With share 1, the cutoff is 1/N.
	run := func(members int) bool {
		r := newTestObjectRouter(&fakeRingView{memberCount: members})
		enableSpread(t, r, 1)
		var hot bool
		for i := range 1000 {
			if i%10 < 3 {
				hot = touchShare(r, "document:hot")
			} else {
				touchShare(r, fmt.Sprintf("document:cold%d", i))
			}
		}
		return hot
	}
	require.False(t, run(2), "30% is below 1/2")
	require.True(t, run(4), "30% is above 1/4")
}

func TestShareTestNoMembersNeverSpreads(t *testing.T) {
	r := newTestObjectRouter(&fakeRingView{})
	enableSpread(t, r, 0.5)
	for range 2 * minSpreadCount {
		require.False(t, touchShare(r, "document:only"))
	}
}

func TestSaltKey(t *testing.T) {
	cold := objectRoutingKey("document", "foo")
	salts := map[byte]int{}
	for range 100 {
		key := saltKey(cold, 2)
		require.Len(t, key, len(cold)+2)
		require.Equal(t, cold, key[:len(cold)])
		require.Equal(t, byte(saltSeparator), key[len(cold)])
		salt := key[len(cold)+1]
		require.Less(t, salt, byte(2))
		salts[salt]++
	}
	require.Len(t, salts, 2, "traffic must actually spread across both peer positions")
}

func TestSaltKeyDoesNotAliasCallerSlice(t *testing.T) {
	backing := make([]byte, 0, 64)
	backing = append(backing, "document:foo"...)
	orig := slices.Clone(backing)
	got := saltKey(backing, 2)
	require.Equal(t, orig, backing)
	require.Zero(t, backing[:cap(backing)][len(orig)], "caller's spare capacity must be untouched")
	got[len(got)-1] = 0xff
	require.Equal(t, orig, backing)
}

func TestSpreadDisabledByDefault(t *testing.T) {
	cd := newTestObjectRoutingDispatcher(t, &fakeCheckClient{}, &fakeRingView{memberCount: 2})
	r := objectRouterOf(t, cd)
	require.Nil(t, r.spreadEstimator)
	require.Nil(t, r.ownerLatency)
	for range 2 * minSpreadCount {
		require.False(t, touchShare(r, "document:foo"))
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
	r := objectRouterOf(t, cd)
	require.NotNil(t, r.spreadEstimator)
	require.Nil(t, r.ownerLatency, "a latency factor of 0 disables the gate and its tracking")
	require.NoError(t, cd.Close())

	d, err = NewClusterDispatcher(&fakeCheckClient{}, newTestConn(t), ClusterDispatcherConfig{
		SpreadShare:         0.5,
		Spread:              2,
		SpreadLatencyFactor: 1.5,
	}, nil, nil, 0)
	require.NoError(t, err)
	require.IsType(t, &requestHashRouter{}, d.(*clusterDispatcher).router,
		"spread and latency tracking require object routing")
	require.NoError(t, d.Close())

	d, err = NewClusterDispatcher(&fakeCheckClient{}, newTestConn(t), ClusterDispatcherConfig{
		ObjectAffinityRouting: true,
		Spread:                2,
		SpreadLatencyFactor:   1.5,
	}, nil, nil, 0)
	require.NoError(t, err)
	r = objectRouterOf(t, d.(*clusterDispatcher))
	require.Nil(t, r.spreadEstimator, "a share of 0 disables spread")
	require.Nil(t, r.ownerLatency, "latency tracking requires spread")
	require.NoError(t, d.Close())
}

// hotOwnerView maps each unsalted key of ids to owner n0, and each salted key to n<salt>.
func hotOwnerView(ids []string) *fakeRingView {
	owners := map[string]string{}
	for _, id := range ids {
		base := string(objectRoutingKey("document", id))
		owners[base] = "n0"
		owners[base+"!\x00"] = "n0"
		owners[base+"!\x01"] = "n1"
	}
	return &fakeRingView{owners: owners, memberCount: 2}
}

func TestOwnerGroupsWithSpreadKeepsRoutingKeyOwner(t *testing.T) {
	// The salt selects the owner, so the routing key of each ID goes to the owner of its owner group.
	ids := []string{"a", "b", "c", "d", "e", "f", "g", "h", "some-longer-document-id_with|chars=+"}
	r := newTestObjectRouter(hotOwnerView(ids))
	// A share of 0.01 and a warm estimator make every key hot.
	enableSpread(t, r, 0.01)
	for range minSpreadCount {
		for _, id := range ids {
			touchShare(r, string(objectRoutingKey("document", id)))
		}
	}

	sawSalted := false
	for range 20 {
		groups, ok := r.ownerGroups(t.Context(), "document", ids)
		require.True(t, ok)
		require.LessOrEqual(t, len(groups), 2)
		seenOwners := map[string]bool{}
		var all []string
		for _, g := range groups {
			members, err := r.ringView.FindN(g.routingKey, 1)
			require.NoError(t, err)
			owner := members[0].Key()
			require.Equal(t, owner, g.owner, "the group owner is the owner of its routing key")
			require.False(t, seenOwners[owner], "one group per owner")
			seenOwners[owner] = true
			require.Len(t, g.routingKey, len("document:a")+2, "every key is hot, so every routing key has a salt")
			sawSalted = true
			all = append(all, g.ids...)
		}
		slices.Sort(all)
		require.Equal(t, ids, all)
	}
	require.True(t, sawSalted)
}

// recordLatency adds count samples of d for owner.
func recordLatency(r *objectRouter, owner string, d time.Duration, count int) {
	for range count {
		r.ownerLatency.record(owner, d)
	}
}

// newGatedRouter returns an object router with spread, a latency gate of 1.5, and three owners.
// Owner n0 owns every unsalted key.
func newGatedRouter(t *testing.T, ids []string) *objectRouter {
	t.Helper()
	view := hotOwnerView(ids)
	view.memberCount = 3
	r := newTestObjectRouter(view)
	enableSpread(t, r, 0.01)
	r.spreadLatencyFactor = 1.5
	r.ownerLatency = newOwnerLatencies(time.Minute, time.Now)
	return r
}

// warm makes every key of ids pass the share test.
func warm(r *objectRouter, ids []string) {
	for range minSpreadCount {
		for _, id := range ids {
			touchShare(r, string(objectRoutingKey("document", id)))
		}
	}
}

func isSalted(g ownerGroup) bool {
	return len(g.routingKey) == len("document:")+1+2
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
			r := newGatedRouter(t, ids)
			recordLatency(r, "n0", tc.n0, minimumDigestCount)
			recordLatency(r, "n1", tc.n1, minimumDigestCount)
			recordLatency(r, "n2", tc.n2, minimumDigestCount)
			if tc.warm {
				warm(r, ids)
			}
			blockedBefore := testutil.ToFloat64(spreadLatencyBlockedTotal)
			groups, ok := r.ownerGroups(t.Context(), "document", ids)
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
	r := newGatedRouter(t, ids)
	// Only one owner has enough samples, so the share test alone decides.
	recordLatency(r, "n0", time.Millisecond, minimumDigestCount)
	recordLatency(r, "n1", time.Second, minimumDigestCount-1)
	warm(r, ids)
	groups, ok := r.ownerGroups(t.Context(), "document", ids)
	require.True(t, ok)
	require.True(t, isSalted(groups[0]))
}

func TestOwnerGroupsLatencyFactorZeroDisablesGate(t *testing.T) {
	ids := []string{"a"}
	r := newGatedRouter(t, ids)
	recordLatency(r, "n0", time.Millisecond, minimumDigestCount)
	recordLatency(r, "n1", time.Second, minimumDigestCount)
	recordLatency(r, "n2", time.Second, minimumDigestCount)
	r.spreadLatencyFactor = 0
	warm(r, ids)
	groups, ok := r.ownerGroups(t.Context(), "document", ids)
	require.True(t, ok)
	require.True(t, isSalted(groups[0]), "a factor of 0 must not block a key that passes the share test")
}

// errCheckClient fails every DispatchCheck with err.
type errCheckClient struct {
	fakeClusterClient
	err error
}

func (c errCheckClient) DispatchCheck(context.Context, *v1.DispatchCheckRequest, ...grpc.CallOption) (*v1.DispatchCheckResponse, error) {
	return &v1.DispatchCheckResponse{Metadata: &v1.ResponseMeta{}}, c.err
}

func TestDeadlineExceededRecordsOwnerLatency(t *testing.T) {
	owners := map[string]string{"document:a": "n1"}
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
			r := objectRouterOf(t, cd)
			r.ownerLatency = newOwnerLatencies(time.Minute, time.Now)
			_, err := cd.DispatchCheck(hinted(t.Context(), "a", "n1"), checkReq("a"))
			require.Error(t, err)
			if tc.record {
				require.Equal(t, map[string]uint64{"n1": 1}, r.ownerLatency.counts())
			} else {
				require.Empty(t, r.ownerLatency.counts())
			}
		})
		t.Run("lookup subjects/"+tc.name, func(t *testing.T) {
			client := newFakeLookupSubjectsClient(map[string][]string{"a": {"evan"}})
			client.failErr = tc.err
			cd := newTestObjectRoutingDispatcher(t, client, &fakeRingView{owners: owners, memberCount: 1})
			r := objectRouterOf(t, cd)
			r.ownerLatency = newOwnerLatencies(time.Minute, time.Now)
			stream := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupSubjectsResponse](hinted(t.Context(), "a", "n1"))
			require.Error(t, cd.DispatchLookupSubjects(lsReq("a"), stream))
			if tc.record {
				require.Equal(t, map[string]uint64{"n1": 1}, r.ownerLatency.counts())
			} else {
				require.Empty(t, r.ownerLatency.counts())
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
		owners[base+"!\x00"] = "n0"
		owners[base+"!\x01"] = "n1"
	}
	view := &fakeRingView{owners: owners, memberCount: 3}
	r := newObjectRouter(&requestHashRouter{keyHandler: &keys.DirectKeyHandler{}}, nil, ClusterDispatcherConfig{
		ObjectAffinityRouting: true,
		SpreadShare:           0.1,
		Spread:                2,
		SpreadLatencyFactor:   1.5,
	})
	b.Cleanup(r.Close)
	r.ringView = view
	for _, owner := range []string{"n0", "n1", "n2"} {
		for range 1000 {
			r.ownerLatency.record(owner, time.Millisecond)
		}
	}
	for range minSpreadCount {
		r.ownerGroups(b.Context(), "document", ids)
	}
	blockedBefore := testutil.ToFloat64(spreadLatencyBlockedTotal)

	b.ResetTimer()
	for b.Loop() {
		groups, ok := r.ownerGroups(b.Context(), "document", ids)
		if !ok || len(groups) == 0 {
			b.Fatal("no groups")
		}
	}
	b.StopTimer()
	require.Greater(b, testutil.ToFloat64(spreadLatencyBlockedTotal), blockedBefore, "the gate must run")
}

// hinted returns ctx with the hint for the object key of document id and owner.
func hinted(ctx context.Context, id, owner string) context.Context {
	return dispatch.WithRoutingHint(ctx, hint(id, owner))
}

func TestCheckRoutesByHint(t *testing.T) {
	client := &fakeCheckClient{}
	cd := newTestObjectRoutingDispatcher(t, client, &fakeRingView{owners: map[string]string{
		"document:a": "n1", "document:b": "n2",
	}})
	r := objectRouterOf(t, cd)
	r.ownerLatency = newOwnerLatencies(time.Minute, time.Now)

	req := checkReq("a", "b")
	resp, err := cd.DispatchCheck(hinted(t.Context(), "a", "n1"), req)
	require.NoError(t, err)
	require.Len(t, client.reqs, 1, "one RPC for each request, also when its IDs have different owners")
	require.Same(t, req, client.reqs[0])
	require.Equal(t, []byte(objectRoutingKey("document", "a")), client.keys[0])
	require.Len(t, resp.ResultsByResourceId, 2)
	require.Equal(t, map[string]uint64{"n1": 1}, r.ownerLatency.counts())
}

func TestCheckHintWithoutOwnerRoutesByKey(t *testing.T) {
	client := &fakeCheckClient{}
	cd := newTestObjectRoutingDispatcher(t, client, &fakeRingView{owners: map[string]string{"document:a": "n1"}})
	r := objectRouterOf(t, cd)
	r.ownerLatency = newOwnerLatencies(time.Minute, time.Now)

	ctx := hinted(t.Context(), "a", "")
	_, err := cd.DispatchCheck(ctx, checkReq("a"))
	require.NoError(t, err)
	require.Len(t, client.reqs, 1)
	require.Equal(t, []byte(objectRoutingKey("document", "a")), client.keys[0])
	require.Empty(t, r.ownerLatency.counts(), "an empty owner records no latency")
}

func TestCheckWithoutHintUsesRequestKey(t *testing.T) {
	client := &fakeCheckClient{}
	view := &fakeRingView{owners: map[string]string{"document:a": "n1", "document:b": "n2"}}
	cd := newTestObjectRoutingDispatcher(t, client, view)

	req := checkReq("a", "b")
	_, err := cd.DispatchCheck(t.Context(), req)
	require.NoError(t, err)
	require.Len(t, client.reqs, 1)
	wantKey, err := (&keys.DirectKeyHandler{}).CheckDispatchKey(t.Context(), req)
	require.NoError(t, err)
	require.Equal(t, wantKey, client.keys[0])
	require.Zero(t, view.calls, "the remote dispatcher does not look up the ring")
}

func TestRoutingOffIgnoresHint(t *testing.T) {
	client := &fakeCheckClient{}
	cd := newTestObjectRoutingDispatcher(t, client, &fakeRingView{})
	cd.router = objectRouterOf(t, cd).fallback

	req := checkReq("a")
	_, err := cd.DispatchCheck(hinted(t.Context(), "a", "n1"), req)
	require.NoError(t, err)
	wantKey, err := (&keys.DirectKeyHandler{}).CheckDispatchKey(t.Context(), req)
	require.NoError(t, err)
	require.Equal(t, wantKey, client.keys[0])

	lsClient := newFakeLookupSubjectsClient(map[string][]string{"a": {"evan"}})
	lsDispatcher := newTestObjectRoutingDispatcher(t, lsClient, &fakeRingView{})
	lsDispatcher.router = objectRouterOf(t, lsDispatcher).fallback
	ls := lsReq("a")
	stream := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupSubjectsResponse](hinted(t.Context(), "a", "n1"))
	require.NoError(t, lsDispatcher.DispatchLookupSubjects(ls, stream))
	wantLSKey, err := (&keys.DirectKeyHandler{}).LookupSubjectsDispatchKey(t.Context(), ls)
	require.NoError(t, err)
	require.Equal(t, wantLSKey, lsClient.routingKeys()[0])
}

// debugCheckClient answers each DispatchCheck with a fixed debug trace and records the routing key.
type debugCheckClient struct {
	fakeClusterClient

	trace *v1.DebugInformation

	mu   sync.Mutex
	keys [][]byte // GUARDED_BY(mu)
}

func (c *debugCheckClient) DispatchCheck(ctx context.Context, _ *v1.DispatchCheckRequest, _ ...grpc.CallOption) (*v1.DispatchCheckResponse, error) {
	key, _ := ctx.Value(consistent.CtxKey).([]byte)
	c.mu.Lock()
	c.keys = append(c.keys, key)
	c.mu.Unlock()
	return &v1.DispatchCheckResponse{
		Metadata: &v1.ResponseMeta{DispatchCount: 1, DebugInfo: c.trace.CloneVT()},
		ResultsByResourceId: map[string]*v1.ResourceCheckResult{
			"a": {Membership: v1.ResourceCheckResult_MEMBER},
		},
	}, nil
}

func TestDebugCheckRoutesByHintAndKeepsTrace(t *testing.T) {
	trace := &v1.DebugInformation{Check: &v1.CheckDebugTrace{TraceId: "trace-1", SourceId: "n1"}}
	client := &debugCheckClient{trace: trace}
	cd := newTestObjectRoutingDispatcher(t, client, &fakeRingView{})

	req := checkReq("a", "b")
	req.Debug = v1.DispatchCheckRequest_ENABLE_BASIC_DEBUGGING
	resp, err := cd.DispatchCheck(hinted(t.Context(), "a", "n1"), req)
	require.NoError(t, err)
	require.Len(t, client.keys, 1)
	require.Equal(t, []byte(objectRoutingKey("document", "a")), client.keys[0], "a debug request routes by its hint")
	require.True(t, proto.Equal(trace, resp.Metadata.DebugInfo), "the trace comes back unchanged")
}

func TestLookupSubjectsRoutesByHint(t *testing.T) {
	client := newFakeLookupSubjectsClient(map[string][]string{"a": {"evan"}, "b": {"tanner"}})
	cd := newTestObjectRoutingDispatcher(t, client, &fakeRingView{owners: map[string]string{
		"document:a": "n1", "document:b": "n2",
	}})
	r := objectRouterOf(t, cd)
	r.ownerLatency = newOwnerLatencies(time.Minute, time.Now)

	stream := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupSubjectsResponse](hinted(t.Context(), "a", "n1"))
	req := lsReq("a", "b")
	require.NoError(t, cd.DispatchLookupSubjects(req, stream))

	reqs := client.requests()
	require.Len(t, reqs, 1, "one stream for each request, also when its IDs have different owners")
	require.Same(t, req, reqs[0])
	require.Equal(t, []byte(objectRoutingKey("document", "a")), client.routingKeys()[0])
	require.Equal(t, map[string]bool{"a": true, "b": true}, foundResourceIDs(stream.Results()))
	require.Equal(t, map[string]uint64{"n1": 1}, r.ownerLatency.counts())
}

func TestLookupSubjectsWithoutHintUsesRequestKey(t *testing.T) {
	client := newFakeLookupSubjectsClient(map[string][]string{"a": {"evan"}})
	view := &fakeRingView{owners: map[string]string{"document:a": "n1"}}
	cd := newTestObjectRoutingDispatcher(t, client, view)

	stream := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupSubjectsResponse](t.Context())
	req := lsReq("a")
	require.NoError(t, cd.DispatchLookupSubjects(req, stream))
	wantKey, err := (&keys.DirectKeyHandler{}).LookupSubjectsDispatchKey(t.Context(), req)
	require.NoError(t, err)
	require.Equal(t, wantKey, client.routingKeys()[0])
	require.Zero(t, view.calls, "the remote dispatcher does not look up the ring")
}

func TestHintedRequestsRecordOwnerLatency(t *testing.T) {
	owners := map[string]string{"document:a": "n1", "document:b": "n2", "document:c": "n1"}
	newDispatcher := func(client ClusterClient) (*clusterDispatcher, *objectRouter) {
		cd := newTestObjectRoutingDispatcher(t, client, &fakeRingView{owners: owners, memberCount: 2})
		r := objectRouterOf(t, cd)
		r.ownerLatency = newOwnerLatencies(time.Minute, time.Now)
		return cd, r
	}

	t.Run("check", func(t *testing.T) {
		cd, r := newDispatcher(&fakeCheckClient{})
		for _, chunk := range cd.router.Chunk(t.Context(), "document", []string{"a", "b", "c"}, 100) {
			_, err := cd.DispatchCheck(dispatch.WithRoutingHint(t.Context(), chunk.Hint), checkReq(chunk.ResourceIDs...))
			require.NoError(t, err)
		}
		require.Equal(t, map[string]uint64{"n1": 1, "n2": 1}, r.ownerLatency.counts())
	})

	t.Run("failed check records nothing", func(t *testing.T) {
		cd, r := newDispatcher(&fakeCheckClient{failOn: "b"})
		_, err := cd.DispatchCheck(hinted(t.Context(), "b", "n2"), checkReq("b"))
		require.Error(t, err)
		require.Empty(t, r.ownerLatency.counts())
	})

	t.Run("lookup subjects", func(t *testing.T) {
		client := newFakeLookupSubjectsClient(map[string][]string{"a": {"evan"}, "b": {"tanner"}, "c": {"jake"}})
		cd, r := newDispatcher(client)
		for _, chunk := range cd.router.Chunk(t.Context(), "document", []string{"a", "b", "c"}, 100) {
			stream := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupSubjectsResponse](dispatch.WithRoutingHint(t.Context(), chunk.Hint))
			require.NoError(t, cd.DispatchLookupSubjects(lsReq(chunk.ResourceIDs...), stream))
		}
		require.Equal(t, map[string]uint64{"n1": 1, "n2": 1}, r.ownerLatency.counts())
	})

	t.Run("no hint records nothing", func(t *testing.T) {
		cd, r := newDispatcher(&fakeCheckClient{})
		_, err := cd.DispatchCheck(t.Context(), checkReq("a", "b"))
		require.NoError(t, err)
		require.Empty(t, r.ownerLatency.counts())
	})

	t.Run("routing off records nothing", func(t *testing.T) {
		cd, r := newDispatcher(&fakeCheckClient{})
		cd.router = r.fallback
		_, err := cd.DispatchCheck(hinted(t.Context(), "a", "n1"), checkReq("a"))
		require.NoError(t, err)
		require.Empty(t, r.ownerLatency.counts())
	})
}

func TestRingChangeAfterChunkingKeepsHint(t *testing.T) {
	view := &fakeRingView{owners: map[string]string{"document:a": "n1", "document:b": "n1"}, memberCount: 2}
	client := &fakeCheckClient{}
	cd := newTestObjectRoutingDispatcher(t, client, view)
	r := objectRouterOf(t, cd)
	r.ownerLatency = newOwnerLatencies(time.Minute, time.Now)

	chunks := r.Chunk(t.Context(), "document", []string{"a", "b"}, 100)
	require.Len(t, chunks, 1)

	// The ring changes after chunking and before the RPC.
	view.owners["document:a"] = "n2"
	lookupsBefore := view.calls

	_, err := cd.DispatchCheck(dispatch.WithRoutingHint(t.Context(), chunks[0].Hint), checkReq(chunks[0].ResourceIDs...))
	require.NoError(t, err, "a moved ID loses locality only")
	require.Len(t, client.reqs, 1)
	require.Equal(t, []byte(objectRoutingKey("document", "a")), client.keys[0], "the balancer gets the hinted key")
	require.Equal(t, lookupsBefore, view.calls, "the remote dispatcher does not look up the ring again")
	require.Equal(t, map[string]uint64{"n1": 1}, r.ownerLatency.counts(), "the latency counts for the hinted owner")
}

func TestChunkThenDispatchTouchesEachKeyOnce(t *testing.T) {
	client := &fakeCheckClient{}
	cd := newTestObjectRoutingDispatcher(t, client, &fakeRingView{owners: map[string]string{
		"document:a": "n1", "document:b": "n2", "document:c": "n1",
	}, memberCount: 2})
	counting := countTouches(t, objectRouterOf(t, cd))

	for _, chunk := range cd.router.Chunk(t.Context(), "document", []string{"a", "b", "c"}, 100) {
		_, err := cd.DispatchCheck(dispatch.WithRoutingHint(t.Context(), chunk.Hint), checkReq(chunk.ResourceIDs...))
		require.NoError(t, err)
	}
	require.Equal(t, map[string]int{"document:a": 1, "document:b": 1, "document:c": 1}, counting.counts(),
		"the chunker touches each key, and the remote dispatcher does not")
}
