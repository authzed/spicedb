package remote

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/dispatch/keys"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

// fakeRouter returns fixed keys and owners and counts its key calls.
type fakeRouter struct {
	checkCalls          int
	lookupSubjectsCalls int
}

var _ router = (*fakeRouter)(nil)

func (f *fakeRouter) Chunk(context.Context, string, []string, uint16) []dispatch.Chunk {
	return nil
}

func (f *fakeRouter) checkKey(context.Context, *v1.DispatchCheckRequest) (dispatch.RoutingKey, string, error) {
	f.checkCalls++
	return dispatch.RoutingKey("fake-check"), "fake-owner", nil
}

func (f *fakeRouter) lookupSubjectsKey(context.Context, *v1.DispatchLookupSubjectsRequest) (dispatch.RoutingKey, string, error) {
	f.lookupSubjectsCalls++
	return dispatch.RoutingKey("fake-lookup-subjects"), "fake-owner", nil
}

func (f *fakeRouter) latencyObserver(string) func(time.Duration) {
	return nil
}

func (f *fakeRouter) Close() {}

func TestObjectRouterWithoutHintUsesFallback(t *testing.T) {
	fallback := &fakeRouter{}
	r := &objectRouter{fallback: fallback, ringView: &fakeRingView{}}

	key, owner, err := r.checkKey(t.Context(), checkReq("a"))
	require.NoError(t, err)
	require.Equal(t, dispatch.RoutingKey("fake-check"), key)
	require.Equal(t, "fake-owner", owner)
	require.Equal(t, 1, fallback.checkCalls)

	key, owner, err = r.lookupSubjectsKey(t.Context(), lsReq("a"))
	require.NoError(t, err)
	require.Equal(t, dispatch.RoutingKey("fake-lookup-subjects"), key)
	require.Equal(t, "fake-owner", owner)
	require.Equal(t, 1, fallback.lookupSubjectsCalls)

	ctx := hinted(t.Context(), "a", "n1")
	key, owner, err = r.checkKey(ctx, checkReq("a"))
	require.NoError(t, err)
	require.Equal(t, objectRoutingKey("document", "a"), key)
	require.Equal(t, "n1", owner)
	require.Equal(t, 1, fallback.checkCalls, "a hinted dispatch does not use the fallback")
}

func TestRequestHashRouter(t *testing.T) {
	r := &requestHashRouter{keyHandler: &keys.DirectKeyHandler{}}
	ctx := hinted(t.Context(), "a", "n1")

	req := checkReq("a")
	key, owner, err := r.checkKey(ctx, req)
	require.NoError(t, err)
	wantKey, err := (&keys.DirectKeyHandler{}).CheckDispatchKey(t.Context(), req)
	require.NoError(t, err)
	require.Equal(t, dispatch.RoutingKey(wantKey), key, "the request hash router ignores the hint")
	require.Empty(t, owner)

	ls := lsReq("a")
	key, owner, err = r.lookupSubjectsKey(ctx, ls)
	require.NoError(t, err)
	wantKey, err = (&keys.DirectKeyHandler{}).LookupSubjectsDispatchKey(t.Context(), ls)
	require.NoError(t, err)
	require.Equal(t, dispatch.RoutingKey(wantKey), key)
	require.Empty(t, owner)

	require.Nil(t, r.latencyObserver("n1"))
	require.Nil(t, r.latencyObserver(""))
}
