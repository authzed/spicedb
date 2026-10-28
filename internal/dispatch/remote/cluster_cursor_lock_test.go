package remote

import (
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/dispatch/keys"
	"github.com/authzed/spicedb/pkg/datalayer"
	corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

// cursorEchoDispatchSvc returns a single result whose cursor is the received cursor with its name appended.
type cursorEchoDispatchSvc struct {
	v1.UnimplementedDispatchServiceServer

	name      string
	sleepTime time.Duration

	lock            sync.Mutex
	receivedCursors [][]string // GUARDED_BY(lock)
}

func (s *cursorEchoDispatchSvc) record(cursor []string) []string {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.receivedCursors = append(s.receivedCursors, slices.Clone(cursor))
	return append(slices.Clone(cursor), s.name)
}

func (s *cursorEchoDispatchSvc) received() [][]string {
	s.lock.Lock()
	defer s.lock.Unlock()
	return slices.Clone(s.receivedCursors)
}

func (s *cursorEchoDispatchSvc) reset() {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.receivedCursors = nil
}

func (s *cursorEchoDispatchSvc) DispatchLookupResources3(req *v1.DispatchLookupResources3Request, srv v1.DispatchService_DispatchLookupResources3Server) error {
	cursor := s.record(req.OptionalCursor)
	time.Sleep(s.sleepTime)
	return srv.Send(&v1.DispatchLookupResources3Response{
		Items: []*v1.LR3Item{{ResourceId: s.name, AfterResponseCursorSections: cursor}},
	})
}

func (s *cursorEchoDispatchSvc) DispatchLookupResources2(req *v1.DispatchLookupResources2Request, srv v1.DispatchService_DispatchLookupResources2Server) error {
	cursor := s.record(req.OptionalCursor.GetSections())
	time.Sleep(s.sleepTime)
	return srv.Send(&v1.DispatchLookupResources2Response{
		Resource:            &v1.PossibleResource{ResourceId: s.name},
		Metadata:            &v1.ResponseMeta{DispatchCount: 1},
		AfterResponseCursor: &v1.Cursor{Sections: cursor, DispatchVersion: 1},
	})
}

func newCursorEchoDispatcher(t *testing.T) (dispatch.Dispatcher, *cursorEchoDispatchSvc, *cursorEchoDispatchSvc) {
	t.Helper()

	primary := &cursorEchoDispatchSvc{name: "primary", sleepTime: 100 * time.Millisecond}
	secondary := &cursorEchoDispatchSvc{name: "secondary"}
	primaryConn := connectionForDispatching(t, primary)
	secondaryConn := connectionForDispatching(t, secondary)

	parsed, err := ParseDispatchExpression("lookupresources", "['secondary']")
	require.NoError(t, err)

	dispatcher, err := NewClusterDispatcher(v1.NewDispatchServiceClient(primaryConn), primaryConn, ClusterDispatcherConfig{
		KeyHandler:             &keys.DirectKeyHandler{},
		DispatchOverallTimeout: 30 * time.Second,
	}, map[string]SecondaryDispatch{
		"secondary": {Name: "secondary", Client: v1.NewDispatchServiceClient(secondaryConn), MaximumPrimaryHedgingDelay: 5 * time.Millisecond},
	}, map[string]*DispatchExpr{
		"lookupresources": parsed,
	}, 0*time.Second)
	require.NoError(t, err)

	return dispatcher, primary, secondary
}

func lr3Page(t *testing.T, dispatcher dispatch.Dispatcher, cursor []string) *v1.LR3Item {
	t.Helper()

	stream := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupResources3Response](t.Context())
	err := dispatcher.DispatchLookupResources3(&v1.DispatchLookupResources3Request{
		ResourceRelation: &corev1.RelationReference{Namespace: "document", Relation: "view"},
		SubjectRelation:  &corev1.RelationReference{Namespace: "user", Relation: "..."},
		SubjectIds:       []string{"subject"},
		TerminalSubject:  &corev1.ObjectAndRelation{Namespace: "user", ObjectId: "subject", Relation: "..."},
		Metadata: &v1.ResolverMeta{
			AtRevision: "1", DepthRemaining: 50, SchemaHash: []byte(datalayer.NoSchemaHashForTesting),
		},
		OptionalCursor: cursor,
		OptionalLimit:  1,
	}, stream)
	require.NoError(t, err)
	require.Len(t, stream.Results(), 1)
	require.Len(t, stream.Results()[0].Items, 1)
	return stream.Results()[0].Items[0]
}

func lr2Page(t *testing.T, dispatcher dispatch.Dispatcher, cursor []string) *v1.DispatchLookupResources2Response {
	t.Helper()

	var optionalCursor *v1.Cursor
	if cursor != nil {
		optionalCursor = &v1.Cursor{Sections: cursor, DispatchVersion: 1}
	}

	stream := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupResources2Response](t.Context())
	err := dispatcher.DispatchLookupResources2(&v1.DispatchLookupResources2Request{
		ResourceRelation: &corev1.RelationReference{Namespace: "document", Relation: "view"},
		SubjectRelation:  &corev1.RelationReference{Namespace: "user", Relation: "..."},
		SubjectIds:       []string{"subject"},
		TerminalSubject:  &corev1.ObjectAndRelation{Namespace: "user", ObjectId: "subject", Relation: "..."},
		Metadata: &v1.ResolverMeta{
			AtRevision: "1", DepthRemaining: 50, SchemaHash: []byte(datalayer.NoSchemaHashForTesting),
		},
		OptionalCursor: optionalCursor,
		OptionalLimit:  1,
	}, stream)
	require.NoError(t, err)
	require.Len(t, stream.Results(), 1)
	return stream.Results()[0]
}

func TestLR3CursorLockedToProducingDispatcher(t *testing.T) {
	t.Parallel()

	dispatcher, primary, secondary := newCursorEchoDispatcher(t)

	// Without a cursor, the secondary races the slower primary and wins; its cursor is tagged at the head.
	first := lr3Page(t, dispatcher, nil)
	require.Equal(t, "secondary", first.ResourceId)
	require.Equal(t, []string{"secondary", secondaryCursorPrefix + "secondary"}, first.AfterResponseCursorSections)

	// The next page goes only to the secondary, with the tag stripped.
	primary.reset()
	secondary.reset()
	second := lr3Page(t, dispatcher, first.AfterResponseCursorSections)
	require.Equal(t, "secondary", second.ResourceId)
	require.Equal(t, [][]string{{"secondary"}}, secondary.received())
	require.Empty(t, primary.received())

	// An untagged cursor was produced by the primary and must never reach a secondary.
	primary.reset()
	secondary.reset()
	fromPrimary := lr3Page(t, dispatcher, []string{"primary"})
	require.Equal(t, "primary", fromPrimary.ResourceId)
	require.Equal(t, []string{"primary", "primary"}, fromPrimary.AfterResponseCursorSections)
	require.Equal(t, [][]string{{"primary"}}, primary.received())
	require.Empty(t, secondary.received())

	// A secondary-produced sub-cursor nested below a primary head stays with the primary, untouched.
	primary.reset()
	secondary.reset()
	nested := []string{"secondary", secondaryCursorPrefix + "secondary", "outer"}
	lr3Page(t, dispatcher, nested)
	require.Equal(t, [][]string{nested}, primary.received())
	require.Empty(t, secondary.received())
}

func TestLR2CursorLockedToProducingDispatcher(t *testing.T) {
	t.Parallel()

	dispatcher, primary, secondary := newCursorEchoDispatcher(t)

	first := lr2Page(t, dispatcher, nil)
	require.Equal(t, "secondary", first.Resource.ResourceId)
	require.Equal(t, []string{secondaryCursorPrefix + "secondary", "secondary"}, first.AfterResponseCursor.Sections)

	primary.reset()
	secondary.reset()
	second := lr2Page(t, dispatcher, first.AfterResponseCursor.Sections)
	require.Equal(t, "secondary", second.Resource.ResourceId)
	require.Equal(t, [][]string{{"secondary"}}, secondary.received())
	require.Empty(t, primary.received())

	primary.reset()
	secondary.reset()
	fromPrimary := lr2Page(t, dispatcher, []string{"primary"})
	require.Equal(t, "primary", fromPrimary.Resource.ResourceId)
	require.Equal(t, []string{"primary", "primary"}, fromPrimary.AfterResponseCursor.Sections)
	require.Equal(t, [][]string{{"primary"}}, primary.received())
	require.Empty(t, secondary.received())
}
