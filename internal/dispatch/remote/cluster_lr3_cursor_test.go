package remote

import (
	"fmt"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/pkg/datalayer"
	corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

// cursorDispatchSvc uses incompatible cursors for each backend, as secondary
// dispatchers need not use the primary's pagination implementation.
type cursorDispatchSvc struct {
	v1.UnimplementedDispatchServiceServer

	name     string
	mu       sync.Mutex
	requests [][]string
	err      error
}

func (s *cursorDispatchSvc) DispatchLookupResources3(req *v1.DispatchLookupResources3Request, stream v1.DispatchService_DispatchLookupResources3Server) error {
	s.mu.Lock()
	s.requests = append(s.requests, slices.Clone(req.OptionalCursor))
	err := s.err
	s.mu.Unlock()
	if err != nil {
		return err
	}

	offset := 0
	if len(req.OptionalCursor) > 0 {
		// The leading section resembles a nested secondary cursor. Only the
		// outermost routing section at the end should be consumed.
		if len(req.OptionalCursor) != 3 || req.OptionalCursor[0] != secondaryCursorPrefix+"nested" || req.OptionalCursor[1] != s.name {
			return status.Error(codes.InvalidArgument, "cursor belongs to another dispatcher")
		}
		offset, err = strconv.Atoi(req.OptionalCursor[2])
		if err != nil {
			return status.Error(codes.InvalidArgument, "invalid cursor offset")
		}
	}

	items := make([]*v1.LR3Item, 0, req.OptionalLimit)
	for i := offset; i < min(offset+int(req.OptionalLimit), 5); i++ {
		items = append(items, &v1.LR3Item{
			ResourceId:                  fmt.Sprintf("%s-%d", s.name, i),
			AfterResponseCursorSections: []string{secondaryCursorPrefix + "nested", s.name, strconv.Itoa(i + 1)},
		})
	}
	if len(items) == 0 {
		return nil
	}
	return stream.Send(&v1.DispatchLookupResources3Response{Items: items})
}

func (s *cursorDispatchSvc) setError(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.err = err
}

func (s *cursorDispatchSvc) receivedCursors() [][]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.requests)
}

func lr3CursorRequest(cursor []string) *v1.DispatchLookupResources3Request {
	return &v1.DispatchLookupResources3Request{
		ResourceRelation: &corev1.RelationReference{Namespace: "document", Relation: "view"},
		SubjectRelation:  &corev1.RelationReference{Namespace: "user", Relation: "..."},
		SubjectIds:       []string{"alice"},
		TerminalSubject:  &corev1.ObjectAndRelation{Namespace: "user", ObjectId: "alice", Relation: "..."},
		Metadata:         &v1.ResolverMeta{DepthRemaining: 50, SchemaHash: []byte(datalayer.NoSchemaHashForTesting)},
		OptionalLimit:    2,
		OptionalCursor:   cursor,
	}
}

func newLR3CursorDispatcher(t *testing.T, primary, secondary *cursorDispatchSvc, enableCursorRouting bool) *clusterDispatcher {
	t.Helper()
	conn := connectionForDispatching(t, primary)
	secondaryConn := connectionForDispatching(t, secondary)
	expr, err := ParseDispatchExpression("lookupresources", "['secondary']")
	require.NoError(t, err)
	dispatcher, err := NewClusterDispatcher(v1.NewDispatchServiceClient(conn), conn, ClusterDispatcherConfig{EnableLookupResources3CursorRouting: enableCursorRouting},
		map[string]SecondaryDispatch{
			"secondary": {Name: "secondary", Client: v1.NewDispatchServiceClient(secondaryConn), MaximumPrimaryHedgingDelay: time.Second},
		}, map[string]*DispatchExpr{"lookupresources": expr}, time.Second)
	require.NoError(t, err)
	return dispatcher.(*clusterDispatcher)
}

func TestLR3CursorPinsWinningDispatcher(t *testing.T) {
	for _, winner := range []string{"primary", "secondary"} {
		t.Run(winner, func(t *testing.T) {
			primary := &cursorDispatchSvc{name: "primary"}
			secondary := &cursorDispatchSvc{name: "secondary"}
			if winner == "primary" {
				secondary.setError(status.Error(codes.Unavailable, "not ready"))
			}
			dispatcher := newLR3CursorDispatcher(t, primary, secondary, true)

			firstPage := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupResources3Response](t.Context())
			require.NoError(t, dispatcher.DispatchLookupResources3(lr3CursorRequest(nil), firstPage))
			require.Len(t, firstPage.Results(), 1)
			items := firstPage.Results()[0].Items
			require.Len(t, items, 2)
			require.Equal(t, winner+"-0", items[0].ResourceId)
			require.Equal(t, winner+"-1", items[1].ResourceId)

			// Change routing after the first page. The primary's secondary is now
			// healthy; the secondary's expression is removed altogether.
			secondary.setError(nil)
			if winner == "secondary" {
				dispatcher.secondaryDispatchExprs = nil
			}
			primaryBefore := len(primary.receivedCursors())
			secondaryBefore := len(secondary.receivedCursors())
			marker := primaryCursorSection
			if winner == "secondary" {
				marker = secondaryCursorPrefix + "secondary"
			}

			// Resume every item in the batch, not just the last one. Reuse each
			// request concurrently to detect mutation of caller-owned cursors.
			for i, item := range items {
				require.Equal(t, []string{secondaryCursorPrefix + "nested", winner, strconv.Itoa(i + 1), marker}, item.AfterResponseCursorSections)
				req := lr3CursorRequest(item.AfterResponseCursorSections)
				original := proto.Clone(req)
				var wg sync.WaitGroup
				for range 2 {
					wg.Go(func() {
						page := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupResources3Response](t.Context())
						if err := dispatcher.DispatchLookupResources3(req, page); err != nil {
							t.Error(err)
							return
						}
						results := page.Results()
						if len(results) != 1 || len(results[0].Items) != 2 {
							t.Errorf("unexpected page: %v", results)
							return
						}
						for j, nextItem := range results[0].Items {
							if nextItem.ResourceId != fmt.Sprintf("%s-%d", winner, i+j+1) {
								t.Errorf("unexpected resource: %s", nextItem.ResourceId)
							}
							if !slices.Equal(nextItem.AfterResponseCursorSections, []string{secondaryCursorPrefix + "nested", winner, strconv.Itoa(i + j + 2), marker}) {
								t.Errorf("unexpected cursor: %v", nextItem.AfterResponseCursorSections)
							}
						}
					})
				}
				wg.Wait()
				require.True(t, proto.Equal(original, req), "caller-owned request was mutated")
			}

			// Finish pagination, including a final empty page, to ensure backend
			// pinning neither skips nor repeats resources at page boundaries.
			resourceIDs := []string{items[0].ResourceId, items[1].ResourceId}
			cursor := items[1].AfterResponseCursorSections
			for range 3 {
				page := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupResources3Response](t.Context())
				require.NoError(t, dispatcher.DispatchLookupResources3(lr3CursorRequest(cursor), page))
				if len(page.Results()) == 0 {
					break
				}
				for _, response := range page.Results() {
					for _, item := range response.Items {
						resourceIDs = append(resourceIDs, item.ResourceId)
						cursor = item.AfterResponseCursorSections
					}
				}
			}
			require.Equal(t, []string{winner + "-0", winner + "-1", winner + "-2", winner + "-3", winner + "-4"}, resourceIDs)
			if winner == "primary" {
				require.Len(t, primary.receivedCursors(), primaryBefore+7)
				require.Len(t, secondary.receivedCursors(), secondaryBefore)
			} else {
				require.Len(t, primary.receivedCursors(), primaryBefore)
				require.Len(t, secondary.receivedCursors(), secondaryBefore+7)
			}
		})
	}
}

func TestLR3CursorOwnerUnavailable(t *testing.T) {
	for _, tc := range []struct {
		name                   string
		owner                  string
		removeSecondaries      bool
		secondaryError         error
		expectedError          string
		expectedSecondaryCalls int
	}{
		{name: "unknown", owner: "unknown", expectedError: "cursor locked to unknown secondary dispatcher"},
		{name: "empty owner", expectedError: "cursor locked to unknown secondary dispatcher"},
		{name: "removed", owner: "secondary", removeSecondaries: true, expectedError: "cursor locked to unknown secondary dispatcher"},
		{name: "unavailable", owner: "secondary", secondaryError: status.Error(codes.Unavailable, "not ready"), expectedError: "not ready", expectedSecondaryCalls: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			primary := &cursorDispatchSvc{name: "primary"}
			secondary := &cursorDispatchSvc{name: "secondary", err: tc.secondaryError}
			dispatcher := newLR3CursorDispatcher(t, primary, secondary, true)
			if tc.removeSecondaries {
				dispatcher.secondaryDispatch = nil
			}
			stream := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupResources3Response](t.Context())
			req := lr3CursorRequest([]string{secondaryCursorPrefix + "nested", "secondary", "1", secondaryCursorPrefix + tc.owner})
			original := proto.Clone(req)
			err := dispatcher.DispatchLookupResources3(req, stream)
			require.ErrorContains(t, err, tc.expectedError)
			require.Empty(t, stream.Results())
			require.Empty(t, primary.receivedCursors(), "must not fall back with a secondary-owned cursor")
			require.Len(t, secondary.receivedCursors(), tc.expectedSecondaryCalls)
			require.True(t, proto.Equal(original, req))
		})
	}
}

func TestLR3LegacyCursorRouting(t *testing.T) {
	for _, owner := range []string{"primary", "secondary"} {
		t.Run(owner, func(t *testing.T) {
			primary := &cursorDispatchSvc{name: "primary"}
			secondary := &cursorDispatchSvc{name: "secondary"}
			dispatcher := newLR3CursorDispatcher(t, primary, secondary, true)
			cursor := []string{secondaryCursorPrefix + "nested", owner, "1"}
			stream := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupResources3Response](t.Context())
			require.NoError(t, dispatcher.DispatchLookupResources3(lr3CursorRequest(cursor), stream))
			require.Len(t, stream.Results(), 1)
			require.Equal(t, owner+"-1", stream.Results()[0].Items[0].ResourceId)
			require.Equal(t, [][]string{cursor}, secondary.receivedCursors())
			if owner == "primary" {
				require.Equal(t, [][]string{cursor}, primary.receivedCursors())
			} else {
				require.Empty(t, primary.receivedCursors())
			}
		})
	}
}

func TestLR3CursorRoutingDisabled(t *testing.T) {
	for _, owner := range []string{"primary", "secondary"} {
		t.Run(owner, func(t *testing.T) {
			primary := &cursorDispatchSvc{name: "primary"}
			secondary := &cursorDispatchSvc{name: "secondary"}
			dispatcher := newLR3CursorDispatcher(t, primary, secondary, false)
			if owner == "primary" {
				dispatcher.secondaryDispatch = nil
			}

			// Upgraded nodes keep the old wire format until emission is enabled.
			firstPage := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupResources3Response](t.Context())
			require.NoError(t, dispatcher.DispatchLookupResources3(lr3CursorRequest(nil), firstPage))
			require.Len(t, firstPage.Results(), 1)
			for i, item := range firstPage.Results()[0].Items {
				require.Equal(t, []string{secondaryCursorPrefix + "nested", owner, strconv.Itoa(i + 1)}, item.AfterResponseCursorSections)
			}

			// Decode and preserve ownership from an enabled node, even if this
			// node has emission disabled and its expression would pick the primary.
			dispatcher.secondaryDispatchExprs = nil
			marker := primaryCursorSection
			if owner == "secondary" {
				marker = secondaryCursorPrefix + "secondary"
			}
			page := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupResources3Response](t.Context())
			req := lr3CursorRequest([]string{secondaryCursorPrefix + "nested", owner, "1", marker})
			require.NoError(t, dispatcher.DispatchLookupResources3(req, page))
			require.Len(t, page.Results(), 1)
			for i, item := range page.Results()[0].Items {
				require.Equal(t, fmt.Sprintf("%s-%d", owner, i+1), item.ResourceId)
				require.Equal(t, []string{secondaryCursorPrefix + "nested", owner, strconv.Itoa(i + 2), marker}, item.AfterResponseCursorSections)
			}
		})
	}
}

func TestLR3CursorRoutingWithoutCursors(t *testing.T) {
	// The graph dispatcher suppresses cursors on unlimited lookups. Routing
	// must not manufacture cursors where none were provided by the backend.
	conn := connectionForDispatching(t, &fakeDispatchSvc{resultCount: 2})
	dispatcher, err := NewClusterDispatcher(v1.NewDispatchServiceClient(conn), conn,
		ClusterDispatcherConfig{EnableLookupResources3CursorRouting: true}, nil, nil, 0)
	require.NoError(t, err)
	stream := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupResources3Response](t.Context())
	req := lr3CursorRequest(nil)
	req.OptionalLimit = 0
	require.NoError(t, dispatcher.DispatchLookupResources3(req, stream))
	require.Len(t, stream.Results(), 2)
	for _, result := range stream.Results() {
		for _, item := range result.Items {
			require.Empty(t, item.AfterResponseCursorSections)
		}
	}
}
