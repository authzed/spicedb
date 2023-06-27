package v1

import (
	"errors"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	grpcvalidate "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/validator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/internal/middleware/usagemetrics"
	"github.com/authzed/spicedb/internal/services/shared"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	dispatchv1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/authzed/spicedb/pkg/zedtoken"
)

type watchServer struct {
	v1.UnimplementedWatchServiceServer
	shared.WithStreamServiceSpecificInterceptor
}

// NewWatchServer creates an instance of the watch server.
func NewWatchServer() v1.WatchServiceServer {
	s := &watchServer{
		WithStreamServiceSpecificInterceptor: shared.WithStreamServiceSpecificInterceptor{
			Stream: grpcvalidate.StreamServerInterceptor(),
		},
	}
	return s
}

func (ws *watchServer) Watch(req *v1.WatchRequest, stream v1.WatchService_WatchServer) error {
	ctx := stream.Context()
	ds := datastoremw.MustFromContext(ctx)

	objectTypesMap := make(map[string]struct{})
	for _, objectType := range req.GetOptionalObjectTypes() {
		objectTypesMap[objectType] = struct{}{}
	}

	var afterRevision datastore.Revision
	if req.OptionalStartCursor != nil && req.OptionalStartCursor.Token != "" {
		decodedRevision, err := zedtoken.DecodeRevision(req.OptionalStartCursor, ds)
		if err != nil {
			return status.Errorf(codes.InvalidArgument, "failed to decode start revision: %s", err)
		}

		afterRevision = decodedRevision
	} else {
		var err error
		afterRevision, err = ds.OptimizedRevision(ctx)
		if err != nil {
			return status.Errorf(codes.Unavailable, "failed to start watch: %s", err)
		}
	}

	usagemetrics.SetInContext(ctx, &dispatchv1.ResponseMeta{
		DispatchCount: 1,
	})

	updates, errchan := ds.Watch(ctx, afterRevision)
	for {
		select {
		case update, ok := <-updates:
			if ok {
				filtered := filterUpdates(objectTypesMap, update.Changes)
				if len(filtered) > 0 {
					if err := stream.Send(&v1.WatchResponse{
						Updates:        filtered,
						ChangesThrough: zedtoken.MustNewFromRevision(update.Revision),
					}); err != nil {
						return status.Errorf(codes.Canceled, "watch canceled by user: %s", err)
					}
				}
			}
		case err := <-errchan:
			switch {
			case errors.As(err, &datastore.ErrWatchCanceled{}):
				return status.Errorf(codes.Canceled, "watch canceled by user: %s", err)
			case errors.As(err, &datastore.ErrWatchDisconnected{}):
				return status.Errorf(codes.ResourceExhausted, "watch disconnected: %s", err)
			default:
				return status.Errorf(codes.Internal, "watch error: %s", err)
			}
		}
	}
}

func filterUpdates(objectTypes map[string]struct{}, candidates []*core.RelationTupleUpdate) []*v1.RelationshipUpdate {
	updates := tuple.UpdatesToRelationshipUpdates(candidates)

	if len(objectTypes) == 0 {
		return updates
	}

	var filtered []*v1.RelationshipUpdate
	for _, update := range updates {
		objectType := update.GetRelationship().GetResource().GetObjectType()

		if _, ok := objectTypes[objectType]; ok {
			filtered = append(filtered, update)
		}
	}

	return filtered
}
