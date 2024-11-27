package v1

import (
	"context"
	"errors"
	"time"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	grpcvalidate "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/validator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/internal/middleware/usagemetrics"
	"github.com/authzed/spicedb/internal/services/shared"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/genutil/mapz"
	dispatchv1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/authzed/spicedb/pkg/zedtoken"
)

type watchServer struct {
	v1.UnimplementedWatchServiceServer
	shared.WithStreamServiceSpecificInterceptor

	heartbeatDuration time.Duration
}

// NewWatchServer creates an instance of the watch server.
func NewWatchServer(heartbeatDuration time.Duration) v1.WatchServiceServer {
	s := &watchServer{
		WithStreamServiceSpecificInterceptor: shared.WithStreamServiceSpecificInterceptor{
			Stream: grpcvalidate.StreamServerInterceptor(),
		},
		heartbeatDuration: heartbeatDuration,
	}
	return s
}

func (ws *watchServer) Watch(req *v1.WatchRequest, stream v1.WatchService_WatchServer) error {
	if len(req.GetOptionalObjectTypes()) > 0 && len(req.OptionalRelationshipFilters) > 0 {
		return status.Errorf(codes.InvalidArgument, "cannot specify both object types and relationship filters")
	}

	objectTypes := mapz.NewSet[string](req.GetOptionalObjectTypes()...)
	filters := make([]datastore.RelationshipsFilter, 0, len(req.OptionalRelationshipFilters))

	ctx := stream.Context()
	ds := datastoremw.MustFromContext(ctx)

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

	reader := ds.SnapshotReader(afterRevision)

	for _, filter := range req.OptionalRelationshipFilters {
		if err := validateRelationshipsFilter(stream.Context(), filter, reader); err != nil {
			return ws.rewriteError(ctx, err)
		}

		dsFilter, err := datastore.RelationshipsFilterFromPublicFilter(filter)
		if err != nil {
			return status.Errorf(codes.InvalidArgument, "failed to parse relationship filter: %s", err)
		}

		filters = append(filters, dsFilter)
	}

	usagemetrics.SetInContext(ctx, &dispatchv1.ResponseMeta{
		DispatchCount: 1,
	})

	updates, errchan := ds.Watch(ctx, afterRevision, datastore.WatchOptions{
		Content:            datastore.WatchRelationships,
		CheckpointInterval: ws.heartbeatDuration,
	})
	for {
		select {
		case update, ok := <-updates:
			if ok {
				filtered := filterUpdates(objectTypes, filters, update.RelationshipChanges)
				if len(filtered) > 0 {
					converted, err := tuple.UpdatesToV1RelationshipUpdates(filtered)
					if err != nil {
						return status.Errorf(codes.Internal, "failed to convert updates: %s", err)
					}

					if err := stream.Send(&v1.WatchResponse{
						Updates:                     converted,
						ChangesThrough:              zedtoken.MustNewFromRevision(update.Revision),
						OptionalTransactionMetadata: update.Metadata,
					}); err != nil {
						return status.Errorf(codes.Canceled, "watch canceled by user: %s", err)
					}
				}
			}
		case err := <-errchan:
			switch {
			case errors.As(err, &datastore.WatchCanceledError{}):
				return status.Errorf(codes.Canceled, "watch canceled by user: %s", err)
			case errors.As(err, &datastore.WatchDisconnectedError{}):
				return status.Errorf(codes.ResourceExhausted, "watch disconnected: %s", err)
			default:
				return status.Errorf(codes.Internal, "watch error: %s", err)
			}
		}
	}
}

func (ws *watchServer) rewriteError(ctx context.Context, err error) error {
	return shared.RewriteError(ctx, err, &shared.ConfigForErrors{})
}

func filterUpdates(objectTypes *mapz.Set[string], filters []datastore.RelationshipsFilter, updates []tuple.RelationshipUpdate) []tuple.RelationshipUpdate {
	if objectTypes.IsEmpty() && len(filters) == 0 {
		return updates
	}

	filtered := make([]tuple.RelationshipUpdate, 0, len(updates))
	for _, update := range updates {
		objectType := update.Relationship.Resource.ObjectType
		if !objectTypes.IsEmpty() && !objectTypes.Has(objectType) {
			continue
		}

		if len(filters) > 0 {
			// If there are filters, we need to check if the update matches any of them.
			matched := false
			for _, filter := range filters {
				if filter.Test(update.Relationship) {
					matched = true
					break
				}
			}

			if !matched {
				continue
			}
		}

		filtered = append(filtered, update)
	}

	return filtered
}
