package v0

import (
	"errors"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	grpcvalidate "github.com/grpc-ecosystem/go-grpc-middleware/validator"
	"github.com/shopspring/decimal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/internal/middleware/usagemetrics"
	"github.com/authzed/spicedb/internal/namespace"
	"github.com/authzed/spicedb/internal/services/shared"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	dispatchv1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/zookie"
)

type watchServer struct {
	v0.UnimplementedWatchServiceServer
	shared.WithStreamServiceSpecificInterceptor
}

// NewWatchServer creates an instance of the watch server.
func NewWatchServer() v0.WatchServiceServer {
	s := &watchServer{
		WithStreamServiceSpecificInterceptor: shared.WithStreamServiceSpecificInterceptor{
			Stream: grpcvalidate.StreamServerInterceptor(),
		},
	}
	return s
}

func (ws *watchServer) Watch(req *v0.WatchRequest, stream v0.WatchService_WatchServer) error {
	ctx := stream.Context()
	ds := datastoremw.MustFromContext(ctx)

	var afterRevision decimal.Decimal
	if req.StartRevision != nil && req.StartRevision.Token != "" {
		decodedRevision, err := zookie.DecodeRevision(core.ToCoreZookie(req.StartRevision))
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

	namespaceMap := make(map[string]struct{})
	for _, ns := range req.Namespaces {
		err := namespace.CheckNamespaceAndRelation(
			ctx,
			ns,
			datastore.Ellipsis,
			true,
			ds.SnapshotReader(afterRevision),
		)
		if err != nil {
			return status.Errorf(codes.FailedPrecondition, "invalid namespace: %s", err)
		}

		namespaceMap[ns] = struct{}{}
	}
	filter := namespaceFilter{namespaces: namespaceMap}

	usagemetrics.SetInContext(ctx, &dispatchv1.ResponseMeta{
		DispatchCount: 1,
	})

	updates, errchan := ds.Watch(ctx, afterRevision)
	for {
		select {
		case update, ok := <-updates:
			if ok {
				filtered := filter.filterUpdates(update.Changes)
				if len(filtered) > 0 {
					if err := stream.Send(&v0.WatchResponse{
						Updates:     core.ToV0RelationTupleUpdates(update.Changes),
						EndRevision: core.ToV0Zookie(zookie.NewFromRevision(update.Revision)),
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

type namespaceFilter struct {
	namespaces map[string]struct{}
}

func (nf namespaceFilter) filterUpdates(candidates []*core.RelationTupleUpdate) []*core.RelationTupleUpdate {
	var filtered []*core.RelationTupleUpdate

	for _, update := range candidates {
		if _, ok := nf.namespaces[update.Tuple.ObjectAndRelation.Namespace]; ok {
			filtered = append(filtered, update)
		}
	}

	return filtered
}
