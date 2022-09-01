package v1

import (
	"context"
	"errors"

	"github.com/authzed/spicedb/pkg/zedtoken"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	dispatchpkg "github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/graph"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/internal/middleware/handwrittenvalidation"
	"github.com/authzed/spicedb/internal/middleware/usagemetrics"
	"github.com/authzed/spicedb/internal/services/shared"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	dispatchv1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	v1lookupwatch "github.com/authzed/spicedb/pkg/proto/lookupwatch/v1"
	grpcmw "github.com/grpc-ecosystem/go-grpc-middleware"
	grpcvalidate "github.com/grpc-ecosystem/go-grpc-middleware/validator"
	"github.com/shopspring/decimal"
)

// NewLookupWatchServer creates a LookupWatchServiceServer instance.
func NewLookupWatchServer(
	dispatch dispatchpkg.Dispatcher,
	defaultDepth uint32,
) v1lookupwatch.LookupWatchServiceServer {
	return &lookupWatchServer{
		dispatch:     dispatch,
		defaultDepth: defaultDepth,
		WithServiceSpecificInterceptors: shared.WithServiceSpecificInterceptors{
			Unary: grpcmw.ChainUnaryServer(
				grpcvalidate.UnaryServerInterceptor(),
				handwrittenvalidation.UnaryServerInterceptor,
				usagemetrics.UnaryServerInterceptor(),
			),
			Stream: grpcmw.ChainStreamServer(
				grpcvalidate.StreamServerInterceptor(),
				handwrittenvalidation.StreamServerInterceptor,
				usagemetrics.StreamServerInterceptor(),
			),
		},
	}
}

type lookupWatchServer struct {
	v1lookupwatch.UnimplementedLookupWatchServiceServer
	shared.WithServiceSpecificInterceptors

	dispatch     dispatchpkg.Dispatcher
	defaultDepth uint32
}

func (lw *lookupWatchServer) WatchAccessibleResources(req *v1lookupwatch.WatchAccessibleResourcesRequest, stream v1lookupwatch.LookupWatchService_WatchAccessibleResourcesServer) error {
	ctx := stream.Context()

	ds := datastoremw.MustFromContext(ctx)
	objectTypesMap := make(map[string]struct{})
	for _, objectType := range []string{req.ResourceObjectType} {
		objectTypesMap[objectType] = struct{}{}
	}

	var afterRevision decimal.Decimal
	if req.OptionalStartTimestamp != nil && req.OptionalStartTimestamp.Token != "" {
		decodedRevision, err := zedtoken.DecodeRevision(req.OptionalStartTimestamp)
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
				//TODO When do we CLOSE STREAM ?
				return lw.processWatchResponse(&ctx, req, update, &stream)
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

// Called at each notification of the Wattch API
func (lw *lookupWatchServer) processWatchResponse(
	ctx *context.Context,
	req *v1lookupwatch.WatchAccessibleResourcesRequest,
	updates *datastore.RevisionChanges,
	stream *v1lookupwatch.LookupWatchService_WatchAccessibleResourcesServer,
) error {
	for _, update := range updates.Changes {
		if err := lw.processUpdate(ctx, req, update, &updates.Revision, stream); err != nil {
			return err
		}
	}
	return nil
}

// Called every time a relation is changed in spicedb
func (lw *lookupWatchServer) processUpdate(
	ctx *context.Context,
	req *v1lookupwatch.WatchAccessibleResourcesRequest,
	update *core.RelationTupleUpdate,
	atRevision *datastore.Revision,
	stream *v1lookupwatch.LookupWatchService_WatchAccessibleResourcesServer,
) error {

	//CALL LOOKUPSUBJECTS
	var subjects []string
	if req.SubjectObjectType == update.Tuple.Subject.Namespace {
		// TODO: we'll revisit that once we have arrow resolution, we leave as it is meanwhile for our testcases
		subjects = append(subjects, update.Tuple.Subject.ObjectId)
	} else if update.Tuple.Subject.Relation == "" {
		return status.Errorf(
			codes.Internal,
			"cannot handle the following relation, arrow resolution not implemented: %s:%s",
			update.Tuple.Subject.Namespace, update.Tuple.Subject.ObjectId,
		)
	} else {
		lsStream := dispatchpkg.NewHandlingDispatchStream(*ctx, func(result *dispatchv1.DispatchLookupSubjectsResponse) error {
			for _, subject := range result.FoundSubjects {
				subjects = append(subjects, subject.SubjectId)
			}
			return nil
		})
		err := lw.dispatch.DispatchLookupSubjects(
			&dispatchv1.DispatchLookupSubjectsRequest{
				Metadata: &dispatchv1.ResolverMeta{
					AtRevision:     atRevision.String(),
					DepthRemaining: lw.defaultDepth,
				},
				ResourceIds: []string{update.Tuple.Subject.ObjectId},
				ResourceRelation: &core.RelationReference{
					Namespace: update.Tuple.Subject.Namespace,
					// TODO: arrow resolution, get Permission from @joey's code
					Relation: update.Tuple.ResourceAndRelation.Relation,
				},
				SubjectRelation: &core.RelationReference{
					Namespace: req.SubjectObjectType,
					// TODO: Name tag as optional whereas dispatchLookupSubject require this parameter
					Relation: req.OptionalSubjectRelation,
				},
			},
			lsStream,
		)
		if err != nil {
			return err
		}
	}

	//CALL ReachableResources
	var resources []string
	rrStream := dispatchpkg.NewHandlingDispatchStream(*ctx, func(result *dispatchv1.DispatchReachableResourcesResponse) error {
		resources = append(resources, result.Resource.ResourceIds...)
		return nil
	})
	err := lw.dispatch.DispatchReachableResources(
		&dispatchv1.DispatchReachableResourcesRequest{
			Metadata: &dispatchv1.ResolverMeta{
				AtRevision:     atRevision.String(),
				DepthRemaining: lw.defaultDepth,
			},
			ResourceRelation: &core.RelationReference{
				Namespace: req.ResourceObjectType,
				Relation:  req.Permission,
			},
			SubjectIds: []string{update.Tuple.ResourceAndRelation.ObjectId},
			SubjectRelation: &core.RelationReference{
				Namespace: update.Tuple.ResourceAndRelation.Namespace,
				// TODO: what is the relation for the subject side ? Is the following line of code ok ?
				Relation: update.Tuple.ResourceAndRelation.Relation,
			},
		},
		rrStream,
	)
	if err != nil {
		return err
	}

	//CROSS JOIN
	permissionUpdates := []*v1lookupwatch.PermissionUpdate{}
	for _, subject := range subjects {
		for _, resource := range resources {
			//CALL CHECK PERMISSION
			permission, err := lw.dispatch.DispatchCheck(*ctx, &dispatchv1.DispatchCheckRequest{
				Metadata: &dispatchv1.ResolverMeta{
					AtRevision:     atRevision.String(),
					DepthRemaining: lw.defaultDepth,
				},
				ResourceAndRelation: &core.ObjectAndRelation{
					Namespace: req.ResourceObjectType,
					ObjectId:  resource,
					Relation:  req.Permission,
				},
				Subject: &core.ObjectAndRelation{
					Namespace: req.SubjectObjectType,
					ObjectId:  subject,
					Relation:  graph.Ellipsis,
				},
			})
			if err != nil {
				return err
			}

			permissionUpdates = append(permissionUpdates, &v1lookupwatch.PermissionUpdate{
				Subject: &v1.SubjectReference{
					Object: &v1.ObjectReference{
						ObjectType: req.SubjectObjectType,
						ObjectId:   subject,
					},
					OptionalRelation: "", //TODO ?
				},
				Resource: &v1.ObjectReference{
					ObjectType: req.ResourceObjectType,
					ObjectId:   resource,
				},
				UpdatedPermission: convertPermission(permission),
			})

		}
	}

	sendErr := (*stream).Send(&v1lookupwatch.WatchAccessibleResourcesResponse{
		Updates:        permissionUpdates,
		ChangesThrough: zedtoken.NewFromRevision(*atRevision),
	})
	if sendErr != nil {
		return sendErr
	}
	return nil
}

func convertPermission(permission *dispatchv1.DispatchCheckResponse) v1.CheckPermissionResponse_Permissionship {
	switch permission.Membership {
	case dispatchv1.DispatchCheckResponse_MEMBER:
		return v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION
	case dispatchv1.DispatchCheckResponse_NOT_MEMBER:
		return v1.CheckPermissionResponse_PERMISSIONSHIP_NO_PERMISSION
	default:
		return v1.CheckPermissionResponse_PERMISSIONSHIP_UNSPECIFIED
	}
}
