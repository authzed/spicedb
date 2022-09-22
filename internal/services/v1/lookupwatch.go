package v1

import (
	"context"
	"errors"

	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/authzed/spicedb/pkg/zedtoken"
	"github.com/jzelinskie/stringz"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	dispatchpkg "github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/graph"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/internal/middleware/handwrittenvalidation"
	"github.com/authzed/spicedb/internal/middleware/usagemetrics"
	"github.com/authzed/spicedb/internal/namespace"
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
	config PermissionsServerConfig,
) v1lookupwatch.LookupWatchServiceServer {
	return &lookupWatchServer{
		dispatch:     dispatch,
		defaultDepth: defaultIfZero(config.MaximumAPIDepth, 50),
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

// The Lookup Watch API service provides a single API, WatchAccessibleResources, for receiving a stream of updates
// for resources of the requested kind, for the requested permission, for a specific subject type.
//
// Under the hood, the Lookup Watch API Service makes use of the Watch and the Reachability Resolution APIs.
//
// The main use case for the Lookup Watch API is to implement ACL filtering.
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
		afterRevision, err = ds.HeadRevision(ctx)
		if err != nil {
			return status.Errorf(codes.Unavailable, "failed to start watch: %s", err)
		}
	}

	usagemetrics.SetInContext(ctx, &dispatchv1.ResponseMeta{
		DispatchCount: 1,
	})

	// The Lookup Watch Service invokes Watch on all object types registered in the system,
	// starting from the optionally provided OptionalStartTimestamp revision.
	updates, errchan := ds.Watch(ctx, afterRevision)

	for {
		select {
		case update, ok := <-updates:
			if ok {
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

// Called at each notification of the Watch API
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
//
// The processUpdate will:
//
//   - Invoke LookupSubjects for the subject of the changed relationship,
//     with a target_subject_type of the monitored subject_type
//     We'll get all the subjects impacted by the changed relation
//
//   - Invoke ReachableResources for the resource of the changed relationship,
//     with a target_object_type of the monitored resource_type
//     We'll get all the resources impacted by the changed relation
//
//   - Invoke CheckPermission for each combinatorial pair of {resource, subject}
//     And send the resulting permission to the LookupWatch client
//
// Notes:
//
//   - the current implementation can be optimized, i.e. the CheckPermission
//     could be sharded out amongst a cluster of watch service nodes,
//     with each reporting its status back to the leader.
//   - the current implementation does not keep track of the existing status
//     of a permission for a resource and subject; this means the Lookup Watch API will
//     likely report subjects gaining and removing permission, even if the relationship
//     change didn't necessarily change that permission
func (lw *lookupWatchServer) processUpdate(
	ctx *context.Context,
	req *v1lookupwatch.WatchAccessibleResourcesRequest,
	update *core.RelationTupleUpdate,
	atRevision *datastore.Revision,
	stream *v1lookupwatch.LookupWatchService_WatchAccessibleResourcesServer,
) error {

	ds := datastoremw.MustFromContext((*stream).Context())

	if update.Tuple.Subject.Relation == "" {
		return status.Errorf(
			codes.Internal,
			"Empty subject relations not handled %s:%s",
			update.Tuple.Subject.Namespace, update.Tuple.Subject.ObjectId,
		)
	}
	reader := ds.SnapshotReader(*atRevision)

	// STEP 1: CALL LOOKUPSUBJECTS
	//
	// LookupSubjects is invoked to compute the set of subjects impacted by the changed relation
	subjects, err := lw.lookupSubjects(
		ctx,
		req,
		update,
		atRevision,
		reader,
	)
	if err != nil {
		return err
	}

	// STEP 2: CALL ReachableResources
	//
	// ReachableResources is invoked to compute the set of resources impacted by the changed relation
	resources, err := lw.lookupResources(
		ctx,
		req,
		update,
		atRevision,
		reader,
	)
	if err != nil {
		return err
	}

	// STEP 3: CROSS JOIN
	//
	// Invoke CheckPermission for each combinatorial pair of {resource, subject}
	err = lw.computePermissions(
		resources,
		subjects,
		ctx,
		req,
		atRevision,
		stream,
	)
	if err != nil {
		return err
	}
	return nil
}

// LookupSubjects is invoked to compute the set of subjects impacted by the changed relation
func (lw *lookupWatchServer) lookupSubjects(
	ctx *context.Context,
	req *v1lookupwatch.WatchAccessibleResourcesRequest,
	update *core.RelationTupleUpdate,
	atRevision *datastore.Revision,
	reader datastore.Reader,
) ([]string, error) {

	var subjects []string
	if req.SubjectObjectType == update.Tuple.Subject.Namespace && update.Tuple.Subject.Relation == graph.Ellipsis {
		subjects = append(subjects, update.Tuple.Subject.ObjectId)
	}

	// Computing relations to follow when calling LookupSubject
	var resourceRelations []string
	// Computing relations with arrow resolution:
	// We check if the update impacts any arrow expression
	// If this is the case, we will add the arrow relation to the LookupSubject call
	//
	// For instance, if we have the following update:
	// ```
	// update {
	//    relationship: "resource:F1"
	//    relation: parent
	//    subject: {
	//      object: "resource_group:G1"
	//      optionalRelation: ""
	//    }
	// }
	// ```
	// And the following schema:
	//
	// ```
	// definition resource {
	//    relation reader: user
	//    relation parent: resource_group | resource
	//
	//    permission read = reader + parent->read
	// ```
	//
	// Then we'll call ResolveArrowRelations("parent"), which returns read, and we'll call
	// LookupSubject with:
	//
	// ```
	// ResourceIds: ["G1"],
	// ResourceRelation: {
	//   Namespace: "resource_group",
	//   Relation:  "read",
	// }
	// ```
	//
	resourceRelations, err := lw.resolveArrowRelations(
		update.Tuple.ResourceAndRelation.Relation,
		update.Tuple.ResourceAndRelation.Namespace,
		reader,
		*ctx,
	)
	if err != nil {
		return nil, err
	}

	// Computing relations: we also need to follow update.Tuple.Subject.Relation
	if update.Tuple.Subject.Relation != graph.Ellipsis {
		resourceRelations = append(resourceRelations, update.Tuple.Subject.Relation)
	}

	// LookupSubject call for each computed relation
	for _, resourceRelation := range resourceRelations {
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
					Relation:  resourceRelation,
				},
				SubjectRelation: &core.RelationReference{
					Namespace: req.SubjectObjectType,
					Relation:  stringz.DefaultEmpty(req.OptionalSubjectRelation, tuple.Ellipsis),
				},
			},
			lsStream,
		)
		if err != nil {
			return nil, err
		}
	}
	return subjects, nil
}

// ReachableResources is invoked to compute the set of resources impacted by the changed relation
func (lw *lookupWatchServer) lookupResources(
	ctx *context.Context,
	req *v1lookupwatch.WatchAccessibleResourcesRequest,
	update *core.RelationTupleUpdate,
	atRevision *datastore.Revision,
	reader datastore.Reader,
) ([]string, error) {

	var resources []string
	rrStream := dispatchpkg.NewHandlingDispatchStream(*ctx, func(result *dispatchv1.DispatchReachableResourcesResponse) error {
		resources = append(resources, result.Resource.ResourceIds...)
		return nil
	})
	var subjectRelation = update.Tuple.ResourceAndRelation.Relation
	if req.ResourceObjectType == update.Tuple.ResourceAndRelation.Namespace && req.Permission != update.Tuple.ResourceAndRelation.Relation {
		subjectRelations, err := lw.resolveArrowRelations(
			update.Tuple.ResourceAndRelation.Relation,
			update.Tuple.ResourceAndRelation.Namespace,
			reader,
			*ctx,
		)
		if err != nil {
			return nil, err
		}
		for _, relation := range subjectRelations {
			if relation == req.Permission {
				subjectRelation = relation
				break
			}
		}

	}
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
				Relation:  subjectRelation,
			},
		},
		rrStream,
	)
	if err != nil {
		return nil, err
	}
	return resources, nil
}

// Invoke CheckPermission for each combinatorial pair of {resource, subject}
// and stream the computed result to the client
func (lw *lookupWatchServer) computePermissions(
	resources []string,
	subjects []string,
	ctx *context.Context,
	req *v1lookupwatch.WatchAccessibleResourcesRequest,
	atRevision *datastore.Revision,
	stream *v1lookupwatch.LookupWatchService_WatchAccessibleResourcesServer,
) error {
	permissionUpdates := []*v1lookupwatch.PermissionUpdate{}
	for _, subject := range subjects {
		for _, resource := range resources {
			permission, err := lw.dispatch.DispatchCheck(*ctx, &dispatchv1.DispatchCheckRequest{
				Metadata: &dispatchv1.ResolverMeta{
					AtRevision:     atRevision.String(),
					DepthRemaining: lw.defaultDepth,
				},
				ResourceRelation: &core.RelationReference{
					Namespace: req.ResourceObjectType,
					Relation:  req.Permission,
				},
				ResourceIds: []string{resource},
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
					OptionalRelation: req.OptionalSubjectRelation,
				},
				Resource: &v1.ObjectReference{
					ObjectType: req.ResourceObjectType,
					ObjectId:   resource,
				},
				UpdatedPermission: convertPermission(permission.ResultsByResourceId[resource]),
			})

		}
	}

	// Send the resulting permissions to the client
	sendErr := (*stream).Send(&v1lookupwatch.WatchAccessibleResourcesResponse{
		Updates:        permissionUpdates,
		ChangesThrough: zedtoken.NewFromRevision(*atRevision),
	})
	if sendErr != nil {
		return sendErr
	}
	return nil
}

func (lw *lookupWatchServer) resolveArrowRelations(
	relationName string,
	namespaceName string,
	reader datastore.Reader,
	context context.Context,
) ([]string, error) {

	_, typeSystem, err := namespace.ReadNamespaceAndTypes(context, namespaceName, reader)
	if err != nil {
		return nil, err
	}
	resourceRelations, err := typeSystem.ResolveArrowRelations(relationName)
	if err != nil {
		return nil, err
	}
	return resourceRelations, nil
}

func convertPermission(permission *dispatchv1.DispatchCheckResponse_ResourceCheckResult) v1.CheckPermissionResponse_Permissionship {
	if permission == nil {
		return v1.CheckPermissionResponse_PERMISSIONSHIP_NO_PERMISSION
	}
	switch permission.Membership {
	case dispatchv1.DispatchCheckResponse_MEMBER:
		return v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION
	case dispatchv1.DispatchCheckResponse_NOT_MEMBER:
		return v1.CheckPermissionResponse_PERMISSIONSHIP_NO_PERMISSION
	default:
		return v1.CheckPermissionResponse_PERMISSIONSHIP_UNSPECIFIED
	}
}
