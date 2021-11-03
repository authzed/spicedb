package v1alpha1

import (
	"io"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	v1alpha1 "github.com/authzed/authzed-go/proto/authzed/api/v1alpha1"
	"github.com/authzed/grpcutil"
	"github.com/authzed/spicedb/internal/services/shared"
	grpcmw "github.com/grpc-ecosystem/go-grpc-middleware"
)

type lookupWatchServiceServer struct {
	v1alpha1.UnimplementedLookupWatchServiceServer
	shared.WithUnaryServiceSpecificInterceptor

	permissionsClient v1.PermissionsServiceClient
	watchClient       v1.WatchServiceClient
}

// NewLookupWatchServer returns an new instance of a server that implements
// authzed.api.v1alpha1.LookupWatchService.
func NewLookupWatchServer() v1alpha1.LookupWatchServiceServer {
	return &lookupWatchServiceServer{
		WithUnaryServiceSpecificInterceptor: shared.WithUnaryServiceSpecificInterceptor{
			Unary: grpcmw.ChainUnaryServer(grpcutil.DefaultUnaryMiddleware...),
		},
	}
}

func (lws *lookupWatchServiceServer) WatchAccessibleResources(req *v1alpha1.WatchAccessibleResourcesRequest, resp v1alpha1.LookupWatchService_WatchAccessibleResourcesServer) error {

	ctx := resp.Context()

	done := make(chan bool)

	go func() {

		stream, err := lws.watchClient.Watch(ctx, &v1.WatchRequest{
			ObjectTypes:         []string{"*"}, // watch for 'all' changes
			OptionalStartCursor: req.GetOptionalStartCursor(),
		})
		if err != nil {
			// handle error
		}

		for {
			watchResponse, err := stream.Recv()
			if err == io.EOF {
				done <- true
				return
			}
			if err != nil {
				// handle error
			}

			updates := watchResponse.GetUpdates()
			revision := watchResponse.GetChangesThrough()

			permissionUpdates := []*v1alpha1.PermissionUpdate{}

			maxConcurrentUpdates := 10
			limiter := make(chan struct{}, maxConcurrentUpdates)

			for _, update := range updates {

				operation := update.GetOperation()

				// todo: optimize this by adding support for an operation filter in the Watch API
				if operation != v1.RelationshipUpdate_OPERATION_CREATE && operation != v1.RelationshipUpdate_OPERATION_DELETE {
					continue
				}

				limiter <- struct{}{}

				go func(update *v1.RelationshipUpdate) {

					// todo(jon-whit): figure out how to do this in a more memory efficient manner

					resources := []*v1.ObjectReference{}
					_ = resources
					subjects := []*v1.SubjectReference{}
					_ = subjects

					go func() {
						// resourceStream, err := ReachableResources(&v1alpha1.ReachResourcesRequest{
						// 	StartingResource: update.GetRelationship().GetResource(),
						// 	TargetObjectType: req.GetResourceObjectType(),
						// })
						// if err != nil {
						// 	// handle error
						// }

						// for {
						// 	resp, err := resourceStream.Recv()
						// 	if err == io.EOF {
						// 		return
						// 	}
						// 	if err != nil {
						// 		// handle error
						// 	}

						// 	resources = append(resources, resp.GetFoundResource())
						// }
					}()

					go func() {
						// subjectStream, err := LookupSubjects(&v1alpha1.LookupSubjectsRequest{
						// 	Resource:          update.GetRelationship().GetResource(),
						// 	TargetSubjectType: req.GetSubjectObjectType(),
						// })
						// if err != nil {
						// 	// handle error
						// }

						// for {
						// 	resp, err := subjectStream.Recv()
						// 	if err == io.EOF {
						// 		return
						// 	}
						// 	if err != nil {
						// 		// handle error
						// 	}

						// 	subjects = append(subjects, resp.GetFoundRelationship().GetSubject())
						// }
					}()

					for _, resource := range resources {
						for _, subject := range subjects {

							checkResponse, err := lws.permissionsClient.CheckPermission(ctx, &v1.CheckPermissionRequest{
								Resource:   resource,
								Subject:    subject,
								Permission: req.GetPermission(),
							})
							if err != nil {
								// handle error
							}

							var updatedPermissionship v1alpha1.PermissionUpdate_Permissionship

							switch checkResponse.GetPermissionship() {
							case v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION:
								updatedPermissionship = v1alpha1.PermissionUpdate_PERMISSIONSHIP_HAS_PERMISSION
							case v1.CheckPermissionResponse_PERMISSIONSHIP_NO_PERMISSION:
								updatedPermissionship = v1alpha1.PermissionUpdate_PERMISSIONSHIP_NO_PERMISSION
							default:
								updatedPermissionship = v1alpha1.PermissionUpdate_PERMISSIONSHIP_UNSPECIFIED
							}

							// todo: verify the safety of this concurrent append
							permissionUpdates = append(permissionUpdates, &v1alpha1.PermissionUpdate{
								Subject: &v1.SubjectReference{},
								Resource: &v1.ObjectReference{
									ObjectType: "",
									ObjectId:   "",
								},
								Relation:          "",
								UpdatedPermission: updatedPermissionship,
							})
						}
					}

					<-limiter

				}(update)
			}

			err = resp.Send(&v1alpha1.WatchAccessibleResourcesResponse{
				Updates:        permissionUpdates,
				ChangesThrough: revision,
			})
			if err != nil {
				// handle error
			}
		}
	}()

	<-done
	return nil
}
