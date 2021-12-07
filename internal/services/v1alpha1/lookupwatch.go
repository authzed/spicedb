package v1alpha1

import (
	"fmt"
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
func NewLookupWatchServer(watchClient v1.WatchServiceClient, permissionsClient v1.PermissionsServiceClient) v1alpha1.LookupWatchServiceServer {

	if watchClient == nil {
		panic("A non-nil watchClient must be provided")
	}

	if permissionsClient == nil {
		panic("A non-nil permissionsClient must be provided")
	}

	return &lookupWatchServiceServer{
		WithUnaryServiceSpecificInterceptor: shared.WithUnaryServiceSpecificInterceptor{
			Unary: grpcmw.ChainUnaryServer(grpcutil.DefaultUnaryMiddleware...),
		},
		permissionsClient: permissionsClient,
		watchClient:       watchClient,
	}
}

func (lws *lookupWatchServiceServer) WatchAccessibleResources(req *v1alpha1.WatchAccessibleResourcesRequest, resp v1alpha1.LookupWatchService_WatchAccessibleResourcesServer) error {

	ctx := resp.Context()

	done := make(chan bool)
	errCh := make(chan error, 1)

	go func() {

		// start a watch over all changes
		stream, err := lws.watchClient.Watch(ctx, &v1.WatchRequest{
			OptionalStartCursor: req.GetOptionalStartCursor(),
		})
		if err != nil {
			errCh <- fmt.Errorf("todo(jon-whit): come up with a better error")
			return
		}

		for {
			watchResponse, err := stream.Recv()
			if err == io.EOF {
				done <- true
				return
			}
			if err != nil {
				errCh <- fmt.Errorf("todo(jon-whit): come up with a better error")
				return
			}

			updates := watchResponse.GetUpdates()
			revision := watchResponse.GetChangesThrough()

			permissionUpdates := []*v1alpha1.PermissionUpdate{}

			maxConcurrentUpdates := 10
			limiter := make(chan struct{}, maxConcurrentUpdates)

			for _, update := range updates {

				operation := update.GetOperation()
				if operation == v1.RelationshipUpdate_OPERATION_UNSPECIFIED {
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
						// resourceStream, err := v1alpha1.ReachableResources(context.TODO(), &v1alpha1.ReachResourcesRequest{
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
						// subjectStream, err := v1alpha1.LookupSubjects(context.TODO(), &v1alpha1.LookupSubjectsRequest{
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
								errCh <- fmt.Errorf("todo(jon-whit): come up with a better error")
								return
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

							// todo(jon-whit): verify the safety of this concurrent append
							permissionUpdates = append(permissionUpdates, &v1alpha1.PermissionUpdate{
								Subject:           subject,
								Resource:          resource,
								Relation:          "todo(jon-whit): rename this to permission?",
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
				errCh <- fmt.Errorf("todo(jon-whit): come up with a better error")
				return
			}
		}
	}()

	select {
	case <-ctx.Done():
		if err := ctx.Err(); err != nil {
			// todo(jon-whit): handle ctx error better
			return err
		}
		return nil
	case <-done:
		return nil
	case err := <-errCh:
		return err
	}
}
