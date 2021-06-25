package v1alpha1

import (
	"context"

	"google.golang.org/grpc"

	v1alpha1 "github.com/authzed/spicedb/pkg/proto/authzed/api/v1alpha1"
)

type schemaServiceServer struct {
	v1alpha1.UnimplementedSchemaServiceServer
}

// func NewSchemaServer() v1alpha1.SchemaServiceServer {
// 	return &schemaServiceServer{}
// }

func (ss *schemaServiceServer) Read(ctx context.Context, in *v1alpha1.SchemaServiceReadRequest, opts ...grpc.CallOption) (*v1alpha1.SchemaServiceReadResponse, error) {
	return nil, nil
}
func (ss *schemaServiceServer) Write(ctx context.Context, in *v1alpha1.SchemaServiceWriteRequest, opts ...grpc.CallOption) (*v1alpha1.SchemaServiceWriteResponse, error) {
	return nil, nil
}
