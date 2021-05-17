package services

import (
	api "github.com/authzed/spicedb/pkg/REDACTEDapi/api"
	"github.com/authzed/spicedb/pkg/grpcutil"
)

type devServer struct {
	api.UnimplementedDeveloperServiceServer
	grpcutil.IgnoreAuthMixin
}

// NewDeveloperServer creates an instance of the developer server.
func NewDeveloperServer() api.DeveloperServiceServer {
	s := &devServer{}
	return s
}
