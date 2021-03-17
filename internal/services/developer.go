package services

import (
	"github.com/authzed/spicedb/internal/auth"
	api "github.com/authzed/spicedb/pkg/REDACTEDapi/api"
)

type devServer struct {
	api.UnimplementedDeveloperServiceServer

	auth.NoAuthRequired
}

// NewDeveloperServer creates an instance of the developer server.
func NewDeveloperServer() api.DeveloperServiceServer {
	s := &devServer{}
	return s
}
