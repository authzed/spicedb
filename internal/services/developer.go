package services

import api "github.com/authzed/spicedb/internal/REDACTEDapi/api"

type devServer struct {
	api.UnimplementedDeveloperServiceServer
}

// NewDeveloperServer creates an instance of the developer server.
func NewDeveloperServer() api.DeveloperServiceServer {
	s := &devServer{}
	return s
}
