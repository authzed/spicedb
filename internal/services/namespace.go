package services

import api "github.com/authzed/spicedb/internal/REDACTEDapi/api"

type nsServer struct {
	api.UnimplementedNamespaceServiceServer
}

// NewNamespaceServer creates an instance of the namespace server.
func NewNamespaceServer() api.NamespaceServiceServer {
	s := &nsServer{}
	return s
}
