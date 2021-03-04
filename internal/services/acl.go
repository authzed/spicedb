package services

import api "github.com/authzed/spicedb/internal/REDACTEDapi/api"

type aclServer struct {
	api.UnimplementedACLServiceServer
}

// NewACLServer creates an instance of the ACL server.
func NewACLServer() api.ACLServiceServer {
	s := &aclServer{}
	return s
}
