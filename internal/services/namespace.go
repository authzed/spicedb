package services

import (
	"context"
	"fmt"

	"github.com/authzed/spicedb/internal/datastore"
	api "github.com/authzed/spicedb/pkg/REDACTEDapi/api"
)

const (
	errUnableToWrite = "Unable to write config: %v"
	errUnableToRead  = "Unable to read config: %v"
)

type nsServer struct {
	api.UnimplementedNamespaceServiceServer

	ds datastore.NamespaceDatastore
}

// NewNamespaceServer creates an instance of the namespace server.
func NewNamespaceServer(ds datastore.NamespaceDatastore) api.NamespaceServiceServer {
	s := &nsServer{ds: ds}
	return s
}

func (nss *nsServer) WriteConfig(ctxt context.Context, req *api.WriteConfigRequest) (*api.WriteConfigResponse, error) {
	_, err := nss.ds.WriteNamespace(req.Config)
	if err != nil {
		return nil, fmt.Errorf(errUnableToWrite, err)
	}

	return &api.WriteConfigResponse{
		Revision: &api.Zookie{
			Token: "not implemented",
		},
	}, nil
}

func (nss *nsServer) ReadConfig(ctxt context.Context, req *api.ReadConfigRequest) (*api.ReadConfigResponse, error) {
	found, _, err := nss.ds.ReadNamespace(req.Namespace)
	if err != nil {
		return nil, fmt.Errorf(errUnableToRead, err)
	}

	return &api.ReadConfigResponse{
		Namespace: req.Namespace,
		Config:    found,
		Revision: &api.Zookie{
			Token: "not implemented",
		},
	}, nil
}
