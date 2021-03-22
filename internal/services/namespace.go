package services

import (
	"context"

	"github.com/authzed/spicedb/internal/datastore"
	api "github.com/authzed/spicedb/pkg/REDACTEDapi/api"
	"github.com/authzed/spicedb/pkg/validation"
	"github.com/authzed/spicedb/pkg/zookie"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type nsServer struct {
	api.UnimplementedNamespaceServiceServer

	ds datastore.Datastore
}

// NewNamespaceServer creates an instance of the namespace server.
func NewNamespaceServer(ds datastore.Datastore) api.NamespaceServiceServer {
	s := &nsServer{ds: ds}
	return s
}

func (nss *nsServer) WriteConfig(ctx context.Context, req *api.WriteConfigRequest) (*api.WriteConfigResponse, error) {
	if err := validation.NamespaceConfig(req.Config); err != nil {
		return nil, rewriteNamespaceError(err)
	}

	revision, err := nss.ds.WriteNamespace(req.Config)
	if err != nil {
		return nil, rewriteNamespaceError(err)
	}

	return &api.WriteConfigResponse{
		Revision: zookie.NewFromRevision(revision),
	}, nil
}

func (nss *nsServer) ReadConfig(ctx context.Context, req *api.ReadConfigRequest) (*api.ReadConfigResponse, error) {
	found, version, err := nss.ds.ReadNamespace(req.Namespace)
	if err != nil {
		return nil, rewriteNamespaceError(err)
	}

	return &api.ReadConfigResponse{
		Namespace: req.Namespace,
		Config:    found,
		Revision:  zookie.NewFromRevision(version),
	}, nil
}

func rewriteNamespaceError(err error) error {
	switch err {
	case datastore.ErrNamespaceNotFound:
		return status.Errorf(codes.NotFound, "namespace not found: %s", err)
	default:
		log.Err(err)
		return err
	}
}
