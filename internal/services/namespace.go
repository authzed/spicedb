package services

import (
	"context"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/namespace"
	api "github.com/authzed/spicedb/pkg/REDACTEDapi/api"
	"github.com/authzed/spicedb/pkg/zookie"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type nsServer struct {
	api.UnimplementedNamespaceServiceServer

	ds  datastore.Datastore
	nsm namespace.Manager
}

// NewNamespaceServer creates an instance of the namespace server.
func NewNamespaceServer(ds datastore.Datastore, nsm namespace.Manager) api.NamespaceServiceServer {
	s := &nsServer{ds: ds, nsm: nsm}
	return s
}

func (nss *nsServer) WriteConfig(ctx context.Context, req *api.WriteConfigRequest) (*api.WriteConfigResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, rewriteNamespaceError(err)
	}

	for _, config := range req.Configs {
		ts, terr := namespace.BuildNamespaceTypeSystem(config, nss.nsm, req.Configs...)
		if terr != nil {
			return nil, rewriteNamespaceError(terr)
		}

		tverr := ts.Validate(ctx)
		if tverr != nil {
			return nil, rewriteNamespaceError(tverr)
		}
	}

	var revision uint64 = 0
	for _, config := range req.Configs {
		var err error
		revision, err = nss.ds.WriteNamespace(ctx, config)
		if err != nil {
			return nil, rewriteNamespaceError(err)
		}
	}

	return &api.WriteConfigResponse{
		Revision: zookie.NewFromRevision(revision),
	}, nil
}

func (nss *nsServer) ReadConfig(ctx context.Context, req *api.ReadConfigRequest) (*api.ReadConfigResponse, error) {
	found, version, err := nss.ds.ReadNamespace(ctx, req.Namespace)
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
