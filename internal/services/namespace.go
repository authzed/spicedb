package services

import (
	"context"
	"fmt"

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
		// Validate the type system for the updated namespace.
		ts, terr := namespace.BuildNamespaceTypeSystem(config, nss.nsm, req.Configs...)
		if terr != nil {
			return nil, rewriteNamespaceError(terr)
		}

		tverr := ts.Validate(ctx)
		if tverr != nil {
			return nil, rewriteNamespaceError(tverr)
		}

		// Ensure that the updated namespace does not break the existing tuple data.
		existing, revision, err := nss.nsm.ReadNamespace(ctx, config.Name)
		if err != nil && err != namespace.ErrInvalidNamespace {
			return nil, rewriteNamespaceError(err)
		}

		diff, err := namespace.DiffNamespaces(existing, config)
		if err != nil {
			return nil, rewriteNamespaceError(err)
		}

		for _, delta := range diff.Deltas() {
			switch delta.Type {
			case namespace.RemovedRelation:
				query, err := nss.ds.QueryTuples(config.Name, revision).WithRelation(delta.RelationName).Execute(ctx)
				if err != nil {
					return nil, rewriteNamespaceError(err)
				}
				defer query.Close()

				rt := query.Next()
				if rt != nil {
					if query.Err() != nil {
						return nil, rewriteNamespaceError(query.Err())
					}

					return nil, status.Errorf(codes.InvalidArgument, fmt.Sprintf("cannot delete relation `%s` in namespace `%s`, as a tuple exists under it", delta.RelationName, config.Name))
				}

				// Also check for right sides of tuples.
				query, err = nss.ds.ReverseQueryTuples(revision).
					WithSubjectRelation(config.Name, delta.RelationName).
					Execute(ctx)
				if err != nil {
					return nil, rewriteNamespaceError(err)
				}
				defer query.Close()

				rt = query.Next()
				if rt != nil {
					if query.Err() != nil {
						return nil, rewriteNamespaceError(query.Err())
					}

					return nil, status.Errorf(codes.InvalidArgument, fmt.Sprintf("cannot delete relation `%s` in namespace `%s`, as a tuple references it", delta.RelationName, config.Name))
				}

			case namespace.RelationDirectTypeRemoved:
				query, err := nss.ds.ReverseQueryTuples(revision).
					WithObjectRelation(config.Name, delta.RelationName).
					WithSubjectRelation(delta.DirectType.Namespace, delta.DirectType.Relation).
					Execute(ctx)
				if err != nil {
					return nil, rewriteNamespaceError(err)
				}
				defer query.Close()

				rt := query.Next()
				if rt != nil {
					if query.Err() != nil {
						return nil, rewriteNamespaceError(query.Err())
					}

					return nil, status.Errorf(codes.InvalidArgument, fmt.Sprintf("cannot remove allowed direct relation `%s#%s` from relation `%s` in namespace `%s`, as a tuple exists with it", delta.DirectType.Namespace, delta.DirectType.Relation, delta.RelationName, config.Name))
				}
			}
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
