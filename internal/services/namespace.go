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
		//
		// NOTE: We use the datastore here to read the namespace, rather than the namespace manager,
		// to ensure there is no caching being used.
		existing, revision, err := nss.ds.ReadNamespace(ctx, config.Name)
		if err != nil && err != datastore.ErrNamespaceNotFound {
			return nil, rewriteNamespaceError(err)
		}

		diff, err := namespace.DiffNamespaces(existing, config)
		if err != nil {
			return nil, rewriteNamespaceError(err)
		}

		for _, delta := range diff.Deltas() {
			switch delta.Type {
			case namespace.RemovedRelation:
				err = errorIfTupleIteratorReturnsTuples(
					nss.ds.QueryTuples(config.Name, revision).WithRelation(delta.RelationName).Limit(1),
					ctx,
					"cannot delete relation `%s` in namespace `%s`, as a tuple exists under it", delta.RelationName, config.Name)
				if err != nil {
					return nil, rewriteNamespaceError(err)
				}

				// Also check for right sides of tuples.
				err = errorIfTupleIteratorReturnsTuples(
					nss.ds.ReverseQueryTuples(revision).WithSubjectRelation(config.Name, delta.RelationName).Limit(1),
					ctx,
					"cannot delete relation `%s` in namespace `%s`, as a tuple references it", delta.RelationName, config.Name)
				if err != nil {
					return nil, rewriteNamespaceError(err)
				}

			case namespace.RelationDirectTypeRemoved:
				err = errorIfTupleIteratorReturnsTuples(
					nss.ds.ReverseQueryTuples(revision).
						WithObjectRelation(config.Name, delta.RelationName).
						WithSubjectRelation(delta.DirectType.Namespace, delta.DirectType.Relation).
						Limit(1),
					ctx,
					"cannot remove allowed direct relation `%s#%s` from relation `%s` in namespace `%s`, as a tuple exists with it",
					delta.DirectType.Namespace, delta.DirectType.Relation, delta.RelationName, config.Name)
				if err != nil {
					return nil, rewriteNamespaceError(err)
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

func errorIfTupleIteratorReturnsTuples(query datastore.CommonTupleQuery, ctx context.Context, message string, args ...interface{}) error {
	qy, err := query.Execute(ctx)
	if err != nil {
		return err
	}
	defer qy.Close()

	rt := qy.Next()
	if rt != nil {
		if qy.Err() != nil {
			return qy.Err()
		}

		return status.Errorf(codes.InvalidArgument, fmt.Sprintf(message, args...))
	}
	return nil
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
