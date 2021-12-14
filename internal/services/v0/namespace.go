package v0

import (
	"context"
	"errors"
	"fmt"
	"time"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	"github.com/authzed/grpcutil"
	grpcmw "github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/rs/zerolog/log"
	"github.com/shopspring/decimal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/namespace"
	"github.com/authzed/spicedb/internal/services/serviceerrors"
	"github.com/authzed/spicedb/internal/services/shared"
	"github.com/authzed/spicedb/pkg/zookie"
)

type nsServer struct {
	v0.UnimplementedNamespaceServiceServer
	shared.WithUnaryServiceSpecificInterceptor

	ds datastore.Datastore
}

// NewNamespaceServer creates an instance of the namespace server.
func NewNamespaceServer(ds datastore.Datastore) v0.NamespaceServiceServer {
	s := &nsServer{
		ds: ds,
		WithUnaryServiceSpecificInterceptor: shared.WithUnaryServiceSpecificInterceptor{
			Unary: grpcmw.ChainUnaryServer(grpcutil.DefaultUnaryMiddleware...),
		},
	}
	return s
}

func (nss *nsServer) WriteConfig(ctx context.Context, req *v0.WriteConfigRequest) (*v0.WriteConfigResponse, error) {
	nsm, err := namespace.NewCachingNamespaceManager(nss.ds, 0*time.Second, nil)
	if err != nil {
		return nil, rewriteNamespaceError(ctx, err)
	}

	readRevision, err := nss.ds.HeadRevision(ctx)
	if err != nil {
		return nil, rewriteNamespaceError(ctx, err)
	}

	for _, config := range req.Configs {
		// Validate the type system for the updated namespace.
		ts, terr := namespace.BuildNamespaceTypeSystemWithFallback(config, nsm, req.Configs, readRevision)
		if terr != nil {
			return nil, rewriteNamespaceError(ctx, terr)
		}

		tverr := ts.Validate(ctx)
		if tverr != nil {
			return nil, rewriteNamespaceError(ctx, tverr)
		}

		// Ensure that the updated namespace does not break the existing tuple data.
		//
		// NOTE: We use the datastore here to read the namespace, rather than the namespace manager,
		// to ensure there is no caching being used.
		existing, _, err := nss.ds.ReadNamespace(ctx, config.Name, readRevision)
		if err != nil && !errors.As(err, &datastore.ErrNamespaceNotFound{}) {
			return nil, rewriteNamespaceError(ctx, err)
		}

		diff, err := namespace.DiffNamespaces(existing, config)
		if err != nil {
			return nil, rewriteNamespaceError(ctx, err)
		}

		headRevision, err := nss.ds.HeadRevision(ctx)
		if err != nil {
			return nil, rewriteNamespaceError(ctx, err)
		}

		for _, delta := range diff.Deltas() {
			switch delta.Type {
			case namespace.RemovedRelation:
				err = errorIfTupleIteratorReturnsTuples(
					ctx,
					nss.ds.QueryTuples(datastore.TupleQueryResourceFilter{
						ResourceType:             config.Name,
						OptionalResourceRelation: delta.RelationName,
					}, headRevision),
					"cannot delete relation `%s` in definition `%s`, as a relationship exists under it", delta.RelationName, config.Name)
				if err != nil {
					return nil, rewriteNamespaceError(ctx, err)
				}

				// Also check for right sides of tuples.
				err = errorIfTupleIteratorReturnsTuples(
					ctx,
					nss.ds.ReverseQueryTuplesFromSubjectRelation(config.Name, delta.RelationName, headRevision),
					"cannot delete relation `%s` in definition `%s`, as a relationship references it", delta.RelationName, config.Name)
				if err != nil {
					return nil, rewriteNamespaceError(ctx, err)
				}

			case namespace.RelationDirectTypeRemoved:
				err = errorIfTupleIteratorReturnsTuples(
					ctx,
					nss.ds.ReverseQueryTuplesFromSubjectRelation(delta.DirectType.Namespace, delta.DirectType.Relation, headRevision).
						WithObjectRelation(config.Name, delta.RelationName),
					"cannot remove allowed relation/permission `%s#%s` from relation `%s` in definition `%s`, as a relationship exists with it",
					delta.DirectType.Namespace, delta.DirectType.Relation, delta.RelationName, config.Name)
				if err != nil {
					return nil, rewriteNamespaceError(ctx, err)
				}
			}
		}
	}

	revision := decimal.Zero
	for _, config := range req.Configs {
		var err error
		revision, err = nss.ds.WriteNamespace(ctx, config)
		if err != nil {
			return nil, rewriteNamespaceError(ctx, err)
		}
	}

	return &v0.WriteConfigResponse{
		Revision: zookie.NewFromRevision(revision),
	}, nil
}

func (nss *nsServer) ReadConfig(ctx context.Context, req *v0.ReadConfigRequest) (*v0.ReadConfigResponse, error) {
	readRevision, err := nss.ds.HeadRevision(ctx)
	if err != nil {
		return nil, rewriteNamespaceError(ctx, err)
	}

	found, _, err := nss.ds.ReadNamespace(ctx, req.Namespace, readRevision)
	if err != nil {
		return nil, rewriteNamespaceError(ctx, err)
	}

	return &v0.ReadConfigResponse{
		Namespace: req.Namespace,
		Config:    found,
		Revision:  zookie.NewFromRevision(readRevision),
	}, nil
}

func (nss *nsServer) DeleteConfigs(ctx context.Context, req *v0.DeleteConfigsRequest) (*v0.DeleteConfigsResponse, error) {
	headRevision, err := nss.ds.HeadRevision(ctx)
	if err != nil {
		return nil, rewriteNamespaceError(ctx, err)
	}

	// Ensure that all the specified namespaces can be deleted.
	for _, nsName := range req.Namespaces {
		// Ensure the namespace exists.
		_, _, err := nss.ds.ReadNamespace(ctx, nsName, headRevision)
		if err != nil {
			return nil, rewriteNamespaceError(ctx, err)
		}

		// Check for relationships under the namespace.
		err = errorIfTupleIteratorReturnsTuples(
			ctx,
			nss.ds.QueryTuples(datastore.TupleQueryResourceFilter{
				ResourceType: nsName,
			}, headRevision),
			"cannot delete definition `%s`, as a relationship exists under it", nsName)
		if err != nil {
			return nil, rewriteNamespaceError(ctx, err)
		}

		// Also check for right sides of relationships.
		err = errorIfTupleIteratorReturnsTuples(
			ctx,
			nss.ds.ReverseQueryTuplesFromSubjectNamespace(nsName, headRevision),
			"cannot delete definition `%s`, as a relationship references it", nsName)
		if err != nil {
			return nil, rewriteNamespaceError(ctx, err)
		}
	}

	// Delete all the namespaces specified.
	for _, nsName := range req.Namespaces {
		if _, err := nss.ds.DeleteNamespace(ctx, nsName); err != nil {
			return nil, rewriteNamespaceError(ctx, err)
		}
	}

	return &v0.DeleteConfigsResponse{
		Revision: zookie.NewFromRevision(headRevision),
	}, nil
}

func errorIfTupleIteratorReturnsTuples(ctx context.Context, query datastore.CommonTupleQuery, message string, args ...interface{}) error {
	qy, err := query.Limit(1).Execute(ctx)
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

func rewriteNamespaceError(ctx context.Context, err error) error {
	switch {
	case errors.As(err, &datastore.ErrNamespaceNotFound{}):
		return status.Errorf(codes.NotFound, "object definition not found: %s", err)

	case errors.As(err, &datastore.ErrReadOnly{}):
		return serviceerrors.ErrServiceReadOnly

	default:
		log.Ctx(ctx).Err(err)
		return err
	}
}
