package v0

import (
	"context"
	"errors"
	"time"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/grpcutil"
	grpcmw "github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/rs/zerolog/log"
	"github.com/shopspring/decimal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/datastore/options"
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
				qy, qyErr := nss.ds.QueryTuples(
					ctx,
					&v1.RelationshipFilter{
						ResourceType:     config.Name,
						OptionalRelation: delta.RelationName,
					},
					headRevision,
					options.WithLimit(options.LimitOne),
				)
				err = shared.ErrorIfTupleIteratorReturnsTuples(
					ctx,
					qy,
					qyErr,
					"cannot delete relation `%s` in definition `%s`, as a relationship exists under it", delta.RelationName, config.Name)
				if err != nil {
					return nil, rewriteNamespaceError(ctx, err)
				}

				// Also check for right sides of tuples.
				qy, qyErr = nss.ds.ReverseQueryTuples(ctx, &v1.SubjectFilter{
					SubjectType: config.Name,
					OptionalRelation: &v1.SubjectFilter_RelationFilter{
						Relation: delta.RelationName,
					},
				}, headRevision, options.WithReverseLimit(options.LimitOne))
				err = shared.ErrorIfTupleIteratorReturnsTuples(
					ctx,
					qy,
					qyErr,
					"cannot delete relation `%s` in definition `%s`, as a relationship references it", delta.RelationName, config.Name)
				if err != nil {
					return nil, rewriteNamespaceError(ctx, err)
				}

			case namespace.RelationDirectTypeRemoved:
				qy, qyErr := nss.ds.ReverseQueryTuples(
					ctx,
					&v1.SubjectFilter{
						SubjectType: delta.DirectType.Namespace,
						OptionalRelation: &v1.SubjectFilter_RelationFilter{
							Relation: delta.DirectType.Relation,
						},
					},
					headRevision,
					options.WithResRelation(&options.ResourceRelation{
						Namespace: config.Name,
						Relation:  delta.RelationName,
					}),
					options.WithReverseLimit(options.LimitOne),
				)
				err = shared.ErrorIfTupleIteratorReturnsTuples(
					ctx,
					qy,
					qyErr,
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
		qy, qyErr := nss.ds.QueryTuples(
			ctx,
			&v1.RelationshipFilter{
				ResourceType: nsName,
			},
			headRevision,
			options.WithLimit(options.LimitOne),
		)
		err = shared.ErrorIfTupleIteratorReturnsTuples(
			ctx,
			qy,
			qyErr,
			"cannot delete definition `%s`, as a relationship exists under it", nsName)
		if err != nil {
			return nil, rewriteNamespaceError(ctx, err)
		}

		// Also check for right sides of relationships.
		qy, qyErr = nss.ds.ReverseQueryTuples(ctx, &v1.SubjectFilter{
			SubjectType: nsName,
		}, headRevision, options.WithReverseLimit(options.LimitOne))
		err = shared.ErrorIfTupleIteratorReturnsTuples(
			ctx,
			qy,
			qyErr,
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
