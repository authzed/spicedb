package v1

import (
	"context"
	"errors"
	"strings"

	"github.com/authzed/grpcutil"
	grpcvalidate "github.com/grpc-ecosystem/go-grpc-middleware/validator"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/namespace"
	"github.com/authzed/spicedb/internal/services/serviceerrors"
	"github.com/authzed/spicedb/internal/services/shared"
	"github.com/authzed/spicedb/internal/sharederrors"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/generator"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
)

// RegisterSchemaServer adds a schema server to a grpc service registrar
// This is preferred over manually registering the service; it will add required middleware
func RegisterSchemaServer(r grpc.ServiceRegistrar, ds datastore.Datastore) *grpc.ServiceDesc {
	s := newSchemaServer(ds)

	wrapped := grpcutil.WrapMethods(
		v1.SchemaService_ServiceDesc,
		grpcvalidate.UnaryServerInterceptor(),
	)

	r.RegisterService(wrapped, s)
	return &v1.SchemaService_ServiceDesc
}

func newSchemaServer(ds datastore.Datastore) v1.SchemaServiceServer {
	return &schemaServer{ds: ds}
}

type schemaServer struct {
	v1.UnimplementedSchemaServiceServer

	ds datastore.Datastore
}

func (ss *schemaServer) ReadSchema(ctx context.Context, in *v1.ReadSchemaRequest) (*v1.ReadSchemaResponse, error) {
	nsDefs, err := ss.ds.ListNamespaces(ctx)
	if err != nil {
		return nil, rewriteSchemaError(err)
	}

	if len(nsDefs) == 0 {
		return nil, status.Errorf(codes.NotFound, "No schema has been defined; please call WriteSchema to start")
	}

	var objectDefs []string
	for _, nsDef := range nsDefs {
		objectDef, _ := generator.GenerateSource(nsDef)
		objectDefs = append(objectDefs, objectDef)
	}

	return &v1.ReadSchemaResponse{
		SchemaText: strings.Join(objectDefs, "\n\n"),
	}, nil
}

func (ss *schemaServer) WriteSchema(ctx context.Context, in *v1.WriteSchemaRequest) (*v1.WriteSchemaResponse, error) {
	log.Trace().Str("schema", in.GetSchema()).Msg("requested Schema to be written")
	nsm, err := namespace.NewCachingNamespaceManager(ss.ds, 0, nil) // non-caching manager
	if err != nil {
		return nil, rewriteSchemaError(err)
	}

	inputSchema := compiler.InputSchema{
		Source:       input.InputSource("schema"),
		SchemaString: in.GetSchema(),
	}

	emptyDefaultPrefix := ""
	nsdefs, err := compiler.Compile([]compiler.InputSchema{inputSchema}, &emptyDefaultPrefix)
	if err != nil {
		return nil, rewriteSchemaError(err)
	}
	log.Trace().Interface("namespace definitions", nsdefs).Msg("compiled namespace definitions")

	for _, nsdef := range nsdefs {
		ts, err := namespace.BuildNamespaceTypeSystem(nsdef, nsm, nsdefs...)
		if err != nil {
			return nil, rewriteSchemaError(err)
		}

		if err := ts.Validate(ctx); err != nil {
			return nil, rewriteSchemaError(err)
		}

		if err := shared.SanityCheckExistingRelationships(ctx, ss.ds, nsdef); err != nil {
			return nil, rewriteSchemaError(err)
		}
	}
	log.Trace().Interface("namespace definitions", nsdefs).Msg("validated namespace definitions")

	var names []string
	for _, nsdef := range nsdefs {
		if _, err := ss.ds.WriteNamespace(ctx, nsdef); err != nil {
			return nil, rewriteSchemaError(err)
		}

		names = append(names, nsdef.Name)
	}
	log.Trace().Interface("namespace definitions", nsdefs).Strs("names", names).Msg("wrote namespace definitions")

	return &v1.WriteSchemaResponse{}, nil
}

func rewriteSchemaError(err error) error {
	var nsNotFoundError sharederrors.UnknownNamespaceError
	var errWithContext compiler.ErrorWithContext

	switch {
	case errors.As(err, &nsNotFoundError):
		return status.Errorf(codes.NotFound, "Object Definition `%s` not found", nsNotFoundError.NotFoundNamespaceName())
	case errors.As(err, &errWithContext):
		return status.Errorf(codes.InvalidArgument, "%s", err)
	case errors.As(err, &datastore.ErrReadOnly{}):
		return serviceerrors.ErrServiceReadOnly
	default:
		log.Err(err)
		return err
	}
}
