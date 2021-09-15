package v1alpha1

import (
	"context"
	"errors"

	v1alpha1 "github.com/authzed/authzed-go/proto/authzed/api/v1alpha1"
	"github.com/authzed/grpcutil"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/namespace"
	"github.com/authzed/spicedb/internal/services/serviceerrors"
	"github.com/authzed/spicedb/internal/services/shared"
	"github.com/authzed/spicedb/internal/sharederrors"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/generator"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
)

// PrefixRequiredOption is an option to the schema server indicating whether
// prefixes are required on schema object definitions.
type PrefixRequiredOption int

const (
	// PrefixNotRequired indicates that prefixes are not required.
	PrefixNotRequired PrefixRequiredOption = iota

	// PrefixRequired indicates that prefixes are required.
	PrefixRequired
)

type schemaServiceServer struct {
	v1alpha1.UnimplementedSchemaServiceServer

	prefixRequired PrefixRequiredOption
	ds             datastore.Datastore
}

// RegisterSchemaServer adds the Schema Server to a grpc service registrar
// This is preferred over manually registering the service; it will add required middleware
func RegisterSchemaServer(r grpc.ServiceRegistrar, s v1alpha1.SchemaServiceServer) *grpc.ServiceDesc {
	r.RegisterService(grpcutil.WrapMethods(v1alpha1.SchemaService_ServiceDesc, grpcutil.DefaultUnaryMiddleware...), s)
	return &v1alpha1.SchemaService_ServiceDesc
}

// NewSchemaServer returns an new instance of a server that implements
// authzed.api.v1alpha1.SchemaService.
func NewSchemaServer(ds datastore.Datastore, prefixRequired PrefixRequiredOption) v1alpha1.SchemaServiceServer {
	return &schemaServiceServer{ds: ds, prefixRequired: prefixRequired}
}

func (ss *schemaServiceServer) ReadSchema(ctx context.Context, in *v1alpha1.ReadSchemaRequest) (*v1alpha1.ReadSchemaResponse, error) {
	var objectDefs []string
	for _, objectDefName := range in.GetObjectDefinitionsNames() {
		found, _, err := ss.ds.ReadNamespace(ctx, objectDefName)
		if err != nil {
			return nil, rewriteError(err)
		}

		objectDef, _ := generator.GenerateSource(found)
		objectDefs = append(objectDefs, objectDef)
	}

	return &v1alpha1.ReadSchemaResponse{
		ObjectDefinitions: objectDefs,
	}, nil
}

func (ss *schemaServiceServer) WriteSchema(ctx context.Context, in *v1alpha1.WriteSchemaRequest) (*v1alpha1.WriteSchemaResponse, error) {
	log.Trace().Str("schema", in.GetSchema()).Msg("requested Schema to be written")
	nsm, err := namespace.NewCachingNamespaceManager(ss.ds, 0, nil) // non-caching manager
	if err != nil {
		return nil, rewriteError(err)
	}

	inputSchema := compiler.InputSchema{
		Source:       input.InputSource("schema"),
		SchemaString: in.GetSchema(),
	}

	var prefix *string
	if ss.prefixRequired == PrefixNotRequired {
		empty := ""
		prefix = &empty
	}

	nsdefs, err := compiler.Compile([]compiler.InputSchema{inputSchema}, prefix)
	if err != nil {
		return nil, rewriteError(err)
	}
	log.Trace().Interface("namespace definitions", nsdefs).Msg("compiled namespace definitions")

	for _, nsdef := range nsdefs {
		ts, err := namespace.BuildNamespaceTypeSystem(nsdef, nsm, nsdefs...)
		if err != nil {
			return nil, rewriteError(err)
		}

		if err := ts.Validate(ctx); err != nil {
			return nil, rewriteError(err)
		}

		if err := shared.SanityCheckExistingRelationships(ctx, ss.ds, nsdef); err != nil {
			return nil, rewriteError(err)
		}
	}
	log.Trace().Interface("namespace definitions", nsdefs).Msg("validated namespace definitions")

	var names []string
	for _, nsdef := range nsdefs {
		if _, err := ss.ds.WriteNamespace(ctx, nsdef); err != nil {
			return nil, rewriteError(err)
		}

		names = append(names, nsdef.Name)
	}
	log.Trace().Interface("namespace definitions", nsdefs).Msg("wrote namespace definitions")

	return &v1alpha1.WriteSchemaResponse{
		ObjectDefinitionsNames: names,
	}, nil
}

func rewriteError(err error) error {
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
