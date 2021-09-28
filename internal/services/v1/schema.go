package v1

import (
	"context"
	"errors"
	"strings"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	grpcvalidate "github.com/grpc-ecosystem/go-grpc-middleware/validator"
	"github.com/rs/zerolog/log"
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

// NewSchemaServer creates a SchemaServiceServer instance.
func NewSchemaServer(ds datastore.Datastore) v1.SchemaServiceServer {
	return &schemaServer{
		ds: ds,
		WithServiceSpecificInterceptors: shared.WithServiceSpecificInterceptors{
			Unary:  grpcvalidate.UnaryServerInterceptor(),
			Stream: grpcvalidate.StreamServerInterceptor(),
		},
	}
}

type schemaServer struct {
	v1.UnimplementedSchemaServiceServer
	shared.WithServiceSpecificInterceptors

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
	inputSchema := compiler.InputSchema{
		Source:       input.InputSource("schema"),
		SchemaString: in.GetSchema(),
	}

	// Build a map of existing definitions to determine those being removed, if any.
	existingDefs, err := ss.ds.ListNamespaces(ctx)
	if err != nil {
		return nil, rewriteSchemaError(err)
	}

	existingDefMap := map[string]bool{}
	for _, existingDef := range existingDefs {
		existingDefMap[existingDef.Name] = true
	}

	// Compile the schema into the namespace definitions.
	emptyDefaultPrefix := ""
	nsdefs, err := compiler.Compile([]compiler.InputSchema{inputSchema}, &emptyDefaultPrefix)
	if err != nil {
		return nil, rewriteSchemaError(err)
	}
	log.Trace().Interface("namespace definitions", nsdefs).Msg("compiled namespace definitions")

	// For each definition, perform a diff and ensure the changes will not result in any
	// relationships left without associated schema.
	for _, nsdef := range nsdefs {
		ts, err := namespace.BuildNamespaceTypeSystemForDefs(nsdef, nsdefs)
		if err != nil {
			return nil, rewriteSchemaError(err)
		}

		if err := ts.Validate(ctx); err != nil {
			return nil, rewriteSchemaError(err)
		}

		if err := shared.SanityCheckExistingRelationships(ctx, ss.ds, nsdef); err != nil {
			return nil, rewriteSchemaError(err)
		}

		existingDefMap[nsdef.Name] = false
	}
	log.Trace().Interface("namespace definitions", nsdefs).Msg("validated namespace definitions")

	// Ensure that deleting namespaces will not result in any relationships left without associated
	// schema.
	for nsdefName, removed := range existingDefMap {
		if !removed {
			continue
		}

		err := shared.EnsureNoRelationshipsExist(ctx, ss.ds, nsdefName)
		if err != nil {
			return nil, rewriteSchemaError(err)
		}
	}

	// Write the new namespaces.
	var names []string
	for _, nsdef := range nsdefs {
		if _, err := ss.ds.WriteNamespace(ctx, nsdef); err != nil {
			return nil, rewriteSchemaError(err)
		}

		names = append(names, nsdef.Name)
	}

	// Delete the removed namespaces.
	var removedNames []string
	for nsdefName, removed := range existingDefMap {
		if !removed {
			continue
		}
		if _, err := ss.ds.DeleteNamespace(ctx, nsdefName); err != nil {
			return nil, rewriteSchemaError(err)
		}
		removedNames = append(removedNames, nsdefName)
	}

	log.Trace().Interface("namespace definitions", nsdefs).Strs("added/changed", names).Strs("removed", removedNames).Msg("wrote namespace definitions")

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
