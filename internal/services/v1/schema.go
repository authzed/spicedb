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
	readRevision, err := ss.ds.HeadRevision(ctx)
	if err != nil {
		return nil, rewritePermissionsError(ctx, err)
	}

	nsDefs, err := ss.ds.ListNamespaces(ctx, readRevision)
	if err != nil {
		return nil, rewriteSchemaError(ctx, err)
	}

	if len(nsDefs) == 0 {
		return nil, status.Errorf(codes.NotFound, "No schema has been defined; please call WriteSchema to start")
	}

	objectDefs := make([]string, 0, len(nsDefs))
	for _, nsDef := range nsDefs {
		objectDef, _ := generator.GenerateSource(nsDef)
		objectDefs = append(objectDefs, objectDef)
	}

	return &v1.ReadSchemaResponse{
		SchemaText: strings.Join(objectDefs, "\n\n"),
	}, nil
}

func (ss *schemaServer) WriteSchema(ctx context.Context, in *v1.WriteSchemaRequest) (*v1.WriteSchemaResponse, error) {
	log.Ctx(ctx).Trace().Str("schema", in.GetSchema()).Msg("requested Schema to be written")

	readRevision, err := ss.ds.HeadRevision(ctx)
	if err != nil {
		return nil, rewritePermissionsError(ctx, err)
	}

	inputSchema := compiler.InputSchema{
		Source:       input.Source("schema"),
		SchemaString: in.GetSchema(),
	}

	// Build a map of existing definitions to determine those being removed, if any.
	existingDefs, err := ss.ds.ListNamespaces(ctx, readRevision)
	if err != nil {
		return nil, rewriteSchemaError(ctx, err)
	}

	existingDefMap := map[string]bool{}
	for _, existingDef := range existingDefs {
		existingDefMap[existingDef.Name] = true
	}

	// Compile the schema into the namespace definitions.
	emptyDefaultPrefix := ""
	nsdefs, err := compiler.Compile([]compiler.InputSchema{inputSchema}, &emptyDefaultPrefix)
	if err != nil {
		return nil, rewriteSchemaError(ctx, err)
	}
	log.Ctx(ctx).Trace().Interface("namespaceDefinitions", nsdefs).Msg("compiled namespace definitions")

	// For each definition, perform a diff and ensure the changes will not result in any
	// relationships left without associated schema.
	for _, nsdef := range nsdefs {
		ts, err := namespace.BuildNamespaceTypeSystemForDefs(nsdef, nsdefs)
		if err != nil {
			return nil, rewriteSchemaError(ctx, err)
		}

		if err := ts.Validate(ctx); err != nil {
			return nil, rewriteSchemaError(ctx, err)
		}

		if err := shared.SanityCheckExistingRelationships(ctx, ss.ds, nsdef, readRevision); err != nil {
			return nil, rewriteSchemaError(ctx, err)
		}

		existingDefMap[nsdef.Name] = false
	}
	log.Ctx(ctx).Trace().Interface("namespaceDefinitions", nsdefs).Msg("validated namespace definitions")

	// Ensure that deleting namespaces will not result in any relationships left without associated
	// schema.
	for nsdefName, removed := range existingDefMap {
		if !removed {
			continue
		}

		err := shared.EnsureNoRelationshipsExist(ctx, ss.ds, nsdefName)
		if err != nil {
			return nil, rewriteSchemaError(ctx, err)
		}
	}

	// Write the new namespaces.
	names := make([]string, 0, len(nsdefs))
	for _, nsdef := range nsdefs {
		if _, err := ss.ds.WriteNamespace(ctx, nsdef); err != nil {
			return nil, rewriteSchemaError(ctx, err)
		}

		names = append(names, nsdef.Name)
	}

	// Delete the removed namespaces.
	removedNames := make([]string, 0, len(existingDefMap))
	for nsdefName, removed := range existingDefMap {
		if !removed {
			continue
		}
		if _, err := ss.ds.DeleteNamespace(ctx, nsdefName); err != nil {
			return nil, rewriteSchemaError(ctx, err)
		}
		removedNames = append(removedNames, nsdefName)
	}

	log.Ctx(ctx).Trace().Interface("namespaceDefinitions", nsdefs).Strs("addedOrChanged", names).Strs("removed", removedNames).Msg("wrote namespace definitions")

	return &v1.WriteSchemaResponse{}, nil
}

func rewriteSchemaError(ctx context.Context, err error) error {
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
		log.Ctx(ctx).Err(err)
		return err
	}
}
