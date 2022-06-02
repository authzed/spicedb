package v1

import (
	"context"
	"errors"
	"strings"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	grpcvalidate "github.com/grpc-ecosystem/go-grpc-middleware/validator"
	"github.com/rs/zerolog/log"
	"github.com/scylladb/go-set/strset"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/authzed/spicedb/internal/middleware/consistency"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/internal/middleware/usagemetrics"
	"github.com/authzed/spicedb/internal/namespace"
	"github.com/authzed/spicedb/internal/services/serviceerrors"
	"github.com/authzed/spicedb/internal/services/shared"
	"github.com/authzed/spicedb/internal/sharederrors"
	"github.com/authzed/spicedb/pkg/commonerrors"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	dispatchv1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/generator"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
)

// NewSchemaServer creates a SchemaServiceServer instance.
func NewSchemaServer() v1.SchemaServiceServer {
	return &schemaServer{
		WithServiceSpecificInterceptors: shared.WithServiceSpecificInterceptors{
			Unary:  grpcvalidate.UnaryServerInterceptor(),
			Stream: grpcvalidate.StreamServerInterceptor(),
		},
	}
}

type schemaServer struct {
	v1.UnimplementedSchemaServiceServer
	shared.WithServiceSpecificInterceptors
}

func (ss *schemaServer) ReadSchema(ctx context.Context, in *v1.ReadSchemaRequest) (*v1.ReadSchemaResponse, error) {
	readRevision, _ := consistency.MustRevisionFromContext(ctx)
	ds := datastoremw.MustFromContext(ctx).SnapshotReader(readRevision)

	nsDefs, err := ds.ListNamespaces(ctx)
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

	usagemetrics.SetInContext(ctx, &dispatchv1.ResponseMeta{
		DispatchCount: uint32(len(nsDefs)),
	})

	return &v1.ReadSchemaResponse{
		SchemaText: strings.Join(objectDefs, "\n\n"),
	}, nil
}

func (ss *schemaServer) WriteSchema(ctx context.Context, in *v1.WriteSchemaRequest) (*v1.WriteSchemaResponse, error) {
	log.Ctx(ctx).Trace().Str("schema", in.GetSchema()).Msg("requested Schema to be written")

	ds := datastoremw.MustFromContext(ctx)

	inputSchema := compiler.InputSchema{
		Source:       input.Source("schema"),
		SchemaString: in.GetSchema(),
	}

	// Compile the schema into the namespace definitions.
	emptyDefaultPrefix := ""
	nsdefs, err := compiler.Compile([]compiler.InputSchema{inputSchema}, &emptyDefaultPrefix)
	if err != nil {
		return nil, rewriteSchemaError(ctx, err)
	}
	log.Ctx(ctx).Trace().Interface("namespaceDefinitions", nsdefs).Msg("compiled namespace definitions")

	// Do as much validation as we can before talking to the datastore
	newDefs := strset.NewWithSize(len(nsdefs))
	for _, nsdef := range nsdefs {
		ts, err := namespace.BuildNamespaceTypeSystemForDefs(nsdef, nsdefs)
		if err != nil {
			return nil, rewriteSchemaError(ctx, err)
		}

		vts, err := ts.Validate(ctx)
		if err != nil {
			return nil, rewriteSchemaError(ctx, err)
		}

		if err := namespace.AnnotateNamespace(vts); err != nil {
			return nil, rewriteSchemaError(ctx, err)
		}

		newDefs.Add(nsdef.Name)
	}

	_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		// Build a map of existing definitions to determine those being removed, if any.
		existingDefs, err := rwt.ListNamespaces(ctx)
		if err != nil {
			return err
		}

		existingDefMap := make(map[string]*core.NamespaceDefinition, len(existingDefs))
		existing := strset.NewWithSize(len(existingDefs))
		for _, existingDef := range existingDefs {
			existingDefMap[existingDef.Name] = existingDef
			existing.Add(existingDef.Name)
		}

		// For each definition, perform a diff and ensure the changes will not result in any
		// relationships left without associated schema.
		for _, nsdef := range nsdefs {
			if err := shared.SanityCheckExistingRelationships(ctx, rwt, nsdef, existingDefMap); err != nil {
				return err
			}
		}
		log.Ctx(ctx).Trace().Interface("namespaceDefinitions", nsdefs).Msg("validated namespace definitions")

		// Ensure that deleting namespaces will not result in any relationships left without associated
		// schema.
		removed := strset.Difference(existing, newDefs)
		var checkRelErr error
		removed.Each(func(nsdefName string) bool {
			checkRelErr = shared.EnsureNoRelationshipsExist(ctx, rwt, nsdefName)
			return checkRelErr == nil
		})
		if checkRelErr != nil {
			return checkRelErr
		}

		// Write the new namespaces.
		if err := rwt.WriteNamespaces(nsdefs...); err != nil {
			return err
		}

		usagemetrics.SetInContext(ctx, &dispatchv1.ResponseMeta{
			DispatchCount: uint32(len(nsdefs)),
		})

		// Delete the removed namespaces.
		var removeErr error
		removed.Each(func(nsdefName string) bool {
			removeErr = rwt.DeleteNamespace(nsdefName)
			return removeErr == nil
		})
		if removeErr != nil {
			return removeErr
		}

		usagemetrics.SetInContext(ctx, &dispatchv1.ResponseMeta{
			DispatchCount: uint32(len(nsdefs) + removed.Size()),
		})

		log.Ctx(ctx).Trace().
			Interface("namespaceDefinitions", nsdefs).
			Strs("addedOrChanged", newDefs.List()).
			Strs("removed", removed.List()).
			Msg("wrote namespace definitions")

		return nil
	})
	if err != nil {
		return nil, rewriteSchemaError(ctx, err)
	}

	return &v1.WriteSchemaResponse{}, nil
}

func rewriteSchemaError(ctx context.Context, err error) error {
	var nsNotFoundError sharederrors.UnknownNamespaceError
	var errWithContext compiler.ErrorWithContext

	errWithSource, ok := commonerrors.AsErrorWithSource(err)
	if ok {
		return status.Errorf(codes.InvalidArgument, "%s", errWithSource.Error())
	}

	switch {
	case errors.As(err, &nsNotFoundError):
		return status.Errorf(codes.NotFound, "Object Definition `%s` not found", nsNotFoundError.NotFoundNamespaceName())
	case errors.As(err, &errWithContext):
		return status.Errorf(codes.InvalidArgument, "%s", err)
	case errors.As(err, &datastore.ErrReadOnly{}):
		return serviceerrors.ErrServiceReadOnly
	default:
		log.Ctx(ctx).Err(err).Msg("received unexpected error")
		return err
	}
}
