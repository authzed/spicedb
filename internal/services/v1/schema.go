package v1

import (
	"context"
	"fmt"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	grpcvalidate "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/validator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/internal/middleware"
	"github.com/authzed/spicedb/internal/middleware/consistency"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/internal/middleware/usagemetrics"
	"github.com/authzed/spicedb/internal/services/shared"
	"github.com/authzed/spicedb/pkg/datastore"
	dispatchv1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/generator"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
)

// NewSchemaServer creates a SchemaServiceServer instance.
func NewSchemaServer(additiveOnly, caveatsEnabled bool) v1.SchemaServiceServer {
	return &schemaServer{
		WithServiceSpecificInterceptors: shared.WithServiceSpecificInterceptors{
			Unary: middleware.ChainUnaryServer(
				grpcvalidate.UnaryServerInterceptor(true),
				usagemetrics.UnaryServerInterceptor(),
			),
			Stream: middleware.ChainStreamServer(
				grpcvalidate.StreamServerInterceptor(true),
				usagemetrics.StreamServerInterceptor(),
			),
		},
		additiveOnly:   additiveOnly,
		caveatsEnabled: caveatsEnabled,
	}
}

type schemaServer struct {
	v1.UnimplementedSchemaServiceServer
	shared.WithServiceSpecificInterceptors

	additiveOnly   bool
	caveatsEnabled bool
}

func (ss *schemaServer) ReadSchema(ctx context.Context, in *v1.ReadSchemaRequest) (*v1.ReadSchemaResponse, error) {
	readRevision, _ := consistency.MustRevisionFromContext(ctx)
	ds := datastoremw.MustFromContext(ctx).SnapshotReader(readRevision)

	nsDefs, err := ds.ListAllNamespaces(ctx)
	if err != nil {
		return nil, rewriteError(ctx, err)
	}

	caveatDefs, err := ds.ListCaveats(ctx)
	if err != nil {
		return nil, rewriteError(ctx, err)
	}

	if len(nsDefs) == 0 {
		return nil, status.Errorf(codes.NotFound, "No schema has been defined; please call WriteSchema to start")
	}

	schemaDefinitions := make([]compiler.SchemaDefinition, 0, len(nsDefs)+len(caveatDefs))
	for _, caveatDef := range caveatDefs {
		schemaDefinitions = append(schemaDefinitions, caveatDef)
	}

	for _, nsDef := range nsDefs {
		schemaDefinitions = append(schemaDefinitions, nsDef)
	}

	schemaText, _, err := generator.GenerateSchema(schemaDefinitions)
	if err != nil {
		return nil, rewriteError(ctx, err)
	}

	usagemetrics.SetInContext(ctx, &dispatchv1.ResponseMeta{
		DispatchCount: uint32(len(nsDefs) + len(caveatDefs)),
	})

	return &v1.ReadSchemaResponse{
		SchemaText: schemaText,
	}, nil
}

func (ss *schemaServer) WriteSchema(ctx context.Context, in *v1.WriteSchemaRequest) (*v1.WriteSchemaResponse, error) {
	log.Ctx(ctx).Trace().Str("schema", in.GetSchema()).Msg("requested Schema to be written")

	ds := datastoremw.MustFromContext(ctx)

	// Compile the schema into the namespace definitions.
	emptyDefaultPrefix := ""
	compiled, err := compiler.Compile(compiler.InputSchema{
		Source:       input.Source("schema"),
		SchemaString: in.GetSchema(),
	}, &emptyDefaultPrefix)
	if err != nil {
		return nil, rewriteError(ctx, err)
	}
	log.Ctx(ctx).Trace().Int("objectDefinitions", len(compiled.ObjectDefinitions)).Int("caveatDefinitions", len(compiled.CaveatDefinitions)).Msg("compiled namespace definitions")

	if !ss.caveatsEnabled && len(compiled.CaveatDefinitions) > 0 {
		return nil, fmt.Errorf("caveats are currently not supported")
	}

	// Do as much validation as we can before talking to the datastore.
	validated, err := shared.ValidateSchemaChanges(ctx, compiled, ss.additiveOnly)
	if err != nil {
		return nil, rewriteError(ctx, err)
	}

	// Update the schema.
	_, err = ds.ReadWriteTx(ctx, func(rwt datastore.ReadWriteTransaction) error {
		applied, err := shared.ApplySchemaChanges(ctx, rwt, validated)
		if err != nil {
			return err
		}
		usagemetrics.SetInContext(ctx, &dispatchv1.ResponseMeta{
			DispatchCount: applied.TotalOperationCount,
		})
		return nil
	})
	if err != nil {
		return nil, rewriteError(ctx, err)
	}

	return &v1.WriteSchemaResponse{}, nil
}
