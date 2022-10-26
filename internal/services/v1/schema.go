package v1

import (
	"context"
	"strings"

	"github.com/authzed/spicedb/internal/util"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	grpcvalidate "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/validator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/internal/middleware/consistency"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/internal/middleware/usagemetrics"
	"github.com/authzed/spicedb/internal/namespace"
	"github.com/authzed/spicedb/internal/services/shared"
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
			Unary:  grpcvalidate.UnaryServerInterceptor(true),
			Stream: grpcvalidate.StreamServerInterceptor(true),
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
		return nil, rewriteError(ctx, err)
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

	// Do as much validation as we can before talking to the datastore:
	// 1) Validate the caveats defined.
	newCaveatDefNames := util.NewSet[string]()
	for _, caveatDef := range compiled.CaveatDefinitions {
		if err := namespace.ValidateCaveatDefinition(caveatDef); err != nil {
			return nil, rewriteError(ctx, err)
		}

		newCaveatDefNames.Add(caveatDef.Name)
	}

	// 2) Validate the namespaces defined.
	newObjectDefNames := util.NewSet[string]()
	for _, nsdef := range compiled.ObjectDefinitions {
		ts, err := namespace.NewNamespaceTypeSystem(nsdef,
			namespace.ResolverForPredefinedDefinitions(namespace.PredefinedElements{
				Namespaces: compiled.ObjectDefinitions,
				Caveats:    compiled.CaveatDefinitions,
			}))
		if err != nil {
			return nil, rewriteError(ctx, err)
		}

		vts, err := ts.Validate(ctx)
		if err != nil {
			return nil, rewriteError(ctx, err)
		}

		if err := namespace.AnnotateNamespace(vts); err != nil {
			return nil, rewriteError(ctx, err)
		}

		newObjectDefNames.Add(nsdef.Name)
	}

	// Start the transaction to update the schema.
	_, err = ds.ReadWriteTx(ctx, func(rwt datastore.ReadWriteTransaction) error {
		// Build a map of existing caveats to determine those being removed, if any.
		existingCaveats, err := rwt.ListCaveats(ctx)
		existingCaveatDefMap := make(map[string]*core.CaveatDefinition, len(existingCaveats))
		existingCaveatDefNames := util.NewSet[string]()
		if err != nil {
			return err
		}

		for _, existingCaveat := range existingCaveats {
			existingCaveatDefMap[existingCaveat.Name] = existingCaveat
			existingCaveatDefNames.Add(existingCaveat.Name)
		}

		// For each caveat definition, perform a diff and ensure the changes will not result in type errors.
		for _, caveatDef := range compiled.CaveatDefinitions {
			if err := shared.SanityCheckCaveatChanges(ctx, rwt, caveatDef, existingCaveatDefMap); err != nil {
				return err
			}
		}

		removedCaveatDefNames := existingCaveatDefNames.Subtract(newCaveatDefNames)

		// Build a map of existing definitions to determine those being removed, if any.
		existingObjectDefs, err := rwt.ListNamespaces(ctx)
		if err != nil {
			return err
		}

		existingObjectDefMap := make(map[string]*core.NamespaceDefinition, len(existingObjectDefs))
		existingObjectDefNames := util.NewSet[string]()
		for _, existingDef := range existingObjectDefs {
			existingObjectDefMap[existingDef.Name] = existingDef
			existingObjectDefNames.Add(existingDef.Name)
		}

		// For each definition, perform a diff and ensure the changes will not result in any
		// breaking changes.
		objectDefsWithChanges := make([]*core.NamespaceDefinition, 0, len(compiled.ObjectDefinitions))
		for _, nsdef := range compiled.ObjectDefinitions {
			diff, err := shared.SanityCheckNamespaceChanges(ctx, rwt, nsdef, existingObjectDefMap)
			if err != nil {
				return err
			}

			if len(diff.Deltas()) > 0 {
				objectDefsWithChanges = append(objectDefsWithChanges, nsdef)
			}
		}

		log.Ctx(ctx).
			Trace().
			Int("objectDefinitions", len(compiled.ObjectDefinitions)).
			Int("caveatDefinitions", len(compiled.CaveatDefinitions)).
			Int("objectDefsWithChanges", len(objectDefsWithChanges)).
			Msg("validated namespace definitions")

		// Ensure that deleting namespaces will not result in any relationships left without associated
		// schema.
		removedObjectDefNames := existingObjectDefNames.Subtract(newObjectDefNames)
		if err := removedObjectDefNames.ForEach(func(nsdefName string) error {
			return shared.EnsureNoRelationshipsExist(ctx, rwt, nsdefName)
		}); err != nil {
			return err
		}

		// Write the new caveats.
		// TODO(jschorr): Only write updated caveats once the diff has been changed to support expressions.
		if len(compiled.CaveatDefinitions) > 0 {
			if err := rwt.WriteCaveats(ctx, compiled.CaveatDefinitions); err != nil {
				return err
			}
		}

		// Write the new/changed namespaces.
		if len(objectDefsWithChanges) > 0 {
			if err := rwt.WriteNamespaces(ctx, objectDefsWithChanges...); err != nil {
				return err
			}
		}

		usagemetrics.SetInContext(ctx, &dispatchv1.ResponseMeta{
			DispatchCount: uint32(len(objectDefsWithChanges)),
		})

		// Delete the removed namespaces.
		if err := removedObjectDefNames.ForEach(func(value string) error {
			return rwt.DeleteNamespace(ctx, value)
		}); err != nil {
			return err
		}

		// Delete the removed caveats.
		if !removedCaveatDefNames.IsEmpty() {
			if err := rwt.DeleteCaveats(ctx, removedCaveatDefNames.AsSlice()); err != nil {
				return err
			}
		}

		usagemetrics.SetInContext(ctx, &dispatchv1.ResponseMeta{
			DispatchCount: uint32(len(compiled.ObjectDefinitions) + len(compiled.CaveatDefinitions) + removedObjectDefNames.Len() + removedCaveatDefNames.Len()),
		})

		log.Ctx(ctx).Trace().
			Interface("objectDefinitions", compiled.ObjectDefinitions).
			Interface("caveatDefinitions", compiled.CaveatDefinitions).
			Object("addedOrChangedObjectDefinitions", util.StringSet(newObjectDefNames)).
			Object("removedObjectDefinitions", util.StringSet(removedObjectDefNames)).
			Object("addedOrChangedCaveatDefinitions", util.StringSet(newCaveatDefNames)).
			Object("removedCaveatDefinitions", util.StringSet(removedCaveatDefNames)).
			Msg("completed schema update")

		return nil
	})
	if err != nil {
		return nil, rewriteError(ctx, err)
	}

	return &v1.WriteSchemaResponse{}, nil
}
