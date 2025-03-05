package v1

import (
	"context"
	"sort"
	"strings"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	grpcvalidate "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/validator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/internal/middleware"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/internal/middleware/usagemetrics"
	"github.com/authzed/spicedb/internal/services/shared"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/genutil"
	"github.com/authzed/spicedb/pkg/middleware/consistency"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	dispatchv1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/schema"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/generator"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/authzed/spicedb/pkg/zedtoken"
)

// NewSchemaServer creates a SchemaServiceServer instance.
func NewSchemaServer(additiveOnly bool, expiringRelsEnabled bool) v1.SchemaServiceServer {
	return &schemaServer{
		WithServiceSpecificInterceptors: shared.WithServiceSpecificInterceptors{
			Unary: middleware.ChainUnaryServer(
				grpcvalidate.UnaryServerInterceptor(),
				usagemetrics.UnaryServerInterceptor(),
			),
			Stream: middleware.ChainStreamServer(
				grpcvalidate.StreamServerInterceptor(),
				usagemetrics.StreamServerInterceptor(),
			),
		},
		additiveOnly:        additiveOnly,
		expiringRelsEnabled: expiringRelsEnabled,
	}
}

type schemaServer struct {
	v1.UnimplementedSchemaServiceServer
	shared.WithServiceSpecificInterceptors

	additiveOnly        bool
	expiringRelsEnabled bool
}

func (ss *schemaServer) rewriteError(ctx context.Context, err error) error {
	return shared.RewriteError(ctx, err, nil)
}

func (ss *schemaServer) ReadSchema(ctx context.Context, _ *v1.ReadSchemaRequest) (*v1.ReadSchemaResponse, error) {
	// Schema is always read from the head revision.
	ds := datastoremw.MustFromContext(ctx)
	headRevision, err := ds.HeadRevision(ctx)
	if err != nil {
		return nil, ss.rewriteError(ctx, err)
	}

	reader := ds.SnapshotReader(headRevision)

	nsDefs, err := reader.ListAllNamespaces(ctx)
	if err != nil {
		return nil, ss.rewriteError(ctx, err)
	}

	caveatDefs, err := reader.ListAllCaveats(ctx)
	if err != nil {
		return nil, ss.rewriteError(ctx, err)
	}

	if len(nsDefs) == 0 {
		return nil, status.Errorf(codes.NotFound, "No schema has been defined; please call WriteSchema to start")
	}

	schemaDefinitions := make([]compiler.SchemaDefinition, 0, len(nsDefs)+len(caveatDefs))
	for _, caveatDef := range caveatDefs {
		schemaDefinitions = append(schemaDefinitions, caveatDef.Definition)
	}

	for _, nsDef := range nsDefs {
		schemaDefinitions = append(schemaDefinitions, nsDef.Definition)
	}

	schemaText, _, err := generator.GenerateSchema(schemaDefinitions)
	if err != nil {
		return nil, ss.rewriteError(ctx, err)
	}

	dispatchCount, err := genutil.EnsureUInt32(len(nsDefs) + len(caveatDefs))
	if err != nil {
		return nil, ss.rewriteError(ctx, err)
	}

	usagemetrics.SetInContext(ctx, &dispatchv1.ResponseMeta{
		DispatchCount: dispatchCount,
	})

	return &v1.ReadSchemaResponse{
		SchemaText: schemaText,
		ReadAt:     zedtoken.MustNewFromRevision(headRevision),
	}, nil
}

func (ss *schemaServer) WriteSchema(ctx context.Context, in *v1.WriteSchemaRequest) (*v1.WriteSchemaResponse, error) {
	log.Ctx(ctx).Trace().Str("schema", in.GetSchema()).Msg("requested Schema to be written")

	ds := datastoremw.MustFromContext(ctx)

	// Compile the schema into the namespace definitions.
	opts := []compiler.Option{}
	if !ss.expiringRelsEnabled {
		opts = append(opts, compiler.DisallowExpirationFlag())
	}

	compiled, err := compiler.Compile(compiler.InputSchema{
		Source:       input.Source("schema"),
		SchemaString: in.GetSchema(),
	}, compiler.AllowUnprefixedObjectType(), opts...)
	if err != nil {
		return nil, ss.rewriteError(ctx, err)
	}
	log.Ctx(ctx).Trace().Int("objectDefinitions", len(compiled.ObjectDefinitions)).Int("caveatDefinitions", len(compiled.CaveatDefinitions)).Msg("compiled namespace definitions")

	// Do as much validation as we can before talking to the datastore.
	validated, err := shared.ValidateSchemaChanges(ctx, compiled, ss.additiveOnly)
	if err != nil {
		return nil, ss.rewriteError(ctx, err)
	}

	// Update the schema.
	revision, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		applied, err := shared.ApplySchemaChanges(ctx, rwt, validated)
		if err != nil {
			return err
		}

		dispatchCount, err := genutil.EnsureUInt32(applied.TotalOperationCount)
		if err != nil {
			return err
		}

		usagemetrics.SetInContext(ctx, &dispatchv1.ResponseMeta{
			DispatchCount: dispatchCount,
		})
		return nil
	})
	if err != nil {
		return nil, ss.rewriteError(ctx, err)
	}

	return &v1.WriteSchemaResponse{
		WrittenAt: zedtoken.MustNewFromRevision(revision),
	}, nil
}

func (ss *schemaServer) ReflectSchema(ctx context.Context, req *v1.ReflectSchemaRequest) (*v1.ReflectSchemaResponse, error) {
	// Get the current schema.
	schema, atRevision, err := loadCurrentSchema(ctx)
	if err != nil {
		return nil, shared.RewriteErrorWithoutConfig(ctx, err)
	}

	filters, err := newSchemaFilters(req.OptionalFilters)
	if err != nil {
		return nil, shared.RewriteErrorWithoutConfig(ctx, err)
	}

	definitions := make([]*v1.ReflectionDefinition, 0, len(schema.ObjectDefinitions))
	if filters.HasNamespaces() {
		for _, ns := range schema.ObjectDefinitions {
			def, err := namespaceAPIRepr(ns, filters)
			if err != nil {
				return nil, shared.RewriteErrorWithoutConfig(ctx, err)
			}

			if def != nil {
				definitions = append(definitions, def)
			}
		}
	}

	caveats := make([]*v1.ReflectionCaveat, 0, len(schema.CaveatDefinitions))
	if filters.HasCaveats() {
		for _, cd := range schema.CaveatDefinitions {
			caveat, err := caveatAPIRepr(cd, filters)
			if err != nil {
				return nil, shared.RewriteErrorWithoutConfig(ctx, err)
			}

			if caveat != nil {
				caveats = append(caveats, caveat)
			}
		}
	}

	return &v1.ReflectSchemaResponse{
		Definitions: definitions,
		Caveats:     caveats,
		ReadAt:      zedtoken.MustNewFromRevision(atRevision),
	}, nil
}

func (ss *schemaServer) DiffSchema(ctx context.Context, req *v1.DiffSchemaRequest) (*v1.DiffSchemaResponse, error) {
	atRevision, _, err := consistency.RevisionFromContext(ctx)
	if err != nil {
		return nil, err
	}

	diff, existingSchema, comparisonSchema, err := schemaDiff(ctx, req.ComparisonSchema)
	if err != nil {
		return nil, shared.RewriteErrorWithoutConfig(ctx, err)
	}

	resp, err := convertDiff(diff, existingSchema, comparisonSchema, atRevision)
	if err != nil {
		return nil, shared.RewriteErrorWithoutConfig(ctx, err)
	}

	return resp, nil
}

func (ss *schemaServer) ComputablePermissions(ctx context.Context, req *v1.ComputablePermissionsRequest) (*v1.ComputablePermissionsResponse, error) {
	atRevision, revisionReadAt, err := consistency.RevisionFromContext(ctx)
	if err != nil {
		return nil, shared.RewriteErrorWithoutConfig(ctx, err)
	}

	ds := datastoremw.MustFromContext(ctx).SnapshotReader(atRevision)
	ts := schema.NewTypeSystem(schema.ResolverForDatastoreReader(ds))
	vdef, err := ts.GetValidatedDefinition(ctx, req.DefinitionName)
	if err != nil {
		return nil, shared.RewriteErrorWithoutConfig(ctx, err)
	}

	relationName := req.RelationName
	if relationName == "" {
		relationName = tuple.Ellipsis
	} else {
		if _, ok := vdef.GetRelation(relationName); !ok {
			return nil, shared.RewriteErrorWithoutConfig(ctx, schema.NewRelationNotFoundErr(req.DefinitionName, relationName))
		}
	}

	allNamespaces, err := ds.ListAllNamespaces(ctx)
	if err != nil {
		return nil, shared.RewriteErrorWithoutConfig(ctx, err)
	}

	allDefinitions := make([]*core.NamespaceDefinition, 0, len(allNamespaces))
	for _, ns := range allNamespaces {
		allDefinitions = append(allDefinitions, ns.Definition)
	}

	rg := vdef.Reachability()
	rr, err := rg.RelationsEncounteredForSubject(ctx, allDefinitions, &core.RelationReference{
		Namespace: req.DefinitionName,
		Relation:  relationName,
	})
	if err != nil {
		return nil, shared.RewriteErrorWithoutConfig(ctx, err)
	}

	relations := make([]*v1.ReflectionRelationReference, 0, len(rr))
	for _, r := range rr {
		if r.Namespace == req.DefinitionName && r.Relation == req.RelationName {
			continue
		}

		if req.OptionalDefinitionNameFilter != "" && !strings.HasPrefix(r.Namespace, req.OptionalDefinitionNameFilter) {
			continue
		}

		ts, err := ts.GetDefinition(ctx, r.Namespace)
		if err != nil {
			return nil, shared.RewriteErrorWithoutConfig(ctx, err)
		}

		relations = append(relations, &v1.ReflectionRelationReference{
			DefinitionName: r.Namespace,
			RelationName:   r.Relation,
			IsPermission:   ts.IsPermission(r.Relation),
		})
	}

	sort.Slice(relations, func(i, j int) bool {
		if relations[i].DefinitionName == relations[j].DefinitionName {
			return relations[i].RelationName < relations[j].RelationName
		}
		return relations[i].DefinitionName < relations[j].DefinitionName
	})

	return &v1.ComputablePermissionsResponse{
		Permissions: relations,
		ReadAt:      revisionReadAt,
	}, nil
}

func (ss *schemaServer) DependentRelations(ctx context.Context, req *v1.DependentRelationsRequest) (*v1.DependentRelationsResponse, error) {
	atRevision, revisionReadAt, err := consistency.RevisionFromContext(ctx)
	if err != nil {
		return nil, shared.RewriteErrorWithoutConfig(ctx, err)
	}

	ds := datastoremw.MustFromContext(ctx).SnapshotReader(atRevision)
	ts := schema.NewTypeSystem(schema.ResolverForDatastoreReader(ds))
	vdef, err := ts.GetValidatedDefinition(ctx, req.DefinitionName)
	if err != nil {
		return nil, shared.RewriteErrorWithoutConfig(ctx, err)
	}

	_, ok := vdef.GetRelation(req.PermissionName)
	if !ok {
		return nil, shared.RewriteErrorWithoutConfig(ctx, schema.NewRelationNotFoundErr(req.DefinitionName, req.PermissionName))
	}

	if !vdef.IsPermission(req.PermissionName) {
		return nil, shared.RewriteErrorWithoutConfig(ctx, NewNotAPermissionError(req.PermissionName))
	}

	rg := vdef.Reachability()
	rr, err := rg.RelationsEncounteredForResource(ctx, &core.RelationReference{
		Namespace: req.DefinitionName,
		Relation:  req.PermissionName,
	})
	if err != nil {
		return nil, shared.RewriteErrorWithoutConfig(ctx, err)
	}

	relations := make([]*v1.ReflectionRelationReference, 0, len(rr))
	for _, r := range rr {
		if r.Namespace == req.DefinitionName && r.Relation == req.PermissionName {
			continue
		}

		ts, err := ts.GetDefinition(ctx, r.Namespace)
		if err != nil {
			return nil, shared.RewriteErrorWithoutConfig(ctx, err)
		}

		relations = append(relations, &v1.ReflectionRelationReference{
			DefinitionName: r.Namespace,
			RelationName:   r.Relation,
			IsPermission:   ts.IsPermission(r.Relation),
		})
	}

	sort.Slice(relations, func(i, j int) bool {
		if relations[i].DefinitionName == relations[j].DefinitionName {
			return relations[i].RelationName < relations[j].RelationName
		}

		return relations[i].DefinitionName < relations[j].DefinitionName
	})

	return &v1.DependentRelationsResponse{
		Relations: relations,
		ReadAt:    revisionReadAt,
	}, nil
}
