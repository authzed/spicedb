package v1

import (
	"context"
	"sort"
	"strings"

	grpcvalidate "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/validator"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/internal/middleware"
	"github.com/authzed/spicedb/internal/middleware/perfinsights"
	"github.com/authzed/spicedb/internal/middleware/usagemetrics"
	"github.com/authzed/spicedb/internal/services/shared"
	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/genutil"
	"github.com/authzed/spicedb/pkg/middleware/consistency"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	dispatchv1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/schema"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/authzed/spicedb/pkg/zedtoken"
)

type SchemaServerConfig struct {
	// CaveatTypeSet is the set of caveat types that are allowed in the schema.
	CaveatTypeSet *caveattypes.TypeSet

	// AdditiveOnly indicates whether the schema is additive only.
	AdditiveOnly bool

	// ExpiringRelsEnabled indicates whether expiring relationships are enabled.
	ExpiringRelsEnabled bool

	// PerformanceInsightMetricsEnabled indicates whether performance insight metrics are enabled.
	PerformanceInsightMetricsEnabled bool
}

// NewSchemaServer creates a SchemaServiceServer instance.
func NewSchemaServer(config SchemaServerConfig) v1.SchemaServiceServer {
	cts := caveattypes.TypeSetOrDefault(config.CaveatTypeSet)
	return &schemaServer{
		WithServiceSpecificInterceptors: shared.WithServiceSpecificInterceptors{
			Unary: middleware.ChainUnaryServer(
				grpcvalidate.UnaryServerInterceptor(),
				usagemetrics.UnaryServerInterceptor(),
				perfinsights.UnaryServerInterceptor(config.PerformanceInsightMetricsEnabled),
			),
			Stream: middleware.ChainStreamServer(
				grpcvalidate.StreamServerInterceptor(),
				usagemetrics.StreamServerInterceptor(),
				perfinsights.StreamServerInterceptor(config.PerformanceInsightMetricsEnabled),
			),
		},
		additiveOnly:        config.AdditiveOnly,
		expiringRelsEnabled: config.ExpiringRelsEnabled,
		caveatTypeSet:       cts,
	}
}

type schemaServer struct {
	v1.UnimplementedSchemaServiceServer
	shared.WithServiceSpecificInterceptors

	caveatTypeSet       *caveattypes.TypeSet
	additiveOnly        bool
	expiringRelsEnabled bool
}

func (ss *schemaServer) rewriteError(ctx context.Context, err error) error {
	return shared.RewriteError(ctx, err, nil)
}

func (ss *schemaServer) ReadSchema(ctx context.Context, _ *v1.ReadSchemaRequest) (*v1.ReadSchemaResponse, error) {
	perfinsights.SetInContext(ctx, perfinsights.NoLabels)

	// Schema is always read from the head revision.
	dl := datalayer.MustFromContext(ctx)
	headRevision, err := dl.HeadRevision(ctx)
	if err != nil {
		return nil, ss.rewriteError(ctx, err)
	}

	reader := dl.SnapshotReader(headRevision)

	sr, err := reader.ReadSchema()
	if err != nil {
		return nil, ss.rewriteError(ctx, err)
	}

	schemaText, err := sr.SchemaText()
	if err != nil {
		return nil, ss.rewriteError(ctx, err)
	}

	usagemetrics.SetInContext(ctx, &dispatchv1.ResponseMeta{
		DispatchCount: 1,
	})

	zedToken, err := zedtoken.NewFromRevision(ctx, headRevision, dl)
	if err != nil {
		return nil, ss.rewriteError(ctx, err)
	}

	return &v1.ReadSchemaResponse{
		SchemaText: schemaText,
		ReadAt:     zedToken,
	}, nil
}

func (ss *schemaServer) WriteSchema(ctx context.Context, in *v1.WriteSchemaRequest) (*v1.WriteSchemaResponse, error) {
	perfinsights.SetInContext(ctx, perfinsights.NoLabels)

	log.Ctx(ctx).Trace().Str("schema", in.GetSchema()).Msg("requested Schema to be written")

	dl := datalayer.MustFromContext(ctx)

	// Compile the schema into the namespace definitions.
	opts := make([]compiler.Option, 0, 3)
	if !ss.expiringRelsEnabled {
		opts = append(opts, compiler.DisallowExpirationFlag())
	}

	opts = append(opts, compiler.CaveatTypeSet(ss.caveatTypeSet))

	compiled, err := compiler.Compile(compiler.InputSchema{
		Source:       input.Source("schema"),
		SchemaString: in.GetSchema(),
	}, compiler.AllowUnprefixedObjectType(), opts...)
	if err != nil {
		return nil, ss.rewriteError(ctx, err)
	}
	log.Ctx(ctx).Trace().Int("objectDefinitions", len(compiled.ObjectDefinitions)).Int("caveatDefinitions", len(compiled.CaveatDefinitions)).Msg("compiled namespace definitions")

	// Do as much validation as we can before talking to the datastore.
	validated, err := shared.ValidateSchemaChanges(ctx, compiled, ss.caveatTypeSet, ss.additiveOnly, in.GetSchema())
	if err != nil {
		return nil, ss.rewriteError(ctx, err)
	}

	revision, err := dl.ReadWriteTx(ctx, func(ctx context.Context, rwt datalayer.ReadWriteTransaction) error {
		applied, err := shared.ApplySchemaChanges(ctx, rwt, ss.caveatTypeSet, validated)
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

	zedToken, err := zedtoken.NewFromRevision(ctx, revision, dl)
	if err != nil {
		return nil, ss.rewriteError(ctx, err)
	}

	return &v1.WriteSchemaResponse{
		WrittenAt: zedToken,
	}, nil
}

func (ss *schemaServer) ReflectSchema(ctx context.Context, req *v1.ReflectSchemaRequest) (*v1.ReflectSchemaResponse, error) {
	perfinsights.SetInContext(ctx, perfinsights.NoLabels)

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
			caveat, err := caveatAPIRepr(cd, filters, ss.caveatTypeSet)
			if err != nil {
				return nil, shared.RewriteErrorWithoutConfig(ctx, err)
			}

			if caveat != nil {
				caveats = append(caveats, caveat)
			}
		}
	}

	dl := datalayer.MustFromContext(ctx)
	zedToken, err := zedtoken.NewFromRevision(ctx, atRevision, dl)
	if err != nil {
		return nil, shared.RewriteErrorWithoutConfig(ctx, err)
	}

	return &v1.ReflectSchemaResponse{
		Definitions: definitions,
		Caveats:     caveats,
		ReadAt:      zedToken,
	}, nil
}

func (ss *schemaServer) DiffSchema(ctx context.Context, req *v1.DiffSchemaRequest) (*v1.DiffSchemaResponse, error) {
	perfinsights.SetInContext(ctx, perfinsights.NoLabels)

	atRevision, _, err := consistency.RevisionFromContext(ctx)
	if err != nil {
		return nil, err
	}

	diff, existingSchema, comparisonSchema, err := schemaDiff(ctx, req.ComparisonSchema, ss.caveatTypeSet)
	if err != nil {
		return nil, shared.RewriteErrorWithoutConfig(ctx, err)
	}

	resp, err := convertDiff(ctx, diff, existingSchema, comparisonSchema, atRevision, ss.caveatTypeSet)
	if err != nil {
		return nil, shared.RewriteErrorWithoutConfig(ctx, err)
	}

	return resp, nil
}

func (ss *schemaServer) ComputablePermissions(ctx context.Context, req *v1.ComputablePermissionsRequest) (*v1.ComputablePermissionsResponse, error) {
	perfinsights.SetInContext(ctx, func() perfinsights.APIShapeLabels {
		return perfinsights.APIShapeLabels{
			perfinsights.ResourceTypeLabel:     req.DefinitionName,
			perfinsights.ResourceRelationLabel: req.RelationName,
			perfinsights.FilterLabel:           req.OptionalDefinitionNameFilter,
		}
	})

	atRevision, revisionReadAt, err := consistency.RevisionFromContext(ctx)
	if err != nil {
		return nil, shared.RewriteErrorWithoutConfig(ctx, err)
	}

	dl := datalayer.MustFromContext(ctx).SnapshotReader(atRevision)
	sr, err := dl.ReadSchema()
	if err != nil {
		return nil, shared.RewriteErrorWithoutConfig(ctx, err)
	}
	ts := schema.NewTypeSystem(schema.ResolverFor(sr))
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

	typeDefs, err := sr.ListAllTypeDefinitions(ctx)
	if err != nil {
		return nil, shared.RewriteErrorWithoutConfig(ctx, err)
	}

	allDefinitions := make([]*core.NamespaceDefinition, 0, len(typeDefs))
	for _, typeDef := range typeDefs {
		allDefinitions = append(allDefinitions, typeDef.Definition)
	}

	rg := vdef.Reachability(ts)
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
	perfinsights.SetInContext(ctx, func() perfinsights.APIShapeLabels {
		return perfinsights.APIShapeLabels{
			perfinsights.ResourceTypeLabel:     req.DefinitionName,
			perfinsights.ResourceRelationLabel: req.PermissionName,
		}
	})

	atRevision, revisionReadAt, err := consistency.RevisionFromContext(ctx)
	if err != nil {
		return nil, shared.RewriteErrorWithoutConfig(ctx, err)
	}

	dl := datalayer.MustFromContext(ctx).SnapshotReader(atRevision)
	sr2, err := dl.ReadSchema()
	if err != nil {
		return nil, shared.RewriteErrorWithoutConfig(ctx, err)
	}
	ts := schema.NewTypeSystem(schema.ResolverFor(sr2))
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

	rg := vdef.Reachability(ts)
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
