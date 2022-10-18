package v1alpha1

import (
	"context"
	"errors"
	"strings"

	"github.com/authzed/authzed-go/proto/authzed/api/v1alpha1"
	"github.com/authzed/grpcutil"

	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/internal/middleware"
	"github.com/authzed/spicedb/internal/middleware/consistency"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/internal/middleware/usagemetrics"
	"github.com/authzed/spicedb/internal/namespace"
	"github.com/authzed/spicedb/internal/services/shared"
	"github.com/authzed/spicedb/internal/sharederrors"
	"github.com/authzed/spicedb/pkg/datastore"
	revisions "github.com/authzed/spicedb/pkg/namespace/v1alpha1"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	dispatchv1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/generator"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
)

// PrefixRequiredOption is an option to the schema server indicating whether
// prefixes are required on schema object definitions.
type PrefixRequiredOption int

type writeSchemaPreconditionFailure struct {
	error
}

const (
	// PrefixNotRequired indicates that prefixes are not required.
	PrefixNotRequired PrefixRequiredOption = iota

	// PrefixRequired indicates that prefixes are required.
	PrefixRequired
)

type schemaServiceServer struct {
	v1alpha1.UnimplementedSchemaServiceServer
	shared.WithUnaryServiceSpecificInterceptor

	prefixRequired PrefixRequiredOption
}

// NewSchemaServer returns an new instance of a server that implements
// authzed.api.v1alpha1.SchemaService.
func NewSchemaServer(prefixRequired PrefixRequiredOption) v1alpha1.SchemaServiceServer {
	return &schemaServiceServer{
		prefixRequired: prefixRequired,
		WithUnaryServiceSpecificInterceptor: shared.WithUnaryServiceSpecificInterceptor{
			Unary: middleware.ChainUnaryServer(grpcutil.DefaultUnaryMiddleware...),
		},
	}
}

const caveatKeyPrefix = "caveat:"

func (ss *schemaServiceServer) ReadSchema(ctx context.Context, in *v1alpha1.ReadSchemaRequest) (*v1alpha1.ReadSchemaResponse, error) {
	headRevision, _ := consistency.MustRevisionFromContext(ctx)
	ds := datastoremw.MustFromContext(ctx).SnapshotReader(headRevision)

	numRequested := len(in.GetObjectDefinitionsNames())

	objectDefs := make([]string, 0, numRequested)
	createdRevisions := make(map[string]datastore.Revision, numRequested)
	for _, objectDefName := range in.GetObjectDefinitionsNames() {
		found, createdAt, err := ds.ReadNamespace(ctx, objectDefName)
		if err != nil {
			return nil, rewriteError(ctx, err)
		}

		createdRevisions[objectDefName] = createdAt

		objectDef, _ := generator.GenerateSource(found)
		objectDefs = append(objectDefs, objectDef)
	}

	usagemetrics.SetInContext(ctx, &dispatchv1.ResponseMeta{
		DispatchCount: uint32(numRequested),
	})

	computedRevision, err := revisions.ComputeV1Alpha1Revision(createdRevisions)
	if err != nil {
		return nil, rewriteError(ctx, err)
	}

	return &v1alpha1.ReadSchemaResponse{
		ObjectDefinitions:           objectDefs,
		ComputedDefinitionsRevision: computedRevision,
	}, nil
}

func (ss *schemaServiceServer) WriteSchema(ctx context.Context, in *v1alpha1.WriteSchemaRequest) (*v1alpha1.WriteSchemaResponse, error) {
	log.Ctx(ctx).Trace().Str("schema", in.GetSchema()).Msg("requested Schema to be written")
	ds := datastoremw.MustFromContext(ctx)

	var prefix *string
	if ss.prefixRequired == PrefixNotRequired {
		empty := ""
		prefix = &empty
	}

	compiled, err := compiler.Compile(compiler.InputSchema{
		Source:       input.Source("schema"),
		SchemaString: in.GetSchema(),
	}, prefix)
	if err != nil {
		return nil, rewriteError(ctx, err)
	}

	log.Ctx(ctx).Trace().Interface("caveatDefinitions", compiled.CaveatDefinitions).Interface("objectDefinitions", compiled.ObjectDefinitions).Msg("compiled schema")

	objectDefMap := make(map[string]*core.NamespaceDefinition, len(compiled.ObjectDefinitions))
	for _, nsDef := range compiled.ObjectDefinitions {
		objectDefMap[nsDef.Name] = nsDef
	}

	caveatNames := make([]string, 0, len(compiled.CaveatDefinitions))
	for _, caveatDef := range compiled.CaveatDefinitions {
		if err := namespace.ValidateCaveatDefinition(caveatDef); err != nil {
			return nil, rewriteError(ctx, err)
		}
		caveatNames = append(caveatNames, caveatDef.Name)
	}

	// Determine the list of all referenced namespaces and caveats in the schema.
	referencedNamespaceNames := namespace.ListReferencedNamespaces(compiled.ObjectDefinitions)

	// Run the schema update.
	revision, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		var existingCaveatDefs []*core.CaveatDefinition
		if len(caveatNames) > 0 {
			found, err := rwt.ListCaveats(ctx, caveatNames...)
			if err != nil {
				return err
			}
			existingCaveatDefs = found
		}

		existingCaveatDefMap := make(map[string]*core.CaveatDefinition, len(compiled.CaveatDefinitions))
		for _, existingCaveatDef := range existingCaveatDefs {
			existingCaveatDefMap[existingCaveatDef.Name] = existingCaveatDef
		}

		for _, caveatDef := range compiled.CaveatDefinitions {
			if err := shared.SanityCheckCaveatChanges(ctx, rwt, caveatDef, existingCaveatDefMap); err != nil {
				return err
			}
		}

		// Check for changes to namespaces.
		existingObjectDefs, err := rwt.LookupNamespaces(ctx, referencedNamespaceNames)
		if err != nil {
			return err
		}

		existingObjectDefMap := make(map[string]*core.NamespaceDefinition, len(referencedNamespaceNames))
		for _, existingObjectDef := range existingObjectDefs {
			existingObjectDefMap[existingObjectDef.Name] = existingObjectDef
		}

		for _, nsdef := range compiled.ObjectDefinitions {
			ts, err := namespace.NewNamespaceTypeSystem(
				nsdef,
				namespace.ResolverForDatastoreReader(rwt).WithPredefinedElements(namespace.PredefinedElements{
					Namespaces: compiled.ObjectDefinitions,
					Caveats:    compiled.CaveatDefinitions,
				}))
			if err != nil {
				return err
			}

			vts, err := ts.Validate(ctx)
			if err != nil {
				return err
			}

			if err := namespace.AnnotateNamespace(vts); err != nil {
				return err
			}

			if _, err := shared.SanityCheckNamespaceChanges(ctx, rwt, nsdef, existingObjectDefMap); err != nil {
				return err
			}
		}
		log.Ctx(ctx).Trace().Interface("caveatDefinitions", compiled.CaveatDefinitions).Interface("objectDefinitions", compiled.ObjectDefinitions).Msg("validated schema")

		// If a precondition was given, decode it, and verify that none of the namespaces specified
		// have changed in any way.
		if in.OptionalDefinitionsRevisionPrecondition != "" {
			decoded, err := revisions.DecodeV1Alpha1Revision(in.OptionalDefinitionsRevisionPrecondition, ds)
			if err != nil {
				return err
			}

			for key, existingRevision := range decoded {
				if strings.HasPrefix(key, "caveat:") {
					_, createdAt, err := rwt.ReadCaveatByName(ctx, key[len(caveatKeyPrefix):])
					if err != nil {
						return &writeSchemaPreconditionFailure{err}
					}

					if !createdAt.Equal(existingRevision) {
						return &writeSchemaPreconditionFailure{
							errors.New("current schema differs from the revision specified"),
						}
					}
				} else {
					_, createdAt, err := rwt.ReadNamespace(ctx, key)
					if err != nil {
						var nsNotFoundError sharederrors.UnknownNamespaceError
						if errors.As(err, &nsNotFoundError) {
							return &writeSchemaPreconditionFailure{
								errors.New("specified revision references a type that no longer exists"),
							}
						}

						return err
					}

					if !createdAt.Equal(existingRevision) {
						return &writeSchemaPreconditionFailure{
							errors.New("current schema differs from the revision specified"),
						}
					}
				}
			}

			log.Ctx(ctx).Trace().Interface("caveatDefinitions", compiled.CaveatDefinitions).Interface("objectDefinitions", compiled.ObjectDefinitions).Msg("checked schema revision")
		}

		if len(compiled.CaveatDefinitions) > 0 {
			if err := rwt.WriteCaveats(compiled.CaveatDefinitions); err != nil {
				return err
			}
		}

		if err := rwt.WriteNamespaces(compiled.ObjectDefinitions...); err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return nil, rewriteError(ctx, err)
	}

	updatedCount := len(compiled.ObjectDefinitions) + len(compiled.CaveatDefinitions)
	revs := make(map[string]datastore.Revision, updatedCount)
	names := make([]string, 0, updatedCount)
	for _, nsdef := range compiled.ObjectDefinitions {
		names = append(names, nsdef.Name)
		revs[nsdef.Name] = revision
	}
	for _, caveat := range compiled.CaveatDefinitions {
		key := caveatKeyPrefix + caveat.Name
		names = append(names, key)
		revs[key] = revision
	}

	computedRevision, err := revisions.ComputeV1Alpha1Revision(revs)
	if err != nil {
		return nil, rewriteError(ctx, err)
	}

	usagemetrics.SetInContext(ctx, &dispatchv1.ResponseMeta{
		DispatchCount: uint32(updatedCount),
	})

	log.Ctx(ctx).Trace().Interface("caveatDefinitions", compiled.CaveatDefinitions).Interface("objectDefinitions", compiled.ObjectDefinitions).Msg("completed v1alpha1 schema update")

	return &v1alpha1.WriteSchemaResponse{
		ObjectDefinitionsNames:      names,
		ComputedDefinitionsRevision: computedRevision,
	}, nil
}
