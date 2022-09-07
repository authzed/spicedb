package v1alpha1

import (
	"context"
	"errors"

	"github.com/authzed/authzed-go/proto/authzed/api/v1alpha1"
	"github.com/authzed/grpcutil"
	grpcmw "github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/rs/zerolog/log"
	"github.com/scylladb/go-set/strset"
	"github.com/shopspring/decimal"
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
	nspkg "github.com/authzed/spicedb/pkg/namespace"
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
			Unary: grpcmw.ChainUnaryServer(grpcutil.DefaultUnaryMiddleware...),
		},
	}
}

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

	computedRevision, err := nspkg.ComputeV1Alpha1Revision(createdRevisions)
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

	inputSchema := compiler.InputSchema{
		Source:       input.Source("schema"),
		SchemaString: in.GetSchema(),
	}

	var prefix *string
	if ss.prefixRequired == PrefixNotRequired {
		empty := ""
		prefix = &empty
	}

	nsdefs, err := compiler.Compile([]compiler.InputSchema{inputSchema}, prefix)
	if err != nil {
		return nil, rewriteError(ctx, err)
	}

	liveDefs := append([]*core.NamespaceDefinition{}, nsdefs...)
	liveDefNames := strset.New()
	for _, nsdef := range nsdefs {
		liveDefNames.Add(nsdef.Name)
	}

	log.Ctx(ctx).Trace().Interface("namespaceDefinitions", nsdefs).Msg("compiled namespace definitions")

	// Determine the list of all referenced namespaces in the schema.
	referencedNamespaceNames := namespace.ListReferencedNamespaces(nsdefs)

	// Run the schema update.
	revision, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		// Build a map of existing referenced definitions
		existingDefMap := make(map[string]*core.NamespaceDefinition, len(referencedNamespaceNames))
		for _, existingNamespaceName := range referencedNamespaceNames {
			existingDef, _, err := rwt.ReadNamespace(ctx, existingNamespaceName)
			if err != nil {
				if errors.As(err, &datastore.ErrNamespaceNotFound{}) {
					continue
				}

				return err
			}

			existingDefMap[existingDef.Name] = existingDef
			if !liveDefNames.Has(existingDef.Name) {
				liveDefNames.Add(existingDef.Name)
				liveDefs = append(liveDefs, existingDef)
			}
		}

		for _, nsdef := range nsdefs {
			ts, err := namespace.BuildNamespaceTypeSystemForDefs(nsdef, liveDefs)
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

			if err := shared.SanityCheckExistingRelationships(ctx, rwt, nsdef, existingDefMap); err != nil {
				return err
			}
		}
		log.Ctx(ctx).Trace().Interface("namespaceDefinitions", nsdefs).Msg("validated namespace definitions")

		// If a precondition was given, decode it, and verify that none of the namespaces specified
		// have changed in any way.
		if in.OptionalDefinitionsRevisionPrecondition != "" {
			decoded, err := nspkg.DecodeV1Alpha1Revision(in.OptionalDefinitionsRevisionPrecondition)
			if err != nil {
				return err
			}

			for nsName, existingRevision := range decoded {
				_, createdAt, err := rwt.ReadNamespace(ctx, nsName)
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

			log.Trace().Interface("namespaceDefinitions", nsdefs).Msg("checked schema revision")
		}

		if err := rwt.WriteNamespaces(nsdefs...); err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return nil, rewriteError(ctx, err)
	}

	revisions := make(map[string]decimal.Decimal, len(nsdefs))
	names := make([]string, 0, len(nsdefs))
	for _, nsdef := range nsdefs {
		names = append(names, nsdef.Name)
		revisions[nsdef.Name] = revision
	}

	computedRevision, err := nspkg.ComputeV1Alpha1Revision(revisions)
	if err != nil {
		return nil, rewriteError(ctx, err)
	}

	usagemetrics.SetInContext(ctx, &dispatchv1.ResponseMeta{
		DispatchCount: uint32(len(nsdefs)),
	})

	log.Ctx(ctx).Trace().Interface("namespaceDefinitions", nsdefs).Stringer("computedRevision", revision).Msg("wrote namespace definitions")

	return &v1alpha1.WriteSchemaResponse{
		ObjectDefinitionsNames:      names,
		ComputedDefinitionsRevision: computedRevision,
	}, nil
}

func rewriteError(ctx context.Context, err error) error {
	var nsNotFoundError sharederrors.UnknownNamespaceError
	var errWithContext compiler.ErrorWithContext
	var errPreconditionFailure *writeSchemaPreconditionFailure

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
	case errors.As(err, &errPreconditionFailure):
		return status.Errorf(codes.FailedPrecondition, "%s", err)
	default:
		log.Ctx(ctx).Err(err).Msg("received unexpected error")
		return err
	}
}
