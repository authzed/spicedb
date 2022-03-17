package v1alpha1

import (
	"context"
	"errors"

	"github.com/authzed/authzed-go/proto/authzed/api/v1alpha1"
	"github.com/authzed/grpcutil"
	grpcmw "github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/middleware/consistency"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/internal/middleware/usagemetrics"
	"github.com/authzed/spicedb/internal/namespace"
	"github.com/authzed/spicedb/internal/services/serviceerrors"
	"github.com/authzed/spicedb/internal/services/shared"
	"github.com/authzed/spicedb/internal/sharederrors"
	"github.com/authzed/spicedb/pkg/commonerrors"
	nspkg "github.com/authzed/spicedb/pkg/namespace"
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
	ds := datastoremw.MustFromContext(ctx)

	numRequested := len(in.GetObjectDefinitionsNames())

	objectDefs := make([]string, 0, numRequested)
	createdRevisions := make(map[string]datastore.Revision, numRequested)
	for _, objectDefName := range in.GetObjectDefinitionsNames() {
		found, createdAt, err := ds.ReadNamespace(ctx, objectDefName, headRevision)
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
	nsm := namespace.NewNonCachingNamespaceManager()

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

	headRevision, _ := consistency.MustRevisionFromContext(ctx)

	log.Ctx(ctx).Trace().Interface("namespaceDefinitions", nsdefs).Msg("compiled namespace definitions")

	for _, nsdef := range nsdefs {
		ts, err := namespace.BuildNamespaceTypeSystemWithFallback(nsdef, nsm, nsdefs, headRevision)
		if err != nil {
			return nil, rewriteError(ctx, err)
		}

		if err := ts.Validate(ctx); err != nil {
			return nil, rewriteError(ctx, err)
		}

		if err := namespace.AnnotateNamespace(ts); err != nil {
			return nil, rewriteError(ctx, err)
		}

		if err := shared.SanityCheckExistingRelationships(ctx, ds, nsdef, headRevision); err != nil {
			return nil, rewriteError(ctx, err)
		}
	}
	log.Ctx(ctx).Trace().Interface("namespaceDefinitions", nsdefs).Msg("validated namespace definitions")

	// If a precondition was given, decode it, and verify that none of the namespaces specified
	// have changed in any way.
	if in.OptionalDefinitionsRevisionPrecondition != "" {
		decoded, err := nspkg.DecodeV1Alpha1Revision(in.OptionalDefinitionsRevisionPrecondition)
		if err != nil {
			return nil, rewriteError(ctx, err)
		}

		for nsName, existingRevision := range decoded {
			_, createdAt, err := ds.ReadNamespace(ctx, nsName, headRevision)
			if err != nil {
				var nsNotFoundError sharederrors.UnknownNamespaceError
				if errors.As(err, &nsNotFoundError) {
					return nil, rewriteError(ctx, &writeSchemaPreconditionFailure{
						errors.New("specified revision references a type that no longer exists"),
					})
				}

				return nil, rewriteError(ctx, err)
			}

			if !createdAt.Equal(existingRevision) {
				return nil, rewriteError(ctx, &writeSchemaPreconditionFailure{
					errors.New("current schema differs from the revision specified"),
				})
			}
		}

		log.Trace().Interface("namespaceDefinitions", nsdefs).Msg("checked schema revision")
	}

	names := make([]string, 0, len(nsdefs))
	revisions := make(map[string]datastore.Revision, len(nsdefs))
	for _, nsdef := range nsdefs {
		revision, err := ds.WriteNamespace(ctx, nsdef)
		if err != nil {
			return nil, rewriteError(ctx, err)
		}

		names = append(names, nsdef.Name)
		revisions[nsdef.Name] = revision
	}

	usagemetrics.SetInContext(ctx, &dispatchv1.ResponseMeta{
		DispatchCount: uint32(len(nsdefs)),
	})

	computedRevision, err := nspkg.ComputeV1Alpha1Revision(revisions)
	if err != nil {
		return nil, rewriteError(ctx, err)
	}

	log.Ctx(ctx).Trace().Interface("namespaceDefinitions", nsdefs).Str("computedRevision", computedRevision).Msg("wrote namespace definitions")

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
		log.Ctx(ctx).Err(err)
		return err
	}
}
