package v1alpha1

import (
	"context"
	"errors"

	v1alpha1 "github.com/authzed/authzed-go/proto/authzed/api/v1alpha1"
	"github.com/authzed/grpcutil"
	grpcmw "github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/namespace"
	"github.com/authzed/spicedb/internal/services/serviceerrors"
	"github.com/authzed/spicedb/internal/services/shared"
	"github.com/authzed/spicedb/internal/sharederrors"
	nspkg "github.com/authzed/spicedb/pkg/namespace"
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
	ds             datastore.Datastore
}

// NewSchemaServer returns an new instance of a server that implements
// authzed.api.v1alpha1.SchemaService.
func NewSchemaServer(ds datastore.Datastore, prefixRequired PrefixRequiredOption) v1alpha1.SchemaServiceServer {
	return &schemaServiceServer{
		ds:             ds,
		prefixRequired: prefixRequired,
		WithUnaryServiceSpecificInterceptor: shared.WithUnaryServiceSpecificInterceptor{
			Unary: grpcmw.ChainUnaryServer(grpcutil.DefaultUnaryMiddleware...),
		},
	}
}

func (ss *schemaServiceServer) ReadSchema(ctx context.Context, in *v1alpha1.ReadSchemaRequest) (*v1alpha1.ReadSchemaResponse, error) {
	headRevision, err := ss.ds.HeadRevision(ctx)
	if err != nil {
		return nil, rewriteError(ctx, err)
	}

	objectDefs := make([]string, 0, len(in.GetObjectDefinitionsNames()))
	createdRevisions := make(map[string]datastore.Revision, len(in.GetObjectDefinitionsNames()))
	for _, objectDefName := range in.GetObjectDefinitionsNames() {
		found, createdAt, err := ss.ds.ReadNamespace(ctx, objectDefName, headRevision)
		if err != nil {
			return nil, rewriteError(ctx, err)
		}

		createdRevisions[objectDefName] = createdAt

		objectDef, _ := generator.GenerateSource(found)
		objectDefs = append(objectDefs, objectDef)
	}

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
	nsm, err := namespace.NewCachingNamespaceManager(ss.ds, 0, nil) // non-caching manager
	if err != nil {
		return nil, rewriteError(ctx, err)
	}

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

	headRevision, err := ss.ds.HeadRevision(ctx)
	if err != nil {
		return nil, rewriteError(ctx, err)
	}

	log.Ctx(ctx).Trace().Interface("namespace definitions", nsdefs).Msg("compiled namespace definitions")

	for _, nsdef := range nsdefs {
		ts, err := namespace.BuildNamespaceTypeSystemWithFallback(nsdef, nsm, nsdefs, headRevision)
		if err != nil {
			return nil, rewriteError(ctx, err)
		}

		if err := ts.Validate(ctx); err != nil {
			return nil, rewriteError(ctx, err)
		}

		if err := shared.SanityCheckExistingRelationships(ctx, ss.ds, nsdef, headRevision); err != nil {
			return nil, rewriteError(ctx, err)
		}
	}
	log.Ctx(ctx).Trace().Interface("namespace definitions", nsdefs).Msg("validated namespace definitions")

	// If a precondition was given, decode it, and verify that none of the namespaces specified
	// have changed in any way.
	if in.OptionalDefinitionsRevisionPrecondition != "" {
		decoded, err := nspkg.DecodeV1Alpha1Revision(in.OptionalDefinitionsRevisionPrecondition)
		if err != nil {
			return nil, rewriteError(ctx, err)
		}

		for nsName, existingRevision := range decoded {
			_, createdAt, err := ss.ds.ReadNamespace(ctx, nsName, headRevision)
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

		log.Trace().Interface("namespace definitions", nsdefs).Msg("checked schema revision")
	}

	names := make([]string, 0, len(nsdefs))
	revisions := make(map[string]datastore.Revision, len(nsdefs))
	for _, nsdef := range nsdefs {
		revision, err := ss.ds.WriteNamespace(ctx, nsdef)
		if err != nil {
			return nil, rewriteError(ctx, err)
		}

		names = append(names, nsdef.Name)
		revisions[nsdef.Name] = revision
	}

	computedRevision, err := nspkg.ComputeV1Alpha1Revision(revisions)
	if err != nil {
		return nil, rewriteError(ctx, err)
	}

	log.Ctx(ctx).Trace().Interface("namespace definitions", nsdefs).Str("computed revision", computedRevision).Msg("wrote namespace definitions")

	return &v1alpha1.WriteSchemaResponse{
		ObjectDefinitionsNames:      names,
		ComputedDefinitionsRevision: computedRevision,
	}, nil
}

func rewriteError(ctx context.Context, err error) error {
	var nsNotFoundError sharederrors.UnknownNamespaceError
	var errWithContext compiler.ErrorWithContext
	var errPreconditionFailure *writeSchemaPreconditionFailure

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
