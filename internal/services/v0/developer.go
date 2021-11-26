package v0

import (
	"context"
	"fmt"
	"strings"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	"github.com/authzed/grpcutil"
	v1 "github.com/authzed/spicedb/internal/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/prototext"

	"github.com/authzed/spicedb/pkg/schemadsl/generator"
	"github.com/authzed/spicedb/pkg/validationfile"
)

const maxDepth = 25

type devServer struct {
	v0.UnimplementedDeveloperServiceServer
	grpcutil.IgnoreAuthMixin

	shareStore ShareStore
}

// RegisterDeveloperServer adds the Developer Server to a grpc service registrar
// This is preferred over manually registering the service; it will add required middleware
func RegisterDeveloperServer(r grpc.ServiceRegistrar, s v0.DeveloperServiceServer) *grpc.ServiceDesc {
	r.RegisterService(grpcutil.WrapMethods(v0.DeveloperService_ServiceDesc, grpcutil.DefaultUnaryMiddleware...), s)
	return &v0.DeveloperService_ServiceDesc
}

// NewDeveloperServer creates an instance of the developer server.
func NewDeveloperServer(store ShareStore) v0.DeveloperServiceServer {
	return &devServer{
		shareStore: store,
	}
}

func (ds *devServer) FormatSchema(ctx context.Context, req *v0.FormatSchemaRequest) (*v0.FormatSchemaResponse, error) {
	namespaces, devError, err := compile(req.Schema)
	if err != nil {
		return nil, err
	}

	if devError != nil {
		return &v0.FormatSchemaResponse{
			Error: devError,
		}, nil
	}

	formatted := ""
	for _, nsDef := range namespaces {
		source, _ := generator.GenerateSource(nsDef)
		formatted += source
		formatted += "\n\n"
	}

	return &v0.FormatSchemaResponse{
		FormattedSchema: strings.TrimSpace(formatted),
	}, nil
}

func (ds *devServer) UpgradeSchema(ctx context.Context, req *v0.UpgradeSchemaRequest) (*v0.UpgradeSchemaResponse, error) {
	upgraded, err := upgradeSchema(req.NamespaceConfigs)
	if err != nil {
		return &v0.UpgradeSchemaResponse{
			Error: &v0.DeveloperError{
				Message: err.Error(),
			},
		}, nil
	}

	return &v0.UpgradeSchemaResponse{
		UpgradedSchema: upgraded,
	}, nil
}

func (ds *devServer) Share(ctx context.Context, req *v0.ShareRequest) (*v0.ShareResponse, error) {
	reference, err := ds.shareStore.StoreShared(SharedDataV2{
		Version:           sharedDataVersion,
		Schema:            req.Schema,
		RelationshipsYaml: req.RelationshipsYaml,
		ValidationYaml:    req.ValidationYaml,
		AssertionsYaml:    req.AssertionsYaml,
	})
	if err != nil {
		return nil, err
	}

	return &v0.ShareResponse{
		ShareReference: reference,
	}, nil
}

func (ds *devServer) LookupShared(ctx context.Context, req *v0.LookupShareRequest) (*v0.LookupShareResponse, error) {
	shared, status, err := ds.shareStore.LookupSharedByReference(req.ShareReference)
	if err != nil {
		log.Debug().Str("id", req.ShareReference).Err(err).Msg("Lookup Shared Error")
		return &v0.LookupShareResponse{
			Status: v0.LookupShareResponse_FAILED_TO_LOOKUP,
		}, nil
	}

	if status == LookupNotFound {
		log.Debug().Str("id", req.ShareReference).Msg("Lookup Shared Not Found")
		return &v0.LookupShareResponse{
			Status: v0.LookupShareResponse_UNKNOWN_REFERENCE,
		}, nil
	}

	if status == LookupConverted {
		return &v0.LookupShareResponse{
			Status:            v0.LookupShareResponse_UPGRADED_REFERENCE,
			Schema:            shared.Schema,
			RelationshipsYaml: shared.RelationshipsYaml,
			ValidationYaml:    shared.ValidationYaml,
			AssertionsYaml:    shared.AssertionsYaml,
		}, nil
	}

	return &v0.LookupShareResponse{
		Status:            v0.LookupShareResponse_VALID_REFERENCE,
		Schema:            shared.Schema,
		RelationshipsYaml: shared.RelationshipsYaml,
		ValidationYaml:    shared.ValidationYaml,
		AssertionsYaml:    shared.AssertionsYaml,
	}, nil
}

func (ds *devServer) EditCheck(ctx context.Context, req *v0.EditCheckRequest) (*v0.EditCheckResponse, error) {
	devContext, ok, err := NewDevContext(ctx, req.Context)
	if err != nil {
		return nil, err
	}

	if !ok {
		return &v0.EditCheckResponse{
			RequestErrors: devContext.RequestErrors,
		}, nil
	}
	defer devContext.Dispose()

	// Run the checks and store their output.
	var results []*v0.EditCheckResult
	for _, checkTpl := range req.CheckRelationships {
		cr, err := devContext.Dispatcher.DispatchCheck(ctx, &v1.DispatchCheckRequest{
			ObjectAndRelation: checkTpl.ObjectAndRelation,
			Subject:           checkTpl.User.GetUserset(),
			Metadata: &v1.ResolverMeta{
				AtRevision:     devContext.Revision.String(),
				DepthRemaining: maxDepth,
			},
		})
		if err != nil {
			requestErrors, wireErr := validationfile.RewriteGraphError(v0.DeveloperError_CHECK_WATCH, 0, 0, tuple.String(checkTpl), err)
			if len(requestErrors) == 1 {
				results = append(results, &v0.EditCheckResult{
					Relationship: checkTpl,
					IsMember:     false,
					Error:        requestErrors[0],
				})
				continue
			}

			return &v0.EditCheckResponse{
				RequestErrors: requestErrors,
			}, wireErr
		}

		results = append(results, &v0.EditCheckResult{
			Relationship: checkTpl,
			IsMember:     cr.Membership == v1.DispatchCheckResponse_MEMBER,
		})
	}

	return &v0.EditCheckResponse{
		CheckResults: results,
	}, nil
}

func (ds *devServer) Validate(ctx context.Context, req *v0.ValidateRequest) (*v0.ValidateResponse, error) {
	devContext, ok, err := NewDevContext(ctx, req.Context)
	if err != nil {
		return nil, err
	}

	if !ok {
		return &v0.ValidateResponse{
			RequestErrors: devContext.RequestErrors,
		}, nil
	}
	defer devContext.Dispose()
	return validationfile.Validate(ctx, req, devContext.Dispatcher, devContext.Revision)
}

func upgradeSchema(configs []string) (string, error) {
	schema := ""
	for _, config := range configs {
		nsDef := v0.NamespaceDefinition{}
		nerr := prototext.Unmarshal([]byte(config), &nsDef)
		if nerr != nil {
			return "", fmt.Errorf("could not upgrade schema due to parse error: %w", nerr)
		}

		generated, _ := generator.GenerateSource(&nsDef)
		schema += generated
		schema += "\n\n"
	}

	return schema, nil
}
