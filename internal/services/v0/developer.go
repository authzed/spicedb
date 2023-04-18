package v0

import (
	"context"
	"fmt"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	"github.com/authzed/grpcutil"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/prototext"

	log "github.com/authzed/spicedb/internal/logging"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/schemadsl/generator"
)

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

func (ds *devServer) UpgradeSchema(_ context.Context, req *v0.UpgradeSchemaRequest) (*v0.UpgradeSchemaResponse, error) {
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

func (ds *devServer) Share(_ context.Context, req *v0.ShareRequest) (*v0.ShareResponse, error) {
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
		log.Ctx(ctx).Debug().Str("id", req.ShareReference).Err(err).Msg("Lookup Shared Error")
		return &v0.LookupShareResponse{
			Status: v0.LookupShareResponse_FAILED_TO_LOOKUP,
		}, nil
	}

	if status == LookupNotFound {
		log.Ctx(ctx).Debug().Str("id", req.ShareReference).Msg("Lookup Shared Not Found")
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

func (ds *devServer) EditCheck(_ context.Context, _ *v0.EditCheckRequest) (*v0.EditCheckResponse, error) {
	return nil, fmt.Errorf("no longer implemented. Please use the WebAssembly development package")
}

func (ds *devServer) Validate(_ context.Context, _ *v0.ValidateRequest) (*v0.ValidateResponse, error) {
	return nil, fmt.Errorf("no longer implemented. Please use the WebAssembly development package")
}

func (ds *devServer) FormatSchema(_ context.Context, _ *v0.FormatSchemaRequest) (*v0.FormatSchemaResponse, error) {
	return nil, fmt.Errorf("no longer implemented. Please use the WebAssembly development package")
}

func upgradeSchema(configs []string) (string, error) {
	schema := ""
	for _, config := range configs {
		nsDef := core.NamespaceDefinition{}
		nerr := prototext.Unmarshal([]byte(config), &nsDef)
		if nerr != nil {
			return "", fmt.Errorf("could not upgrade schema due to parse error: %w", nerr)
		}

		generated, _, err := generator.GenerateSource(&nsDef)
		if err != nil {
			return "", err
		}

		schema += generated
		schema += "\n\n"
	}

	return schema, nil
}
