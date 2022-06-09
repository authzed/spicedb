package v0

import (
	"context"
	"fmt"
	"strings"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	"github.com/authzed/grpcutil"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/prototext"

	"github.com/authzed/spicedb/pkg/development"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	devinterface "github.com/authzed/spicedb/pkg/proto/developer/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/schemadsl/generator"
	"github.com/authzed/spicedb/pkg/tuple"
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

func toV0Errors(errs []*devinterface.DeveloperError) []*v0.DeveloperError {
	slice := make([]*v0.DeveloperError, 0, len(errs))
	for _, err := range errs {
		slice = append(slice, toV0Error(err))
	}
	return slice
}

func toV0Error(err *devinterface.DeveloperError) *v0.DeveloperError {
	return &v0.DeveloperError{
		Message: err.Message,
		Line:    err.Line,
		Column:  err.Column,
		Source:  v0.DeveloperError_Source(err.Source),
		Kind:    v0.DeveloperError_ErrorKind(err.Kind),
		Path:    err.Path,
		Context: err.Context,
	}
}

func fromV0Context(rctx *v0.RequestContext) *devinterface.RequestContext {
	relationships := make([]*core.RelationTuple, 0, len(rctx.Relationships))
	for _, rel := range rctx.Relationships {
		relationships = append(relationships, core.ToCoreRelationTuple(rel))
	}

	return &devinterface.RequestContext{
		Schema:        rctx.Schema,
		Relationships: relationships,
	}
}

func (ds *devServer) FormatSchema(ctx context.Context, req *v0.FormatSchemaRequest) (*v0.FormatSchemaResponse, error) {
	namespaces, devError, err := development.CompileSchema(req.Schema)
	if err != nil {
		return nil, err
	}

	if devError != nil {
		return &v0.FormatSchemaResponse{
			Error: toV0Error(devError),
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

func (ds *devServer) EditCheck(ctx context.Context, req *v0.EditCheckRequest) (*v0.EditCheckResponse, error) {
	devContext, devErr, err := development.NewDevContext(ctx, fromV0Context(req.Context))
	if err != nil {
		return nil, err
	}

	if devErr != nil {
		return &v0.EditCheckResponse{
			RequestErrors: toV0Errors(devErr.InputErrors),
		}, nil
	}
	defer devContext.Dispose()

	// Run the checks and store their output.
	results := make([]*v0.EditCheckResult, 0, len(req.CheckRelationships))
	for _, checkTpl := range req.CheckRelationships {
		cr, err := development.RunCheck(
			devContext,
			core.ToCoreObjectAndRelation(checkTpl.ObjectAndRelation),
			core.ToCoreObjectAndRelation(checkTpl.User.GetUserset()),
		)
		if err != nil {
			devErr, wireErr := development.DistinguishGraphError(
				devContext,
				err,
				devinterface.DeveloperError_CHECK_WATCH,
				0, 0,
				tuple.String(core.ToCoreRelationTuple(checkTpl)),
			)
			if wireErr != nil {
				return nil, wireErr
			}

			results = append(results, &v0.EditCheckResult{
				Relationship: checkTpl,
				IsMember:     false,
				Error:        toV0Error(devErr),
			})
			continue
		}

		results = append(results, &v0.EditCheckResult{
			Relationship: checkTpl,
			IsMember:     cr == v1.DispatchCheckResponse_MEMBER,
		})
	}

	return &v0.EditCheckResponse{
		CheckResults: results,
	}, nil
}

func (ds *devServer) Validate(ctx context.Context, req *v0.ValidateRequest) (*v0.ValidateResponse, error) {
	devContext, devErrs, err := development.NewDevContext(ctx, fromV0Context(req.Context))
	if err != nil {
		return nil, err
	}

	if devErrs != nil {
		return &v0.ValidateResponse{
			RequestErrors: toV0Errors(devErrs.InputErrors),
		}, nil
	}
	defer devContext.Dispose()

	// Parse the assertions YAML.
	assertions, devErr := development.ParseAssertionsYAML(req.AssertionsYaml)
	if devErr != nil {
		return &v0.ValidateResponse{
			RequestErrors: []*v0.DeveloperError{toV0Error(devErr)},
		}, nil
	}

	// Parse the expected relations YAML.
	expectedRelationsMap, devErr := development.ParseExpectedRelationsYAML(req.ValidationYaml)
	if devErr != nil {
		return &v0.ValidateResponse{
			RequestErrors: []*v0.DeveloperError{toV0Error(devErr)},
		}, nil
	}

	// Run assertions.
	var failures []*v0.DeveloperError
	assertDevErrs, aerr := development.RunAllAssertions(devContext, assertions)
	if aerr != nil {
		return nil, aerr
	}

	if assertDevErrs != nil {
		if len(assertDevErrs.InputErrors) > 0 {
			return &v0.ValidateResponse{
				RequestErrors: toV0Errors(assertDevErrs.InputErrors),
			}, nil
		}

		failures = append(failures, toV0Errors(assertDevErrs.ValidationErrors)...)
	}

	// Run expected relations validation.
	membershipSet, erDevErrs, wireErr := development.RunValidation(devContext, expectedRelationsMap)
	if wireErr != nil {
		return nil, wireErr
	}

	if erDevErrs != nil {
		if len(erDevErrs.InputErrors) > 0 {
			return &v0.ValidateResponse{
				RequestErrors: toV0Errors(erDevErrs.InputErrors),
			}, nil
		}

		failures = append(failures, toV0Errors(erDevErrs.ValidationErrors)...)
	}

	// If requested, regenerate the expected relations YAML.
	updatedValidationYaml := ""
	if membershipSet != nil && req.UpdateValidationYaml {
		generatedValidationYaml, gerr := development.GenerateValidation(membershipSet)
		if gerr != nil {
			return nil, gerr
		}
		updatedValidationYaml = generatedValidationYaml
	}

	return &v0.ValidateResponse{
		ValidationErrors:      failures,
		UpdatedValidationYaml: updatedValidationYaml,
	}, nil
}

func upgradeSchema(configs []string) (string, error) {
	schema := ""
	for _, config := range configs {
		nsDef := core.NamespaceDefinition{}
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
