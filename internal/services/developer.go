package services

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/google/go-cmp/cmp"

	"github.com/authzed/spicedb/internal/graph"
	"github.com/authzed/spicedb/internal/sharederrors"
	api "github.com/authzed/spicedb/pkg/REDACTEDapi/api"
	"github.com/authzed/spicedb/pkg/grpcutil"
	"github.com/authzed/spicedb/pkg/membership"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/authzed/spicedb/pkg/validationfile"
)

type devServer struct {
	api.UnimplementedDeveloperServiceServer
	grpcutil.IgnoreAuthMixin

	shareStore ShareStore
}

const maxDepth = 25

// NewDeveloperServer creates an instance of the developer server.
func NewDeveloperServer(store ShareStore) api.DeveloperServiceServer {
	return &devServer{
		shareStore: store,
	}
}

func (ds *devServer) Share(ctx context.Context, req *api.ShareRequest) (*api.ShareResponse, error) {
	reference, err := ds.shareStore.StoreShared(SharedData{
		Version:          sharedDataVersion,
		NamespaceConfigs: req.NamespaceConfigs,
		RelationTuples:   req.RelationTuples,
		ValidationYaml:   req.ValidationYaml,
		AssertionsYaml:   req.AssertionsYaml,
	})
	if err != nil {
		return nil, err
	}

	return &api.ShareResponse{
		ShareReference: reference,
	}, nil
}

func (ds *devServer) LookupShared(ctx context.Context, req *api.LookupShareRequest) (*api.LookupShareResponse, error) {
	shared, ok, err := ds.shareStore.LookupSharedByReference(req.ShareReference)
	if err != nil {
		return &api.LookupShareResponse{
			Status: api.LookupShareResponse_FAILED_TO_LOOKUP,
		}, nil
	}

	if !ok {
		return &api.LookupShareResponse{
			Status: api.LookupShareResponse_UNKNOWN_REFERENCE,
		}, nil
	}

	return &api.LookupShareResponse{
		Status:           api.LookupShareResponse_VALID_REFERENCE,
		NamespaceConfigs: shared.NamespaceConfigs,
		RelationTuples:   shared.RelationTuples,
		ValidationYaml:   shared.ValidationYaml,
		AssertionsYaml:   shared.AssertionsYaml,
	}, nil
}

func (ds *devServer) EditCheck(ctx context.Context, req *api.EditCheckRequest) (*api.EditCheckResponse, error) {
	devContext, ok, err := NewDevContext(ctx, req.Context)
	if err != nil {
		return nil, err
	}

	if !ok {
		return &api.EditCheckResponse{
			ContextNamespaces: devContext.Namespaces,
			AdditionalErrors:  devContext.Errors,
		}, nil
	}

	// Run the checks and store their output.
	var results []*api.EditCheckResult
	for _, checkTpl := range req.CheckTuples {
		cr := devContext.Dispatcher.Check(ctx, graph.CheckRequest{
			Start:          checkTpl.ObjectAndRelation,
			Goal:           checkTpl.User.GetUserset(),
			AtRevision:     devContext.Revision,
			DepthRemaining: maxDepth,
		})
		if cr.Err != nil {
			validationErrs, wireErr := rewriteGraphError(api.ValidationError_CHECK_WATCH, tuple.String(checkTpl), cr.Err)
			return &api.EditCheckResponse{
				AdditionalErrors: validationErrs,
			}, wireErr
		}

		results = append(results, &api.EditCheckResult{
			Tuple:    checkTpl,
			IsMember: cr.IsMember,
		})
	}

	return &api.EditCheckResponse{
		ContextNamespaces: devContext.Namespaces,
		CheckResults:      results,
	}, nil
}

func (ds *devServer) Validate(ctx context.Context, req *api.ValidateRequest) (*api.ValidateResponse, error) {
	devContext, ok, err := NewDevContext(ctx, req.Context)
	if err != nil {
		return nil, err
	}

	if !ok {
		return &api.ValidateResponse{
			ContextNamespaces: devContext.Namespaces,
			ValidationErrors:  devContext.Errors,
		}, nil
	}

	// Parse the validation YAML.
	validation, err := validationfile.ParseValidationBlock([]byte(req.ValidationYaml))
	if err != nil {
		return &api.ValidateResponse{
			ValidationErrors: []*api.ValidationError{
				convertYamlError(api.ValidationError_VALIDATION_YAML, err),
			},
		}, nil
	}

	// Parse the assertions YAML.
	assertions, err := validationfile.ParseAssertionsBlock([]byte(req.AssertionsYaml))
	if err != nil {
		return &api.ValidateResponse{
			ValidationErrors: []*api.ValidationError{
				convertYamlError(api.ValidationError_ASSERTION, err),
			},
		}, nil
	}

	// Run assertions.
	assertTrueRelationships, aerr := assertions.AssertTrueRelationships()
	if aerr != nil {
		return &api.ValidateResponse{
			ValidationErrors: []*api.ValidationError{
				convertSourceError(api.ValidationError_ASSERTION, aerr),
			},
		}, nil
	}

	assertFalseRelationships, aerr := assertions.AssertFalseRelationships()
	if aerr != nil {
		return &api.ValidateResponse{
			ValidationErrors: []*api.ValidationError{
				convertSourceError(api.ValidationError_ASSERTION, aerr),
			},
		}, nil
	}

	trueFailures, err := runAssertions(ctx, devContext, assertTrueRelationships, true, "Expected relation or permission %s to exist")
	if err != nil {
		return nil, err
	}

	falseFailures, err := runAssertions(ctx, devContext, assertFalseRelationships, false, "Expected relation or permission %s to not exist")
	if err != nil {
		return nil, err
	}

	failures := append(trueFailures, falseFailures...)

	// Run validation.
	membershipSet, validationFailures, wireErr := runValidation(ctx, devContext, validation)
	if wireErr != nil {
		return nil, wireErr
	}

	// If requested, regenerate the validation YAML.
	updatedValidationYaml := ""
	if membershipSet != nil && req.UpdateValidationYaml {
		updatedValidationYaml, err = generateValidation(membershipSet)
		if err != nil {
			return nil, err
		}
	}

	return &api.ValidateResponse{
		ContextNamespaces:     devContext.Namespaces,
		ValidationErrors:      append(failures, validationFailures...),
		UpdatedValidationYaml: updatedValidationYaml,
	}, nil
}

func runAssertions(ctx context.Context, devContext *DevContext, relationships []*api.RelationTuple, expected bool, fmtString string) ([]*api.ValidationError, error) {
	var failures []*api.ValidationError
	for _, relationship := range relationships {
		cr := devContext.Dispatcher.Check(ctx, graph.CheckRequest{
			Start:          relationship.ObjectAndRelation,
			Goal:           relationship.User.GetUserset(),
			AtRevision:     devContext.Revision,
			DepthRemaining: maxDepth,
		})
		if cr.Err != nil {
			validationErrs, wireErr := rewriteGraphError(api.ValidationError_ASSERTION, tuple.String(relationship), cr.Err)
			failures = append(failures, validationErrs...)
			if wireErr != nil {
				return nil, wireErr
			}
		} else if cr.IsMember != expected {
			failures = append(failures, &api.ValidationError{
				Message:  fmt.Sprintf(fmtString, tuple.String(relationship)),
				Source:   api.ValidationError_ASSERTION,
				Kind:     api.ValidationError_ASSERTION_FAILED,
				Metadata: tuple.String(relationship),
			})
		}
	}

	return failures, nil
}

func generateValidation(membershipSet *membership.MembershipSet) (string, error) {
	validationMap := validationfile.ValidationMap{}
	subjectsByONR := membershipSet.SubjectsByONR()

	var onrStrings []string
	for onrString := range subjectsByONR {
		onrStrings = append(onrStrings, onrString)
	}

	// Sort to ensure stability of output.
	sort.Strings(onrStrings)

	for _, onrString := range onrStrings {
		foundSubjects := subjectsByONR[onrString]
		var strs []string
		for _, fs := range foundSubjects.ListFound() {
			strs = append(strs,
				fmt.Sprintf("[%s] is %s",
					tuple.StringONR(fs.Subject()),
					strings.Join(wrapRelationships(tuple.StringsONRs(fs.Relationships())), "/"),
				))
		}

		// Sort to ensure stability of output.
		sort.Strings(strs)

		var validationStrings []validationfile.ValidationString
		for _, s := range strs {
			validationStrings = append(validationStrings, validationfile.ValidationString(s))
		}

		validationMap[validationfile.ObjectRelationString(onrString)] = validationStrings
	}

	return validationMap.AsYAML()
}

func runValidation(ctx context.Context, devContext *DevContext, validation validationfile.ValidationMap) (*membership.MembershipSet, []*api.ValidationError, error) {
	var failures []*api.ValidationError
	membershipSet := membership.NewMembershipSet()

	for onrKey, validationStrings := range validation {
		// Unmarshal the ONR to expand from its string form.
		onr, err := onrKey.ONR()
		if err != nil {
			failures = append(failures,
				&api.ValidationError{
					Message:  err.Error(),
					Source:   api.ValidationError_VALIDATION_YAML,
					Kind:     api.ValidationError_PARSE_ERROR,
					Metadata: err.Source,
				},
			)
			continue
		}

		// Run a full recursive expansion over the ONR.
		er := devContext.Dispatcher.Expand(ctx, graph.ExpandRequest{
			Start:          onr,
			AtRevision:     devContext.Revision,
			DepthRemaining: maxDepth,
			ExpansionMode:  graph.RecursiveExpansion,
		})
		if er.Err != nil {
			validationErrs, wireErr := rewriteGraphError(api.ValidationError_VALIDATION_YAML, string(onrKey), er.Err)
			if validationErrs != nil {
				failures = append(failures, validationErrs...)
				continue
			}

			return nil, nil, wireErr
		}

		// Add the ONR and its expansion to the membership set.
		foundSubjects, _ := membershipSet.AddExpansion(onr, er.Tree)

		// Compare the terminal subjects found to those specified.
		errs := validateSubjects(onr, foundSubjects, validationStrings)
		failures = append(failures, errs...)
	}

	return membershipSet, failures, nil
}

func wrapRelationships(onrStrings []string) []string {
	var wrapped []string
	for _, str := range onrStrings {
		wrapped = append(wrapped, fmt.Sprintf("<%s>", str))
	}

	// Sort to ensure stability.
	sort.Strings(wrapped)
	return wrapped
}

func validateSubjects(onr *api.ObjectAndRelation, fs membership.FoundSubjects, validationStrings []validationfile.ValidationString) []*api.ValidationError {
	var failures []*api.ValidationError

	// Verify that every referenced subject is found in the membership.
	encounteredSubjects := map[string]struct{}{}
	for _, validationString := range validationStrings {
		subjectONR, err := validationString.Subject()
		if err != nil {
			failures = append(failures, &api.ValidationError{
				Message:  fmt.Sprintf("For object and permission/relation `%s`, %s", tuple.StringONR(onr), err.Error()),
				Source:   api.ValidationError_VALIDATION_YAML,
				Kind:     api.ValidationError_PARSE_ERROR,
				Metadata: fmt.Sprintf("[%s]", err.Source),
			})
			continue
		}

		if subjectONR == nil {
			continue
		}

		encounteredSubjects[tuple.StringONR(subjectONR)] = struct{}{}

		expectedRelationships, err := validationString.ONRS()
		if err != nil {
			failures = append(failures, &api.ValidationError{
				Message:  fmt.Sprintf("For object and permission/relation `%s`, %s", tuple.StringONR(onr), err.Error()),
				Source:   api.ValidationError_VALIDATION_YAML,
				Kind:     api.ValidationError_PARSE_ERROR,
				Metadata: fmt.Sprintf("<%s>", err.Source),
			})
			continue
		}

		subject, ok := fs.LookupSubject(subjectONR)
		if !ok {
			failures = append(failures, &api.ValidationError{
				Message:  fmt.Sprintf("For object and permission/relation `%s`, missing expected subject `%s`", tuple.StringONR(onr), tuple.StringONR(subjectONR)),
				Source:   api.ValidationError_VALIDATION_YAML,
				Kind:     api.ValidationError_MISSING_EXPECTED_TUPLE,
				Metadata: tuple.StringONR(subjectONR),
			})
			continue
		}

		foundRelationships := subject.Relationships()

		// Verify that the relationships are the same.
		expectedONRStrings := tuple.StringsONRs(expectedRelationships)
		foundONRStrings := tuple.StringsONRs(foundRelationships)
		if !cmp.Equal(expectedONRStrings, foundONRStrings) {
			failures = append(failures, &api.ValidationError{
				Message: fmt.Sprintf("For object and permission/relation `%s`, found different relationships for subject `%s`: Specified: `%s`, Computed: `%s`",
					tuple.StringONR(onr),
					tuple.StringONR(subjectONR),
					strings.Join(wrapRelationships(expectedONRStrings), "/"),
					strings.Join(wrapRelationships(foundONRStrings), "/"),
				),
				Source:   api.ValidationError_VALIDATION_YAML,
				Kind:     api.ValidationError_MISSING_EXPECTED_TUPLE,
				Metadata: string(validationString),
			})
		}
	}

	// Verify that every subject found was referenced.
	for _, foundSubject := range fs.ListFound() {
		_, ok := encounteredSubjects[tuple.StringONR(foundSubject.Subject())]
		if !ok {
			failures = append(failures, &api.ValidationError{
				Message: fmt.Sprintf("For object and permission/relation `%s`, subject `%s` found but missing from specified",
					tuple.StringONR(onr),
					tuple.StringONR(foundSubject.Subject()),
				),
				Source:   api.ValidationError_VALIDATION_YAML,
				Kind:     api.ValidationError_EXTRA_TUPLE_FOUND,
				Metadata: tuple.StringONR(onr),
			})
		}
	}

	return failures
}

func rewriteGraphError(source api.ValidationError_Source, metadata string, checkError error) ([]*api.ValidationError, error) {
	var nsNotFoundError sharederrors.UnknownNamespaceError = nil
	var relNotFoundError sharederrors.UnknownRelationError = nil

	if errors.As(checkError, &nsNotFoundError) {
		return []*api.ValidationError{
			&api.ValidationError{
				Message:  checkError.Error(),
				Source:   source,
				Kind:     api.ValidationError_UNKNOWN_NAMESPACE,
				Metadata: metadata,
			},
		}, nil
	}

	if errors.As(checkError, &relNotFoundError) {
		return []*api.ValidationError{
			&api.ValidationError{
				Message:  checkError.Error(),
				Source:   source,
				Kind:     api.ValidationError_UNKNOWN_RELATION,
				Metadata: metadata,
			},
		}, nil
	}

	return nil, rewriteACLError(checkError)
}

func convertSourceError(source api.ValidationError_Source, err *validationfile.ErrorWithSource) *api.ValidationError {
	return &api.ValidationError{
		Message:  err.Error(),
		Kind:     api.ValidationError_PARSE_ERROR,
		Source:   source,
		Metadata: err.Source,
	}
}

var yamlLineRegex = regexp.MustCompile(`line ([0-9]+): (.+)`)

func convertYamlError(source api.ValidationError_Source, err error) *api.ValidationError {
	var lineNumber uint64 = 0
	var msg = err.Error()

	pieces := yamlLineRegex.FindStringSubmatch(err.Error())
	if len(pieces) == 3 {
		// We can safely ignore the error here because it will default to 0, which is the not found
		// case.
		lineNumber, _ = strconv.ParseUint(pieces[1], 10, 0)
		msg = pieces[2]
	}

	return &api.ValidationError{
		Message: msg,
		Kind:    api.ValidationError_PARSE_ERROR,
		Source:  source,
		Line:    uint32(lineNumber),
	}
}
