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

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/graph"
	"github.com/authzed/spicedb/internal/namespace"
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
	s := &devServer{
		shareStore: store,
	}
	return s
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
	if !ok {
		return &api.LookupShareResponse{
			Status: api.LookupShareResponse_UNKNOWN_REFERENCE,
		}, nil
	}

	if err != nil {
		return &api.LookupShareResponse{
			Status: api.LookupShareResponse_FAILED_TO_LOOKUP,
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
	devContext, okay, err := NewDevContext(ctx, req.Context)
	if err != nil {
		return nil, err
	}

	if !okay {
		return &api.EditCheckResponse{
			ContextNamespaces: devContext.Namespaces,
			AdditionalErrors:  devContext.Errors,
		}, nil
	}

	// Run the checks and store their output.
	results := []*api.EditCheckResult{}
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
	devContext, okay, err := NewDevContext(ctx, req.Context)
	if err != nil {
		return nil, err
	}

	if !okay {
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

	failures := []*api.ValidationError{}
	for _, relationship := range assertTrueRelationships {
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
		} else if !cr.IsMember {
			failures = append(failures, &api.ValidationError{
				Message:  fmt.Sprintf("Expected relation or permission %s to exist", tuple.String(relationship)),
				Source:   api.ValidationError_ASSERTION,
				Kind:     api.ValidationError_ASSERTION_FAILED,
				Metadata: tuple.String(relationship),
			})
		}
	}

	for _, relationship := range assertFalseRelationships {
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
		} else if cr.IsMember {
			failures = append(failures, &api.ValidationError{
				Message:  fmt.Sprintf("Expected relation or permission %s to not exist", tuple.String(relationship)),
				Source:   api.ValidationError_ASSERTION,
				Kind:     api.ValidationError_ASSERTION_FAILED,
				Metadata: tuple.String(relationship),
			})
		}
	}

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

func generateValidation(membershipSet *membership.MembershipSet) (string, error) {
	validationMap := validationfile.ValidationMap{}
	subjectsByONR := membershipSet.SubjectsByONR()

	onrStrings := []string{}
	for onrString := range subjectsByONR {
		onrStrings = append(onrStrings, onrString)
	}

	// Sort to ensure stability of output.
	sort.Strings(onrStrings)

	for _, onrString := range onrStrings {
		foundSubjects := subjectsByONR[onrString]
		strs := []string{}
		for _, fs := range foundSubjects.ListFound() {
			strs = append(strs,
				fmt.Sprintf("[%s] is %s",
					tuple.StringONR(fs.Subject()),
					strings.Join(wrapRelationships(tuple.StringsONRs(fs.Relationships())), "/"),
				))
		}

		// Sort to ensure stability of output.
		sort.Strings(strs)

		validationStrings := []validationfile.ValidationString{}
		for _, s := range strs {
			validationStrings = append(validationStrings, validationfile.ValidationString(s))
		}

		validationMap[validationfile.ObjectRelationString(onrString)] = validationStrings
	}

	return validationMap.AsYAML()
}

func runValidation(ctx context.Context, devContext DevContext, validation validationfile.ValidationMap) (*membership.MembershipSet, []*api.ValidationError, error) {
	failures := []*api.ValidationError{}
	membershipSet := membership.NewMembershipSet()

	for onrKey, validationStrings := range validation {
		// Parse the ONR to expand.
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

			return nil, []*api.ValidationError{}, wireErr
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
	wrapped := []string{}
	for _, str := range onrStrings {
		wrapped = append(wrapped, fmt.Sprintf("<%s>", str))
	}

	// Sort to ensure stability.
	sort.Strings(wrapped)
	return wrapped
}

func validateSubjects(onr *api.ObjectAndRelation, fs membership.FoundSubjects, validationStrings []validationfile.ValidationString) []*api.ValidationError {
	failures := []*api.ValidationError{}

	// Verify that every referenced subject is found in the membership.
	encounteredSubjects := map[string]bool{}
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

		encounteredSubjects[tuple.StringONR(subjectONR)] = true

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
				Message: fmt.Sprintf("For object and permission/relation `%s`, found unspecified subject `%s`",
					tuple.StringONR(onr),
					tuple.StringONR(foundSubject.Subject()),
				),
				Source: api.ValidationError_VALIDATION_YAML,
				Kind:   api.ValidationError_EXTRA_TUPLE_FOUND,
			})
		}
	}

	return failures
}

func rewriteGraphError(source api.ValidationError_Source, metadata string, checkError error) ([]*api.ValidationError, error) {
	// TODO(jschorr): once the errors returned by the graph are more detailed,
	// extract the information here and unify all of this. See: https://github.com/REDACTED/code/issues/800
	if errors.Is(checkError, graph.ErrNamespaceNotFound) || errors.Is(checkError, namespace.ErrInvalidNamespace) {
		return []*api.ValidationError{
			&api.ValidationError{
				Message:  fmt.Sprintf("Unknown namespace in check %s", metadata),
				Source:   source,
				Kind:     api.ValidationError_UNKNOWN_NAMESPACE,
				Metadata: metadata,
			},
		}, nil
	}

	if errors.Is(checkError, graph.ErrRelationNotFound) || errors.Is(checkError, namespace.ErrInvalidRelation) || errors.Is(checkError, datastore.ErrRelationNotFound) {
		return []*api.ValidationError{
			&api.ValidationError{
				Message:  fmt.Sprintf("Unknown relation in check %s", metadata),
				Source:   source,
				Kind:     api.ValidationError_UNKNOWN_RELATION,
				Metadata: metadata,
			},
		}, nil
	}

	return []*api.ValidationError{}, rewriteACLError(checkError)
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
