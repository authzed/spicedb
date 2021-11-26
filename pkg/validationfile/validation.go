package validationfile

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	"github.com/authzed/spicedb/internal/dispatch"
	v1 "github.com/authzed/spicedb/internal/proto/dispatch/v1"
	"github.com/authzed/spicedb/internal/sharederrors"
	"github.com/authzed/spicedb/pkg/membership"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/google/go-cmp/cmp"
	"github.com/shopspring/decimal"
	"gopkg.in/yaml.v3"
)

const maxDepth = 25

var yamlLineRegex = regexp.MustCompile(`line ([0-9]+): (.+)`)

func RequestFromFile(ctx context.Context, validationFile ValidationFile) (*v0.ValidateRequest, error) {
	validationYAML, err := yaml.Marshal(validationFile.Validations)
	if err != nil {
		return nil, fmt.Errorf("failed to parse playground YAML validations: %w", err)
	}
	assertionYAML, err := yaml.Marshal(validationFile.Assertions)
	if err != nil {
		return nil, fmt.Errorf("failed to parse playground YAML assertions: %w", err)
	}
	tuples, err := ParseRelationships(validationFile.Relationships)
	if err != nil {
		return nil, fmt.Errorf("failed to parse playground YAML relationships: %w", err)
	}
	return &v0.ValidateRequest{
		Context: &v0.RequestContext{
			Schema:        validationFile.Schema,
			Relationships: tuples,
		},
		AssertionsYaml:       string(assertionYAML),
		ValidationYaml:       string(validationYAML),
		UpdateValidationYaml: false,
	}, nil
}

func Validate(ctx context.Context, req *v0.ValidateRequest, dispatcher dispatch.Dispatcher, revision decimal.Decimal) (*v0.ValidateResponse, error) {
	// Parse the validation YAML.
	validation, err := ParseValidationBlock([]byte(req.ValidationYaml))
	if err != nil {
		return &v0.ValidateResponse{
			RequestErrors: []*v0.DeveloperError{
				convertYamlError(v0.DeveloperError_VALIDATION_YAML, err),
			},
		}, nil
	}

	// Parse the assertions YAML.
	assertions, err := ParseAssertionsBlock([]byte(req.AssertionsYaml))
	if err != nil {
		return &v0.ValidateResponse{
			RequestErrors: []*v0.DeveloperError{
				convertYamlError(v0.DeveloperError_ASSERTION, err),
			},
		}, nil
	}

	// Run assertions.
	assertTrueRelationships, aerr := assertions.AssertTrueRelationships()
	if aerr != nil {
		return &v0.ValidateResponse{
			RequestErrors: []*v0.DeveloperError{
				convertSourceError(v0.DeveloperError_ASSERTION, aerr),
			},
		}, nil
	}

	assertFalseRelationships, aerr := assertions.AssertFalseRelationships()
	if aerr != nil {
		return &v0.ValidateResponse{
			RequestErrors: []*v0.DeveloperError{
				convertSourceError(v0.DeveloperError_ASSERTION, aerr),
			},
		}, nil
	}

	trueFailures, err := runAssertions(ctx, dispatcher, revision, assertTrueRelationships, true, "Expected relation or permission %s to exist")
	if err != nil {
		return nil, err
	}

	falseFailures, err := runAssertions(ctx, dispatcher, revision, assertFalseRelationships, false, "Expected relation or permission %s to not exist")
	if err != nil {
		return nil, err
	}

	failures := append(trueFailures, falseFailures...)

	// Run validation.
	membershipSet, validationFailures, wireErr := runValidation(ctx, dispatcher, revision, validation)
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

	return &v0.ValidateResponse{
		ValidationErrors:      append(failures, validationFailures...),
		UpdatedValidationYaml: updatedValidationYaml,
	}, nil
}

func runValidation(ctx context.Context, dispatcher dispatch.Dispatcher, revision decimal.Decimal, validation ValidationMap) (*membership.MembershipSet, []*v0.DeveloperError, error) {
	var failures []*v0.DeveloperError
	membershipSet := membership.NewMembershipSet()

	for onrKey, validationStrings := range validation {
		// Unmarshal the ONR to expand from its string form.
		onr, err := onrKey.ONR()
		if err != nil {
			failures = append(failures,
				&v0.DeveloperError{
					Message: err.Error(),
					Source:  v0.DeveloperError_VALIDATION_YAML,
					Kind:    v0.DeveloperError_PARSE_ERROR,
					Context: err.Source,
				},
			)
			continue
		}

		// Run a full recursive expansion over the ONR.
		er, dispatchErr := dispatcher.DispatchExpand(ctx, &v1.DispatchExpandRequest{
			ObjectAndRelation: onr,
			Metadata: &v1.ResolverMeta{
				AtRevision:     revision.String(),
				DepthRemaining: maxDepth,
			},
			ExpansionMode: v1.DispatchExpandRequest_RECURSIVE,
		})
		if dispatchErr != nil {
			validationErrs, wireErr := RewriteGraphError(v0.DeveloperError_VALIDATION_YAML, 0, 0, string(onrKey), dispatchErr)
			if validationErrs != nil {
				failures = append(failures, validationErrs...)
				return nil, failures, nil
			}

			return nil, nil, wireErr
		}

		// Add the ONR and its expansion to the membership set.
		foundSubjects, _, aerr := membershipSet.AddExpansion(onr, er.TreeNode)
		if aerr != nil {
			validationErrs, wireErr := RewriteGraphError(v0.DeveloperError_VALIDATION_YAML, 0, 0, string(onrKey), aerr)
			if validationErrs != nil {
				failures = append(failures, validationErrs...)
				continue
			}

			return nil, nil, wireErr
		}

		// Compare the terminal subjects found to those specified.
		errs := validateSubjects(onr, foundSubjects, validationStrings)
		failures = append(failures, errs...)
	}

	return membershipSet, failures, nil
}

func validateSubjects(onr *v0.ObjectAndRelation, fs membership.FoundSubjects, validationStrings []ValidationString) []*v0.DeveloperError {
	var failures []*v0.DeveloperError

	// Verify that every referenced subject is found in the membership.
	encounteredSubjects := map[string]struct{}{}
	for _, validationString := range validationStrings {
		subjectONR, err := validationString.Subject()
		if err != nil {
			failures = append(failures, &v0.DeveloperError{
				Message: fmt.Sprintf("For object and permission/relation `%s`, %s", tuple.StringONR(onr), err.Error()),
				Source:  v0.DeveloperError_VALIDATION_YAML,
				Kind:    v0.DeveloperError_PARSE_ERROR,
				Context: fmt.Sprintf("[%s]", err.Source),
			})
			continue
		}

		if subjectONR == nil {
			continue
		}

		encounteredSubjects[tuple.StringONR(subjectONR)] = struct{}{}

		expectedRelationships, err := validationString.ONRS()
		if err != nil {
			failures = append(failures, &v0.DeveloperError{
				Message: fmt.Sprintf("For object and permission/relation `%s`, %s", tuple.StringONR(onr), err.Error()),
				Source:  v0.DeveloperError_VALIDATION_YAML,
				Kind:    v0.DeveloperError_PARSE_ERROR,
				Context: fmt.Sprintf("<%s>", err.Source),
			})
			continue
		}

		subject, ok := fs.LookupSubject(subjectONR)
		if !ok {
			failures = append(failures, &v0.DeveloperError{
				Message: fmt.Sprintf("For object and permission/relation `%s`, missing expected subject `%s`", tuple.StringONR(onr), tuple.StringONR(subjectONR)),
				Source:  v0.DeveloperError_VALIDATION_YAML,
				Kind:    v0.DeveloperError_MISSING_EXPECTED_RELATIONSHIP,
				Context: tuple.StringONR(subjectONR),
			})
			continue
		}

		foundRelationships := subject.Relationships()

		// Verify that the relationships are the same.
		expectedONRStrings := tuple.StringsONRs(expectedRelationships)
		foundONRStrings := tuple.StringsONRs(foundRelationships)
		if !cmp.Equal(expectedONRStrings, foundONRStrings) {
			failures = append(failures, &v0.DeveloperError{
				Message: fmt.Sprintf("For object and permission/relation `%s`, found different relationships for subject `%s`: Specified: `%s`, Computed: `%s`",
					tuple.StringONR(onr),
					tuple.StringONR(subjectONR),
					strings.Join(wrapRelationships(expectedONRStrings), "/"),
					strings.Join(wrapRelationships(foundONRStrings), "/"),
				),
				Source:  v0.DeveloperError_VALIDATION_YAML,
				Kind:    v0.DeveloperError_MISSING_EXPECTED_RELATIONSHIP,
				Context: string(validationString),
			})
		}
	}

	// Verify that every subject found was referenced.
	for _, foundSubject := range fs.ListFound() {
		_, ok := encounteredSubjects[tuple.StringONR(foundSubject.Subject())]
		if !ok {
			failures = append(failures, &v0.DeveloperError{
				Message: fmt.Sprintf("For object and permission/relation `%s`, subject `%s` found but missing from specified",
					tuple.StringONR(onr),
					tuple.StringONR(foundSubject.Subject()),
				),
				Source:  v0.DeveloperError_VALIDATION_YAML,
				Kind:    v0.DeveloperError_EXTRA_RELATIONSHIP_FOUND,
				Context: tuple.StringONR(onr),
			})
		}
	}

	return failures
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

func convertSourceError(source v0.DeveloperError_Source, err *ErrorWithSource) *v0.DeveloperError {
	return &v0.DeveloperError{
		Message: err.Error(),
		Kind:    v0.DeveloperError_PARSE_ERROR,
		Source:  source,
		Line:    err.LineNumber,
		Column:  err.ColumnPosition,
		Context: err.Source,
	}
}

func convertYamlError(source v0.DeveloperError_Source, err error) *v0.DeveloperError {
	var lineNumber uint64
	msg := err.Error()

	pieces := yamlLineRegex.FindStringSubmatch(err.Error())
	if len(pieces) == 3 {
		// We can safely ignore the error here because it will default to 0, which is the not found
		// case.
		lineNumber, _ = strconv.ParseUint(pieces[1], 10, 0)
		msg = pieces[2]
	}

	return &v0.DeveloperError{
		Message: msg,
		Kind:    v0.DeveloperError_PARSE_ERROR,
		Source:  source,
		Line:    uint32(lineNumber),
	}
}

func runAssertions(ctx context.Context, dispatcher dispatch.Dispatcher, revision decimal.Decimal, assertions []ParsedAssertion, expected bool, fmtString string) ([]*v0.DeveloperError, error) {
	var failures []*v0.DeveloperError
	for _, assertion := range assertions {
		cr, err := dispatcher.DispatchCheck(ctx, &v1.DispatchCheckRequest{
			ObjectAndRelation: assertion.Relationship.ObjectAndRelation,
			Subject:           assertion.Relationship.User.GetUserset(),
			Metadata: &v1.ResolverMeta{
				AtRevision:     revision.String(),
				DepthRemaining: maxDepth,
			},
		})
		if err != nil {
			validationErrs, wireErr := RewriteGraphError(
				v0.DeveloperError_ASSERTION,
				assertion.LineNumber,
				assertion.ColumnPosition,
				tuple.String(assertion.Relationship),
				err,
			)
			failures = append(failures, validationErrs...)
			if wireErr != nil {
				return nil, wireErr
			}
		} else if (cr.Membership == v1.DispatchCheckResponse_MEMBER) != expected {
			failures = append(failures, &v0.DeveloperError{
				Message: fmt.Sprintf(fmtString, tuple.String(assertion.Relationship)),
				Source:  v0.DeveloperError_ASSERTION,
				Kind:    v0.DeveloperError_ASSERTION_FAILED,
				Context: tuple.String(assertion.Relationship),
				Line:    assertion.LineNumber,
				Column:  assertion.ColumnPosition,
			})
		}
	}

	return failures, nil
}

type invalidRelationError struct {
	error
	subject *v0.User
	onr     *v0.ObjectAndRelation
}

func generateValidation(membershipSet *membership.MembershipSet) (string, error) {
	validationMap := ValidationMap{}
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

		var validationStrings []ValidationString
		for _, s := range strs {
			validationStrings = append(validationStrings, ValidationString(s))
		}

		validationMap[ObjectRelationString(onrString)] = validationStrings
	}

	return validationMap.AsYAML()
}

func RewriteGraphError(source v0.DeveloperError_Source, line uint32, column uint32, context string, checkError error) ([]*v0.DeveloperError, error) {
	var nsNotFoundError sharederrors.UnknownNamespaceError
	var relNotFoundError sharederrors.UnknownRelationError

	if errors.As(checkError, &nsNotFoundError) {
		return []*v0.DeveloperError{
			{
				Message: checkError.Error(),
				Source:  source,
				Kind:    v0.DeveloperError_UNKNOWN_OBJECT_TYPE,
				Line:    line,
				Column:  column,
				Context: context,
			},
		}, nil
	}

	if errors.As(checkError, &relNotFoundError) {
		return []*v0.DeveloperError{
			{
				Message: checkError.Error(),
				Source:  source,
				Kind:    v0.DeveloperError_UNKNOWN_RELATION,
				Line:    line,
				Column:  column,
				Context: context,
			},
		}, nil
	}

	var ire invalidRelationError
	if errors.As(checkError, &ire) {
		return []*v0.DeveloperError{
			{
				Message: checkError.Error(),
				Source:  source,
				Kind:    v0.DeveloperError_UNKNOWN_RELATION,
				Line:    line,
				Column:  column,
				Context: context,
			},
		}, nil
	}

	return nil, checkError
}
