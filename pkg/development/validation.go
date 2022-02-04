package development

import (
	"context"
	"fmt"
	"sort"
	"strings"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	"github.com/google/go-cmp/cmp"

	"github.com/authzed/spicedb/internal/membership"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/authzed/spicedb/pkg/validationfile"
)

// RunValidation runs the parsed validation block against the data in the dev context.
func RunValidation(ctx context.Context, devContext *DevContext, validation validationfile.ValidationMap) (*membership.Set, *DeveloperErrors, error) {
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
		er, derr := devContext.Dispatcher.DispatchExpand(ctx, &v1.DispatchExpandRequest{
			ObjectAndRelation: onr,
			Metadata: &v1.ResolverMeta{
				AtRevision:     devContext.Revision.String(),
				DepthRemaining: maxDispatchDepth,
			},
			ExpansionMode: v1.DispatchExpandRequest_RECURSIVE,
		})
		if derr != nil {
			devErr, wireErr := DistinguishGraphError(ctx, derr, v0.DeveloperError_VALIDATION_YAML, 0, 0, string(onrKey))
			if wireErr != nil {
				return nil, nil, wireErr
			}

			failures = append(failures, devErr)
			continue
		}

		// Add the ONR and its expansion to the membership set.
		foundSubjects, _, aerr := membershipSet.AddExpansion(onr, er.TreeNode)
		if aerr != nil {
			devErr, wireErr := DistinguishGraphError(ctx, aerr, v0.DeveloperError_VALIDATION_YAML, 0, 0, string(onrKey))
			if wireErr != nil {
				return nil, nil, wireErr
			}

			failures = append(failures, devErr)
			continue
		}

		// Compare the terminal subjects found to those specified.
		errs := validateSubjects(onr, foundSubjects, validationStrings)
		failures = append(failures, errs...)
	}

	if len(failures) > 0 {
		return membershipSet, &DeveloperErrors{ValidationErrors: failures}, nil
	}

	return membershipSet, nil, nil
}

func wrapRelationships(onrStrings []string) []string {
	wrapped := make([]string, 0, len(onrStrings))
	for _, str := range onrStrings {
		wrapped = append(wrapped, fmt.Sprintf("<%s>", str))
	}

	// Sort to ensure stability.
	sort.Strings(wrapped)
	return wrapped
}

func validateSubjects(onr *v0.ObjectAndRelation, fs membership.FoundSubjects, validationStrings []validationfile.ValidationString) []*v0.DeveloperError {
	var failures []*v0.DeveloperError

	// Verify that every referenced subject is found in the membership.
	encounteredSubjects := map[string]struct{}{}
	for _, validationString := range validationStrings {
		expectedSubject, err := validationString.Subject()
		if err != nil {
			failures = append(failures, &v0.DeveloperError{
				Message: fmt.Sprintf("For object and permission/relation `%s`, %s", tuple.StringONR(onr), err.Error()),
				Source:  v0.DeveloperError_VALIDATION_YAML,
				Kind:    v0.DeveloperError_PARSE_ERROR,
				Context: fmt.Sprintf("[%s]", err.Source),
			})
			continue
		}

		if expectedSubject == nil {
			continue
		}

		subjectONR := expectedSubject.Subject
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

		// Verify exclusions are the same, if any.
		foundExcludedSubjects, isWildcard := subject.ExcludedSubjectsFromWildcard()
		expectedExcludedSubjects := expectedSubject.Exceptions
		if isWildcard {
			expectedExcludedONRStrings := tuple.StringsONRs(expectedExcludedSubjects)
			foundExcludedONRStrings := tuple.StringsONRs(foundExcludedSubjects)
			if !cmp.Equal(expectedExcludedONRStrings, foundExcludedONRStrings) {
				failures = append(failures, &v0.DeveloperError{
					Message: fmt.Sprintf("For object and permission/relation `%s`, found different excluded subjects for subject `%s`: Specified: `%s`, Computed: `%s`",
						tuple.StringONR(onr),
						tuple.StringONR(subjectONR),
						strings.Join(wrapRelationships(expectedExcludedONRStrings), ", "),
						strings.Join(wrapRelationships(foundExcludedONRStrings), ", "),
					),
					Source:  v0.DeveloperError_VALIDATION_YAML,
					Kind:    v0.DeveloperError_MISSING_EXPECTED_RELATIONSHIP,
					Context: string(validationString),
				})
			}
		} else {
			if len(expectedExcludedSubjects) > 0 {
				failures = append(failures, &v0.DeveloperError{
					Message: fmt.Sprintf("For object and permission/relation `%s`, found unexpected excluded subjects",
						tuple.StringONR(onr),
					),
					Source:  v0.DeveloperError_VALIDATION_YAML,
					Kind:    v0.DeveloperError_EXTRA_RELATIONSHIP_FOUND,
					Context: string(validationString),
				})
			}
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

// GenerateValidation generates the validation block based on a membership set.
func GenerateValidation(membershipSet *membership.Set) (string, error) {
	validationMap := validationfile.ValidationMap{}
	subjectsByONR := membershipSet.SubjectsByONR()

	onrStrings := make([]string, 0, len(subjectsByONR))
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
					fs.ToValidationString(),
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
