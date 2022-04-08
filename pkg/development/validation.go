package development

import (
	"fmt"
	"sort"
	"strings"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	"github.com/google/go-cmp/cmp"
	yaml "gopkg.in/yaml.v2"

	"github.com/authzed/spicedb/internal/membership"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/authzed/spicedb/pkg/validationfile/blocks"
)

// RunValidation runs the parsed validation block against the data in the dev context.
func RunValidation(devContext *DevContext, validation *blocks.ParsedExpectedRelations) (*membership.Set, *DeveloperErrors, error) {
	var failures []*v0.DeveloperError
	membershipSet := membership.NewMembershipSet()
	ctx := devContext.Ctx

	for onrKey, expectedSubjects := range validation.ValidationMap {
		if onrKey.ObjectAndRelation == nil {
			panic("Got nil ObjectAndRelation")
		}

		// Run a full recursive expansion over the ONR.
		er, derr := devContext.Dispatcher.DispatchExpand(ctx, &v1.DispatchExpandRequest{
			ObjectAndRelation: onrKey.ObjectAndRelation,
			Metadata: &v1.ResolverMeta{
				AtRevision:     devContext.Revision.String(),
				DepthRemaining: maxDispatchDepth,
			},
			ExpansionMode: v1.DispatchExpandRequest_RECURSIVE,
		})
		if derr != nil {
			devErr, wireErr := DistinguishGraphError(devContext, derr, v0.DeveloperError_VALIDATION_YAML, 0, 0, onrKey.ObjectRelationString)
			if wireErr != nil {
				return nil, nil, wireErr
			}

			failures = append(failures, devErr)
			continue
		}

		// Add the ONR and its expansion to the membership set.
		foundSubjects, _, aerr := membershipSet.AddExpansion(onrKey.ObjectAndRelation, er.TreeNode)
		if aerr != nil {
			devErr, wireErr := DistinguishGraphError(devContext, aerr, v0.DeveloperError_VALIDATION_YAML, 0, 0, onrKey.ObjectRelationString)
			if wireErr != nil {
				return nil, nil, wireErr
			}

			failures = append(failures, devErr)
			continue
		}

		// Compare the terminal subjects found to those specified.
		errs := validateSubjects(onrKey, foundSubjects, expectedSubjects)
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

func validateSubjects(onrKey blocks.ObjectRelation, fs membership.FoundSubjects, expectedSubjects []blocks.ExpectedSubject) []*v0.DeveloperError {
	onr := onrKey.ObjectAndRelation

	var failures []*v0.DeveloperError

	// Verify that every referenced subject is found in the membership.
	encounteredSubjects := map[string]struct{}{}
	for _, expectedSubject := range expectedSubjects {
		subjectWithExceptions := expectedSubject.SubjectWithExceptions
		if subjectWithExceptions == nil {
			failures = append(failures, &v0.DeveloperError{
				Message: fmt.Sprintf("For object and permission/relation `%s`, no expected subject specified in `%s`", tuple.StringONR(onr), expectedSubject.ValidationString),
				Source:  v0.DeveloperError_VALIDATION_YAML,
				Kind:    v0.DeveloperError_MISSING_EXPECTED_RELATIONSHIP,
				Context: string(expectedSubject.ValidationString),
				Line:    uint32(expectedSubject.SourcePosition.LineNumber),
				Column:  uint32(expectedSubject.SourcePosition.ColumnPosition),
			})
			continue
		}

		encounteredSubjects[tuple.StringONR(subjectWithExceptions.Subject)] = struct{}{}

		subject, ok := fs.LookupSubject(subjectWithExceptions.Subject)
		if !ok {
			failures = append(failures, &v0.DeveloperError{
				Message: fmt.Sprintf("For object and permission/relation `%s`, missing expected subject `%s`", tuple.StringONR(onr), tuple.StringONR(subjectWithExceptions.Subject)),
				Source:  v0.DeveloperError_VALIDATION_YAML,
				Kind:    v0.DeveloperError_MISSING_EXPECTED_RELATIONSHIP,
				Context: string(expectedSubject.ValidationString),
				Line:    uint32(expectedSubject.SourcePosition.LineNumber),
				Column:  uint32(expectedSubject.SourcePosition.ColumnPosition),
			})
			continue
		}

		foundRelationships := subject.Relationships()

		// Verify that the relationships are the same.
		expectedONRStrings := tuple.StringsONRs(expectedSubject.Resources)
		foundONRStrings := tuple.StringsONRs(foundRelationships)
		if !cmp.Equal(expectedONRStrings, foundONRStrings) {
			failures = append(failures, &v0.DeveloperError{
				Message: fmt.Sprintf("For object and permission/relation `%s`, found different relationships for subject `%s`: Specified: `%s`, Computed: `%s`",
					tuple.StringONR(onr),
					tuple.StringONR(subjectWithExceptions.Subject),
					strings.Join(wrapRelationships(expectedONRStrings), "/"),
					strings.Join(wrapRelationships(foundONRStrings), "/"),
				),
				Source:  v0.DeveloperError_VALIDATION_YAML,
				Kind:    v0.DeveloperError_MISSING_EXPECTED_RELATIONSHIP,
				Context: string(expectedSubject.ValidationString),
				Line:    uint32(expectedSubject.SourcePosition.LineNumber),
				Column:  uint32(expectedSubject.SourcePosition.ColumnPosition),
			})
		}

		// Verify exclusions are the same, if any.
		foundExcludedSubjects, isWildcard := subject.ExcludedSubjectsFromWildcard()
		expectedExcludedSubjects := subjectWithExceptions.Exceptions
		if isWildcard {
			expectedExcludedONRStrings := tuple.StringsONRs(expectedExcludedSubjects)
			foundExcludedONRStrings := tuple.StringsONRs(foundExcludedSubjects)
			if !cmp.Equal(expectedExcludedONRStrings, foundExcludedONRStrings) {
				failures = append(failures, &v0.DeveloperError{
					Message: fmt.Sprintf("For object and permission/relation `%s`, found different excluded subjects for subject `%s`: Specified: `%s`, Computed: `%s`",
						tuple.StringONR(onr),
						tuple.StringONR(subjectWithExceptions.Subject),
						strings.Join(wrapRelationships(expectedExcludedONRStrings), ", "),
						strings.Join(wrapRelationships(foundExcludedONRStrings), ", "),
					),
					Source:  v0.DeveloperError_VALIDATION_YAML,
					Kind:    v0.DeveloperError_MISSING_EXPECTED_RELATIONSHIP,
					Context: string(expectedSubject.ValidationString),
					Line:    uint32(expectedSubject.SourcePosition.LineNumber),
					Column:  uint32(expectedSubject.SourcePosition.ColumnPosition),
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
					Context: string(expectedSubject.ValidationString),
					Line:    uint32(expectedSubject.SourcePosition.LineNumber),
					Column:  uint32(expectedSubject.SourcePosition.ColumnPosition),
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
				Line:    uint32(onrKey.SourcePosition.LineNumber),
				Column:  uint32(onrKey.SourcePosition.ColumnPosition),
			})
		}
	}

	return failures
}

// GenerateValidation generates the validation block based on a membership set.
func GenerateValidation(membershipSet *membership.Set) (string, error) {
	validationMap := map[string][]string{}
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
		validationMap[onrString] = strs
	}

	contents, err := yaml.Marshal(validationMap)
	if err != nil {
		return "", err
	}

	return string(contents), nil
}
