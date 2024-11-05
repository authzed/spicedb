package development

import (
	"fmt"
	"sort"
	"strings"

	"github.com/ccoveille/go-safecast"
	"github.com/google/go-cmp/cmp"
	yaml "gopkg.in/yaml.v2"

	"github.com/authzed/spicedb/internal/developmentmembership"
	log "github.com/authzed/spicedb/internal/logging"
	devinterface "github.com/authzed/spicedb/pkg/proto/developer/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/authzed/spicedb/pkg/validationfile/blocks"
)

// RunValidation runs the parsed validation block against the data in the dev context.
func RunValidation(devContext *DevContext, validation *blocks.ParsedExpectedRelations) (*developmentmembership.Set, []*devinterface.DeveloperError, error) {
	var failures []*devinterface.DeveloperError
	membershipSet := developmentmembership.NewMembershipSet()
	ctx := devContext.Ctx

	for onrKey, expectedSubjects := range validation.ValidationMap {
		// Run a full recursive expansion over the ONR.
		er, derr := devContext.Dispatcher.DispatchExpand(ctx, &v1.DispatchExpandRequest{
			ResourceAndRelation: onrKey.ObjectAndRelation.ToCoreONR(),
			Metadata: &v1.ResolverMeta{
				AtRevision:     devContext.Revision.String(),
				DepthRemaining: maxDispatchDepth,
				TraversalBloom: v1.MustNewTraversalBloomFilter(uint(maxDispatchDepth)),
			},
			ExpansionMode: v1.DispatchExpandRequest_RECURSIVE,
		})
		if derr != nil {
			devErr, wireErr := DistinguishGraphError(devContext, derr, devinterface.DeveloperError_VALIDATION_YAML, 0, 0, onrKey.ObjectRelationString)
			if wireErr != nil {
				return nil, nil, wireErr
			}

			failures = append(failures, devErr)
			continue
		}

		// Add the ONR and its expansion to the membership set.
		foundSubjects, _, aerr := membershipSet.AddExpansion(onrKey.ObjectAndRelation, er.TreeNode)
		if aerr != nil {
			devErr, wireErr := DistinguishGraphError(devContext, aerr, devinterface.DeveloperError_VALIDATION_YAML, 0, 0, onrKey.ObjectRelationString)
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
		return membershipSet, failures, nil
	}

	return membershipSet, nil, nil
}

func wrapResources(onrStrings []string) []string {
	wrapped := make([]string, 0, len(onrStrings))
	for _, str := range onrStrings {
		wrapped = append(wrapped, "<"+str+">")
	}

	// Sort to ensure stability.
	sort.Strings(wrapped)
	return wrapped
}

func validateSubjects(onrKey blocks.ObjectRelation, fs developmentmembership.FoundSubjects, expectedSubjects []blocks.ExpectedSubject) []*devinterface.DeveloperError {
	onr := onrKey.ObjectAndRelation

	var failures []*devinterface.DeveloperError

	// Verify that every referenced subject is found in the membership.
	encounteredSubjects := map[string]struct{}{}
	for _, expectedSubject := range expectedSubjects {
		subjectWithExceptions := expectedSubject.SubjectWithExceptions
		// NOTE: zeroes are fine here on failure.
		lineNumber, err := safecast.ToUint32(expectedSubject.SourcePosition.LineNumber)
		if err != nil {
			log.Err(err).Msg("could not cast lineNumber to uint32")
		}
		columnPosition, err := safecast.ToUint32(expectedSubject.SourcePosition.ColumnPosition)
		if err != nil {
			log.Err(err).Msg("could not cast columnPosition to uint32")
		}
		if subjectWithExceptions == nil {
			failures = append(failures, &devinterface.DeveloperError{
				Message: fmt.Sprintf("For object and permission/relation `%s`, no expected subject specified in `%s`", tuple.StringONR(onr), expectedSubject.ValidationString),
				Source:  devinterface.DeveloperError_VALIDATION_YAML,
				Kind:    devinterface.DeveloperError_MISSING_EXPECTED_RELATIONSHIP,
				Context: string(expectedSubject.ValidationString),
				Line:    lineNumber,
				Column:  columnPosition,
			})
			continue
		}

		encounteredSubjects[tuple.StringONR(subjectWithExceptions.Subject.Subject)] = struct{}{}

		subject, ok := fs.LookupSubject(subjectWithExceptions.Subject.Subject)
		if !ok {
			failures = append(failures, &devinterface.DeveloperError{
				Message: fmt.Sprintf("For object and permission/relation `%s`, missing expected subject `%s`", tuple.StringONR(onr), tuple.StringONR(subjectWithExceptions.Subject.Subject)),
				Source:  devinterface.DeveloperError_VALIDATION_YAML,
				Kind:    devinterface.DeveloperError_MISSING_EXPECTED_RELATIONSHIP,
				Context: string(expectedSubject.ValidationString),
				Line:    lineNumber,
				Column:  columnPosition,
			})
			continue
		}

		// Verify that the relationships are the same.
		foundParentResources := subject.ParentResources()
		expectedONRStrings := tuple.StringsONRs(expectedSubject.Resources)
		foundONRStrings := tuple.StringsONRs(foundParentResources)
		if !cmp.Equal(expectedONRStrings, foundONRStrings) {
			failures = append(failures, &devinterface.DeveloperError{
				Message: fmt.Sprintf("For object and permission/relation `%s`, found different relationships for subject `%s`: Specified: `%s`, Computed: `%s`",
					tuple.StringONR(onr),
					tuple.StringONR(subjectWithExceptions.Subject.Subject),
					strings.Join(wrapResources(expectedONRStrings), "/"),
					strings.Join(wrapResources(foundONRStrings), "/"),
				),
				Source:  devinterface.DeveloperError_VALIDATION_YAML,
				Kind:    devinterface.DeveloperError_MISSING_EXPECTED_RELATIONSHIP,
				Context: string(expectedSubject.ValidationString),
				Line:    lineNumber,
				Column:  columnPosition,
			})
		}

		// Verify exclusions are the same, if any.
		foundExcludedSubjects, isWildcard := subject.ExcludedSubjectsFromWildcard()
		expectedExcludedSubjects := subjectWithExceptions.Exceptions
		if isWildcard {
			expectedExcludedStrings := toExpectedRelationshipsStrings(expectedExcludedSubjects)
			foundExcludedONRStrings := toFoundRelationshipsStrings(foundExcludedSubjects)

			sort.Strings(expectedExcludedStrings)
			sort.Strings(foundExcludedONRStrings)

			if !cmp.Equal(expectedExcludedStrings, foundExcludedONRStrings) {
				failures = append(failures, &devinterface.DeveloperError{
					Message: fmt.Sprintf("For object and permission/relation `%s`, found different excluded subjects for subject `%s`: Specified: `%s`, Computed: `%s`",
						tuple.StringONR(onr),
						tuple.StringONR(subjectWithExceptions.Subject.Subject),
						strings.Join(wrapResources(expectedExcludedStrings), ", "),
						strings.Join(wrapResources(foundExcludedONRStrings), ", "),
					),
					Source:  devinterface.DeveloperError_VALIDATION_YAML,
					Kind:    devinterface.DeveloperError_MISSING_EXPECTED_RELATIONSHIP,
					Context: string(expectedSubject.ValidationString),
					Line:    lineNumber,
					Column:  columnPosition,
				})
			}
		} else {
			if len(expectedExcludedSubjects) > 0 {
				failures = append(failures, &devinterface.DeveloperError{
					Message: fmt.Sprintf("For object and permission/relation `%s`, found unexpected excluded subjects",
						tuple.StringONR(onr),
					),
					Source:  devinterface.DeveloperError_VALIDATION_YAML,
					Kind:    devinterface.DeveloperError_EXTRA_RELATIONSHIP_FOUND,
					Context: string(expectedSubject.ValidationString),
					Line:    lineNumber,
					Column:  columnPosition,
				})
			}
		}

		// Verify caveats.
		if (subject.GetCaveatExpression() != nil) != subjectWithExceptions.Subject.IsCaveated {
			failures = append(failures, &devinterface.DeveloperError{
				Message: fmt.Sprintf("For object and permission/relation `%s`, found caveat mismatch",
					tuple.StringONR(onr),
				),
				Source:  devinterface.DeveloperError_VALIDATION_YAML,
				Kind:    devinterface.DeveloperError_MISSING_EXPECTED_RELATIONSHIP,
				Context: string(expectedSubject.ValidationString),
				Line:    lineNumber,
				Column:  columnPosition,
			})
		}
	}

	// Verify that every subject found was referenced.
	for _, foundSubject := range fs.ListFound() {
		_, ok := encounteredSubjects[tuple.StringONR(foundSubject.Subject())]
		if !ok {
			onrLineNumber, err := safecast.ToUint32(onrKey.SourcePosition.LineNumber)
			if err != nil {
				log.Err(err).Msg("could not cast lineNumber to uint32")
			}
			onrColumnPosition, err := safecast.ToUint32(onrKey.SourcePosition.ColumnPosition)
			if err != nil {
				log.Err(err).Msg("could not cast columnPosition to uint32")
			}
			failures = append(failures, &devinterface.DeveloperError{
				Message: fmt.Sprintf("For object and permission/relation `%s`, subject `%s` found but not listed in expected subjects",
					tuple.StringONR(onr),
					tuple.StringONR(foundSubject.Subject()),
				),
				Source:  devinterface.DeveloperError_VALIDATION_YAML,
				Kind:    devinterface.DeveloperError_EXTRA_RELATIONSHIP_FOUND,
				Context: tuple.StringONR(onr),
				Line:    onrLineNumber,
				Column:  onrColumnPosition,
			})
		}
	}

	return failures
}

// GenerateValidation generates the validation block based on a membership set.
func GenerateValidation(membershipSet *developmentmembership.Set) (string, error) {
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
					strings.Join(wrapResources(tuple.StringsONRs(fs.ParentResources())), "/"),
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

func toExpectedRelationshipsStrings(subs []blocks.SubjectAndCaveat) []string {
	mapped := make([]string, 0, len(subs))
	for _, sub := range subs {
		if sub.IsCaveated {
			mapped = append(mapped, tuple.StringONR(sub.Subject)+"[...]")
		} else {
			mapped = append(mapped, tuple.StringONR(sub.Subject))
		}
	}
	return mapped
}

func toFoundRelationshipsStrings(subs []developmentmembership.FoundSubject) []string {
	mapped := make([]string, 0, len(subs))
	for _, sub := range subs {
		mapped = append(mapped, sub.ToValidationString())
	}
	return mapped
}
