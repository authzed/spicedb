package development

import (
	"fmt"

	v1t "github.com/authzed/authzed-go/proto/authzed/api/v1"

	devinterface "github.com/authzed/spicedb/pkg/proto/developer/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/authzed/spicedb/pkg/validationfile/blocks"
)

const maxDispatchDepth = 25

// RunAllAssertions runs all assertions found in the given assertions block against the
// developer context, returning whether any errors occurred.
func RunAllAssertions(devContext *DevContext, assertions *blocks.Assertions) ([]*devinterface.DeveloperError, error) {
	trueFailures, err := runAssertions(devContext, assertions.AssertTrue, v1.ResourceCheckResult_MEMBER, "Expected relation or permission %s to exist")
	if err != nil {
		return nil, err
	}

	caveatedFailures, err := runAssertions(devContext, assertions.AssertCaveated, v1.ResourceCheckResult_CAVEATED_MEMBER, "Expected relation or permission %s to be caveated")
	if err != nil {
		return nil, err
	}

	falseFailures, err := runAssertions(devContext, assertions.AssertFalse, v1.ResourceCheckResult_NOT_MEMBER, "Expected relation or permission %s to not exist")
	if err != nil {
		return nil, err
	}

	failures := append(trueFailures, caveatedFailures...)
	failures = append(failures, falseFailures...)
	return failures, nil
}

func runAssertions(devContext *DevContext, assertions []blocks.Assertion, expected v1.ResourceCheckResult_Membership, fmtString string) ([]*devinterface.DeveloperError, error) {
	var failures []*devinterface.DeveloperError

	for _, assertion := range assertions {
		tpl := tuple.MustFromRelationship[*v1t.ObjectReference, *v1t.SubjectReference, *v1t.ContextualizedCaveat](assertion.Relationship)

		if tpl.Caveat != nil {
			failures = append(failures, &devinterface.DeveloperError{
				Message: fmt.Sprintf("cannot specify a caveat on an assertion: `%s`", assertion.RelationshipWithContextString),
				Source:  devinterface.DeveloperError_ASSERTION,
				Kind:    devinterface.DeveloperError_UNKNOWN_RELATION,
				Context: assertion.RelationshipWithContextString,
				Line:    uint32(assertion.SourcePosition.LineNumber),
				Column:  uint32(assertion.SourcePosition.ColumnPosition),
			})
			continue
		}

		cr, err := RunCheck(devContext, tpl.ResourceAndRelation, tpl.Subject, assertion.CaveatContext)
		if err != nil {
			devErr, wireErr := DistinguishGraphError(
				devContext,
				err,
				devinterface.DeveloperError_ASSERTION,
				uint32(assertion.SourcePosition.LineNumber),
				uint32(assertion.SourcePosition.ColumnPosition),
				assertion.RelationshipWithContextString,
			)
			if wireErr != nil {
				return nil, wireErr
			}
			if devErr != nil {
				failures = append(failures, devErr)
			}
		} else if cr.Permissionship != expected {
			failures = append(failures, &devinterface.DeveloperError{
				Message:                       fmt.Sprintf(fmtString, assertion.RelationshipWithContextString),
				Source:                        devinterface.DeveloperError_ASSERTION,
				Kind:                          devinterface.DeveloperError_ASSERTION_FAILED,
				Context:                       assertion.RelationshipWithContextString,
				Line:                          uint32(assertion.SourcePosition.LineNumber),
				Column:                        uint32(assertion.SourcePosition.ColumnPosition),
				CheckDebugInformation:         cr.DispatchDebugInfo,
				CheckResolvedDebugInformation: cr.V1DebugInfo,
			})
		}
	}

	return failures, nil
}
