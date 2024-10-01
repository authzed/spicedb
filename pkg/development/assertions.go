package development

import (
	"fmt"

	"github.com/ccoveille/go-safecast"

	log "github.com/authzed/spicedb/internal/logging"
	devinterface "github.com/authzed/spicedb/pkg/proto/developer/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
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
		// NOTE: zeroes are fine here to mean "unknown"
		lineNumber, err := safecast.ToUint32(assertion.SourcePosition.LineNumber)
		if err != nil {
			log.Err(err).Msg("could not cast lineNumber to uint32")
		}
		columnPosition, err := safecast.ToUint32(assertion.SourcePosition.ColumnPosition)
		if err != nil {
			log.Err(err).Msg("could not cast columnPosition to uint32")
		}

		rel := assertion.Relationship
		if rel.OptionalCaveat != nil {
			failures = append(failures, &devinterface.DeveloperError{
				Message: fmt.Sprintf("cannot specify a caveat on an assertion: `%s`", assertion.RelationshipWithContextString),
				Source:  devinterface.DeveloperError_ASSERTION,
				Kind:    devinterface.DeveloperError_UNKNOWN_RELATION,
				Context: assertion.RelationshipWithContextString,
				Line:    lineNumber,
				Column:  columnPosition,
			})
			continue
		}

		cr, err := RunCheck(devContext, rel.Resource, rel.Subject, assertion.CaveatContext)
		if err != nil {
			devErr, wireErr := DistinguishGraphError(
				devContext,
				err,
				devinterface.DeveloperError_ASSERTION,
				lineNumber,
				columnPosition,
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
				Line:                          lineNumber,
				Column:                        columnPosition,
				CheckDebugInformation:         cr.DispatchDebugInfo,
				CheckResolvedDebugInformation: cr.V1DebugInfo,
			})
		}
	}

	return failures, nil
}
