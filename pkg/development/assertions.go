package development

import (
	"fmt"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"

	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/authzed/spicedb/pkg/validationfile/blocks"
)

const maxDispatchDepth = 25

// RunAllAssertions runs all assertions found in the given assertions block against the
// developer context, returning whether any errors occurred.
func RunAllAssertions(devContext *DevContext, assertions *blocks.Assertions) (*DeveloperErrors, error) {
	trueFailures, err := runAssertions(devContext, assertions.AssertTrue, true, "Expected relation or permission %s to exist")
	if err != nil {
		return nil, err
	}

	falseFailures, err := runAssertions(devContext, assertions.AssertFalse, false, "Expected relation or permission %s to not exist")
	if err != nil {
		return nil, err
	}

	failures := append(trueFailures, falseFailures...)
	if len(failures) > 0 {
		return &DeveloperErrors{
			ValidationErrors: failures,
		}, nil
	}

	return nil, nil
}

func runAssertions(devContext *DevContext, assertions []blocks.Assertion, expected bool, fmtString string) ([]*v0.DeveloperError, error) {
	var failures []*v0.DeveloperError
	for _, assertion := range assertions {
		tpl := tuple.MustFromRelationship(assertion.Relationship)
		cr, err := RunCheck(devContext, tpl.ObjectAndRelation, tpl.User.GetUserset())
		if err != nil {
			devErr, wireErr := DistinguishGraphError(
				devContext,
				err,
				v0.DeveloperError_ASSERTION,
				uint32(assertion.SourcePosition.LineNumber),
				uint32(assertion.SourcePosition.ColumnPosition),
				tuple.String(tpl),
			)
			if wireErr != nil {
				return nil, wireErr
			}
			if devErr != nil {
				failures = append(failures, devErr)
			}
		} else if (cr == v1.DispatchCheckResponse_MEMBER) != expected {
			failures = append(failures, &v0.DeveloperError{
				Message: fmt.Sprintf(fmtString, tuple.String(tpl)),
				Source:  v0.DeveloperError_ASSERTION,
				Kind:    v0.DeveloperError_ASSERTION_FAILED,
				Context: tuple.String(tpl),
				Line:    uint32(assertion.SourcePosition.LineNumber),
				Column:  uint32(assertion.SourcePosition.ColumnPosition),
			})
		}
	}

	return failures, nil
}
