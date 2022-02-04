package development

import (
	"context"
	"fmt"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"

	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/authzed/spicedb/pkg/validationfile"
)

const maxDispatchDepth = 25

// RunAllAssertions runs all assertions found in the given assertions block against the
// developer context, returning whether any errors occurred.
func RunAllAssertions(ctx context.Context, devContext *DevContext, assertions *validationfile.Assertions) (*DeveloperErrors, error) {
	// Parse out the assertions from the block.
	assertTrueRelationships, aerr := assertions.AssertTrueRelationships()
	if aerr != nil {
		return &DeveloperErrors{
			InputErrors: []*v0.DeveloperError{
				convertSourceError(v0.DeveloperError_ASSERTION, aerr),
			},
		}, nil
	}

	assertFalseRelationships, aerr := assertions.AssertFalseRelationships()
	if aerr != nil {
		return &DeveloperErrors{
			InputErrors: []*v0.DeveloperError{
				convertSourceError(v0.DeveloperError_ASSERTION, aerr),
			},
		}, nil
	}

	// Run assertions.
	trueFailures, err := runAssertions(ctx, devContext, assertTrueRelationships, true, "Expected relation or permission %s to exist")
	if err != nil {
		return nil, err
	}

	falseFailures, err := runAssertions(ctx, devContext, assertFalseRelationships, false, "Expected relation or permission %s to not exist")
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

func runAssertions(ctx context.Context, devContext *DevContext, assertions []validationfile.ParsedAssertion, expected bool, fmtString string) ([]*v0.DeveloperError, error) {
	var failures []*v0.DeveloperError
	for _, assertion := range assertions {
		cr, err := RunCheck(ctx, devContext, assertion.Relationship.ObjectAndRelation, assertion.Relationship.User.GetUserset())
		if err != nil {
			devErr, wireErr := DistinguishGraphError(
				ctx,
				err,
				v0.DeveloperError_ASSERTION,
				assertion.LineNumber,
				assertion.ColumnPosition,
				tuple.String(assertion.Relationship),
			)
			if wireErr != nil {
				return nil, wireErr
			}
			if devErr != nil {
				failures = append(failures, devErr)
			}
		} else if (cr == v1.DispatchCheckResponse_MEMBER) != expected {
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

func convertSourceError(source v0.DeveloperError_Source, err *validationfile.ErrorWithSource) *v0.DeveloperError {
	return &v0.DeveloperError{
		Message: err.Error(),
		Kind:    v0.DeveloperError_PARSE_ERROR,
		Source:  source,
		Line:    err.LineNumber,
		Column:  err.ColumnPosition,
		Context: err.Source,
	}
}
