package development

import (
	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"

	"github.com/authzed/spicedb/pkg/commonerrors"
	"github.com/authzed/spicedb/pkg/validationfile"
	"github.com/authzed/spicedb/pkg/validationfile/blocks"
)

// ParseAssertionsYAML parses the YAML form of an assertions block.
func ParseAssertionsYAML(assertionsYaml string) (*blocks.Assertions, *v0.DeveloperError) {
	assertions, err := validationfile.ParseAssertionsBlock([]byte(assertionsYaml))
	if err != nil {
		serr, ok := commonerrors.AsErrorWithSource(err)
		if ok {
			return nil, convertSourceError(v0.DeveloperError_ASSERTION, serr)
		}
	}

	return assertions, convertError(v0.DeveloperError_ASSERTION, err)
}

// ParseExpectedRelationsYAML parses the YAML form of an expected relations block.
func ParseExpectedRelationsYAML(expectedRelationsYaml string) (*blocks.ParsedExpectedRelations, *v0.DeveloperError) {
	block, err := validationfile.ParseExpectedRelationsBlock([]byte(expectedRelationsYaml))
	if err != nil {
		serr, ok := commonerrors.AsErrorWithSource(err)
		if ok {
			return nil, convertSourceError(v0.DeveloperError_VALIDATION_YAML, serr)
		}
	}
	return block, convertError(v0.DeveloperError_VALIDATION_YAML, err)
}

func convertError(source v0.DeveloperError_Source, err error) *v0.DeveloperError {
	if err == nil {
		return nil
	}

	return &v0.DeveloperError{
		Message: err.Error(),
		Kind:    v0.DeveloperError_PARSE_ERROR,
		Source:  source,
		Line:    0,
	}
}

func convertSourceError(source v0.DeveloperError_Source, err *commonerrors.ErrorWithSource) *v0.DeveloperError {
	return &v0.DeveloperError{
		Message: err.Error(),
		Kind:    v0.DeveloperError_PARSE_ERROR,
		Source:  source,
		Line:    uint32(err.LineNumber),
		Column:  uint32(err.ColumnPosition),
		Context: err.SourceCodeString,
	}
}
