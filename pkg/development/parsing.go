package development

import (
	devinterface "github.com/authzed/spicedb/pkg/proto/developer/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/validationfile"
	"github.com/authzed/spicedb/pkg/validationfile/blocks"
)

// ParseAssertionsYAML parses the YAML form of an assertions block.
func ParseAssertionsYAML(assertionsYaml string) (*blocks.Assertions, *devinterface.DeveloperError) {
	assertions, err := validationfile.ParseAssertionsBlock([]byte(assertionsYaml))
	if err != nil {
		serr, ok := spiceerrors.AsErrorWithSource(err)
		if ok {
			return nil, convertSourceError(devinterface.DeveloperError_ASSERTION, serr)
		}
	}

	return assertions, convertError(devinterface.DeveloperError_ASSERTION, err)
}

// ParseExpectedRelationsYAML parses the YAML form of an expected relations block.
func ParseExpectedRelationsYAML(expectedRelationsYaml string) (*blocks.ParsedExpectedRelations, *devinterface.DeveloperError) {
	block, err := validationfile.ParseExpectedRelationsBlock([]byte(expectedRelationsYaml))
	if err != nil {
		serr, ok := spiceerrors.AsErrorWithSource(err)
		if ok {
			return nil, convertSourceError(devinterface.DeveloperError_VALIDATION_YAML, serr)
		}
	}
	return block, convertError(devinterface.DeveloperError_VALIDATION_YAML, err)
}

func convertError(source devinterface.DeveloperError_Source, err error) *devinterface.DeveloperError {
	if err == nil {
		return nil
	}

	return &devinterface.DeveloperError{
		Message: err.Error(),
		Kind:    devinterface.DeveloperError_PARSE_ERROR,
		Source:  source,
		Line:    0,
	}
}

func convertSourceError(source devinterface.DeveloperError_Source, err *spiceerrors.ErrorWithSource) *devinterface.DeveloperError {
	return &devinterface.DeveloperError{
		Message: err.Error(),
		Kind:    devinterface.DeveloperError_PARSE_ERROR,
		Source:  source,
		Line:    uint32(err.LineNumber),
		Column:  uint32(err.ColumnPosition),
		Context: err.SourceCodeString,
	}
}
