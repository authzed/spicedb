package development

import (
	"errors"
	"regexp"
	"strconv"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"

	"github.com/authzed/spicedb/pkg/validationfile"
)

// ParseAssertionsYAML parses the YAML form of an assertions block.
func ParseAssertionsYAML(assertionsYaml string) (*validationfile.Assertions, *v0.DeveloperError) {
	assertions, err := validationfile.ParseAssertionsBlock([]byte(assertionsYaml))
	if err != nil {
		var serr validationfile.ErrorWithSource
		if errors.As(err, &serr) {
			return nil, convertSourceError(v0.DeveloperError_ASSERTION, &serr)
		}
	}

	return assertions, convertYamlError(v0.DeveloperError_ASSERTION, err)
}

// ParseExpectedRelationsYAML parses the YAML form of an expected relations block.
func ParseExpectedRelationsYAML(expectedRelationsYaml string) (validationfile.ValidationMap, *v0.DeveloperError) {
	validation, err := validationfile.ParseValidationBlock([]byte(expectedRelationsYaml))
	return validation, convertYamlError(v0.DeveloperError_VALIDATION_YAML, err)
}

var yamlLineRegex = regexp.MustCompile(`line ([0-9]+): (.+)`)

func convertYamlError(source v0.DeveloperError_Source, err error) *v0.DeveloperError {
	if err == nil {
		return nil
	}

	devErr := &v0.DeveloperError{
		Message: err.Error(),
		Kind:    v0.DeveloperError_PARSE_ERROR,
		Source:  source,
		Line:    0,
	}

	pieces := yamlLineRegex.FindStringSubmatch(devErr.Message)
	if len(pieces) == 3 {
		lineNumber, parseErr := strconv.ParseUint(pieces[1], 10, 32)
		if parseErr != nil {
			lineNumber = 0
		}
		devErr.Line = uint32(lineNumber)
		devErr.Message = pieces[2]
	}

	return devErr
}
