package development

import (
	"testing"

	"github.com/stretchr/testify/require"

	devinterface "github.com/authzed/spicedb/pkg/proto/developer/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

func TestParseAssertionsYAML(t *testing.T) {
	// Test valid assertions YAML
	validYAML := `assertTrue:
  - document:doc1#viewer@user:user1
assertFalse:
  - document:doc1#editor@user:user2`

	assertions, devErr := ParseAssertionsYAML(validYAML)
	require.Nil(t, devErr)
	require.NotNil(t, assertions)
	require.Len(t, assertions.AssertTrue, 1)
	require.Len(t, assertions.AssertFalse, 1)
}

func TestParseAssertionsYAMLInvalid(t *testing.T) {
	// Test invalid YAML syntax
	invalidYAML := `invalid: yaml: syntax: [[[`

	assertions, devErr := ParseAssertionsYAML(invalidYAML)
	require.Nil(t, assertions)
	require.NotNil(t, devErr)
	require.Equal(t, devinterface.DeveloperError_ASSERTION, devErr.Source)
	require.Equal(t, devinterface.DeveloperError_PARSE_ERROR, devErr.Kind)
}

func TestParseAssertionsYAMLWithInvalidRelationship(t *testing.T) {
	// Test YAML with invalid relationship syntax
	invalidRelYAML := `assertTrue:
  - invalid::relationship::syntax`

	assertions, devErr := ParseAssertionsYAML(invalidRelYAML)
	require.Nil(t, assertions)
	require.NotNil(t, devErr)
	require.Equal(t, devinterface.DeveloperError_ASSERTION, devErr.Source)
	require.Equal(t, devinterface.DeveloperError_PARSE_ERROR, devErr.Kind)
}

func TestParseExpectedRelationsYAMLValid(t *testing.T) {
	// Test valid expected relations YAML
	validYAML := `document:doc1#viewer:
  - user:user1`

	expectedRels, devErr := ParseExpectedRelationsYAML(validYAML)
	require.Nil(t, devErr)
	require.NotNil(t, expectedRels)
	require.Len(t, expectedRels.ValidationMap, 1)
}

func TestParseExpectedRelationsYAMLInvalid(t *testing.T) {
	// Test invalid YAML syntax
	invalidYAML := `invalid: yaml: syntax: [[[`

	expectedRels, devErr := ParseExpectedRelationsYAML(invalidYAML)
	require.Nil(t, expectedRels)
	require.NotNil(t, devErr)
	require.Equal(t, devinterface.DeveloperError_VALIDATION_YAML, devErr.Source)
	require.Equal(t, devinterface.DeveloperError_PARSE_ERROR, devErr.Kind)
}

func TestConvertError(t *testing.T) {
	// Test with nil error
	devErr := convertError(devinterface.DeveloperError_ASSERTION, nil)
	require.Nil(t, devErr)

	// Test with regular error
	err := &testError{message: "test error"}
	devErr = convertError(devinterface.DeveloperError_ASSERTION, err)
	require.NotNil(t, devErr)
	require.Equal(t, "test error", devErr.Message)
	require.Equal(t, devinterface.DeveloperError_ASSERTION, devErr.Source)
	require.Equal(t, devinterface.DeveloperError_PARSE_ERROR, devErr.Kind)
	require.Equal(t, uint32(0), devErr.Line)
}

func TestConvertSourceError(t *testing.T) {
	sourceErr := spiceerrors.NewWithSourceError(
		&testError{message: "source error"},
		"some source code",
		5,
		10,
	)

	devErr := convertSourceError(devinterface.DeveloperError_VALIDATION_YAML, sourceErr)
	require.NotNil(t, devErr)
	require.Equal(t, "source error", devErr.Message)
	require.Equal(t, devinterface.DeveloperError_VALIDATION_YAML, devErr.Source)
	require.Equal(t, devinterface.DeveloperError_PARSE_ERROR, devErr.Kind)
	require.Equal(t, uint32(5), devErr.Line)
	require.Equal(t, uint32(10), devErr.Column)
	require.Equal(t, "some source code", devErr.Context)
}

// Helper type for testing
type testError struct {
	message string
}

func (e *testError) Error() string {
	return e.message
}
