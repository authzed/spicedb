package validationfile

import (
	"fmt"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	yamlv3 "gopkg.in/yaml.v3"
)

// DecodeValidationFile decodes the validation file as found in the contents bytes
// and returns it.
func DecodeValidationFile(contents []byte) (*FullyParsedValidationFile, error) {
	p := ValidationFile{}
	err := yamlv3.Unmarshal(contents, &p)
	if err != nil {
		return nil, err
	}

	f, _, err := ParsedValidationFile(p)
	return f, err
}

// ValidationFile represents the contents of a YAML validation file, as
// exported by the playground.
//
// NOTE: This is exported from the module solely so that tooling can decode into
// it, maintain line and column position information (as yamlv3.Node) and then
// call the various Parse*Block functions if necessary.
//
// Most callers should use DecodeValidationFile.
type ValidationFile struct {
	// Schema is the defined schema, in DSL format. Optional if at least one
	// NamespaceConfig is specified.
	Schema string `yaml:"schema"`

	// Relationships are the validation relationships, as a single string of
	// newline separated tuple string syntax.
	Relationships yamlv3.Node `yaml:"relationships"`

	// Assertions are the (optional) assertions for the validation file.
	Assertions yamlv3.Node `yaml:"assertions"`

	// Validation is the expected relations block.
	Validation yamlv3.Node `yaml:"validation"`

	// NamespaceConfigs are the namespace configuration protos, in text format.
	// Deprecated: only for internal use. Use Schema.
	NamespaceConfigs []string `yaml:"namespace_configs"`

	// ValidationTuples are the validation tuples, in tuple string syntax.
	// Deprecated: only for internal use. Use Relationships.
	ValidationTuples []string `yaml:"validation_tuples"`
}

// FullyParsedValidationFile is a validation file that has been fully parsed.
type FullyParsedValidationFile struct {
	// Schema is the schema.
	Schema string

	// Relationships are the relationships specified in the validation file.
	Relationships []*v1.Relationship

	// Assertions are the assertions defined in the validation file. May be nil
	// if no assertions are defined.
	Assertions *Assertions

	// ExpectedRelations is the map of expected relations.
	ExpectedRelations ValidationMap

	// NamespaceConfigs are the namespace configuration protos, in text format.
	// Deprecated: only for internal use. Use Schema.
	NamespaceConfigs []string `yaml:"namespace_configs"`

	// ValidationTuples are the validation tuples, in tuple string syntax.
	// Deprecated: only for internal use. Use Relationships.
	ValidationTuples []string `yaml:"validation_tuples"`
}

// ErrorBlockKind is the kind of block that raised the error.
type ErrorBlockKind = int

const (
	// NoBlockError indicates no error occurred.
	NoBlockError ErrorBlockKind = iota

	// RelationshipsBlockError is an error in the `relationships` block.
	RelationshipsBlockError

	// AssertionsBlockError is an error in the `assertions` block.
	AssertionsBlockError

	// ExpectedRelationsBlockError is an error in the `validation` block.
	ExpectedRelationsBlockError
)

// ParsedValidationFile parses the YAML-encoded ValidationFile. If an error occurs,
// it likely is an ErrorWithSource, so callers should check that.
func ParsedValidationFile(p ValidationFile) (*FullyParsedValidationFile, ErrorBlockKind, error) {
	// Parse the test relationships.
	var relationships []*v1.Relationship
	if p.Relationships.Kind != 0 && p.Relationships.Kind != yamlv3.ScalarNode {
		return nil, RelationshipsBlockError, ErrorWithSource{
			fmt.Errorf("invalid YAML node for relationships"),
			"relationships",
			uint32(p.Relationships.Line),
			uint32(p.Relationships.Column),
		}
	}

	if p.Relationships.Kind != 0 {
		r, err := DecodeRelationshipsBlock(p.Relationships.Value, p.Relationships.Line+1) // +1 for the key
		if err != nil {
			return nil, RelationshipsBlockError, err
		}
		relationships = r
	}

	// Parse the assertions.
	var assertions *Assertions
	if p.Assertions.Kind != 0 && len(p.Assertions.Content) > 0 {
		parsed, err := DecodeAssertionsBlock(&p.Assertions)
		if err != nil {
			return nil, AssertionsBlockError, err
		}

		assertions = parsed
	}

	// Parse the expected relations.
	expectedRelations, err := DecodeValidationBlock(p.Validation)
	if err != nil {
		return nil, ExpectedRelationsBlockError, err
	}

	return &FullyParsedValidationFile{
		Schema:            p.Schema,
		Relationships:     relationships,
		Assertions:        assertions,
		ExpectedRelations: expectedRelations,
		NamespaceConfigs:  p.NamespaceConfigs,
		ValidationTuples:  p.ValidationTuples,
	}, NoBlockError, nil
}
