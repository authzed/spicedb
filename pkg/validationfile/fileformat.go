package validationfile

import (
	yamlv3 "gopkg.in/yaml.v3"

	"github.com/authzed/spicedb/pkg/validationfile/blocks"
)

// DecodeValidationFile decodes the validation file as found in the contents bytes
// and returns it.
func DecodeValidationFile(contents []byte) (*ValidationFile, error) {
	p := ValidationFile{}
	err := yamlv3.Unmarshal(contents, &p)
	if err != nil {
		return nil, err
	}
	return &p, nil
}

// ValidationFile is a structural representation of the validation file format.
type ValidationFile struct {
	// Schema is the schema.
	Schema blocks.ParsedSchema `yaml:"schema"`

	// Relationships are the relationships specified in the validation file.
	Relationships blocks.ParsedRelationships `yaml:"relationships"`

	// Assertions are the assertions defined in the validation file. May be nil
	// if no assertions are defined.
	Assertions blocks.Assertions `yaml:"assertions"`

	// ExpectedRelations is the map of expected relations.
	ExpectedRelations blocks.ParsedExpectedRelations `yaml:"validation"`

	// NamespaceConfigs are the namespace configuration protos, in text format.
	// Deprecated: only for internal use. Use `schema`.
	NamespaceConfigs []string `yaml:"namespace_configs"`

	// ValidationTuples are the validation tuples, in tuple string syntax.
	// Deprecated: only for internal use. Use `relationships`.
	ValidationTuples []string `yaml:"validation_tuples"`

	// Schema file represents the path specified for the schema file
	SchemaFile string `yaml:"schemaFile"`
}

// ParseAssertionsBlock parses the given contents as an assertions block.
func ParseAssertionsBlock(contents []byte) (*blocks.Assertions, error) {
	return blocks.ParseAssertionsBlock(contents)
}

// ParseExpectedRelationsBlock parses the given contents as an expected relations block.
func ParseExpectedRelationsBlock(contents []byte) (*blocks.ParsedExpectedRelations, error) {
	return blocks.ParseExpectedRelationsBlock(contents)
}
