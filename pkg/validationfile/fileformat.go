package validationfile

import "gopkg.in/yaml.v2"

// ValidationFile represents the contents of a YAML validation file, as
// exported by the playground.
type ValidationFile struct {
	// NamespaceConfigs are the namespace configuration protos, in text format.
	NamespaceConfigs []string `yaml:"namespace_configs"`

	// ValidationTuples are the validation tuples, in tuple string syntax.
	ValidationTuples []string `yaml:"validation_tuples"`
}

// ParseValidationFile attempts to parse the given contents as a YAML
// validation file.
func ParseValidationFile(contents []byte) (ValidationFile, error) {
	file := ValidationFile{}
	err := yaml.Unmarshal(contents, &file)
	return file, err
}
