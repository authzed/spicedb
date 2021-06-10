package validationfile

import (
	"fmt"
	"regexp"

	"gopkg.in/yaml.v2"

	api "github.com/authzed/spicedb/pkg/REDACTEDapi/api"
	"github.com/authzed/spicedb/pkg/tuple"
)

// ParseValidationBlock attempts to parse the given contents as a YAML
// validation block.
func ParseValidationBlock(contents []byte) (ValidationMap, error) {
	block := ValidationMap{}
	err := yaml.Unmarshal(contents, &block)
	return block, err
}

// ParseAssertionsBlock attempts to parse the given contents as a YAML assertions block.
func ParseAssertionsBlock(contents []byte) (Assertions, error) {
	block := Assertions{}
	err := yaml.Unmarshal(contents, &block)
	return block, err
}

// ParseValidationFile attempts to parse the given contents as a YAML
// validation file.
func ParseValidationFile(contents []byte) (ValidationFile, error) {
	file := ValidationFile{}
	err := yaml.Unmarshal(contents, &file)
	return file, err
}

// ValidationFile represents the contents of a YAML validation file, as
// exported by the playground.
type ValidationFile struct {
	// NamespaceConfigs are the namespace configuration protos, in text format.
	NamespaceConfigs []string `yaml:"namespace_configs"`

	// RelationTuples are the preloaded relation tuples, in tuple string syntax.
	RelationTuples []string `yaml:"relation_tuples"`

	// ValidationTuples are the validation tuples, in tuple string syntax.
	ValidationTuples []string `yaml:"validation_tuples"`

	// Validation holds the validation data, as a map from Object Relation to list of strings containing
	// the valid Subjects and their Relations.
	Validation *ValidationMap `yaml:"validation"`
}

// ValidationMap is a map from an Object Relation (as a Relationship) to the validation strings containing
// the Subjects for that Object Relation.
type ValidationMap map[ObjectRelationString][]ValidationString

func (vm ValidationMap) AsYAML() (string, error) {
	data, err := yaml.Marshal(vm)
	if err != nil {
		return "", err
	}

	return string(data), nil
}

// ObjectRelationString represents an ONR defined as a string in the key for the ValidationMap.
type ObjectRelationString string

func (ors ObjectRelationString) ONR() (*api.ObjectAndRelation, *ErrorWithSource) {
	parsed := tuple.ScanONR(string(ors))
	if parsed == nil {
		return nil, &ErrorWithSource{fmt.Errorf("could not parse %s", ors), string(ors)}
	}
	return parsed, nil
}

var vsSubjectRegex = regexp.MustCompile(`(.*?)\[(?P<user_str>.*)\](.*?)`)
var vsObjectAndRelationRegex = regexp.MustCompile(`(.*?)<(?P<onr_str>[^\>]+)>(.*?)`)

// ValidationString holds a validation string containing a Subject and one or more Relations to
// the parent Object.
// Example: `[tenant/user:someuser#...] is <tenant/document:example#viewer>`
type ValidationString string

// SubjectString returns the subject contained in the ValidationString, if any.
func (vs ValidationString) SubjectString() (string, bool) {
	result := vsSubjectRegex.FindStringSubmatch(string(vs))
	if len(result) != 4 {
		return "", false
	}

	return result[2], true
}

// Subject returns the subject contained in the ValidationString, if any. If none, returns nil.
func (vs ValidationString) Subject() (*api.ObjectAndRelation, *ErrorWithSource) {
	subjectStr, ok := vs.SubjectString()
	if !ok {
		return nil, nil
	}

	found := tuple.ScanONR(subjectStr)
	if found == nil {
		return nil, &ErrorWithSource{fmt.Errorf("invalid subject: %s", subjectStr), subjectStr}
	}
	return found, nil
}

// ONRStrings returns the ONRs contained in the ValidationString, if any.
func (vs ValidationString) ONRStrings() []string {
	results := vsObjectAndRelationRegex.FindAllStringSubmatch(string(vs), -1)
	onrStrings := []string{}
	for _, result := range results {
		onrStrings = append(onrStrings, result[2])
	}
	return onrStrings
}

// ONRS returns the subject ONRs in the ValidationString, if any.
func (vs ValidationString) ONRS() ([]*api.ObjectAndRelation, *ErrorWithSource) {
	onrStrings := vs.ONRStrings()

	onrs := []*api.ObjectAndRelation{}
	for _, onrString := range onrStrings {
		found := tuple.ScanONR(onrString)
		if found == nil {
			return nil, &ErrorWithSource{fmt.Errorf("invalid object and relation: %s", onrString), onrString}
		}

		onrs = append(onrs, found)
	}
	return onrs, nil
}

// Assertions represents assertions defined in the validation file.
type Assertions struct {
	// AssertTrue is the set of relationships to assert true.
	AssertTrue []string `yaml:"assertTrue"`

	// AssertFalse is the set of relationships to assert false.
	AssertFalse []string `yaml:"assertFalse"`
}

// ErrorWithSource is an error that includes the source text.
type ErrorWithSource struct {
	error

	// Source is the source text for the error.
	Source string
}

// AssertTrueRelationships returns the relationships for which to assert existance.
func (a Assertions) AssertTrueRelationships() ([]*api.RelationTuple, *ErrorWithSource) {
	relationships := []*api.RelationTuple{}
	for _, tplString := range a.AssertTrue {
		parsed := tuple.Scan(tplString)
		if parsed == nil {
			return relationships, &ErrorWithSource{fmt.Errorf("could not parse relationship %s", tplString), tplString}
		}
		relationships = append(relationships, parsed)
	}
	return relationships, nil
}

// AssertFalseRelationships returns the relationships for which to assert non-existance.
func (a Assertions) AssertFalseRelationships() ([]*api.RelationTuple, *ErrorWithSource) {
	relationships := []*api.RelationTuple{}
	for _, tplString := range a.AssertFalse {
		parsed := tuple.Scan(tplString)
		if parsed == nil {
			return relationships, &ErrorWithSource{fmt.Errorf("could not parse relationship %s", tplString), tplString}
		}
		relationships = append(relationships, parsed)
	}
	return relationships, nil
}
