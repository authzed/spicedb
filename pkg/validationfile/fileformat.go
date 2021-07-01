package validationfile

import (
	"fmt"
	"regexp"

	yaml "gopkg.in/yaml.v2"
	yamlv3 "gopkg.in/yaml.v3"

	v0 "github.com/authzed/spicedb/pkg/proto/authzed/api/v0"
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
	var node yamlv3.Node
	err := yamlv3.Unmarshal(contents, &node)
	if err != nil {
		return Assertions{}, err
	}

	if len(node.Content) == 0 {
		return Assertions{}, nil
	}

	if node.Content[0].Kind != yamlv3.MappingNode {
		return Assertions{}, fmt.Errorf("expected object at top level")
	}

	mapping := node.Content[0]
	var key = ""
	var parsed Assertions
	for _, child := range mapping.Content {
		if child.Kind == yamlv3.ScalarNode {
			key = child.Value
			continue
		}

		if child.Kind == yamlv3.SequenceNode {
			for _, contentChild := range child.Content {
				if contentChild.Kind == yamlv3.ScalarNode {
					assertion := Assertion{
						relationshipString: contentChild.Value,
						lineNumber:         contentChild.Line,
						columnPosition:     contentChild.Column,
					}

					switch key {
					case "assertTrue":
						parsed.AssertTrue = append(parsed.AssertTrue, assertion)

					case "assertFalse":
						parsed.AssertFalse = append(parsed.AssertFalse, assertion)
					}
				}
			}
		}
	}

	return parsed, nil
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
	return string(data), err
}

// ObjectRelationString represents an ONR defined as a string in the key for the ValidationMap.
type ObjectRelationString string

func (ors ObjectRelationString) ONR() (*v0.ObjectAndRelation, *ErrorWithSource) {
	parsed := tuple.ScanONR(string(ors))
	if parsed == nil {
		return nil, &ErrorWithSource{fmt.Errorf("could not parse %s", ors), string(ors), 0, 0}
	}
	return parsed, nil
}

var (
	vsSubjectRegex           = regexp.MustCompile(`(.*?)\[(?P<user_str>.*)\](.*?)`)
	vsObjectAndRelationRegex = regexp.MustCompile(`(.*?)<(?P<onr_str>[^\>]+)>(.*?)`)
)

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
func (vs ValidationString) Subject() (*v0.ObjectAndRelation, *ErrorWithSource) {
	subjectStr, ok := vs.SubjectString()
	if !ok {
		return nil, nil
	}

	found := tuple.ScanONR(subjectStr)
	if found == nil {
		return nil, &ErrorWithSource{fmt.Errorf("invalid subject: %s", subjectStr), subjectStr, 0, 0}
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
func (vs ValidationString) ONRS() ([]*v0.ObjectAndRelation, *ErrorWithSource) {
	onrStrings := vs.ONRStrings()

	onrs := []*v0.ObjectAndRelation{}
	for _, onrString := range onrStrings {
		found := tuple.ScanONR(onrString)
		if found == nil {
			return nil, &ErrorWithSource{fmt.Errorf("invalid object and relation: %s", onrString), onrString, 0, 0}
		}

		onrs = append(onrs, found)
	}
	return onrs, nil
}

// Assertion is an unparsed assertion.
type Assertion struct {
	relationshipString string
	lineNumber         int
	columnPosition     int
}

// Assertions represents assertions defined in the validation file.
type Assertions struct {
	// AssertTrue is the set of relationships to assert true.
	AssertTrue []Assertion `yaml:"assertTrue"`

	// AssertFalse is the set of relationships to assert false.
	AssertFalse []Assertion `yaml:"assertFalse"`
}

// ErrorWithSource is an error that includes the source text and position information.
type ErrorWithSource struct {
	error

	// Source is the source text for the error.
	Source         string
	LineNumber     uint32
	ColumnPosition uint32
}

// ParsedAssertion contains information about a parsed assertion relationship.
type ParsedAssertion struct {
	Relationship   *v0.RelationTuple
	LineNumber     uint32
	ColumnPosition uint32
}

// AssertTrueRelationships returns the relationships for which to assert existance.
func (a Assertions) AssertTrueRelationships() ([]ParsedAssertion, *ErrorWithSource) {
	var relationships []ParsedAssertion
	for _, assertion := range a.AssertTrue {
		parsed := tuple.Scan(assertion.relationshipString)
		if parsed == nil {
			return relationships, &ErrorWithSource{
				fmt.Errorf("could not parse relationship `%s`", assertion.relationshipString),
				assertion.relationshipString,
				uint32(assertion.lineNumber),
				uint32(assertion.columnPosition),
			}
		}
		relationships = append(relationships, ParsedAssertion{
			Relationship:   parsed,
			LineNumber:     uint32(assertion.lineNumber),
			ColumnPosition: uint32(assertion.columnPosition),
		})
	}
	return relationships, nil
}

// AssertFalseRelationships returns the relationships for which to assert non-existance.
func (a Assertions) AssertFalseRelationships() ([]ParsedAssertion, *ErrorWithSource) {
	var relationships []ParsedAssertion
	for _, assertion := range a.AssertFalse {
		parsed := tuple.Scan(assertion.relationshipString)
		if parsed == nil {
			return relationships, &ErrorWithSource{
				fmt.Errorf("could not parse relationship `%s`", assertion.relationshipString),
				assertion.relationshipString,
				uint32(assertion.lineNumber),
				uint32(assertion.columnPosition),
			}
		}
		relationships = append(relationships, ParsedAssertion{
			Relationship:   parsed,
			LineNumber:     uint32(assertion.lineNumber),
			ColumnPosition: uint32(assertion.columnPosition),
		})
	}
	return relationships, nil
}
