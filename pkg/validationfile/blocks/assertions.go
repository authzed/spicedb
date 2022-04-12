package blocks

import (
	"fmt"
	"strings"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	yamlv3 "gopkg.in/yaml.v3"

	"github.com/authzed/spicedb/pkg/commonerrors"
	"github.com/authzed/spicedb/pkg/tuple"
)

// Assertions represents assertions defined in the validation file.
type Assertions struct {
	// AssertTrue is the set of relationships to assert true.
	AssertTrue []Assertion `yaml:"assertTrue"`

	// AssertFalse is the set of relationships to assert false.
	AssertFalse []Assertion `yaml:"assertFalse"`

	// SourcePosition is the position of the assertions in the file.
	SourcePosition commonerrors.SourcePosition
}

// Assertion is a parsed assertion.
type Assertion struct {
	// RelationshipString is the string form of the assertion.
	RelationshipString string

	// Relationship is the parsed relationship on which the assertion is being
	// run.
	Relationship *v1.Relationship

	// SourcePosition is the position of the assertion in the file.
	SourcePosition commonerrors.SourcePosition
}

type internalAssertions struct {
	// AssertTrue is the set of relationships to assert true.
	AssertTrue []Assertion `yaml:"assertTrue"`

	// AssertFalse is the set of relationships to assert false.
	AssertFalse []Assertion `yaml:"assertFalse"`
}

// UnmarshalYAML is a custom unmarshaller.
func (a *Assertions) UnmarshalYAML(node *yamlv3.Node) error {
	ia := internalAssertions{}
	if err := node.Decode(&ia); err != nil {
		return convertYamlError(err)
	}

	a.AssertTrue = ia.AssertTrue
	a.AssertFalse = ia.AssertFalse
	a.SourcePosition = commonerrors.SourcePosition{LineNumber: node.Line, ColumnPosition: node.Column}
	return nil
}

// UnmarshalYAML is a custom unmarshaller.
func (a *Assertion) UnmarshalYAML(node *yamlv3.Node) error {
	if err := node.Decode(&a.RelationshipString); err != nil {
		return convertYamlError(err)
	}

	trimmed := strings.TrimSpace(a.RelationshipString)
	tpl := tuple.Parse(trimmed)
	if tpl == nil {
		return commonerrors.NewErrorWithSource(
			fmt.Errorf("error parsing relationship `%s`", trimmed),
			trimmed,
			uint64(node.Line),
			uint64(node.Column),
		)
	}

	a.Relationship = tuple.MustToRelationship(tpl)
	a.SourcePosition = commonerrors.SourcePosition{LineNumber: node.Line, ColumnPosition: node.Column}
	return nil
}

// ParseAssertionsBlock parses the given contents as an assertions block.
func ParseAssertionsBlock(contents []byte) (*Assertions, error) {
	a := internalAssertions{}
	if err := yamlv3.Unmarshal(contents, &a); err != nil {
		return nil, convertYamlError(err)
	}
	return &Assertions{
		AssertTrue:  a.AssertTrue,
		AssertFalse: a.AssertFalse,
	}, nil
}
