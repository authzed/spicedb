package blocks

import (
	"encoding/json"
	"fmt"
	"strings"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	yamlv3 "gopkg.in/yaml.v3"

	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/tuple"
)

// Assertions represents assertions defined in the validation file.
type Assertions struct {
	// AssertTrue is the set of relationships to assert true.
	AssertTrue []Assertion `yaml:"assertTrue"`

	// AssertCaveated is the set of relationships to assert that are caveated.
	AssertCaveated []Assertion `yaml:"assertCaveated"`

	// AssertFalse is the set of relationships to assert false.
	AssertFalse []Assertion `yaml:"assertFalse"`

	// SourcePosition is the position of the assertions in the file.
	SourcePosition spiceerrors.SourcePosition
}

// Assertion is a parsed assertion.
type Assertion struct {
	// RelationshipWithContextString is the string form of the assertion, including optional context.
	// Forms:
	// `document:firstdoc#view@user:tom`
	// `document:seconddoc#view@user:sarah with {"some":"contexthere"}`
	RelationshipWithContextString string

	// Relationship is the parsed relationship on which the assertion is being
	// run.
	Relationship *v1.Relationship

	// CaveatContext is the caveat context for the assertion, if any.
	CaveatContext map[string]any

	// SourcePosition is the position of the assertion in the file.
	SourcePosition spiceerrors.SourcePosition
}

type internalAssertions struct {
	// AssertTrue is the set of relationships to assert true.
	AssertTrue []Assertion `yaml:"assertTrue"`

	// AssertCaveated is the set of relationships to assert that are caveated.
	AssertCaveated []Assertion `yaml:"assertCaveated"`

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
	a.AssertCaveated = ia.AssertCaveated
	a.SourcePosition = spiceerrors.SourcePosition{LineNumber: node.Line, ColumnPosition: node.Column}
	return nil
}

// UnmarshalYAML is a custom unmarshaller.
func (a *Assertion) UnmarshalYAML(node *yamlv3.Node) error {
	relationshipWithContextString := ""

	if err := node.Decode(&relationshipWithContextString); err != nil {
		return convertYamlError(err)
	}

	trimmed := strings.TrimSpace(relationshipWithContextString)

	// Check for caveat context.
	parts := strings.SplitN(trimmed, " with ", 2)
	if len(parts) == 0 {
		return spiceerrors.NewErrorWithSource(
			fmt.Errorf("error parsing assertion `%s`", trimmed),
			trimmed,
			uint64(node.Line),
			uint64(node.Column),
		)
	}

	tpl := tuple.Parse(strings.TrimSpace(parts[0]))
	if tpl == nil {
		return spiceerrors.NewErrorWithSource(
			fmt.Errorf("error parsing relationship in assertion `%s`", trimmed),
			trimmed,
			uint64(node.Line),
			uint64(node.Column),
		)
	}

	a.Relationship = tuple.MustToRelationship(tpl)

	if len(parts) == 2 {
		caveatContextMap := make(map[string]any, 0)
		err := json.Unmarshal([]byte(parts[1]), &caveatContextMap)
		if err != nil {
			return spiceerrors.NewErrorWithSource(
				fmt.Errorf("error parsing caveat context in assertion `%s`: %w", trimmed, err),
				trimmed,
				uint64(node.Line),
				uint64(node.Column),
			)
		}

		a.CaveatContext = caveatContextMap
	}

	a.RelationshipWithContextString = relationshipWithContextString
	a.SourcePosition = spiceerrors.SourcePosition{LineNumber: node.Line, ColumnPosition: node.Column}
	return nil
}

// ParseAssertionsBlock parses the given contents as an assertions block.
func ParseAssertionsBlock(contents []byte) (*Assertions, error) {
	a := internalAssertions{}
	if err := yamlv3.Unmarshal(contents, &a); err != nil {
		return nil, convertYamlError(err)
	}
	return &Assertions{
		AssertTrue:     a.AssertTrue,
		AssertCaveated: a.AssertCaveated,
		AssertFalse:    a.AssertFalse,
	}, nil
}
