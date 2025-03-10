package blocks

import (
	yamlv3 "gopkg.in/yaml.v3"

	"github.com/authzed/spicedb/pkg/spiceerrors"
)

// SchemaWithPosition is the schema string together with the
// position of the schema within the validation file.
type SchemaWithPosition struct {
	// Schema is the schema found.
	Schema string

	// SourcePosition is the position of the schema in the file.
	SourcePosition spiceerrors.SourcePosition
}

// UnmarshalYAML is a custom unmarshaller.
func (ps *SchemaWithPosition) UnmarshalYAML(node *yamlv3.Node) error {
	err := node.Decode(&ps.Schema)
	if err != nil {
		return convertYamlError(err)
	}

	ps.SourcePosition = spiceerrors.SourcePosition{LineNumber: node.Line, ColumnPosition: node.Column}
	return nil
}
