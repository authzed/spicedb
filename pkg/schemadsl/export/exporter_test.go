package export

import (
	_ "embed"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

//go:embed test_schema.zed
var schema string

const expectedJson = `[{"name":"user","namespace":"default","relations":[],"permissions":[]},{"name":"platform","namespace":"default","relations":[{"name":"administrator"}],"permissions":[{"name":"super_admin"},{"name":"create_tenant"}]},{"name":"tenant","namespace":"default","relations":[{"name":"platform"},{"name":"parent"},{"name":"administrator"},{"name":"agent"},{"name":"tenant_administrator"},{"name":"admin_administrator"}],"permissions":[{"name":"administer_user"},{"name":"create_admin"}]},{"name":"administrator","namespace":"default","relations":[{"name":"self"},{"name":"tenant"}],"permissions":[{"name":"write"},{"name":"read"}]}]`

func TestExportSchema(t *testing.T) {

	in := compiler.InputSchema{
		Source:       input.Source("schema.zed"),
		SchemaString: schema,
	}

	namespace := "default"

	def, err := compiler.Compile([]compiler.InputSchema{in}, &namespace)
	assert.NoError(t, err)

	var buf strings.Builder
	err = WriteSchemaTo(def, &buf)
	assert.Equal(t, expectedJson, buf.String())
}
