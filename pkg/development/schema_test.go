package development

import (
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/require"

	devinterface "github.com/authzed/spicedb/pkg/proto/developer/v1"
)

func TestCompileSchemaValid(t *testing.T) {
	schema := `definition user {}

definition document {
	relation viewer: user
	permission view = viewer
}`

	compiled, devErr, err := CompileSchema(schema)
	require.NoError(t, err)
	require.Nil(t, devErr)
	require.NotNil(t, compiled)
	require.Len(t, compiled.ObjectDefinitions, 2)
}

func TestCompileSchemaInvalidSyntax(t *testing.T) {
	schema := `definition user {
		invalid syntax here
	}`

	compiled, devErr, err := CompileSchema(schema)
	require.NoError(t, err)
	require.Nil(t, compiled)
	require.NotNil(t, devErr)
	require.Equal(t, devinterface.DeveloperError_SCHEMA, devErr.Source)
	require.Equal(t, devinterface.DeveloperError_SCHEMA_ISSUE, devErr.Kind)
	require.Positive(t, devErr.Line)
	require.Positive(t, devErr.Column)
}

func TestCompileSchemaUndefinedRelation(t *testing.T) {
	schema := `definition user {}

definition document {
	relation viewer: user
	permission view = nonexistent_relation
}`

	compiled, devErr, err := CompileSchema(schema)
	require.NoError(t, err)

	// Note: The schema compiler allows undefined relations, it's the type system that catches them
	// So this test case actually compiles successfully but should fail during validation
	require.NotNil(t, compiled)
	require.Nil(t, devErr)
}

func TestCompileSchemaCircularReference(t *testing.T) {
	schema := `definition user {}

definition document {
	relation viewer: user
	permission view = view
}`

	compiled, devErr, err := CompileSchema(schema)
	require.NoError(t, err)

	// Note: The schema compiler allows circular references, it's the type system that catches them
	// So this test case actually compiles successfully but should fail during validation
	require.NotNil(t, compiled)
	require.Nil(t, devErr)
}

func TestCompileSchemaWithCaveats(t *testing.T) {
	schema := `definition user {}

caveat somecaveat(condition int) {
	condition == 42
}

definition document {
	relation viewer: user with somecaveat
	permission view = viewer
}`

	compiled, devErr, err := CompileSchema(schema)
	require.NoError(t, err)
	require.Nil(t, devErr)
	require.NotNil(t, compiled)
	require.Len(t, compiled.ObjectDefinitions, 2)
	require.Len(t, compiled.CaveatDefinitions, 1)
}

func TestCompileSchemaInvalidCaveat(t *testing.T) {
	schema := `definition user {}

caveat somecaveat(condition unknown_type) {
	condition == 42
}

definition document {
	relation viewer: user with somecaveat
}`

	compiled, devErr, err := CompileSchema(schema)
	require.NoError(t, err)
	require.Nil(t, compiled)
	require.NotNil(t, devErr)
	require.Equal(t, devinterface.DeveloperError_SCHEMA, devErr.Source)
	require.Equal(t, devinterface.DeveloperError_SCHEMA_ISSUE, devErr.Kind)
	require.Contains(t, devErr.Message, "unknown_type")
}

func TestCompileSchemaWithRootFileNameOnImportError(t *testing.T) {
	rootSchema := `use import

definition user {}

import "doesnotexist.zed"
`
	sourceFS := fstest.MapFS{} // empty FS, so the import will fail

	compiled, devErr, err := CompileSchema(rootSchema,
		WithSourceFS(sourceFS),
		WithRootFileName("myroot.zed"),
	)
	require.NoError(t, err)
	require.Nil(t, compiled)
	require.NotNil(t, devErr)
	require.Equal(t, "myroot.zed", devErr.Context)
	require.Equal(t, "failed to read import \"doesnotexist.zed\": open doesnotexist.zed: file does not exist", devErr.Message)
}

func TestCompileSchemaWithErrorInImportedFile(t *testing.T) {
	rootSchema := `use import

definition user {}

import "imported.zed"
`
	sourceFS := fstest.MapFS{
		"imported.zed": &fstest.MapFile{Data: []byte("definition document {\n\tinvalid syntax here\n}")},
	}

	compiled, devErr, err := CompileSchema(rootSchema,
		WithSourceFS(sourceFS),
		WithRootFileName("myroot.zed"),
	)
	require.NoError(t, err)
	require.Nil(t, compiled)
	require.NotNil(t, devErr)
	t.Log(devErr.Message)
	require.Equal(t, "Expected end of statement or definition, found: TokenTypeIdentifier", devErr.Message)
	require.Equal(t, devinterface.DeveloperError_SCHEMA, devErr.Source)
	require.Equal(t, devinterface.DeveloperError_SCHEMA_ISSUE, devErr.Kind)
	require.Equal(t, "invalid", devErr.Context)
	require.Len(t, devErr.Path, 1)
	require.Equal(t, "imported.zed", devErr.Path[0])
	require.Positive(t, devErr.Line)
	require.Positive(t, devErr.Column)
}
