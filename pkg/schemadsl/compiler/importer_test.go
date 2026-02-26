package compiler_test

import (
	"embed"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/generator"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
)

type importerTest struct {
	name   string
	folder string
}

//go:embed importer-test
var testFS embed.FS

func (it *importerTest) input() string {
	b, err := os.ReadFile(fmt.Sprintf("importer-test/%s/root.zed", it.folder))
	if err != nil {
		panic(err)
	}

	return string(b)
}

func (it *importerTest) relativePath() string {
	return fmt.Sprintf("importer-test/%s", it.folder)
}

func (it *importerTest) expected() string {
	b, err := os.ReadFile(fmt.Sprintf("importer-test/%s/expected.zed", it.folder))
	if err != nil {
		panic(err)
	}

	return string(b)
}

func (it *importerTest) writeExpected(schema string) {
	err := os.WriteFile(fmt.Sprintf("importer-test/%s/expected.zed", it.folder), []byte(schema), 0o_600)
	if err != nil {
		panic(err)
	}
}

func TestImporter(t *testing.T) {
	t.Parallel()

	workingDir, err := os.Getwd()
	require.NoError(t, err)

	importerTests := []importerTest{
		{"simple local import", "simple-local"},
		{"simple local import with transitive hop", "simple-local-with-hop"},
		{"nested local import", "nested-local"},
		{"nested local import with transitive hop", "nested-local-with-hop"},
		{"nested local two layers deep import", "nested-two-layer-local"},
		{"diamond-shaped imports are fine", "diamond-shaped"},
		{"multiple use directives are fine", "multiple-use-directives"},
		{"many imports are correctly resolved", "many-imports"},
	}

	for _, test := range importerTests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			sourceFolder := path.Join(workingDir, test.relativePath())

			inputSchema := test.input()

			compiled, err := compiler.Compile(compiler.InputSchema{
				Source:       input.Source("schema"),
				SchemaString: inputSchema,
			}, compiler.AllowUnprefixedObjectType(),
				compiler.SourceFolder(sourceFolder))
			require.NoError(t, err)

			generated, _, err := generator.GenerateSchema(compiled.OrderedDefinitions)
			require.NoError(t, err)

			if os.Getenv("REGEN") == "true" {
				test.writeExpected(generated)
			} else {
				expected := test.expected()
				if !assert.Equal(t, expected, generated, test.name) {
					t.Log(generated)
				}
			}
		})
		t.Run("fs/"+test.name, func(t *testing.T) {
			t.Parallel()

			fsys, err := fs.Sub(testFS, filepath.Join("importer-test", test.folder))
			require.NoError(t, err)

			inputSchema := test.input()

			compiled, err := compiler.Compile(compiler.InputSchema{
				Source:       input.Source("schema"),
				SchemaString: inputSchema,
			}, compiler.AllowUnprefixedObjectType(),
				compiler.SourceFS(fsys))
			require.NoError(t, err)

			generated, _, err := generator.GenerateSchema(compiled.OrderedDefinitions)
			require.NoError(t, err)

			if os.Getenv("REGEN") == "true" {
				test.writeExpected(generated)
			} else {
				expected := test.expected()
				if !assert.Equal(t, expected, generated, test.name) {
					t.Log(generated)
				}
			}
		})
	}
}

func TestImportCycleCausesError(t *testing.T) {
	t.Parallel()

	workingDir, err := os.Getwd()
	require.NoError(t, err)
	test := importerTest{"", "circular-import"}

	sourceFolder := path.Join(workingDir, test.relativePath())

	inputSchema := test.input()

	_, err = compiler.Compile(compiler.InputSchema{
		Source:       input.Source("schema"),
		SchemaString: inputSchema,
	}, compiler.AllowUnprefixedObjectType(),
		compiler.SourceFolder(sourceFolder))

	require.ErrorContains(t, err, "circular import")
}

func TestEscapeAttemptCausesError(t *testing.T) {
	t.Parallel()

	workingDir, err := os.Getwd()
	require.NoError(t, err)
	test := importerTest{"", "escape-attempt"}

	sourceFolder := path.Join(workingDir, test.relativePath())

	inputSchema := test.input()

	_, err = compiler.Compile(compiler.InputSchema{
		Source:       input.Source("schema"),
		SchemaString: inputSchema,
	}, compiler.AllowUnprefixedObjectType(),
		compiler.SourceFolder(sourceFolder))

	require.ErrorContains(t, err, "must stay within")
}

func TestConflictingDefinitionsCausesError(t *testing.T) {
	t.Parallel()

	workingDir, err := os.Getwd()
	require.NoError(t, err)
	test := importerTest{"", "conflicting-definitions"}

	sourceFolder := path.Join(workingDir, test.relativePath())

	inputSchema := test.input()

	_, err = compiler.Compile(compiler.InputSchema{
		Source:       input.Source("schema"),
		SchemaString: inputSchema,
	}, compiler.AllowUnprefixedObjectType(),
		compiler.SourceFolder(sourceFolder))

	require.ErrorContains(t, err, "found name reused between multiple definitions and/or caveats")
}
