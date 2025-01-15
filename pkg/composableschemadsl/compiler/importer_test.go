package compiler_test

import (
	"fmt"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/composableschemadsl/compiler"
	"github.com/authzed/spicedb/pkg/composableschemadsl/generator"
	"github.com/authzed/spicedb/pkg/composableschemadsl/input"
)

type importerTest struct {
	name   string
	folder string
}

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
	workingDir, err := os.Getwd()
	require.NoError(t, err)

	importerTests := []importerTest{
		{"simple local import", "simple-local"},
		{"simple local import with transitive hop", "simple-local-with-hop"},
		{"nested local import", "nested-local"},
		{"nested local import with transitive hop", "nested-local-with-hop"},
		{"nested local two layers deep import", "nested-two-layer-local"},
		{"diamond-shaped imports are fine", "diamond-shaped"},
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
