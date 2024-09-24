package namespace

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/authzed/spicedb/pkg/genutil/mapz"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
	"github.com/authzed/spicedb/pkg/testutil"
)

/*
NOTE: this test exists because we found a place where MarshalVT
was producing a different serialization than proto.Marshal. The
idea is that each time we regenerate our _vtproto.pb.go files,
we run this generation, and then the serialization_test will
use these snapshots to assert that nothing has changed.
*/

type serializationTest struct {
	name   string
	schema string
}

var serializationTests = []serializationTest{
	{"Basic serialization test", "basic"},
}

type definitionInterface interface {
	protoreflect.ProtoMessage
	UnmarshalVT([]byte) error
}

func assertParity(t *testing.T, filenames []string, emptyProto definitionInterface) {
	vtProtoDefinitions := make(map[string]definitionInterface)
	standardProtoDefinitions := make(map[string]definitionInterface)
	definitions := mapz.NewSet[string]()
	for _, filename := range filenames {
		definition := strings.Split(filename, ".")[0]
		definitions.Add(definition)
		bytes, err := os.ReadFile(fmt.Sprintf("testdata/proto/%s", filename))
		require.NoError(t, err)

		standardRepresentation := proto.Clone(emptyProto).(definitionInterface)
		err = proto.Unmarshal(bytes, standardRepresentation)
		require.NoError(t, err)
		standardProtoDefinitions[filename] = standardRepresentation

		vtRepresentation := proto.Clone(emptyProto).(definitionInterface)
		err = vtRepresentation.UnmarshalVT(bytes)
		require.NoError(t, err)
		vtProtoDefinitions[filename] = vtRepresentation
	}

	// For each namespace, we want to assert that all of the following are equivalent:
	// standard serialization -> standard deserialization
	// vt serialization -> standard deserialization
	// standard serialization -> vt deserialization
	// This is to validate that the vt serialization/deserialization isn't doing anything unexpected
	// compared to the "official" serialization/deserialization

	for _, definition := range definitions.AsSlice() {
		vtFilename := fmt.Sprintf("%s.vtproto", definition)
		standardFilename := fmt.Sprintf("%s.proto", definition)

		testutil.RequireProtoEqual(t, standardProtoDefinitions[vtFilename], standardProtoDefinitions[standardFilename], "vt and standard serializations disagree")
		testutil.RequireProtoEqual(t, standardProtoDefinitions[standardFilename], vtProtoDefinitions[standardFilename], "vt and standard deserializations of standard proto disagree")
		testutil.RequireProtoEqual(t, vtProtoDefinitions[standardFilename], vtProtoDefinitions[vtFilename], "vt and standard deserializations of vt proto disagree")
	}
}

func TestSerialization(t *testing.T) {
	for _, test := range serializationTests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			require := require.New(t)

			if os.Getenv("REGEN") == "true" {
				schema, err := os.ReadFile(fmt.Sprintf("testdata/schema/%s.zed", test.schema))
				require.NoError(err)
				compiled, _ := compiler.Compile(compiler.InputSchema{
					Source:       input.Source("schema"),
					SchemaString: string(schema),
				}, compiler.AllowUnprefixedObjectType())

				for _, objectDef := range compiled.ObjectDefinitions {
					protoSerialized, err := proto.Marshal(objectDef)
					require.NoError(err)
					err = os.WriteFile(fmt.Sprintf("testdata/proto/%s-definition-%s.proto", test.schema, objectDef.Name), protoSerialized, 0o600)
					require.NoError(err)

					vtSerialized, err := objectDef.MarshalVT()
					require.NoError(err)
					err = os.WriteFile(fmt.Sprintf("testdata/proto/%s-definition-%s.vtproto", test.schema, objectDef.Name), vtSerialized, 0o600)
					require.NoError(err)
				}
				for _, caveatDef := range compiled.CaveatDefinitions {
					protoSerialized, err := proto.Marshal(caveatDef)
					require.NoError(err)
					err = os.WriteFile(fmt.Sprintf("testdata/proto/%s-caveat-%s.proto", test.schema, caveatDef.Name), protoSerialized, 0o600)
					require.NoError(err)

					vtSerialized, err := caveatDef.MarshalVT()
					require.NoError(err)
					err = os.WriteFile(fmt.Sprintf("testdata/proto/%s-caveat-%s.vtproto", test.schema, caveatDef.Name), vtSerialized, 0o600)
					require.NoError(err)
				}
			} else {
				files, err := os.ReadDir("testdata/proto")
				require.NoError(err)

				definitionFiles := mapz.NewSet[string]()
				caveatFiles := mapz.NewSet[string]()
				for _, file := range files {
					filename := file.Name()
					if strings.Contains(filename, test.schema) {
						// NOTE: this makes some assumptions about the names of the files,
						// namely that a schema file name will not have either of these
						// keywords.
						if strings.Contains(filename, "definition") {
							definitionFiles.Add(filename)
						}
						if strings.Contains(filename, "caveat") {
							caveatFiles.Add(filename)
						}
					}
				}

				assertParity(t, definitionFiles.AsSlice(), &core.NamespaceDefinition{})
				assertParity(t, caveatFiles.AsSlice(), &core.CaveatDefinition{})
			}
		})
	}
}
