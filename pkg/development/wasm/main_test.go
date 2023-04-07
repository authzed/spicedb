//go:build wasm
// +build wasm

// To Run:
// 1) Install wasmbrowsertest: `go install github.com/agnivade/wasmbrowsertest@latest`
// 2) Run: `GOOS=js GOARCH=wasm go test -exec wasmbrowsertest`

package main

import (
	"syscall/js"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	devinterface "github.com/authzed/spicedb/pkg/proto/developer/v1"
)

func TestMissingArgument(t *testing.T) {
	encodedResult := runDeveloperRequest(js.Null(), []js.Value{})
	response := &devinterface.DeveloperResponse{}
	err := protojson.Unmarshal([]byte(encodedResult.(js.Value).String()), response)
	require.NoError(t, err)
	require.Equal(t, "invalid number of arguments specified", response.GetInternalError())
}

func TestEmptyRequest(t *testing.T) {
	response := run(t, &devinterface.DeveloperRequest{})
	require.Equal(t, "missing required context", response.GetInternalError())
}

func TestEmptySchema(t *testing.T) {
	response := run(t, &devinterface.DeveloperRequest{
		Context: &devinterface.RequestContext{},
	})
	require.Equal(t, "", response.GetInternalError())
}

func TestInvalidSchema(t *testing.T) {
	response := run(t, &devinterface.DeveloperRequest{
		Context: &devinterface.RequestContext{
			Schema: "definitio user {",
		},
	})
	require.NotNil(t, response.GetDeveloperErrors())
	require.Equal(t, 1, len(response.GetDeveloperErrors().InputErrors))
	require.Equal(t, "Unexpected token at root level: TokenTypeIdentifier", response.GetDeveloperErrors().InputErrors[0].Message)
}

func TestInvalidRelationship(t *testing.T) {
	response := run(t, &devinterface.DeveloperRequest{
		Context: &devinterface.RequestContext{
			Schema: `definition user {}
			
			definition document {
				relation viewer: user
			}
			`,
			Relationships: []*core.RelationTuple{
				{
					ResourceAndRelation: &core.ObjectAndRelation{
						Namespace: "document",
						ObjectId:  "*",
						Relation:  "viewer",
					},
					Subject: &core.ObjectAndRelation{
						Namespace: "user",
						ObjectId:  "tom",
						Relation:  "...",
					},
				},
			},
		},
	})
	require.NotNil(t, response.GetInternalError())
	require.Equal(t, `invalid resource id; must match ([a-zA-Z0-9/_|\-=+]{1,})`, response.GetInternalError())
}

func run(t *testing.T, request *devinterface.DeveloperRequest) *devinterface.DeveloperResponse {
	encodedRequest, err := protojson.Marshal(request)
	require.NoError(t, err)

	encodedResponse := runDeveloperRequest(js.Null(), []js.Value{js.ValueOf(string(encodedRequest))})
	response := &devinterface.DeveloperResponse{}
	err = protojson.Unmarshal([]byte(encodedResponse.(js.Value).String()), response)
	require.NoError(t, err)
	return response
}
