//go:build wasitests && !wasip1

package main

import (
	"context"
	"encoding/binary"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
	spb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb/pkg/cmd/testserver/inprocess"
	"github.com/authzed/spicedb/pkg/tuple"
)

// resultError is the status byte of an error result.
const resultError = 1

// module is a host for the WASI module, mirroring what client test doubles do.
type module struct {
	t   *testing.T
	ctx context.Context
	mod api.Module
}

func buildModule(t *testing.T) []byte {
	t.Helper()
	out := filepath.Join(t.TempDir(), "spicedb.wasm")
	cmd := exec.Command("go", "build", "-buildmode=c-shared", "-o", out, ".")
	cmd.Env = append(os.Environ(), "GOOS=wasip1", "GOARCH=wasm")
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, string(output))

	wasm, err := os.ReadFile(out)
	require.NoError(t, err)
	return wasm
}

func instantiate(t *testing.T, wasm []byte) *module {
	t.Helper()
	ctx := t.Context()
	rt := wazero.NewRuntime(ctx)
	t.Cleanup(func() { require.NoError(t, rt.Close(context.Background())) })
	wasi_snapshot_preview1.MustInstantiate(ctx, rt)

	mod, err := rt.InstantiateWithConfig(ctx, wasm, wazero.NewModuleConfig().
		WithStartFunctions("_initialize").
		WithSysWalltime().
		WithSysNanotime().
		WithSysNanosleep().
		WithRandSource(nil))
	require.NoError(t, err)
	return &module{t: t, ctx: ctx, mod: mod}
}

func (m *module) call(name string, params ...uint64) []uint64 {
	m.t.Helper()
	results, err := m.mod.ExportedFunction(name).Call(m.ctx, params...)
	require.NoError(m.t, err)
	return results
}

// write copies data into a module buffer, returning its pointer and length.
func (m *module) write(data []byte) (uint64, uint64) {
	m.t.Helper()
	ptr := m.call("alloc", uint64(len(data)))[0]
	require.True(m.t, m.mod.Memory().Write(uint32(ptr), data)) //nolint:gosec // wasm32 pointers are 32-bit
	return ptr, uint64(len(data))
}

// result reads and frees a packed result, returning the payload, or the error status.
func (m *module) result(packed uint64) ([]byte, *spb.Status) {
	m.t.Helper()
	ptr, length := uint32(packed>>32), uint32(packed) //nolint:gosec // unpacking (ptr << 32 | len)
	buf, ok := m.mod.Memory().Read(ptr, length)
	require.True(m.t, ok)
	buf = append([]byte(nil), buf...)
	m.call("free", uint64(ptr))

	if buf[0] == resultError {
		st := &spb.Status{}
		require.NoError(m.t, proto.Unmarshal(buf[1:], st))
		return nil, st
	}
	return buf[1:], nil
}

func (m *module) newSession(schema string) (uint64, *spb.Status) {
	m.t.Helper()
	ptr, length := m.write([]byte(schema))
	defer m.call("free", ptr)
	payload, st := m.result(m.call("session_new", ptr, length)[0])
	if st != nil {
		return 0, st
	}
	return uint64(binary.LittleEndian.Uint32(payload)), nil
}

func (m *module) sessionCall(handle uint64, method string, req []byte) ([]byte, *spb.Status) {
	m.t.Helper()
	methodPtr, methodLen := m.write([]byte(method))
	defer m.call("free", methodPtr)
	reqPtr, reqLen := m.write(req)
	defer m.call("free", reqPtr)
	return m.result(m.call("session_call", handle, methodPtr, methodLen, reqPtr, reqLen)[0])
}

func (m *module) unary(handle uint64, method string, req interface{ MarshalVT() ([]byte, error) }, resp proto.Message) *spb.Status {
	m.t.Helper()
	reqBytes, err := req.MarshalVT()
	require.NoError(m.t, err)
	payload, st := m.sessionCall(handle, method, reqBytes)
	if st != nil {
		return st
	}
	require.NoError(m.t, proto.Unmarshal(payload, resp))
	return nil
}

const testSchema = `
definition user {}

definition group {
	relation member: user | group#member
}

definition document {
	relation viewer: user | group#member
	permission view = viewer
}`

func TestWASIModule(t *testing.T) {
	m := instantiate(t, buildModule(t))

	t.Run("invalid schema", func(t *testing.T) {
		_, st := m.newSession("definition user {")
		require.NotNil(t, st)
		require.Equal(t, int32(codes.InvalidArgument), st.Code)
	})

	t.Run("unknown session", func(t *testing.T) {
		_, st := m.sessionCall(9999, "/authzed.api.v1.SchemaService/ReadSchema", nil)
		require.NotNil(t, st)
		require.Equal(t, int32(codes.InvalidArgument), st.Code)
	})

	handle, st := m.newSession(testSchema)
	require.Nil(t, st)

	rel := func(rel string) *v1.RelationshipUpdate {
		return &v1.RelationshipUpdate{Operation: v1.RelationshipUpdate_OPERATION_TOUCH, Relationship: tuple.ToV1Relationship(tuple.MustParse(rel))}
	}

	require.Nil(t, m.unary(handle, "/authzed.api.v1.PermissionsService/WriteRelationships", &v1.WriteRelationshipsRequest{
		Updates: []*v1.RelationshipUpdate{
			rel("group:eng#member@user:alice"),
			rel("document:doc1#viewer@group:eng#member"),
			rel("document:doc2#viewer@user:bob"),
		},
	}, &v1.WriteRelationshipsResponse{}))

	fully := &v1.Consistency{Requirement: &v1.Consistency_FullyConsistent{FullyConsistent: true}}
	user := func(id string) *v1.SubjectReference {
		return &v1.SubjectReference{Object: &v1.ObjectReference{ObjectType: "user", ObjectId: id}}
	}

	t.Run("check", func(t *testing.T) {
		resp := &v1.CheckPermissionResponse{}
		require.Nil(t, m.unary(handle, "/authzed.api.v1.PermissionsService/CheckPermission", &v1.CheckPermissionRequest{
			Consistency: fully,
			Resource:    &v1.ObjectReference{ObjectType: "document", ObjectId: "doc1"},
			Permission:  "view",
			Subject:     user("alice"),
		}, resp))
		require.Equal(t, v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION, resp.Permissionship)
	})

	t.Run("lookup resources", func(t *testing.T) {
		req, err := (&v1.LookupResourcesRequest{
			Consistency:        fully,
			ResourceObjectType: "document",
			Permission:         "view",
			Subject:            user("alice"),
		}).MarshalVT()
		require.NoError(t, err)
		payload, st := m.sessionCall(handle, "/authzed.api.v1.PermissionsService/LookupResources", req)
		require.Nil(t, st)
		frames, err := inprocess.ReadFrames(payload)
		require.NoError(t, err)
		require.Len(t, frames, 1)
		resp := &v1.LookupResourcesResponse{}
		require.NoError(t, proto.Unmarshal(frames[0], resp))
		require.Equal(t, "doc1", resp.ResourceObjectId)
	})

	t.Run("unknown definition", func(t *testing.T) {
		st := m.unary(handle, "/authzed.api.v1.PermissionsService/CheckPermission", &v1.CheckPermissionRequest{
			Resource:   &v1.ObjectReference{ObjectType: "folder", ObjectId: "f1"},
			Permission: "view",
			Subject:    user("alice"),
		}, &v1.CheckPermissionResponse{})
		require.NotNil(t, st)
		require.Equal(t, int32(codes.FailedPrecondition), st.Code)
	})

	t.Run("session free", func(t *testing.T) {
		m.call("session_free", handle)
		_, st := m.sessionCall(handle, "/authzed.api.v1.SchemaService/ReadSchema", nil)
		require.NotNil(t, st)
		require.Equal(t, int32(codes.InvalidArgument), st.Code)
	})
}
