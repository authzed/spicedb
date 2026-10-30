package inprocess

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
)

const inProcessTestSchema = `
definition user {}

definition document {
	relation viewer: user
	permission view = viewer
}`

// vtMessage is a request message with a vtprotobuf marshaler.
type vtMessage interface {
	proto.Message
	MarshalVT() ([]byte, error)
}

func callProto[Req vtMessage, Resp proto.Message](t *testing.T, s *Session, method string, req Req, resp Resp) error {
	t.Helper()
	reqBytes, err := req.MarshalVT()
	require.NoError(t, err)
	respBytes, err := s.Call(t.Context(), method, reqBytes)
	if err != nil {
		return err
	}
	require.NoError(t, proto.Unmarshal(respBytes, resp))
	return nil
}

func writeTestData(t *testing.T, s *Session) {
	t.Helper()
	require.NoError(t, callProto(t, s, "/authzed.api.v1.SchemaService/WriteSchema",
		&v1.WriteSchemaRequest{Schema: inProcessTestSchema}, &v1.WriteSchemaResponse{}))
	require.NoError(t, callProto(t, s, "/authzed.api.v1.PermissionsService/WriteRelationships",
		&v1.WriteRelationshipsRequest{Updates: []*v1.RelationshipUpdate{
			{Operation: v1.RelationshipUpdate_OPERATION_CREATE, Relationship: testRel("doc1", "alice")},
			{Operation: v1.RelationshipUpdate_OPERATION_CREATE, Relationship: testRel("doc2", "alice")},
		}}, &v1.WriteRelationshipsResponse{}))
}

func testRel(doc, user string) *v1.Relationship {
	return &v1.Relationship{
		Resource: &v1.ObjectReference{ObjectType: "document", ObjectId: doc},
		Relation: "viewer",
		Subject:  &v1.SubjectReference{Object: &v1.ObjectReference{ObjectType: "user", ObjectId: user}},
	}
}

func checkView(t *testing.T, s *Session, doc, user string) v1.CheckPermissionResponse_Permissionship {
	t.Helper()
	resp := &v1.CheckPermissionResponse{}
	require.NoError(t, callProto(t, s, "/authzed.api.v1.PermissionsService/CheckPermission", &v1.CheckPermissionRequest{
		Consistency: &v1.Consistency{Requirement: &v1.Consistency_FullyConsistent{FullyConsistent: true}},
		Resource:    &v1.ObjectReference{ObjectType: "document", ObjectId: doc},
		Permission:  "view",
		Subject:     &v1.SubjectReference{Object: &v1.ObjectReference{ObjectType: "user", ObjectId: user}},
	}, resp))
	return resp.Permissionship
}

func newTestServer(t *testing.T) *Server {
	t.Helper()
	p, err := NewServer(nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, p.Close()) })
	return p
}

func TestInProcessUnaryCalls(t *testing.T) {
	s := newTestServer(t).NewSession()
	writeTestData(t, s)

	require.Equal(t, v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION, checkView(t, s, "doc1", "alice"))
	require.Equal(t, v1.CheckPermissionResponse_PERMISSIONSHIP_NO_PERMISSION, checkView(t, s, "doc1", "bob"))
}

func TestInProcessServerStreaming(t *testing.T) {
	s := newTestServer(t).NewSession()
	writeTestData(t, s)

	reqBytes, err := (&v1.LookupResourcesRequest{
		Consistency:        &v1.Consistency{Requirement: &v1.Consistency_FullyConsistent{FullyConsistent: true}},
		ResourceObjectType: "document",
		Permission:         "view",
		Subject:            &v1.SubjectReference{Object: &v1.ObjectReference{ObjectType: "user", ObjectId: "alice"}},
	}).MarshalVT()
	require.NoError(t, err)
	respBytes, err := s.Call(t.Context(), "/authzed.api.v1.PermissionsService/LookupResources", reqBytes)
	require.NoError(t, err)

	frames, err := ReadFrames(respBytes)
	require.NoError(t, err)
	ids := make([]string, 0, len(frames))
	for _, frame := range frames {
		resp := &v1.LookupResourcesResponse{}
		require.NoError(t, proto.Unmarshal(frame, resp))
		ids = append(ids, resp.ResourceObjectId)
	}
	require.ElementsMatch(t, []string{"doc1", "doc2"}, ids)
}

func TestInProcessClientStreaming(t *testing.T) {
	s := newTestServer(t).NewSession()
	require.NoError(t, callProto(t, s, "/authzed.api.v1.SchemaService/WriteSchema",
		&v1.WriteSchemaRequest{Schema: inProcessTestSchema}, &v1.WriteSchemaResponse{}))

	var request []byte
	for _, batch := range [][]*v1.Relationship{
		{testRel("doc1", "alice"), testRel("doc2", "alice")},
		{testRel("doc3", "bob")},
	} {
		b, err := (&v1.ImportBulkRelationshipsRequest{Relationships: batch}).MarshalVT()
		require.NoError(t, err)
		request = AppendFrame(request, b)
	}
	respBytes, err := s.Call(t.Context(), "/authzed.api.v1.PermissionsService/ImportBulkRelationships", request)
	require.NoError(t, err)
	resp := &v1.ImportBulkRelationshipsResponse{}
	require.NoError(t, proto.Unmarshal(respBytes, resp))
	require.Equal(t, uint64(3), resp.NumLoaded)

	require.Equal(t, v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION, checkView(t, s, "doc3", "bob"))
}

func TestInProcessErrors(t *testing.T) {
	s := newTestServer(t).NewSession()

	t.Run("invalid schema", func(t *testing.T) {
		err := callProto(t, s, "/authzed.api.v1.SchemaService/WriteSchema",
			&v1.WriteSchemaRequest{Schema: "definition user {"}, &v1.WriteSchemaResponse{})
		require.Equal(t, codes.InvalidArgument, status.Code(err))
	})

	t.Run("unknown definition", func(t *testing.T) {
		writeTestData(t, s)
		err := callProto(t, s, "/authzed.api.v1.PermissionsService/CheckPermission", &v1.CheckPermissionRequest{
			Resource:   &v1.ObjectReference{ObjectType: "folder", ObjectId: "f1"},
			Permission: "view",
			Subject:    &v1.SubjectReference{Object: &v1.ObjectReference{ObjectType: "user", ObjectId: "alice"}},
		}, &v1.CheckPermissionResponse{})
		require.Equal(t, codes.FailedPrecondition, status.Code(err))
	})

	t.Run("request validation", func(t *testing.T) {
		err := callProto(t, s, "/authzed.api.v1.PermissionsService/CheckPermission", &v1.CheckPermissionRequest{}, &v1.CheckPermissionResponse{})
		require.Equal(t, codes.InvalidArgument, status.Code(err))
	})

	t.Run("unknown method", func(t *testing.T) {
		_, err := s.Call(t.Context(), "/authzed.api.v1.PermissionsService/DoesNotExist", nil)
		require.Equal(t, codes.Unimplemented, status.Code(err))
	})

	t.Run("watch is not callable", func(t *testing.T) {
		_, err := s.Call(t.Context(), "/authzed.api.v1.WatchService/Watch", nil)
		require.Equal(t, codes.Unimplemented, status.Code(err))
	})

	t.Run("malformed method", func(t *testing.T) {
		_, err := s.Call(t.Context(), "CheckPermission", nil)
		require.Equal(t, codes.InvalidArgument, status.Code(err))
	})

	t.Run("malformed client stream", func(t *testing.T) {
		_, err := s.Call(t.Context(), "/authzed.api.v1.PermissionsService/ImportBulkRelationships", []byte{1, 2})
		require.Equal(t, codes.InvalidArgument, status.Code(err))
	})
}

func TestInProcessSessionIsolation(t *testing.T) {
	p := newTestServer(t)
	first := p.NewSession()
	writeTestData(t, first)

	second := p.NewSession()
	require.NoError(t, callProto(t, second, "/authzed.api.v1.SchemaService/WriteSchema",
		&v1.WriteSchemaRequest{Schema: inProcessTestSchema}, &v1.WriteSchemaResponse{}))
	require.Equal(t, v1.CheckPermissionResponse_PERMISSIONSHIP_NO_PERMISSION, checkView(t, second, "doc1", "alice"))
	require.Equal(t, v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION, checkView(t, first, "doc1", "alice"))

	// Closing a session discards its data.
	require.NoError(t, first.Close())
	err := callProto(t, first, "/authzed.api.v1.SchemaService/ReadSchema", &v1.ReadSchemaRequest{}, &v1.ReadSchemaResponse{})
	require.Equal(t, codes.NotFound, status.Code(err))
}

func TestFrames(t *testing.T) {
	buf := AppendFrame(nil, []byte("one"))
	buf = AppendFrame(buf, nil)
	buf = AppendFrame(buf, []byte("three"))

	frames, err := ReadFrames(buf)
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("one"), {}, []byte("three")}, frames)

	_, err = ReadFrames(buf[:len(buf)-1])
	require.Error(t, err)
	_, err = ReadFrames([]byte{1})
	require.Error(t, err)
}
