package inprocess

import (
	"context"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/known/structpb"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb/internal/testingserver"
	"github.com/authzed/spicedb/pkg/cmd/testserver"
	"github.com/authzed/spicedb/pkg/cmd/util"
	"github.com/authzed/spicedb/pkg/tuple"
)

const paritySchema = `
use expiration

caveat only_on(day string, allowed string) {
	day == allowed
}

definition user {}

definition group {
	relation member: user | group#member
}

definition organization {
	relation admin: user
	relation member: user | group#member
	permission manage = admin
	permission view = member + manage
}

definition document {
	relation org: organization
	relation viewer: user | user:* | group#member | user with only_on
	relation editor: user with expiration
	relation banned: user
	permission edit = editor + org->manage
	permission view = (viewer + edit) - banned
	permission org_view = viewer & org->view
	permission all_orgs_manage = org.all(manage)
	permission any_org_view = org.any(view)
}`

var parityRelationships = []string{
	"organization:acme#admin@user:olivia",
	"organization:acme#member@group:eng#member",
	"group:eng#member@user:alice",
	"group:eng#member@group:interns#member",
	"group:interns#member@user:ivan",
	"document:plan#org@organization:acme",
	"document:plan#viewer@group:eng#member",
	"document:plan#banned@user:ivan",
	"document:plan#editor@user:ed[expiration:2300-01-01T00:00:00Z]",
	"document:plan#editor@user:gone[expiration:2000-01-01T00:00:00Z]",
	"document:public#viewer@user:*",
	"document:weekly#viewer@user:carol[only_on:{\"allowed\":\"monday\"}]",
	"group:loop1#member@group:loop2#member",
	"group:loop2#member@group:loop1#member",
}

type parityCall struct {
	name   string
	method string
	req    vtMessage
}

func parityCalls(t *testing.T) []parityCall {
	fully := &v1.Consistency{Requirement: &v1.Consistency_FullyConsistent{FullyConsistent: true}}
	obj := func(typ, id string) *v1.ObjectReference { return &v1.ObjectReference{ObjectType: typ, ObjectId: id} }
	user := func(id string) *v1.SubjectReference { return &v1.SubjectReference{Object: obj("user", id)} }
	check := func(doc, perm, subject string, caveatContext map[string]any) *v1.CheckPermissionRequest {
		req := &v1.CheckPermissionRequest{Consistency: fully, Resource: obj("document", doc), Permission: perm, Subject: user(subject)}
		if caveatContext != nil {
			var err error
			req.Context, err = structpb.NewStruct(caveatContext)
			require.NoError(t, err)
		}
		return req
	}

	updates := make([]*v1.RelationshipUpdate, 0, len(parityRelationships))
	for _, rel := range parityRelationships {
		updates = append(updates, &v1.RelationshipUpdate{
			Operation:    v1.RelationshipUpdate_OPERATION_CREATE,
			Relationship: tuple.ToV1Relationship(tuple.MustParse(rel)),
		})
	}

	const (
		checkMethod = "/authzed.api.v1.PermissionsService/CheckPermission"
		lrMethod    = "/authzed.api.v1.PermissionsService/LookupResources"
		lsMethod    = "/authzed.api.v1.PermissionsService/LookupSubjects"
		writeMethod = "/authzed.api.v1.PermissionsService/WriteRelationships"
	)

	return []parityCall{
		{"invalid schema", "/authzed.api.v1.SchemaService/WriteSchema", &v1.WriteSchemaRequest{Schema: "definition user {"}},
		{"read schema before write", "/authzed.api.v1.SchemaService/ReadSchema", &v1.ReadSchemaRequest{}},
		{"write schema", "/authzed.api.v1.SchemaService/WriteSchema", &v1.WriteSchemaRequest{Schema: paritySchema}},
		{"read schema", "/authzed.api.v1.SchemaService/ReadSchema", &v1.ReadSchemaRequest{}},
		{"write relationships", writeMethod, &v1.WriteRelationshipsRequest{Updates: updates}},
		{"create conflict", writeMethod, &v1.WriteRelationshipsRequest{Updates: updates[:1]}},
		{"failed precondition", writeMethod, &v1.WriteRelationshipsRequest{
			Updates: []*v1.RelationshipUpdate{{Operation: v1.RelationshipUpdate_OPERATION_TOUCH, Relationship: tuple.ToV1Relationship(tuple.MustParse("document:x#viewer@user:x"))}},
			OptionalPreconditions: []*v1.Precondition{{
				Operation: v1.Precondition_OPERATION_MUST_MATCH,
				Filter:    &v1.RelationshipFilter{ResourceType: "document", OptionalResourceId: "nope"},
			}},
		}},
		{"write unknown relation", writeMethod, &v1.WriteRelationshipsRequest{Updates: []*v1.RelationshipUpdate{{
			Operation: v1.RelationshipUpdate_OPERATION_TOUCH, Relationship: tuple.ToV1Relationship(tuple.MustParse("document:x#nope@user:x")),
		}}}},
		{"computed permission", checkMethod, check("plan", "edit", "olivia", nil)},
		{"nested group", checkMethod, check("plan", "view", "alice", nil)},
		{"exclusion", checkMethod, check("plan", "view", "ivan", nil)},
		{"no permission", checkMethod, check("plan", "view", "mallory", nil)},
		{"expiring", checkMethod, check("plan", "edit", "ed", nil)},
		{"expired", checkMethod, check("plan", "edit", "gone", nil)},
		{"intersection", checkMethod, check("plan", "org_view", "alice", nil)},
		{"arrow all", checkMethod, check("plan", "all_orgs_manage", "olivia", nil)},
		{"arrow any", checkMethod, check("plan", "any_org_view", "ivan", nil)},
		{"wildcard", checkMethod, check("public", "view", "anyone", nil)},
		{"caveat missing context", checkMethod, check("weekly", "view", "carol", nil)},
		{"caveat true", checkMethod, check("weekly", "view", "carol", map[string]any{"day": "monday"})},
		{"caveat false", checkMethod, check("weekly", "view", "carol", map[string]any{"day": "friday"})},
		{"caveat bad context type", checkMethod, check("weekly", "view", "carol", map[string]any{"day": 42})},
		{"unknown definition", checkMethod, &v1.CheckPermissionRequest{Consistency: fully, Resource: obj("folder", "f"), Permission: "view", Subject: user("alice")}},
		{"unknown permission", checkMethod, check("plan", "nope", "alice", nil)},
		{"invalid request", checkMethod, &v1.CheckPermissionRequest{}},
		{"depth exceeded", checkMethod, &v1.CheckPermissionRequest{
			Consistency: fully, Resource: obj("group", "loop1"), Permission: "member", Subject: user("nobody"),
		}},
		{"bulk check", "/authzed.api.v1.PermissionsService/CheckBulkPermissions", &v1.CheckBulkPermissionsRequest{
			Consistency: fully,
			Items: []*v1.CheckBulkPermissionsRequestItem{
				{Resource: obj("document", "plan"), Permission: "view", Subject: user("alice")},
				{Resource: obj("document", "weekly"), Permission: "view", Subject: user("carol")},
				{Resource: obj("folder", "f"), Permission: "view", Subject: user("alice")},
			},
		}},
		{"lookup resources", lrMethod, &v1.LookupResourcesRequest{Consistency: fully, ResourceObjectType: "document", Permission: "view", Subject: user("carol")}},
		{"lookup resources with limit", lrMethod, &v1.LookupResourcesRequest{Consistency: fully, ResourceObjectType: "document", Permission: "view", Subject: user("alice"), OptionalLimit: 1}},
		{"lookup subjects", lsMethod, &v1.LookupSubjectsRequest{Consistency: fully, Resource: obj("document", "plan"), Permission: "view", SubjectObjectType: "user"}},
		{"read relationships", "/authzed.api.v1.PermissionsService/ReadRelationships", &v1.ReadRelationshipsRequest{
			Consistency: fully, RelationshipFilter: &v1.RelationshipFilter{ResourceType: "document"},
		}},
		{"expand", "/authzed.api.v1.PermissionsService/ExpandPermissionTree", &v1.ExpandPermissionTreeRequest{
			Consistency: fully, Resource: obj("document", "plan"), Permission: "view",
		}},
		{"delete relationships", "/authzed.api.v1.PermissionsService/DeleteRelationships", &v1.DeleteRelationshipsRequest{
			RelationshipFilter: &v1.RelationshipFilter{ResourceType: "document", OptionalResourceId: "public"},
		}},
		{"wildcard after delete", checkMethod, check("public", "view", "anyone", nil)},
		{"reflect schema", "/authzed.api.v1.SchemaService/ReflectSchema", &v1.ReflectSchemaRequest{Consistency: fully}},
	}
}

// TestParityWithServeTesting runs the same calls against the in-process server and the server
// run by `serve-testing`, and requires identical results.
func TestParityWithServeTesting(t *testing.T) {
	inProcess := newTestServer(t)
	session := inProcess.NewSession()

	serveTestingConn := serveTesting(t)
	token := "parity-" + t.Name() + "-" + strconv.FormatInt(time.Now().UnixNano(), 10)

	for _, c := range parityCalls(t) {
		reqBytes, err := c.req.MarshalVT()
		require.NoError(t, err)

		gotResp, gotErr := session.Call(t.Context(), c.method, reqBytes)
		wantResp, wantErr := call(t.Context(), serveTestingConn, token, c.method, reqBytes)

		if wantErr != nil {
			require.Error(t, gotErr, "%s: expected an error", c.name)
			require.Equal(t, prototext.Format(status.Convert(wantErr).Proto()), prototext.Format(status.Convert(gotErr).Proto()), c.name)
			continue
		}
		require.NoError(t, gotErr, c.name)
		require.Equal(t, normalizeResponse(t, c.method, wantResp), normalizeResponse(t, c.method, gotResp), c.name)
	}
}

var serveTestingOnce = sync.OnceValues(func() (*grpc.ClientConn, error) {
	cfg := &testserver.Config{
		GRPCServer:         util.GRPCServerConfig{Enabled: true, Network: util.BufferedNetwork, Address: "parity"},
		ReadOnlyGRPCServer: util.GRPCServerConfig{Address: "parity-readonly"},
	}
	limits := testingserver.DefaultLimits()
	cfg.MaximumUpdatesPerWrite = limits.MaximumUpdatesPerWrite
	cfg.MaximumPreconditionCount = limits.MaximumPreconditionCount
	cfg.MaxCaveatContextSize = limits.MaxCaveatContextSize
	cfg.MaxReadRelationshipsLimit = limits.MaxReadRelationshipsLimit
	cfg.MaxDeleteRelationshipsLimit = limits.MaxDeleteRelationshipsLimit
	cfg.MaxLookupResourcesLimit = limits.MaxLookupResourcesLimit
	cfg.MaxBulkExportRelationshipsLimit = limits.MaxBulkExportRelationshipsLimit

	srv, err := cfg.Complete(context.Background())
	if err != nil {
		return nil, err
	}
	go func() {
		_ = srv.Run(context.Background())
	}()
	return srv.NewClient()
})

// serveTesting returns a connection to the server run by `serve-testing`. It is started once per
// process, since it registers its metrics globally, and lives until the process exits.
func serveTesting(t *testing.T) *grpc.ClientConn {
	t.Helper()
	conn, err := serveTestingOnce()
	require.NoError(t, err)
	return conn
}

// normalizeResponse decodes a response, clearing the revision-dependent fields (ZedTokens and
// cursors), which legitimately differ between servers, and renders it as text. Framed
// responses of server-streaming methods are rendered message by message, and sorted, since
// their order is unspecified.
func normalizeResponse(t *testing.T, fullMethod string, resp []byte) []string {
	t.Helper()
	method, err := lookupCallableMethod(fullMethod)
	require.NoError(t, err)
	msgType, err := protoregistry.GlobalTypes.FindMessageByName(method.Output().FullName())
	require.NoError(t, err)

	frames := [][]byte{resp}
	if method.IsStreamingServer() {
		frames, err = ReadFrames(resp)
		require.NoError(t, err)
	}

	rendered := make([]string, 0, len(frames))
	for _, frame := range frames {
		msg := msgType.New().Interface()
		require.NoError(t, proto.Unmarshal(frame, msg))
		clearRevisionFields(msg.ProtoReflect())
		rendered = append(rendered, prototext.Format(msg))
	}
	slices.Sort(rendered)
	return rendered
}

func clearRevisionFields(msg protoreflect.Message) {
	msg.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		if fd.Message() == nil {
			return true
		}
		switch fd.Message().FullName() {
		case "authzed.api.v1.ZedToken", "authzed.api.v1.Cursor":
			msg.Clear(fd)
			return true
		}
		switch {
		case fd.IsList():
			for i := range v.List().Len() {
				clearRevisionFields(v.List().Get(i).Message())
			}
		case fd.IsMap():
			if fd.MapValue().Message() != nil {
				v.Map().Range(func(_ protoreflect.MapKey, mv protoreflect.Value) bool {
					clearRevisionFields(mv.Message())
					return true
				})
			}
		default:
			clearRevisionFields(v.Message())
		}
		return true
	})
}
