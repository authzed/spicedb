package tuple

import (
	"strings"
	"testing"

	b64 "encoding/base64"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/testutil"
)

func makeTuple(onr *core.ObjectAndRelation, subject *core.ObjectAndRelation) *core.RelationTuple {
	return &core.RelationTuple{
		ResourceAndRelation: onr,
		Subject:             subject,
	}
}

func rel(resType, resID, relation, subType, subID, subRel string) *v1.Relationship {
	return &v1.Relationship{
		Resource: &v1.ObjectReference{
			ObjectType: resType,
			ObjectId:   resID,
		},
		Relation: relation,
		Subject: &v1.SubjectReference{
			Object: &v1.ObjectReference{
				ObjectType: subType,
				ObjectId:   subID,
			},
			OptionalRelation: subRel,
		},
	}
}

func crel(resType, resID, relation, subType, subID, subRel, caveatName string, caveatContext map[string]any) *v1.Relationship {
	context, err := structpb.NewStruct(caveatContext)
	if err != nil {
		panic(err)
	}

	if len(context.Fields) == 0 {
		context = nil
	}

	return &v1.Relationship{
		Resource: &v1.ObjectReference{
			ObjectType: resType,
			ObjectId:   resID,
		},
		Relation: relation,
		Subject: &v1.SubjectReference{
			Object: &v1.ObjectReference{
				ObjectType: subType,
				ObjectId:   subID,
			},
			OptionalRelation: subRel,
		},
		OptionalCaveat: &v1.ContextualizedCaveat{
			CaveatName: caveatName,
			Context:    context,
		},
	}
}

var superLongID = strings.Repeat("f", 1024)

var testCases = []struct {
	input                  string
	expectedOutput         string
	tupleFormat            *core.RelationTuple
	relFormat              *v1.Relationship
	stableCanonicalization string
}{
	{
		input:          "testns:testobj#testrel@user:testusr",
		expectedOutput: "testns:testobj#testrel@user:testusr",
		tupleFormat: makeTuple(
			ObjectAndRelation("testns", "testobj", "testrel"),
			ObjectAndRelation("user", "testusr", "..."),
		),
		relFormat:              rel("testns", "testobj", "testrel", "user", "testusr", ""),
		stableCanonicalization: "dGVzdG5zOnRlc3RvYmojdGVzdHJlbEB1c2VyOnRlc3R1c3IjLi4u",
	},
	{
		input:          "testns:testobj#testrel@user:testusr#...",
		expectedOutput: "testns:testobj#testrel@user:testusr",
		tupleFormat: makeTuple(
			ObjectAndRelation("testns", "testobj", "testrel"),
			ObjectAndRelation("user", "testusr", "..."),
		),
		relFormat:              rel("testns", "testobj", "testrel", "user", "testusr", ""),
		stableCanonicalization: "dGVzdG5zOnRlc3RvYmojdGVzdHJlbEB1c2VyOnRlc3R1c3IjLi4u",
	},
	{
		input:          "tenant/testns:testobj#testrel@tenant/user:testusr",
		expectedOutput: "tenant/testns:testobj#testrel@tenant/user:testusr",
		tupleFormat: makeTuple(
			ObjectAndRelation("tenant/testns", "testobj", "testrel"),
			ObjectAndRelation("tenant/user", "testusr", "..."),
		),
		relFormat:              rel("tenant/testns", "testobj", "testrel", "tenant/user", "testusr", ""),
		stableCanonicalization: "dGVuYW50L3Rlc3Ruczp0ZXN0b2JqI3Rlc3RyZWxAdGVuYW50L3VzZXI6dGVzdHVzciMuLi4=",
	},
	{
		input:          "tenant/testns:testobj#testrel@tenant/user:testusr#...",
		expectedOutput: "tenant/testns:testobj#testrel@tenant/user:testusr",
		tupleFormat: makeTuple(
			ObjectAndRelation("tenant/testns", "testobj", "testrel"),
			ObjectAndRelation("tenant/user", "testusr", "..."),
		),
		relFormat:              rel("tenant/testns", "testobj", "testrel", "tenant/user", "testusr", ""),
		stableCanonicalization: "dGVuYW50L3Rlc3Ruczp0ZXN0b2JqI3Rlc3RyZWxAdGVuYW50L3VzZXI6dGVzdHVzciMuLi4=",
	},
	{
		input:          "tenant/testns:testobj#testrel@tenant/user:testusr#somerel",
		expectedOutput: "tenant/testns:testobj#testrel@tenant/user:testusr#somerel",
		tupleFormat: makeTuple(
			ObjectAndRelation("tenant/testns", "testobj", "testrel"),
			ObjectAndRelation("tenant/user", "testusr", "somerel"),
		),
		relFormat:              rel("tenant/testns", "testobj", "testrel", "tenant/user", "testusr", "somerel"),
		stableCanonicalization: "dGVuYW50L3Rlc3Ruczp0ZXN0b2JqI3Rlc3RyZWxAdGVuYW50L3VzZXI6dGVzdHVzciNzb21lcmVs",
	},
	{
		input:          "org/division/team/testns:testobj#testrel@org/division/identity_team/user:testusr#somerel",
		expectedOutput: "org/division/team/testns:testobj#testrel@org/division/identity_team/user:testusr#somerel",
		tupleFormat: makeTuple(
			ObjectAndRelation("org/division/team/testns", "testobj", "testrel"),
			ObjectAndRelation("org/division/identity_team/user", "testusr", "somerel"),
		),
		relFormat:              rel("org/division/team/testns", "testobj", "testrel", "org/division/identity_team/user", "testusr", "somerel"),
		stableCanonicalization: "b3JnL2RpdmlzaW9uL3RlYW0vdGVzdG5zOnRlc3RvYmojdGVzdHJlbEBvcmcvZGl2aXNpb24vaWRlbnRpdHlfdGVhbS91c2VyOnRlc3R1c3Ijc29tZXJlbA==",
	},
	{
		input:          "tenant/testns:testobj#testrel@tenant/user:testusr something",
		expectedOutput: "tenant/testns:testobj#testrel@tenant/user:testusr",
		tupleFormat:    nil,
		relFormat:      nil,
	},
	{
		input:          "tenant/testns:testobj#testrel@tenant/user:testusr:",
		expectedOutput: "tenant/testns:testobj#testrel@tenant/user:testusr",
		tupleFormat:    nil,
		relFormat:      nil,
	},
	{
		input:          "tenant/testns:testobj#testrel@tenant/user:testusr#",
		expectedOutput: "tenant/testns:testobj#testrel@tenant/user:testusr",
		tupleFormat:    nil,
		relFormat:      nil,
	},
	{
		input:          "",
		expectedOutput: "",
		tupleFormat:    nil,
		relFormat:      nil,
	},
	{
		input:          "foos:bar#bazzy@groo:grar#...",
		expectedOutput: "foos:bar#bazzy@groo:grar",
		tupleFormat: makeTuple(
			ObjectAndRelation("foos", "bar", "bazzy"),
			ObjectAndRelation("groo", "grar", "..."),
		),
		relFormat:              rel("foos", "bar", "bazzy", "groo", "grar", ""),
		stableCanonicalization: "Zm9vczpiYXIjYmF6enlAZ3JvbzpncmFyIy4uLg==",
	},
	{
		input:          "tenant/testns:testobj#testrel@tenant/user:*#...",
		expectedOutput: "tenant/testns:testobj#testrel@tenant/user:*",
		tupleFormat: makeTuple(
			ObjectAndRelation("tenant/testns", "testobj", "testrel"),
			ObjectAndRelation("tenant/user", "*", "..."),
		),
		relFormat:              rel("tenant/testns", "testobj", "testrel", "tenant/user", "*", ""),
		stableCanonicalization: "dGVuYW50L3Rlc3Ruczp0ZXN0b2JqI3Rlc3RyZWxAdGVuYW50L3VzZXI6KiMuLi4=",
	},
	{
		input:          "tenant/testns:testobj#testrel@tenant/user:authn|foo",
		expectedOutput: "tenant/testns:testobj#testrel@tenant/user:authn|foo",
		tupleFormat: makeTuple(
			ObjectAndRelation("tenant/testns", "testobj", "testrel"),
			ObjectAndRelation("tenant/user", "authn|foo", "..."),
		),
		relFormat:              rel("tenant/testns", "testobj", "testrel", "tenant/user", "authn|foo", ""),
		stableCanonicalization: "dGVuYW50L3Rlc3Ruczp0ZXN0b2JqI3Rlc3RyZWxAdGVuYW50L3VzZXI6YXV0aG58Zm9vIy4uLg==",
	},
	{
		input:          "document:foo#viewer@user:tom[somecaveat]",
		expectedOutput: "document:foo#viewer@user:tom[somecaveat]",
		tupleFormat: MustWithCaveat(
			makeTuple(
				ObjectAndRelation("document", "foo", "viewer"),
				ObjectAndRelation("user", "tom", "..."),
			),
			"somecaveat",
		),
		relFormat:              crel("document", "foo", "viewer", "user", "tom", "", "somecaveat", nil),
		stableCanonicalization: "ZG9jdW1lbnQ6Zm9vI3ZpZXdlckB1c2VyOnRvbSMuLi4gd2l0aCBzb21lY2F2ZWF0",
	},
	{
		input:          "document:foo#viewer@user:tom[tenant/somecaveat]",
		expectedOutput: "document:foo#viewer@user:tom[tenant/somecaveat]",
		tupleFormat: MustWithCaveat(
			makeTuple(
				ObjectAndRelation("document", "foo", "viewer"),
				ObjectAndRelation("user", "tom", "..."),
			),
			"tenant/somecaveat",
		),
		relFormat:              crel("document", "foo", "viewer", "user", "tom", "", "tenant/somecaveat", nil),
		stableCanonicalization: "ZG9jdW1lbnQ6Zm9vI3ZpZXdlckB1c2VyOnRvbSMuLi4gd2l0aCB0ZW5hbnQvc29tZWNhdmVhdA==",
	},
	{
		input:          "document:foo#viewer@user:tom[tenant/division/somecaveat]",
		expectedOutput: "document:foo#viewer@user:tom[tenant/division/somecaveat]",
		tupleFormat: MustWithCaveat(
			makeTuple(
				ObjectAndRelation("document", "foo", "viewer"),
				ObjectAndRelation("user", "tom", "..."),
			),
			"tenant/division/somecaveat",
		),
		relFormat:              crel("document", "foo", "viewer", "user", "tom", "", "tenant/division/somecaveat", nil),
		stableCanonicalization: "ZG9jdW1lbnQ6Zm9vI3ZpZXdlckB1c2VyOnRvbSMuLi4gd2l0aCB0ZW5hbnQvZGl2aXNpb24vc29tZWNhdmVhdA==",
	},
	{
		input:          "document:foo#viewer@user:tom[somecaveat",
		expectedOutput: "",
		tupleFormat:    nil,
		relFormat:      nil,
	},
	{
		input:          "document:foo#viewer@user:tom[]",
		expectedOutput: "",
		tupleFormat:    nil,
		relFormat:      nil,
	},
	{
		input:          `document:foo#viewer@user:tom[somecaveat:{"hi": "there"}]`,
		expectedOutput: `document:foo#viewer@user:tom[somecaveat:{"hi":"there"}]`,
		tupleFormat: MustWithCaveat(
			makeTuple(
				ObjectAndRelation("document", "foo", "viewer"),
				ObjectAndRelation("user", "tom", "..."),
			),
			"somecaveat",
			map[string]any{
				"hi": "there",
			},
		),
		relFormat:              crel("document", "foo", "viewer", "user", "tom", "", "somecaveat", map[string]any{"hi": "there"}),
		stableCanonicalization: "ZG9jdW1lbnQ6Zm9vI3ZpZXdlckB1c2VyOnRvbSMuLi4gd2l0aCBzb21lY2F2ZWF0OntoaTp0aGVyZX0=",
	},
	{
		input:          `document:foo#viewer@user:tom[somecaveat:{"hi":{"yo": 123}}]`,
		expectedOutput: `document:foo#viewer@user:tom[somecaveat:{"hi":{"yo":123}}]`,
		tupleFormat: MustWithCaveat(
			makeTuple(
				ObjectAndRelation("document", "foo", "viewer"),
				ObjectAndRelation("user", "tom", "..."),
			),
			"somecaveat",
			map[string]any{
				"hi": map[string]any{
					"yo": 123,
				},
			},
		),
		relFormat: crel("document", "foo", "viewer", "user", "tom", "", "somecaveat", map[string]any{
			"hi": map[string]any{
				"yo": 123,
			},
		}),
		stableCanonicalization: "ZG9jdW1lbnQ6Zm9vI3ZpZXdlckB1c2VyOnRvbSMuLi4gd2l0aCBzb21lY2F2ZWF0OntoaTp7eW86MTIzLjAwMDAwMH19",
	},
	{
		input:          `document:foo#viewer@user:tom[somecaveat:{"hi":{"yo":{"hey":true}}}]`,
		expectedOutput: `document:foo#viewer@user:tom[somecaveat:{"hi":{"yo":{"hey":true}}}]`,
		tupleFormat: MustWithCaveat(
			makeTuple(
				ObjectAndRelation("document", "foo", "viewer"),
				ObjectAndRelation("user", "tom", "..."),
			),
			"somecaveat",
			map[string]any{
				"hi": map[string]any{
					"yo": map[string]any{
						"hey": true,
					},
				},
			},
		),
		relFormat: crel("document", "foo", "viewer", "user", "tom", "", "somecaveat", map[string]any{
			"hi": map[string]any{
				"yo": map[string]any{
					"hey": true,
				},
			},
		}),
		stableCanonicalization: "ZG9jdW1lbnQ6Zm9vI3ZpZXdlckB1c2VyOnRvbSMuLi4gd2l0aCBzb21lY2F2ZWF0OntoaTp7eW86e2hleTp0cnVlfX19",
	},
	{
		input:          `document:foo#viewer@user:tom[somecaveat:{"hi":{"yo":{"hey":[1,2,3]}}}]`,
		expectedOutput: `document:foo#viewer@user:tom[somecaveat:{"hi":{"yo":{"hey":[1,2,3]}}}]`,
		tupleFormat: MustWithCaveat(
			makeTuple(
				ObjectAndRelation("document", "foo", "viewer"),
				ObjectAndRelation("user", "tom", "..."),
			),
			"somecaveat",
			map[string]any{
				"hi": map[string]any{
					"yo": map[string]any{
						"hey": []any{1, 2, 3},
					},
				},
			},
		),
		relFormat: crel("document", "foo", "viewer", "user", "tom", "", "somecaveat", map[string]any{
			"hi": map[string]any{
				"yo": map[string]any{
					"hey": []any{1, 2, 3},
				},
			},
		}),
		stableCanonicalization: "ZG9jdW1lbnQ6Zm9vI3ZpZXdlckB1c2VyOnRvbSMuLi4gd2l0aCBzb21lY2F2ZWF0OntoaTp7eW86e2hleTpbMS4wMDAwMDAsMi4wMDAwMDAsMy4wMDAwMDBdfX19",
	},
	{
		input:          `document:foo#viewer@user:tom[somecaveat:{"hi":{"yo":"hey":true}}}]`,
		expectedOutput: "",
		tupleFormat:    nil,
		relFormat:      nil,
	},
	{
		input:          "testns:" + superLongID + "#testrel@user:testusr",
		expectedOutput: "testns:" + superLongID + "#testrel@user:testusr",
		tupleFormat: makeTuple(
			ObjectAndRelation("testns", superLongID, "testrel"),
			ObjectAndRelation("user", "testusr", "..."),
		),
		relFormat:              rel("testns", superLongID, "testrel", "user", "testusr", ""),
		stableCanonicalization: "dGVzdG5zOmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmYjdGVzdHJlbEB1c2VyOnRlc3R1c3IjLi4u",
	},
	{
		input:          "testns:foo#testrel@user:" + superLongID,
		expectedOutput: "testns:foo#testrel@user:" + superLongID,
		tupleFormat: makeTuple(
			ObjectAndRelation("testns", "foo", "testrel"),
			ObjectAndRelation("user", superLongID, "..."),
		),
		relFormat:              rel("testns", "foo", "testrel", "user", superLongID, ""),
		stableCanonicalization: "dGVzdG5zOmZvbyN0ZXN0cmVsQHVzZXI6ZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZiMuLi4=",
	},
	{
		input:          "testns:foo#testrel@user:" + superLongID + "more",
		expectedOutput: "",
		tupleFormat:    nil,
		relFormat:      nil,
	},
	{
		input:          "testns:-base64YWZzZGZh-ZHNmZHPwn5iK8J+YivC/fmIrwn5iK==#testrel@user:-base65YWZzZGZh-ZHNmZHPwn5iK8J+YivC/fmIrwn5iK==",
		expectedOutput: "testns:-base64YWZzZGZh-ZHNmZHPwn5iK8J+YivC/fmIrwn5iK==#testrel@user:-base65YWZzZGZh-ZHNmZHPwn5iK8J+YivC/fmIrwn5iK==",
		tupleFormat: makeTuple(
			ObjectAndRelation("testns", "-base64YWZzZGZh-ZHNmZHPwn5iK8J+YivC/fmIrwn5iK==", "testrel"),
			ObjectAndRelation("user", "-base65YWZzZGZh-ZHNmZHPwn5iK8J+YivC/fmIrwn5iK==", "..."),
		),
		relFormat:              rel("testns", "-base64YWZzZGZh-ZHNmZHPwn5iK8J+YivC/fmIrwn5iK==", "testrel", "user", "-base65YWZzZGZh-ZHNmZHPwn5iK8J+YivC/fmIrwn5iK==", ""),
		stableCanonicalization: "dGVzdG5zOi1iYXNlNjRZV1p6WkdaaC1aSE5tWkhQd241aUs4SitZaXZDL2ZtSXJ3bjVpSz09I3Rlc3RyZWxAdXNlcjotYmFzZTY1WVdaelpHWmgtWkhObVpIUHduNWlLOEorWWl2Qy9mbUlyd241aUs9PSMuLi4=",
	},
	{
		input:          `document:foo#viewer@user:tom[somecaveat:{"hi":"a@example.com"}]`,
		expectedOutput: `document:foo#viewer@user:tom[somecaveat:{"hi":"a@example.com"}]`,
		tupleFormat: MustWithCaveat(
			makeTuple(
				ObjectAndRelation("document", "foo", "viewer"),
				ObjectAndRelation("user", "tom", "..."),
			),
			"somecaveat",
			map[string]any{
				"hi": "a@example.com",
			},
		),
		relFormat: crel("document", "foo", "viewer", "user", "tom", "", "somecaveat", map[string]any{
			"hi": "a@example.com",
		}),
		stableCanonicalization: "ZG9jdW1lbnQ6Zm9vI3ZpZXdlckB1c2VyOnRvbSMuLi4gd2l0aCBzb21lY2F2ZWF0OntoaTphQGV4YW1wbGUuY29tfQ==",
	},
	{
		input:          `document:foo#viewer@user:tom[somecaveat:{"first":"a@example.com", "second": "b@example.com"}]`,
		expectedOutput: `document:foo#viewer@user:tom[somecaveat:{"first":"a@example.com","second":"b@example.com"}]`,
		tupleFormat: MustWithCaveat(
			makeTuple(
				ObjectAndRelation("document", "foo", "viewer"),
				ObjectAndRelation("user", "tom", "..."),
			),
			"somecaveat",
			map[string]any{
				"first":  "a@example.com",
				"second": "b@example.com",
			},
		),
		relFormat: crel("document", "foo", "viewer", "user", "tom", "", "somecaveat", map[string]any{
			"first":  "a@example.com",
			"second": "b@example.com",
		}),
		stableCanonicalization: "ZG9jdW1lbnQ6Zm9vI3ZpZXdlckB1c2VyOnRvbSMuLi4gd2l0aCBzb21lY2F2ZWF0OntmaXJzdDphQGV4YW1wbGUuY29tLHNlY29uZDpiQGV4YW1wbGUuY29tfQ==",
	},
	{
		input:          `document:foo#viewer@user:tom[somecaveat:{"second": "b@example.com", "first":"a@example.com"}]`,
		expectedOutput: `document:foo#viewer@user:tom[somecaveat:{"first":"a@example.com","second":"b@example.com"}]`,
		tupleFormat: MustWithCaveat(
			makeTuple(
				ObjectAndRelation("document", "foo", "viewer"),
				ObjectAndRelation("user", "tom", "..."),
			),
			"somecaveat",
			map[string]any{
				"first":  "a@example.com",
				"second": "b@example.com",
			},
		),
		relFormat: crel("document", "foo", "viewer", "user", "tom", "", "somecaveat", map[string]any{
			"first":  "a@example.com",
			"second": "b@example.com",
		}),
		stableCanonicalization: "ZG9jdW1lbnQ6Zm9vI3ZpZXdlckB1c2VyOnRvbSMuLi4gd2l0aCBzb21lY2F2ZWF0OntmaXJzdDphQGV4YW1wbGUuY29tLHNlY29uZDpiQGV4YW1wbGUuY29tfQ==",
	},
}

func TestCanonicalBytes(t *testing.T) {
	foundBytes := make(map[string]string)

	for _, tc := range testCases {
		if tc.tupleFormat == nil {
			continue
		}

		tc := tc

		t.Run(tc.input, func(t *testing.T) {
			// Ensure the serialization is stable.
			serialized, err := CanonicalBytes(tc.tupleFormat)
			require.NoError(t, err)

			encoded := b64.StdEncoding.EncodeToString(serialized)
			require.Equal(t, tc.stableCanonicalization, encoded)

			// Ensure the serialization is unique.
			existing, ok := foundBytes[string(serialized)]
			if ok {
				parsedInput := MustParse(tc.input)
				parsedExisting := MustParse(existing)

				require.True(t, parsedInput.EqualVT(parsedExisting), "duplicate canonical bytes found. input: %s; found for input: %s", tc.input, existing)
			}
			foundBytes[string(serialized)] = tc.input
		})
	}
}

func BenchmarkMustCanonicalBytes(b *testing.B) {
	for _, tc := range testCases {
		tc := tc
		if tc.tupleFormat == nil {
			continue
		}

		b.Run(tc.input, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, err := CanonicalBytes(tc.tupleFormat)
				require.NoError(b, err)
			}
		})
	}
}

func TestSerialize(t *testing.T) {
	for _, tc := range testCases {
		tc := tc
		t.Run("tuple/"+tc.input, func(t *testing.T) {
			if tc.tupleFormat == nil {
				return
			}

			serialized := strings.Replace(MustString(tc.tupleFormat), " ", "", -1)
			require.Equal(t, tc.expectedOutput, serialized)

			withoutCaveat := StringWithoutCaveat(tc.tupleFormat)
			require.Contains(t, tc.expectedOutput, withoutCaveat)
			require.NotContains(t, withoutCaveat, "[")
		})
	}

	for _, tc := range testCases {
		tc := tc
		t.Run("relationship/"+tc.input, func(t *testing.T) {
			if tc.relFormat == nil {
				return
			}

			serialized := strings.Replace(MustRelString(tc.relFormat), " ", "", -1)
			require.Equal(t, tc.expectedOutput, serialized)

			withoutCaveat := StringRelationshipWithoutCaveat(tc.relFormat)
			require.Contains(t, tc.expectedOutput, withoutCaveat)
			require.NotContains(t, withoutCaveat, "[")
		})
	}
}

func TestParse(t *testing.T) {
	for _, tc := range testCases {
		tc := tc
		t.Run("tuple/"+tc.input, func(t *testing.T) {
			testutil.RequireProtoEqual(t, tc.tupleFormat, Parse(tc.input), "found difference in parsed tuple")
		})
	}

	for _, tc := range testCases {
		tc := tc
		t.Run("relationship/"+tc.input, func(t *testing.T) {
			testutil.RequireProtoEqual(t, tc.relFormat, ParseRel(tc.input), "found difference in parsed relationship")
		})
	}
}

func TestConvert(t *testing.T) {
	for _, tc := range testCases {
		tc := tc
		t.Run(tc.input, func(t *testing.T) {
			require := require.New(t)

			parsed := Parse(tc.input)
			testutil.RequireProtoEqual(t, tc.tupleFormat, parsed, "found difference in parsed tuple")
			if parsed == nil {
				return
			}

			relationship := ToRelationship(parsed)
			relString := strings.Replace(MustRelString(relationship), " ", "", -1)
			require.Equal(tc.expectedOutput, relString)

			backToTpl := FromRelationship[*v1.ObjectReference, *v1.SubjectReference, *v1.ContextualizedCaveat](relationship)
			testutil.RequireProtoEqual(t, tc.tupleFormat, backToTpl, "found difference in converted tuple")

			serialized := strings.Replace(MustString(backToTpl), " ", "", -1)
			require.Equal(tc.expectedOutput, serialized)
		})
	}
}

func TestValidate(t *testing.T) {
	for _, tc := range testCases {
		tc := tc
		t.Run("validate/"+tc.input, func(t *testing.T) {
			parsed := ParseRel(tc.input)
			if parsed != nil {
				require.NoError(t, ValidateResourceID(parsed.Resource.ObjectId))
				require.NoError(t, ValidateSubjectID(parsed.Subject.Object.ObjectId))
			}
		})
	}
}

func TestCopyRelationTupleToRelationship(t *testing.T) {
	testCases := []*core.RelationTuple{
		{
			ResourceAndRelation: &core.ObjectAndRelation{
				Namespace: "abc",
				ObjectId:  "def",
				Relation:  "ghi",
			},
			Subject: &core.ObjectAndRelation{
				Namespace: "jkl",
				ObjectId:  "mno",
				Relation:  "pqr",
			},
		},
		{
			ResourceAndRelation: &core.ObjectAndRelation{
				Namespace: "abc",
				ObjectId:  "def",
				Relation:  "ghi",
			},
			Subject: &core.ObjectAndRelation{
				Namespace: "jkl",
				ObjectId:  "mno",
				Relation:  "...",
			},
		},
		{
			ResourceAndRelation: &core.ObjectAndRelation{
				Namespace: "abc",
				ObjectId:  "def",
				Relation:  "ghi",
			},
			Subject: &core.ObjectAndRelation{
				Namespace: "jkl",
				ObjectId:  "mno",
				Relation:  "pqr",
			},
			Caveat: &core.ContextualizedCaveat{
				CaveatName: "stu",
				Context:    &structpb.Struct{},
			},
		},
		{
			ResourceAndRelation: &core.ObjectAndRelation{
				Namespace: "abc",
				ObjectId:  "def",
				Relation:  "ghi",
			},
			Subject: &core.ObjectAndRelation{
				Namespace: "jkl",
				ObjectId:  "mno",
				Relation:  "pqr",
			},
			Caveat: &core.ContextualizedCaveat{
				CaveatName: "stu",
				Context: &structpb.Struct{
					Fields: map[string]*structpb.Value{
						"vwx": {
							Kind: &structpb.Value_StringValue{
								StringValue: "yz",
							},
						},
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(MustString(tc), func(t *testing.T) {
			require := require.New(t)

			dst := &v1.Relationship{
				Resource: &v1.ObjectReference{},
				Subject: &v1.SubjectReference{
					Object: &v1.ObjectReference{},
				},
			}
			optionalCaveat := &v1.ContextualizedCaveat{}

			CopyRelationTupleToRelationship(tc, dst, optionalCaveat)

			expectedSubjectRelation := tc.Subject.Relation
			if tc.Subject.Relation == "..." {
				expectedSubjectRelation = ""
			}

			require.Equal(tc.ResourceAndRelation.Namespace, dst.Resource.ObjectType)
			require.Equal(tc.ResourceAndRelation.ObjectId, dst.Resource.ObjectId)
			require.Equal(tc.ResourceAndRelation.Relation, dst.Relation)
			require.Equal(tc.Subject.Namespace, dst.Subject.Object.ObjectType)
			require.Equal(tc.Subject.ObjectId, dst.Subject.Object.ObjectId)
			require.Equal(expectedSubjectRelation, dst.Subject.OptionalRelation)

			if tc.Caveat != nil {
				require.Equal(tc.Caveat.CaveatName, dst.OptionalCaveat.CaveatName)
				require.Equal(tc.Caveat.Context, dst.OptionalCaveat.Context)
			} else {
				require.Nil(dst.OptionalCaveat)
			}
		})
	}
}

func TestEqual(t *testing.T) {
	equalTestCases := []*core.RelationTuple{
		makeTuple(
			ObjectAndRelation("testns", "testobj", "testrel"),
			ObjectAndRelation("user", "testusr", "..."),
		),
		MustWithCaveat(
			makeTuple(
				ObjectAndRelation("testns", "testobj", "testrel"),
				ObjectAndRelation("user", "testusr", "..."),
			),
			"somecaveat",
			map[string]any{
				"context": map[string]any{
					"deeply": map[string]any{
						"nested": true,
					},
				},
			},
		),
		MustParse("document:foo#viewer@user:tom[somecaveat:{\"hi\":\"there\"}]"),
		MustParse("document:foo#viewer@user:tom[somecaveat:{\"hi\":{\"yo\":123}}]"),
		MustParse("document:foo#viewer@user:tom[somecaveat:{\"hi\":{\"yo\":{\"hey\":true}}, \"hi2\":{\"yo2\":{\"hey2\":false}}}]"),
		MustParse("document:foo#viewer@user:tom[somecaveat:{\"hi\":{\"yo\":{\"hey\":true}}, \"hi2\":{\"yo2\":{\"hey2\":[1,2,3]}}}]"),
	}

	for _, tc := range equalTestCases {
		t.Run(MustString(tc), func(t *testing.T) {
			require := require.New(t)
			require.True(Equal(tc, tc.CloneVT()))
			require.True(Equal(tc, MustParse(MustString(tc))))
		})
	}

	notEqualTestCases := []struct {
		name string
		lhs  *core.RelationTuple
		rhs  *core.RelationTuple
	}{
		{
			name: "Mismatch Resource Type",
			lhs: makeTuple(
				ObjectAndRelation("testns1", "testobj", "testrel"),
				ObjectAndRelation("user", "testusr", "..."),
			),
			rhs: makeTuple(
				ObjectAndRelation("testns2", "testobj", "testrel"),
				ObjectAndRelation("user", "testusr", "..."),
			),
		},
		{
			name: "Mismatch Resource ID",
			lhs: makeTuple(
				ObjectAndRelation("testns", "testobj1", "testrel"),
				ObjectAndRelation("user", "testusr", "..."),
			),
			rhs: makeTuple(
				ObjectAndRelation("testns", "testobj2", "testrel"),
				ObjectAndRelation("user", "testusr", "..."),
			),
		},
		{
			name: "Mismatch Resource Relationship",
			lhs: makeTuple(
				ObjectAndRelation("testns", "testobj", "testrel1"),
				ObjectAndRelation("user", "testusr", "..."),
			),
			rhs: makeTuple(
				ObjectAndRelation("testns", "testobj", "testrel2"),
				ObjectAndRelation("user", "testusr", "..."),
			),
		},
		{
			name: "Mismatch Subject Type",
			lhs: makeTuple(
				ObjectAndRelation("testns", "testobj", "testrel"),
				ObjectAndRelation("user1", "testusr", "..."),
			),
			rhs: makeTuple(
				ObjectAndRelation("testns", "testobj", "testrel"),
				ObjectAndRelation("user2", "testusr", "..."),
			),
		},
		{
			name: "Mismatch Subject ID",
			lhs: makeTuple(
				ObjectAndRelation("testns", "testobj", "testrel"),
				ObjectAndRelation("user", "testusr1", "..."),
			),
			rhs: makeTuple(
				ObjectAndRelation("testns", "testobj", "testrel"),
				ObjectAndRelation("user", "testusr2", "..."),
			),
		},
		{
			name: "Mismatch Subject Relationship",
			lhs: makeTuple(
				ObjectAndRelation("testns", "testobj", "testrel"),
				ObjectAndRelation("user", "testusr", "testrel1"),
			),
			rhs: makeTuple(
				ObjectAndRelation("testns", "testobj", "testrel"),
				ObjectAndRelation("user", "testusr", "testrel2"),
			),
		},
		{
			name: "Mismatch Caveat Name",
			lhs: MustWithCaveat(
				makeTuple(
					ObjectAndRelation("testns", "testobj", "testrel"),
					ObjectAndRelation("user", "testusr", "..."),
				),
				"somecaveat1",
				map[string]any{
					"context": map[string]any{
						"deeply": map[string]any{
							"nested": true,
						},
					},
				},
			),
			rhs: MustWithCaveat(
				makeTuple(
					ObjectAndRelation("testns", "testobj", "testrel"),
					ObjectAndRelation("user", "testusr", "..."),
				),
				"somecaveat2",
				map[string]any{
					"context": map[string]any{
						"deeply": map[string]any{
							"nested": true,
						},
					},
				},
			),
		},
		{
			name: "Mismatch Caveat Content",
			lhs: MustWithCaveat(
				makeTuple(
					ObjectAndRelation("testns", "testobj", "testrel"),
					ObjectAndRelation("user", "testusr", "..."),
				),
				"somecaveat",
				map[string]any{
					"context": map[string]any{
						"deeply": map[string]any{
							"nested": "1",
						},
					},
				},
			),
			rhs: MustWithCaveat(
				makeTuple(
					ObjectAndRelation("testns", "testobj", "testrel"),
					ObjectAndRelation("user", "testusr", "..."),
				),
				"somecaveat",
				map[string]any{
					"context": map[string]any{
						"deeply": map[string]any{
							"nested": "2",
						},
					},
				},
			),
		},
		{
			name: "missing caveat context via string",
			lhs:  MustParse("document:foo#viewer@user:tom[somecaveat:{\"hi\":\"there\"}]"),
			rhs:  MustParse("document:foo#viewer@user:tom[somecaveat]"),
		},
		{
			name: "mismatch caveat context via string",
			lhs:  MustParse("document:foo#viewer@user:tom[somecaveat:{\"hi\":\"there\"}]"),
			rhs:  MustParse("document:foo#viewer@user:tom[somecaveat:{\"hi\":\"there2\"}]"),
		},
		{
			name: "mismatch caveat name",
			lhs:  MustParse("document:foo#viewer@user:tom[somecaveat]"),
			rhs:  MustParse("document:foo#viewer@user:tom[somecaveat2]"),
		},
		{
			name: "mismatch caveat context, deeply nested",
			lhs:  MustParse("document:foo#viewer@user:tom[somecaveat:{\"hi\":{\"yo\":123}}]"),
			rhs:  MustParse("document:foo#viewer@user:tom[somecaveat:{\"hi\":{\"yo\":124}}]"),
		},
		{
			name: "mismatch caveat context, deeply nested with array",
			lhs:  MustParse("document:foo#viewer@user:tom[somecaveat:{\"hi\":{\"yo\":[1,2,3]}}]"),
			rhs:  MustParse("document:foo#viewer@user:tom[somecaveat:{\"hi\":{\"yo\":[1,2,4]}}]"),
		},
	}

	for _, tc := range notEqualTestCases {
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)
			require.False(Equal(tc.lhs, tc.rhs))
			require.False(Equal(tc.rhs, tc.lhs))
			require.False(Equal(tc.lhs, MustParse(MustString(tc.rhs))))
			require.False(Equal(tc.rhs, MustParse(MustString(tc.lhs))))
		})
	}
}
