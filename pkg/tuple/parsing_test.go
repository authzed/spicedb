package tuple

import (
	"strings"
	"testing"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/authzed/spicedb/pkg/testutil"
)

func makeRel(onr ObjectAndRelation, subject ObjectAndRelation) Relationship {
	return Relationship{
		RelationshipReference: RelationshipReference{
			Resource: onr,
			Subject:  subject,
		},
	}
}

func v1rel(resType, resID, relation, subType, subID, subRel string) *v1.Relationship {
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

func cv1rel(resType, resID, relation, subType, subID, subRel, caveatName string, caveatContext map[string]any) *v1.Relationship {
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
	relFormat              Relationship
	v1Format               *v1.Relationship
	stableCanonicalization string
}{
	{
		input:          "testns:testobj#testrel@user:testusr",
		expectedOutput: "testns:testobj#testrel@user:testusr",
		relFormat: makeRel(
			StringToONR("testns", "testobj", "testrel"),
			StringToONR("user", "testusr", "..."),
		),
		v1Format:               v1rel("testns", "testobj", "testrel", "user", "testusr", ""),
		stableCanonicalization: "dGVzdG5zOnRlc3RvYmojdGVzdHJlbEB1c2VyOnRlc3R1c3IjLi4u",
	},
	{
		input:          "testns:testobj#testrel@user:testusr#...",
		expectedOutput: "testns:testobj#testrel@user:testusr",
		relFormat: makeRel(
			StringToONR("testns", "testobj", "testrel"),
			StringToONR("user", "testusr", "..."),
		),
		v1Format:               v1rel("testns", "testobj", "testrel", "user", "testusr", ""),
		stableCanonicalization: "dGVzdG5zOnRlc3RvYmojdGVzdHJlbEB1c2VyOnRlc3R1c3IjLi4u",
	},
	{
		input:          "tenant/testns:testobj#testrel@tenant/user:testusr",
		expectedOutput: "tenant/testns:testobj#testrel@tenant/user:testusr",
		relFormat: makeRel(
			StringToONR("tenant/testns", "testobj", "testrel"),
			StringToONR("tenant/user", "testusr", "..."),
		),
		v1Format:               v1rel("tenant/testns", "testobj", "testrel", "tenant/user", "testusr", ""),
		stableCanonicalization: "dGVuYW50L3Rlc3Ruczp0ZXN0b2JqI3Rlc3RyZWxAdGVuYW50L3VzZXI6dGVzdHVzciMuLi4=",
	},
	{
		input:          "tenant/testns:testobj#testrel@tenant/user:testusr#...",
		expectedOutput: "tenant/testns:testobj#testrel@tenant/user:testusr",
		relFormat: makeRel(
			StringToONR("tenant/testns", "testobj", "testrel"),
			StringToONR("tenant/user", "testusr", "..."),
		),
		v1Format:               v1rel("tenant/testns", "testobj", "testrel", "tenant/user", "testusr", ""),
		stableCanonicalization: "dGVuYW50L3Rlc3Ruczp0ZXN0b2JqI3Rlc3RyZWxAdGVuYW50L3VzZXI6dGVzdHVzciMuLi4=",
	},
	{
		input:          "tenant/testns:testobj#testrel@tenant/user:testusr#somerel",
		expectedOutput: "tenant/testns:testobj#testrel@tenant/user:testusr#somerel",
		relFormat: makeRel(
			StringToONR("tenant/testns", "testobj", "testrel"),
			StringToONR("tenant/user", "testusr", "somerel"),
		),
		v1Format:               v1rel("tenant/testns", "testobj", "testrel", "tenant/user", "testusr", "somerel"),
		stableCanonicalization: "dGVuYW50L3Rlc3Ruczp0ZXN0b2JqI3Rlc3RyZWxAdGVuYW50L3VzZXI6dGVzdHVzciNzb21lcmVs",
	},
	{
		input:          "org/division/team/testns:testobj#testrel@org/division/identity_team/user:testusr#somerel",
		expectedOutput: "org/division/team/testns:testobj#testrel@org/division/identity_team/user:testusr#somerel",
		relFormat: makeRel(
			StringToONR("org/division/team/testns", "testobj", "testrel"),
			StringToONR("org/division/identity_team/user", "testusr", "somerel"),
		),
		v1Format:               v1rel("org/division/team/testns", "testobj", "testrel", "org/division/identity_team/user", "testusr", "somerel"),
		stableCanonicalization: "b3JnL2RpdmlzaW9uL3RlYW0vdGVzdG5zOnRlc3RvYmojdGVzdHJlbEBvcmcvZGl2aXNpb24vaWRlbnRpdHlfdGVhbS91c2VyOnRlc3R1c3Ijc29tZXJlbA==",
	},
	{
		input:          "tenant/testns:testobj#testrel@tenant/user:testusr something",
		expectedOutput: "tenant/testns:testobj#testrel@tenant/user:testusr",
	},
	{
		input:          "tenant/testns:testobj#testrel@tenant/user:testusr:",
		expectedOutput: "tenant/testns:testobj#testrel@tenant/user:testusr",
	},
	{
		input:          "tenant/testns:testobj#testrel@tenant/user:testusr#",
		expectedOutput: "tenant/testns:testobj#testrel@tenant/user:testusr",
	},
	{
		input:          "",
		expectedOutput: "",
	},
	{
		input:          "foos:bar#bazzy@groo:grar#...",
		expectedOutput: "foos:bar#bazzy@groo:grar",
		relFormat: makeRel(
			StringToONR("foos", "bar", "bazzy"),
			StringToONR("groo", "grar", "..."),
		),
		v1Format:               v1rel("foos", "bar", "bazzy", "groo", "grar", ""),
		stableCanonicalization: "Zm9vczpiYXIjYmF6enlAZ3JvbzpncmFyIy4uLg==",
	},
	{
		input:          "tenant/testns:testobj#testrel@tenant/user:*#...",
		expectedOutput: "tenant/testns:testobj#testrel@tenant/user:*",
		relFormat: makeRel(
			StringToONR("tenant/testns", "testobj", "testrel"),
			StringToONR("tenant/user", "*", "..."),
		),
		v1Format:               v1rel("tenant/testns", "testobj", "testrel", "tenant/user", "*", ""),
		stableCanonicalization: "dGVuYW50L3Rlc3Ruczp0ZXN0b2JqI3Rlc3RyZWxAdGVuYW50L3VzZXI6KiMuLi4=",
	},
	{
		input:          "tenant/testns:testobj#testrel@tenant/user:authn|foo",
		expectedOutput: "tenant/testns:testobj#testrel@tenant/user:authn|foo",
		relFormat: makeRel(
			StringToONR("tenant/testns", "testobj", "testrel"),
			StringToONR("tenant/user", "authn|foo", "..."),
		),
		v1Format:               v1rel("tenant/testns", "testobj", "testrel", "tenant/user", "authn|foo", ""),
		stableCanonicalization: "dGVuYW50L3Rlc3Ruczp0ZXN0b2JqI3Rlc3RyZWxAdGVuYW50L3VzZXI6YXV0aG58Zm9vIy4uLg==",
	},
	{
		input:          "document:foo#viewer@user:tom[somecaveat]",
		expectedOutput: "document:foo#viewer@user:tom[somecaveat]",
		relFormat: MustWithCaveat(
			makeRel(
				StringToONR("document", "foo", "viewer"),
				StringToONR("user", "tom", "..."),
			),
			"somecaveat",
		),
		v1Format:               cv1rel("document", "foo", "viewer", "user", "tom", "", "somecaveat", nil),
		stableCanonicalization: "ZG9jdW1lbnQ6Zm9vI3ZpZXdlckB1c2VyOnRvbSMuLi4gd2l0aCBzb21lY2F2ZWF0",
	},
	{
		input:          "document:foo#viewer@user:tom[tenant/somecaveat]",
		expectedOutput: "document:foo#viewer@user:tom[tenant/somecaveat]",
		relFormat: MustWithCaveat(
			makeRel(
				StringToONR("document", "foo", "viewer"),
				StringToONR("user", "tom", "..."),
			),
			"tenant/somecaveat",
		),
		v1Format:               cv1rel("document", "foo", "viewer", "user", "tom", "", "tenant/somecaveat", nil),
		stableCanonicalization: "ZG9jdW1lbnQ6Zm9vI3ZpZXdlckB1c2VyOnRvbSMuLi4gd2l0aCB0ZW5hbnQvc29tZWNhdmVhdA==",
	},
	{
		input:          "document:foo#viewer@user:tom[tenant/division/somecaveat]",
		expectedOutput: "document:foo#viewer@user:tom[tenant/division/somecaveat]",
		relFormat: MustWithCaveat(
			makeRel(
				StringToONR("document", "foo", "viewer"),
				StringToONR("user", "tom", "..."),
			),
			"tenant/division/somecaveat",
		),
		v1Format:               cv1rel("document", "foo", "viewer", "user", "tom", "", "tenant/division/somecaveat", nil),
		stableCanonicalization: "ZG9jdW1lbnQ6Zm9vI3ZpZXdlckB1c2VyOnRvbSMuLi4gd2l0aCB0ZW5hbnQvZGl2aXNpb24vc29tZWNhdmVhdA==",
	},
	{
		input:          "document:foo#viewer@user:tom[somecaveat",
		expectedOutput: "",
	},
	{
		input:          "document:foo#viewer@user:tom[]",
		expectedOutput: "",
	},
	{
		input:          `document:foo#viewer@user:tom[somecaveat:{"hi": "there"}]`,
		expectedOutput: `document:foo#viewer@user:tom[somecaveat:{"hi":"there"}]`,
		relFormat: MustWithCaveat(
			makeRel(
				StringToONR("document", "foo", "viewer"),
				StringToONR("user", "tom", "..."),
			),
			"somecaveat",
			map[string]any{
				"hi": "there",
			},
		),
		v1Format:               cv1rel("document", "foo", "viewer", "user", "tom", "", "somecaveat", map[string]any{"hi": "there"}),
		stableCanonicalization: "ZG9jdW1lbnQ6Zm9vI3ZpZXdlckB1c2VyOnRvbSMuLi4gd2l0aCBzb21lY2F2ZWF0OntoaTp0aGVyZX0=",
	},
	{
		input:          `document:foo#viewer@user:tom[somecaveat:{"hi":{"yo": 123}}]`,
		expectedOutput: `document:foo#viewer@user:tom[somecaveat:{"hi":{"yo":123}}]`,
		relFormat: MustWithCaveat(
			makeRel(
				StringToONR("document", "foo", "viewer"),
				StringToONR("user", "tom", "..."),
			),
			"somecaveat",
			map[string]any{
				"hi": map[string]any{
					"yo": 123,
				},
			},
		),
		v1Format: cv1rel("document", "foo", "viewer", "user", "tom", "", "somecaveat", map[string]any{
			"hi": map[string]any{
				"yo": 123,
			},
		}),
		stableCanonicalization: "ZG9jdW1lbnQ6Zm9vI3ZpZXdlckB1c2VyOnRvbSMuLi4gd2l0aCBzb21lY2F2ZWF0OntoaTp7eW86MTIzLjAwMDAwMH19",
	},
	{
		input:          `document:foo#viewer@user:tom[somecaveat:{"hi":{"yo":{"hey":true}}}]`,
		expectedOutput: `document:foo#viewer@user:tom[somecaveat:{"hi":{"yo":{"hey":true}}}]`,
		relFormat: MustWithCaveat(
			makeRel(
				StringToONR("document", "foo", "viewer"),
				StringToONR("user", "tom", "..."),
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
		v1Format: cv1rel("document", "foo", "viewer", "user", "tom", "", "somecaveat", map[string]any{
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
		relFormat: MustWithCaveat(
			makeRel(
				StringToONR("document", "foo", "viewer"),
				StringToONR("user", "tom", "..."),
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
		v1Format: cv1rel("document", "foo", "viewer", "user", "tom", "", "somecaveat", map[string]any{
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
	},
	{
		input:          "testns:" + superLongID + "#testrel@user:testusr",
		expectedOutput: "testns:" + superLongID + "#testrel@user:testusr",
		relFormat: makeRel(
			StringToONR("testns", superLongID, "testrel"),
			StringToONR("user", "testusr", "..."),
		),
		v1Format:               v1rel("testns", superLongID, "testrel", "user", "testusr", ""),
		stableCanonicalization: "dGVzdG5zOmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmYjdGVzdHJlbEB1c2VyOnRlc3R1c3IjLi4u",
	},
	{
		input:          "testns:foo#testrel@user:" + superLongID,
		expectedOutput: "testns:foo#testrel@user:" + superLongID,
		relFormat: makeRel(
			StringToONR("testns", "foo", "testrel"),
			StringToONR("user", superLongID, "..."),
		),
		v1Format:               v1rel("testns", "foo", "testrel", "user", superLongID, ""),
		stableCanonicalization: "dGVzdG5zOmZvbyN0ZXN0cmVsQHVzZXI6ZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZiMuLi4=",
	},
	{
		input:          "testns:foo#testrel@user:" + superLongID + "more",
		expectedOutput: "",
	},
	{
		input:          "testns:-base64YWZzZGZh-ZHNmZHPwn5iK8J+YivC/fmIrwn5iK==#testrel@user:-base65YWZzZGZh-ZHNmZHPwn5iK8J+YivC/fmIrwn5iK==",
		expectedOutput: "testns:-base64YWZzZGZh-ZHNmZHPwn5iK8J+YivC/fmIrwn5iK==#testrel@user:-base65YWZzZGZh-ZHNmZHPwn5iK8J+YivC/fmIrwn5iK==",
		relFormat: makeRel(
			StringToONR("testns", "-base64YWZzZGZh-ZHNmZHPwn5iK8J+YivC/fmIrwn5iK==", "testrel"),
			StringToONR("user", "-base65YWZzZGZh-ZHNmZHPwn5iK8J+YivC/fmIrwn5iK==", "..."),
		),
		v1Format:               v1rel("testns", "-base64YWZzZGZh-ZHNmZHPwn5iK8J+YivC/fmIrwn5iK==", "testrel", "user", "-base65YWZzZGZh-ZHNmZHPwn5iK8J+YivC/fmIrwn5iK==", ""),
		stableCanonicalization: "dGVzdG5zOi1iYXNlNjRZV1p6WkdaaC1aSE5tWkhQd241aUs4SitZaXZDL2ZtSXJ3bjVpSz09I3Rlc3RyZWxAdXNlcjotYmFzZTY1WVdaelpHWmgtWkhObVpIUHduNWlLOEorWWl2Qy9mbUlyd241aUs9PSMuLi4=",
	},
	{
		input:          `document:foo#viewer@user:tom[somecaveat:{"hi":"a@example.com"}]`,
		expectedOutput: `document:foo#viewer@user:tom[somecaveat:{"hi":"a@example.com"}]`,
		relFormat: MustWithCaveat(
			makeRel(
				StringToONR("document", "foo", "viewer"),
				StringToONR("user", "tom", "..."),
			),
			"somecaveat",
			map[string]any{
				"hi": "a@example.com",
			},
		),
		v1Format: cv1rel("document", "foo", "viewer", "user", "tom", "", "somecaveat", map[string]any{
			"hi": "a@example.com",
		}),
		stableCanonicalization: "ZG9jdW1lbnQ6Zm9vI3ZpZXdlckB1c2VyOnRvbSMuLi4gd2l0aCBzb21lY2F2ZWF0OntoaTphQGV4YW1wbGUuY29tfQ==",
	},
	{
		input:          `document:foo#viewer@user:tom[somecaveat:{"first":"a@example.com", "second": "b@example.com"}]`,
		expectedOutput: `document:foo#viewer@user:tom[somecaveat:{"first":"a@example.com","second":"b@example.com"}]`,
		relFormat: MustWithCaveat(
			makeRel(
				StringToONR("document", "foo", "viewer"),
				StringToONR("user", "tom", "..."),
			),
			"somecaveat",
			map[string]any{
				"first":  "a@example.com",
				"second": "b@example.com",
			},
		),
		v1Format: cv1rel("document", "foo", "viewer", "user", "tom", "", "somecaveat", map[string]any{
			"first":  "a@example.com",
			"second": "b@example.com",
		}),
		stableCanonicalization: "ZG9jdW1lbnQ6Zm9vI3ZpZXdlckB1c2VyOnRvbSMuLi4gd2l0aCBzb21lY2F2ZWF0OntmaXJzdDphQGV4YW1wbGUuY29tLHNlY29uZDpiQGV4YW1wbGUuY29tfQ==",
	},
	{
		input:          `document:foo#viewer@user:tom[somecaveat:{"second": "b@example.com", "first":"a@example.com"}]`,
		expectedOutput: `document:foo#viewer@user:tom[somecaveat:{"first":"a@example.com","second":"b@example.com"}]`,
		relFormat: MustWithCaveat(
			makeRel(
				StringToONR("document", "foo", "viewer"),
				StringToONR("user", "tom", "..."),
			),
			"somecaveat",
			map[string]any{
				"first":  "a@example.com",
				"second": "b@example.com",
			},
		),
		v1Format: cv1rel("document", "foo", "viewer", "user", "tom", "", "somecaveat", map[string]any{
			"first":  "a@example.com",
			"second": "b@example.com",
		}),
		stableCanonicalization: "ZG9jdW1lbnQ6Zm9vI3ZpZXdlckB1c2VyOnRvbSMuLi4gd2l0aCBzb21lY2F2ZWF0OntmaXJzdDphQGV4YW1wbGUuY29tLHNlY29uZDpiQGV4YW1wbGUuY29tfQ==",
	},
}

func TestSerialize(t *testing.T) {
	for _, tc := range testCases {
		tc := tc
		t.Run("tuple/"+tc.input, func(t *testing.T) {
			if tc.relFormat.Resource.ObjectType == "" {
				return
			}

			serialized := strings.Replace(MustString(tc.relFormat), " ", "", -1)
			require.Equal(t, tc.expectedOutput, serialized)

			withoutCaveat := StringWithoutCaveat(tc.relFormat)
			require.Contains(t, tc.expectedOutput, withoutCaveat)
			require.NotContains(t, withoutCaveat, "[")
		})
	}

	for _, tc := range testCases {
		tc := tc
		t.Run("relationship/"+tc.input, func(t *testing.T) {
			if tc.v1Format == nil {
				return
			}

			serialized := strings.Replace(MustV1RelString(tc.v1Format), " ", "", -1)
			require.Equal(t, tc.expectedOutput, serialized)

			withoutCaveat := V1StringRelationshipWithoutCaveat(tc.v1Format)
			require.Contains(t, tc.expectedOutput, withoutCaveat)
			require.NotContains(t, withoutCaveat, "[")
		})
	}
}

func TestParse(t *testing.T) {
	for _, tc := range testCases {
		tc := tc
		t.Run("relationship/"+tc.input, func(t *testing.T) {
			parsed, err := Parse(tc.input)
			if tc.relFormat.Resource.ObjectType == "" {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.True(t, Equal(tc.relFormat, parsed), "found difference in parsed relationship: %v vs %v", tc.relFormat, parsed)
		})
	}

	for _, tc := range testCases {
		tc := tc
		t.Run("v1/"+tc.input, func(t *testing.T) {
			parsed, err := ParseV1Rel(tc.input)
			if tc.relFormat.Resource.ObjectType == "" {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			testutil.RequireProtoEqual(t, tc.v1Format, parsed, "found difference in parsed V1 relationship")
		})
	}
}

func TestConvert(t *testing.T) {
	for _, tc := range testCases {
		tc := tc
		t.Run(tc.input, func(t *testing.T) {
			require := require.New(t)

			parsed, err := Parse(tc.input)
			if tc.relFormat.Resource.ObjectType == "" {
				require.Error(err)
				return
			}

			require.NoError(err)
			require.True(Equal(tc.relFormat, parsed))

			relationship := ToV1Relationship(parsed)
			relString := strings.Replace(MustV1RelString(relationship), " ", "", -1)
			require.Equal(tc.expectedOutput, relString)
		})
	}
}

func TestValidate(t *testing.T) {
	for _, tc := range testCases {
		tc := tc
		t.Run("validate/"+tc.input, func(t *testing.T) {
			parsed, err := ParseV1Rel(tc.input)
			if err == nil {
				require.NoError(t, ValidateResourceID(parsed.Resource.ObjectId))
				require.NoError(t, ValidateSubjectID(parsed.Subject.Object.ObjectId))
			}
		})
	}
}
