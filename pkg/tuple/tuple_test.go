package tuple

import (
	"testing"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/stretchr/testify/require"
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

var testCases = []struct {
	input          string
	expectedOutput string
	tupleFormat    *core.RelationTuple
	relFormat      *v1.Relationship
}{
	{
		input:          "testns:testobj#testrel@user:testusr",
		expectedOutput: "testns:testobj#testrel@user:testusr",
		tupleFormat: makeTuple(
			ObjectAndRelation("testns", "testobj", "testrel"),
			ObjectAndRelation("user", "testusr", "..."),
		),
		relFormat: rel("testns", "testobj", "testrel", "user", "testusr", ""),
	},
	{
		input:          "testns:testobj#testrel@user:testusr#...",
		expectedOutput: "testns:testobj#testrel@user:testusr",
		tupleFormat: makeTuple(
			ObjectAndRelation("testns", "testobj", "testrel"),
			ObjectAndRelation("user", "testusr", "..."),
		),
		relFormat: rel("testns", "testobj", "testrel", "user", "testusr", ""),
	},
	{
		input:          "tenant/testns:testobj#testrel@tenant/user:testusr",
		expectedOutput: "tenant/testns:testobj#testrel@tenant/user:testusr",
		tupleFormat: makeTuple(
			ObjectAndRelation("tenant/testns", "testobj", "testrel"),
			ObjectAndRelation("tenant/user", "testusr", "..."),
		),
		relFormat: rel("tenant/testns", "testobj", "testrel", "tenant/user", "testusr", ""),
	},
	{
		input:          "tenant/testns:testobj#testrel@tenant/user:testusr#...",
		expectedOutput: "tenant/testns:testobj#testrel@tenant/user:testusr",
		tupleFormat: makeTuple(
			ObjectAndRelation("tenant/testns", "testobj", "testrel"),
			ObjectAndRelation("tenant/user", "testusr", "..."),
		),
		relFormat: rel("tenant/testns", "testobj", "testrel", "tenant/user", "testusr", ""),
	},
	{
		input:          "tenant/testns:testobj#testrel@tenant/user:testusr#somerel",
		expectedOutput: "tenant/testns:testobj#testrel@tenant/user:testusr#somerel",
		tupleFormat: makeTuple(
			ObjectAndRelation("tenant/testns", "testobj", "testrel"),
			ObjectAndRelation("tenant/user", "testusr", "somerel"),
		),
		relFormat: rel("tenant/testns", "testobj", "testrel", "tenant/user", "testusr", "somerel"),
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
		relFormat: rel("foos", "bar", "bazzy", "groo", "grar", ""),
	},
	{
		input:          "tenant/testns:testobj#testrel@tenant/user:*#...",
		expectedOutput: "tenant/testns:testobj#testrel@tenant/user:*",
		tupleFormat: makeTuple(
			ObjectAndRelation("tenant/testns", "testobj", "testrel"),
			ObjectAndRelation("tenant/user", "*", "..."),
		),
		relFormat: rel("tenant/testns", "testobj", "testrel", "tenant/user", "*", ""),
	},
	{
		input:          "tenant/testns:testobj#testrel@tenant/user:authn|foo",
		expectedOutput: "tenant/testns:testobj#testrel@tenant/user:authn|foo",
		tupleFormat: makeTuple(
			ObjectAndRelation("tenant/testns", "testobj", "testrel"),
			ObjectAndRelation("tenant/user", "authn|foo", "..."),
		),
		relFormat: rel("tenant/testns", "testobj", "testrel", "tenant/user", "authn|foo", ""),
	},
}

func TestSerialize(t *testing.T) {
	for _, tc := range testCases {
		t.Run("tuple/"+tc.input, func(t *testing.T) {
			if tc.tupleFormat == nil {
				return
			}

			serialized := String(tc.tupleFormat)
			require.Equal(t, tc.expectedOutput, serialized)
		})
	}

	for _, tc := range testCases {
		t.Run("relationship/"+tc.input, func(t *testing.T) {
			if tc.relFormat == nil {
				return
			}

			serialized := MustRelString(tc.relFormat)
			require.Equal(t, tc.expectedOutput, serialized)
		})
	}
}

func TestParse(t *testing.T) {
	for _, tc := range testCases {
		t.Run("tuple/"+tc.input, func(t *testing.T) {
			require.Equal(t, tc.tupleFormat, Parse(tc.input))
		})
	}

	for _, tc := range testCases {
		t.Run("relationship/"+tc.input, func(t *testing.T) {
			require.Equal(t, tc.relFormat, ParseRel(tc.input))
		})
	}
}

func TestConvert(t *testing.T) {
	for _, tc := range testCases {
		t.Run(tc.input, func(t *testing.T) {
			require := require.New(t)

			parsed := Parse(tc.input)
			require.Equal(tc.tupleFormat, parsed)
			if parsed == nil {
				return
			}

			relationship := ToRelationship(parsed)
			relString := MustRelString(relationship)
			require.Equal(tc.expectedOutput, relString)

			backToTpl := FromRelationship(relationship)
			require.Equal(tc.tupleFormat, backToTpl)

			serialized := String(backToTpl)
			require.Equal(tc.expectedOutput, serialized)
		})
	}
}

func TestValidate(t *testing.T) {
	for _, tc := range testCases {
		t.Run("validate/"+tc.input, func(t *testing.T) {
			parsed := ParseRel(tc.input)
			if parsed != nil {
				require.NoError(t, ValidateResourceID(parsed.Resource.ObjectId))
				require.NoError(t, ValidateSubjectID(parsed.Subject.Object.ObjectId))
			}
		})
	}
}
