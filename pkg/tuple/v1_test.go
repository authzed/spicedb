package tuple

import (
	"testing"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/testutil"
)

func objRef(typ, id string) *v1.ObjectReference {
	return &v1.ObjectReference{ObjectType: typ, ObjectId: id}
}

func subRef(typ, id, rel string) *v1.SubjectReference {
	return &v1.SubjectReference{Object: objRef(typ, id), OptionalRelation: rel}
}

func TestStringObjectRef(t *testing.T) {
	table := []struct {
		ref      *v1.ObjectReference
		expected string
	}{
		// This code assumes input has already been validated.
		{objRef("document", "1"), "document:1"},
		{objRef("", ""), ":"},
		{objRef(":", ":"), ":::"},
	}
	for _, tt := range table {
		require.Equal(t, tt.expected, V1StringObjectRef(tt.ref))
		require.Equal(t, tt.expected, V1StringSubjectRef(subRef(tt.ref.ObjectType, tt.ref.ObjectId, "")))
	}
}

func TestJoinSubjectRef(t *testing.T) {
	table := []struct {
		ref      *v1.SubjectReference
		expected string
	}{
		// This code assumes input has already been validated.
		{subRef("document", "1", ""), "document:1"},
		{subRef("document", "1", "reader"), "document:1#reader"},
	}
	for _, tt := range table {
		require.Equal(t, tt.expected, V1StringSubjectRef(tt.ref))
	}
}

func TestBackAndForth(t *testing.T) {
	for _, tc := range testCases {
		tc := tc
		if tc.stableCanonicalization == "" {
			continue
		}

		t.Run("relationship/"+tc.input, func(t *testing.T) {
			v1rel := ToV1Relationship(tc.relFormat)
			testutil.RequireProtoEqual(t, tc.v1Format, v1rel, "mismatch in v1 proto")
			adjusted := tc.relFormat

			// Converting to V1 removes the timezones.
			if adjusted.OptionalExpiration != nil {
				t := adjusted.OptionalExpiration.UTC()
				adjusted.OptionalExpiration = &t
			}

			require.Equal(t, adjusted, FromV1Relationship(v1rel))
		})
	}
}
