package tuple

import (
	"testing"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/stretchr/testify/require"
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
		require.Equal(t, tt.expected, StringObjectRef(tt.ref))
		require.Equal(t, tt.expected, StringSubjectRef(subRef(tt.ref.ObjectType, tt.ref.ObjectId, "")))
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
		require.Equal(t, tt.expected, StringSubjectRef(tt.ref))
	}
}
