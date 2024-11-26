package tuple

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

func TestONRStringToCore(t *testing.T) {
	tests := []struct {
		expected *core.ObjectAndRelation
		name     string
		ns       string
		oid      string
		rel      string
	}{
		{
			name: "basic",
			ns:   "testns",
			oid:  "testobj",
			rel:  "testrel",
			expected: &core.ObjectAndRelation{
				Namespace: "testns",
				ObjectId:  "testobj",
				Relation:  "testrel",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ONRStringToCore(tt.ns, tt.oid, tt.rel)
			require.Equal(t, tt.expected, got)
		})
	}
}

func TestRelationReference(t *testing.T) {
	tests := []struct {
		expected *core.RelationReference
		name     string
		ns       string
		rel      string
	}{
		{
			name: "basic",
			ns:   "testns",
			rel:  "testrel",
			expected: &core.RelationReference{
				Namespace: "testns",
				Relation:  "testrel",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := RRStringToCore(tt.ns, tt.rel)
			require.Equal(t, tt.expected, got)
		})
	}
}

// NOTE: this tests both the normal and Must* version of this fn
func TestCoreRelationToString(t *testing.T) {
	caveatContext, err := structpb.NewStruct(map[string]any{
		"letters": []any{"a", "b", "c"},
	})
	require.NoError(t, err)

	testTime, err := time.Parse(expirationFormat, "2021-01-01T00:00:00Z")
	require.NoError(t, err)

	tests := []struct {
		name     string
		input    *core.RelationTuple
		expected string
	}{
		{
			name: "normal relation",
			input: &core.RelationTuple{
				ResourceAndRelation: &core.ObjectAndRelation{
					Namespace: "resource",
					ObjectId:  "1",
					Relation:  "view",
				},
				Subject: &core.ObjectAndRelation{
					Namespace: "user",
					ObjectId:  "1",
					Relation:  "...",
				},
			},
			expected: `resource:1#view@user:1`,
		},
		{
			name: "relation with caveat",
			input: &core.RelationTuple{
				ResourceAndRelation: &core.ObjectAndRelation{
					Namespace: "resource",
					ObjectId:  "1",
					Relation:  "view",
				},
				Subject: &core.ObjectAndRelation{
					Namespace: "user",
					ObjectId:  "1",
					Relation:  "...",
				},
				Caveat: &core.ContextualizedCaveat{
					CaveatName: "cav",
					Context:    caveatContext,
				},
			},
			expected: `resource:1#view@user:1[cav:{"letters":["a","b","c"]}]`,
		},
		{
			name: "relation with expiration",
			input: &core.RelationTuple{
				ResourceAndRelation: &core.ObjectAndRelation{
					Namespace: "resource",
					ObjectId:  "1",
					Relation:  "view",
				},
				Subject: &core.ObjectAndRelation{
					Namespace: "user",
					ObjectId:  "1",
					Relation:  "...",
				},
				OptionalExpirationTime: timestamppb.New(testTime),
			},
			expected: `resource:1#view@user:1[expiration:2021-01-01T00:00:00Z]`,
		},
		{
			name: "relation with caveat and expiration",
			input: &core.RelationTuple{
				ResourceAndRelation: &core.ObjectAndRelation{
					Namespace: "resource",
					ObjectId:  "1",
					Relation:  "view",
				},
				Subject: &core.ObjectAndRelation{
					Namespace: "user",
					ObjectId:  "1",
					Relation:  "...",
				},
				Caveat: &core.ContextualizedCaveat{
					CaveatName: "cav",
					Context:    caveatContext,
				},
				OptionalExpirationTime: timestamppb.New(testTime),
			},
			expected: `resource:1#view@user:1[cav:{"letters":["a","b","c"]}][expiration:2021-01-01T00:00:00Z]`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := CoreRelationToString(tt.input)
			require.NoError(t, err)

			// Golang randomly injects spaces.
			got = strings.ReplaceAll(got, " ", "")
			require.Equal(t, tt.expected, got)

			got = MustCoreRelationToString(tt.input)
			got = strings.ReplaceAll(got, " ", "")
			require.Equal(t, tt.expected, got)
		})
	}
}
