package tuple

import (
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/testutil"
)

func TestONRStructSize(t *testing.T) {
	size := int(unsafe.Sizeof(ObjectAndRelation{}))
	require.Equal(t, onrStructSize, size)
}

func TestRelationshipStructSize(t *testing.T) {
	size := int(unsafe.Sizeof(Relationship{}))
	require.Equal(t, relStructSize, size)
}

func TestToCoreTuple(t *testing.T) {
	tcs := []struct {
		name string
		rel  Relationship
		exp  *core.RelationTuple
	}{
		{
			name: "simple",
			rel:  MustParse("type:object#relation@subjecttype:subject"),
			exp: &core.RelationTuple{
				ResourceAndRelation: &core.ObjectAndRelation{
					Namespace: "type",
					ObjectId:  "object",
					Relation:  "relation",
				},
				Subject: &core.ObjectAndRelation{
					Namespace: "subjecttype",
					ObjectId:  "subject",
					Relation:  Ellipsis,
				},
			},
		},
		{
			name: "public wildcard",
			rel:  MustParse("type:object#relation@subjecttype:*"),
			exp: &core.RelationTuple{
				ResourceAndRelation: &core.ObjectAndRelation{
					Namespace: "type",
					ObjectId:  "object",
					Relation:  "relation",
				},
				Subject: &core.ObjectAndRelation{
					Namespace: "subjecttype",
					ObjectId:  PublicWildcard,
					Relation:  Ellipsis,
				},
			},
		},
		{
			name: "subject relation",
			rel:  MustParse("type:object#relation@subjecttype:subject#subjectrelation"),
			exp: &core.RelationTuple{
				ResourceAndRelation: &core.ObjectAndRelation{
					Namespace: "type",
					ObjectId:  "object",
					Relation:  "relation",
				},
				Subject: &core.ObjectAndRelation{
					Namespace: "subjecttype",
					ObjectId:  "subject",
					Relation:  "subjectrelation",
				},
			},
		},
		{
			name: "caveated with no context",
			rel:  MustParse("type:object#relation@subjecttype:subject[somecaveat]"),
			exp: &core.RelationTuple{
				ResourceAndRelation: &core.ObjectAndRelation{
					Namespace: "type",
					ObjectId:  "object",
					Relation:  "relation",
				},
				Subject: &core.ObjectAndRelation{
					Namespace: "subjecttype",
					ObjectId:  "subject",
					Relation:  Ellipsis,
				},
				Caveat: &core.ContextualizedCaveat{
					CaveatName: "somecaveat",
				},
			},
		},
		{
			name: "caveated with context",
			rel:  MustParse("type:object#relation@subjecttype:subject[somecaveat:{\"foo\": 42}]"),
			exp: &core.RelationTuple{
				ResourceAndRelation: &core.ObjectAndRelation{
					Namespace: "type",
					ObjectId:  "object",
					Relation:  "relation",
				},
				Subject: &core.ObjectAndRelation{
					Namespace: "subjecttype",
					ObjectId:  "subject",
					Relation:  Ellipsis,
				},
				Caveat: &core.ContextualizedCaveat{
					CaveatName: "somecaveat",
					Context: (func() *structpb.Struct {
						s, _ := structpb.NewStruct(map[string]any{"foo": 42})
						return s
					})(),
				},
			},
		},
		{
			name: "with expiration",
			rel:  MustParse("type:object#relation@subjecttype:subject[expiration:2021-01-01T00:00:00Z]"),
			exp: &core.RelationTuple{
				ResourceAndRelation: &core.ObjectAndRelation{
					Namespace: "type",
					ObjectId:  "object",
					Relation:  "relation",
				},
				Subject: &core.ObjectAndRelation{
					Namespace: "subjecttype",
					ObjectId:  "subject",
					Relation:  Ellipsis,
				},
				OptionalExpirationTime: timestamppb.New(time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)),
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			testutil.RequireProtoEqual(t, tc.exp, tc.rel.ToCoreTuple(), "mismatch")
		})
	}
}
