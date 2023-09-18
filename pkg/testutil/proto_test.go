package testutil

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

func TestAreProtoEqual(t *testing.T) {
	tcs := []struct {
		name          string
		first         proto.Message
		second        proto.Message
		expectedEqual bool
	}{
		{
			"empty types",
			&core.ObjectAndRelation{},
			&core.ObjectAndRelation{},
			true,
		},
		{
			"filled in",
			&core.ObjectAndRelation{
				Namespace: "foo",
			},
			&core.ObjectAndRelation{
				Namespace: "foo",
			},
			true,
		},
		{
			"nested",
			&core.RelationTuple{
				ResourceAndRelation: &core.ObjectAndRelation{
					Namespace: "foo",
				},
			},
			&core.RelationTuple{
				ResourceAndRelation: &core.ObjectAndRelation{
					Namespace: "foo",
				},
			},
			true,
		},
		{
			"nested difference",
			&core.RelationTuple{
				ResourceAndRelation: &core.ObjectAndRelation{
					Namespace: "foo",
				},
			},
			&core.RelationTuple{
				ResourceAndRelation: &core.ObjectAndRelation{
					Namespace: "bar",
				},
			},
			false,
		},
		{
			"empty slice vs nil",
			&core.CaveatTypeReference{
				TypeName:   "test",
				ChildTypes: []*core.CaveatTypeReference{},
			},
			&core.CaveatTypeReference{
				TypeName:   "test",
				ChildTypes: nil,
			},
			true,
		},
		{
			"element diff",
			&core.CaveatTypeReference{
				TypeName: "test",
				ChildTypes: []*core.CaveatTypeReference{
					{
						TypeName: "foo",
					},
				},
			},
			&core.CaveatTypeReference{
				TypeName: "test",
				ChildTypes: []*core.CaveatTypeReference{
					{
						TypeName: "bar",
					},
				},
			},
			false,
		},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			err := AreProtoEqual(tc.first, tc.second, "something went wrong")
			require.True(t, (err == nil) == (tc.expectedEqual))
		})
	}
}

func TestRequireProtoEqual(t *testing.T) {
	tcs := []struct {
		name   string
		first  proto.Message
		second proto.Message
	}{
		{
			"empty types",
			&core.ObjectAndRelation{},
			&core.ObjectAndRelation{},
		},
		{
			"filled in",
			&core.ObjectAndRelation{
				Namespace: "foo",
			},
			&core.ObjectAndRelation{
				Namespace: "foo",
			},
		},
		{
			"nested",
			&core.RelationTuple{
				ResourceAndRelation: &core.ObjectAndRelation{
					Namespace: "foo",
				},
			},
			&core.RelationTuple{
				ResourceAndRelation: &core.ObjectAndRelation{
					Namespace: "foo",
				},
			},
		},
		{
			"empty slice vs nil",
			&core.CaveatTypeReference{
				TypeName:   "test",
				ChildTypes: []*core.CaveatTypeReference{},
			},
			&core.CaveatTypeReference{
				TypeName:   "test",
				ChildTypes: nil,
			},
		},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			RequireProtoEqual(t, tc.first, tc.second, "something went wrong")
		})
	}
}

func TestAreProtoSlicesEqual(t *testing.T) {
	tcs := []struct {
		name          string
		first         []*core.ObjectAndRelation
		second        []*core.ObjectAndRelation
		expectedEqual bool
	}{
		{
			"empty slices",
			[]*core.ObjectAndRelation{},
			[]*core.ObjectAndRelation{},
			true,
		},
		{
			"ordered slices",
			[]*core.ObjectAndRelation{
				{Namespace: "document", ObjectId: "first", Relation: "viewer"},
				{Namespace: "document", ObjectId: "second", Relation: "viewer"},
			},
			[]*core.ObjectAndRelation{
				{Namespace: "document", ObjectId: "first", Relation: "viewer"},
				{Namespace: "document", ObjectId: "second", Relation: "viewer"},
			},
			true,
		},
		{
			"unordered slices",
			[]*core.ObjectAndRelation{
				{Namespace: "document", ObjectId: "first", Relation: "viewer"},
				{Namespace: "document", ObjectId: "second", Relation: "viewer"},
			},
			[]*core.ObjectAndRelation{
				{Namespace: "document", ObjectId: "second", Relation: "viewer"},
				{Namespace: "document", ObjectId: "first", Relation: "viewer"},
			},
			true,
		},
		{
			"different slices",
			[]*core.ObjectAndRelation{
				{Namespace: "document", ObjectId: "first", Relation: "viewer"},
				{Namespace: "document", ObjectId: "second", Relation: "viewer"},
			},
			[]*core.ObjectAndRelation{
				{Namespace: "document", ObjectId: "second", Relation: "viewer"},
			},
			false,
		},
		{
			"different slice values",
			[]*core.ObjectAndRelation{
				{Namespace: "document", ObjectId: "first", Relation: "viewer"},
			},
			[]*core.ObjectAndRelation{
				{Namespace: "document", ObjectId: "second", Relation: "viewer"},
			},
			false,
		},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			err := AreProtoSlicesEqual(tc.first, tc.second, func(first, second *core.ObjectAndRelation) int {
				return strings.Compare(first.ObjectId, second.ObjectId)
			}, "something went wrong")
			require.True(t, (err == nil) == (tc.expectedEqual))
		})
	}
}

func TestRequireProtoSlicesEqual(t *testing.T) {
	tcs := []struct {
		name   string
		first  []*core.ObjectAndRelation
		second []*core.ObjectAndRelation
	}{
		{
			"empty slices",
			[]*core.ObjectAndRelation{},
			[]*core.ObjectAndRelation{},
		},
		{
			"ordered slices",
			[]*core.ObjectAndRelation{
				{Namespace: "document", ObjectId: "first", Relation: "viewer"},
				{Namespace: "document", ObjectId: "second", Relation: "viewer"},
			},
			[]*core.ObjectAndRelation{
				{Namespace: "document", ObjectId: "first", Relation: "viewer"},
				{Namespace: "document", ObjectId: "second", Relation: "viewer"},
			},
		},
		{
			"unordered slices",
			[]*core.ObjectAndRelation{
				{Namespace: "document", ObjectId: "first", Relation: "viewer"},
				{Namespace: "document", ObjectId: "second", Relation: "viewer"},
			},
			[]*core.ObjectAndRelation{
				{Namespace: "document", ObjectId: "second", Relation: "viewer"},
				{Namespace: "document", ObjectId: "first", Relation: "viewer"},
			},
		},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			RequireProtoSlicesEqual(t, tc.first, tc.second, func(first *core.ObjectAndRelation, second *core.ObjectAndRelation) int {
				return strings.Compare(first.ObjectId, second.ObjectId)
			}, "something went wrong")
		})
	}
}
