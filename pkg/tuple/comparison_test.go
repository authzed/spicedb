package tuple

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestONREqual(t *testing.T) {
	tests := []struct {
		name string
		lhs  ObjectAndRelation
		rhs  ObjectAndRelation
		want bool
	}{
		{
			name: "equal",
			lhs:  ObjectAndRelation{"testns", "testobj", nil, "testrel"},
			rhs:  ObjectAndRelation{"testns", "testobj", nil, "testrel"},
			want: true,
		},
		{
			name: "different object type",
			lhs:  ObjectAndRelation{"testns1", "testobj", nil, "testrel"},
			rhs:  ObjectAndRelation{"testns2", "testobj", nil, "testrel"},
			want: false,
		},
		{
			name: "different object id",
			lhs:  ObjectAndRelation{"testns", "testobj1", nil, "testrel"},
			rhs:  ObjectAndRelation{"testns", "testobj2", nil, "testrel"},
			want: false,
		},
		{
			name: "different relation",
			lhs:  ObjectAndRelation{"testns", "testobj", nil, "testrel1"},
			rhs:  ObjectAndRelation{"testns", "testobj", nil, "testrel2"},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ONREqual(tt.lhs, tt.rhs)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestEqual(t *testing.T) {
	equalTestCases := []Relationship{
		makeRel(
			StringToONR("testns", "testobj", "testrel"),
			StringToONR("user", "testusr", "..."),
		),
		MustWithCaveat(
			makeRel(
				StringToONR("testns", "testobj", "testrel"),
				StringToONR("user", "testusr", "..."),
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
		MustParse("document:foo#viewer@user:tom[expiration:2020-01-01T00:00:00Z]"),
		MustParse("document:foo#viewer@user:tom[somecaveat:{\"hi\":\"there\"}][expiration:2020-01-01T00:00:00Z]"),
	}

	for _, tc := range equalTestCases {
		t.Run(MustString(tc), func(t *testing.T) {
			require := require.New(t)
			require.True(Equal(tc, MustParse(MustString(tc))))
		})
	}

	notEqualTestCases := []struct {
		lhs  Relationship
		rhs  Relationship
		name string
	}{
		{
			name: "Mismatch Resource Type",
			lhs: makeRel(
				StringToONR("testns1", "testobj", "testrel"),
				StringToONR("user", "testusr", "..."),
			),
			rhs: makeRel(
				StringToONR("testns2", "testobj", "testrel"),
				StringToONR("user", "testusr", "..."),
			),
		},
		{
			name: "Mismatch Resource ID",
			lhs: makeRel(
				StringToONR("testns", "testobj1", "testrel"),
				StringToONR("user", "testusr", "..."),
			),
			rhs: makeRel(
				StringToONR("testns", "testobj2", "testrel"),
				StringToONR("user", "testusr", "..."),
			),
		},
		{
			name: "Mismatch Resource Relationship",
			lhs: makeRel(
				StringToONR("testns", "testobj", "testrel1"),
				StringToONR("user", "testusr", "..."),
			),
			rhs: makeRel(
				StringToONR("testns", "testobj", "testrel2"),
				StringToONR("user", "testusr", "..."),
			),
		},
		{
			name: "Mismatch Subject Type",
			lhs: makeRel(
				StringToONR("testns", "testobj", "testrel"),
				StringToONR("user1", "testusr", "..."),
			),
			rhs: makeRel(
				StringToONR("testns", "testobj", "testrel"),
				StringToONR("user2", "testusr", "..."),
			),
		},
		{
			name: "Mismatch Subject ID",
			lhs: makeRel(
				StringToONR("testns", "testobj", "testrel"),
				StringToONR("user", "testusr1", "..."),
			),
			rhs: makeRel(
				StringToONR("testns", "testobj", "testrel"),
				StringToONR("user", "testusr2", "..."),
			),
		},
		{
			name: "Mismatch Subject Relationship",
			lhs: makeRel(
				StringToONR("testns", "testobj", "testrel"),
				StringToONR("user", "testusr", "testrel1"),
			),
			rhs: makeRel(
				StringToONR("testns", "testobj", "testrel"),
				StringToONR("user", "testusr", "testrel2"),
			),
		},
		{
			name: "Mismatch Caveat Name",
			lhs: MustWithCaveat(
				makeRel(
					StringToONR("testns", "testobj", "testrel"),
					StringToONR("user", "testusr", "..."),
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
				makeRel(
					StringToONR("testns", "testobj", "testrel"),
					StringToONR("user", "testusr", "..."),
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
				makeRel(
					StringToONR("testns", "testobj", "testrel"),
					StringToONR("user", "testusr", "..."),
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
				makeRel(
					StringToONR("testns", "testobj", "testrel"),
					StringToONR("user", "testusr", "..."),
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
		{
			name: "one with expiration, the other without",
			lhs:  MustParse("document:foo#viewer@user:tom[expiration:2020-01-01T00:00:00Z]"),
			rhs:  MustParse("document:foo#viewer@user:tom"),
		},
		{
			name: "mismatch expiration",
			lhs:  MustParse("document:foo#viewer@user:tom[expiration:2020-01-01T00:00:00Z]"),
			rhs:  MustParse("document:foo#viewer@user:tom[expiration:2020-01-02T00:00:00Z]"),
		},
		{
			name: "same expiration, one with caveat",
			lhs:  MustParse("document:foo#viewer@user:tom[expiration:2020-01-01T00:00:00Z]"),
			rhs:  MustParse("document:foo#viewer@user:tom[somecaveat][expiration:2020-01-01T00:00:00Z]"),
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
