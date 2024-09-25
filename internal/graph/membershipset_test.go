package graph

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/exp/maps"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/authzed/spicedb/internal/caveats"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

var invert = caveats.Invert

func caveat(name string, context map[string]any) *core.CaveatExpression {
	s, _ := structpb.NewStruct(context)
	return wrapCaveat(
		&core.ContextualizedCaveat{
			CaveatName: name,
			Context:    s,
		})
}

func TestMembershipSetAddDirectMember(t *testing.T) {
	tcs := []struct {
		name                string
		existingMembers     map[string]*core.CaveatExpression
		directMemberID      string
		directMemberCaveat  *core.CaveatExpression
		expectedMembers     map[string]*core.CaveatExpression
		hasDeterminedMember bool
	}{
		{
			"add determined member to empty set",
			nil,
			"somedoc",
			nil,
			map[string]*core.CaveatExpression{
				"somedoc": nil,
			},
			true,
		},
		{
			"add caveated member to empty set",
			nil,
			"somedoc",
			caveat("somecaveat", nil),
			map[string]*core.CaveatExpression{
				"somedoc": caveat("somecaveat", nil),
			},
			false,
		},
		{
			"add caveated member to set with other members",
			map[string]*core.CaveatExpression{
				"somedoc": caveat("somecaveat", nil),
			},
			"anotherdoc",
			caveat("anothercaveat", nil),
			map[string]*core.CaveatExpression{
				"somedoc":    caveat("somecaveat", nil),
				"anotherdoc": caveat("anothercaveat", nil),
			},
			false,
		},
		{
			"add non-caveated member to caveated member",
			map[string]*core.CaveatExpression{
				"somedoc": caveat("somecaveat", nil),
			},
			"somedoc",
			nil,
			map[string]*core.CaveatExpression{
				"somedoc": nil,
			},
			true,
		},
		{
			"add caveated member to non-caveated member",
			map[string]*core.CaveatExpression{
				"somedoc": nil,
			},
			"somedoc",
			caveat("somecaveat", nil),
			map[string]*core.CaveatExpression{
				"somedoc": nil,
			},
			true,
		},
		{
			"add caveated member to caveated member",
			map[string]*core.CaveatExpression{
				"somedoc": caveat("c1", nil),
			},
			"somedoc",
			caveat("c2", nil),
			map[string]*core.CaveatExpression{
				"somedoc": caveatOr(
					caveat("c1", nil),
					caveat("c2", nil),
				),
			},
			false,
		},
		{
			"add caveats with the same name, different args",
			map[string]*core.CaveatExpression{
				"somedoc": caveat("c1", nil),
			},
			"somedoc",
			caveat("c1", map[string]any{
				"hi": "hello",
			}),
			map[string]*core.CaveatExpression{
				"somedoc": caveatOr(
					caveat("c1", nil),
					caveat("c1", map[string]any{
						"hi": "hello",
					}),
				),
			},
			false,
		},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			ms := membershipSetFromMap(tc.existingMembers)
			ms.AddDirectMember(tc.directMemberID, unwrapCaveat(tc.directMemberCaveat))
			require.Equal(t, tc.expectedMembers, ms.membersByID)
			require.Equal(t, tc.hasDeterminedMember, ms.HasDeterminedMember())
			require.False(t, ms.IsEmpty())
		})
	}
}

func TestMembershipSetAddMemberViaRelationship(t *testing.T) {
	tcs := []struct {
		name                     string
		existingMembers          map[string]*core.CaveatExpression
		resourceID               string
		resourceCaveatExpression *core.CaveatExpression
		parentRelationship       tuple.Relationship
		expectedMembers          map[string]*core.CaveatExpression
		hasDeterminedMember      bool
	}{
		{
			"add determined member to empty set",
			nil,
			"somedoc",
			nil,
			tuple.MustParse("document:foo#viewer@user:tom"),
			map[string]*core.CaveatExpression{
				"somedoc": nil,
			},
			true,
		},
		{
			"add caveated member to empty set",
			nil,
			"somedoc",
			caveat("somecaveat", nil),
			tuple.MustParse("document:foo#viewer@user:tom"),
			map[string]*core.CaveatExpression{
				"somedoc": caveat("somecaveat", nil),
			},
			false,
		},
		{
			"add determined member, via caveated relationship, to empty set",
			nil,
			"somedoc",
			nil,
			withCaveat(tuple.MustParse("document:foo#viewer@user:tom"), caveat("somecaveat", nil)),
			map[string]*core.CaveatExpression{
				"somedoc": caveat("somecaveat", nil),
			},
			false,
		},
		{
			"add caveated member, via caveated relationship, to empty set",
			nil,
			"somedoc",
			caveat("c1", nil),
			withCaveat(tuple.MustParse("document:foo#viewer@user:tom"), caveat("c2", nil)),
			map[string]*core.CaveatExpression{
				"somedoc": caveatAnd(
					caveat("c2", nil),
					caveat("c1", nil),
				),
			},
			false,
		},
		{
			"add caveated member, via caveated relationship, to determined set",
			map[string]*core.CaveatExpression{
				"somedoc": nil,
			},
			"somedoc",
			caveat("c1", nil),
			withCaveat(tuple.MustParse("document:foo#viewer@user:tom"), caveat("c2", nil)),
			map[string]*core.CaveatExpression{
				"somedoc": nil,
			},
			true,
		},
		{
			"add caveated member, via caveated relationship, to caveated set",
			map[string]*core.CaveatExpression{
				"somedoc": caveat("c0", nil),
			},
			"somedoc",
			caveat("c1", nil),
			withCaveat(tuple.MustParse("document:foo#viewer@user:tom"), caveat("c2", nil)),
			map[string]*core.CaveatExpression{
				"somedoc": caveatOr(
					caveat("c0", nil),
					caveatAnd(
						caveat("c2", nil),
						caveat("c1", nil),
					),
				),
			},
			false,
		},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			ms := membershipSetFromMap(tc.existingMembers)
			ms.AddMemberViaRelationship(tc.resourceID, tc.resourceCaveatExpression, tc.parentRelationship)
			require.Equal(t, tc.expectedMembers, ms.membersByID)
			require.Equal(t, tc.hasDeterminedMember, ms.HasDeterminedMember())
		})
	}
}

func TestMembershipSetUnionWith(t *testing.T) {
	tcs := []struct {
		name                string
		set1                map[string]*core.CaveatExpression
		set2                map[string]*core.CaveatExpression
		expected            map[string]*core.CaveatExpression
		hasDeterminedMember bool
		isEmpty             bool
	}{
		{
			"empty with empty",
			nil,
			nil,
			map[string]*core.CaveatExpression{},
			false,
			true,
		},
		{
			"set with empty",
			map[string]*core.CaveatExpression{
				"somedoc": nil,
			},
			nil,
			map[string]*core.CaveatExpression{
				"somedoc": nil,
			},
			true,
			false,
		},
		{
			"empty with set",
			nil,
			map[string]*core.CaveatExpression{
				"somedoc": nil,
			},
			map[string]*core.CaveatExpression{
				"somedoc": nil,
			},
			true,
			false,
		},
		{
			"non-overlapping",
			map[string]*core.CaveatExpression{
				"somedoc": nil,
			},
			map[string]*core.CaveatExpression{
				"anotherdoc": nil,
			},
			map[string]*core.CaveatExpression{
				"somedoc":    nil,
				"anotherdoc": nil,
			},
			true,
			false,
		},
		{
			"non-overlapping with caveats",
			map[string]*core.CaveatExpression{
				"somedoc": nil,
			},
			map[string]*core.CaveatExpression{
				"anotherdoc": caveat("c1", nil),
			},
			map[string]*core.CaveatExpression{
				"somedoc":    nil,
				"anotherdoc": caveat("c1", nil),
			},
			true,
			false,
		},
		{
			"overlapping without caveats",
			map[string]*core.CaveatExpression{
				"somedoc": nil,
			},
			map[string]*core.CaveatExpression{
				"somedoc": nil,
			},
			map[string]*core.CaveatExpression{
				"somedoc": nil,
			},
			true,
			false,
		},
		{
			"overlapping with single caveat",
			map[string]*core.CaveatExpression{
				"somedoc": nil,
			},
			map[string]*core.CaveatExpression{
				"somedoc": caveat("c1", nil),
			},
			map[string]*core.CaveatExpression{
				"somedoc": nil,
			},
			true,
			false,
		},
		{
			"overlapping with multiple caveats",
			map[string]*core.CaveatExpression{
				"somedoc": caveat("c1", nil),
			},
			map[string]*core.CaveatExpression{
				"somedoc": caveat("c2", nil),
			},
			map[string]*core.CaveatExpression{
				"somedoc": caveatOr(caveat("c1", nil), caveat("c2", nil)),
			},
			false,
			false,
		},
		{
			"overlapping with multiple caveats and a determined member",
			map[string]*core.CaveatExpression{
				"somedoc":    caveat("c1", nil),
				"anotherdoc": nil,
			},
			map[string]*core.CaveatExpression{
				"somedoc": caveat("c2", nil),
			},
			map[string]*core.CaveatExpression{
				"anotherdoc": nil,
				"somedoc":    caveatOr(caveat("c1", nil), caveat("c2", nil)),
			},
			true,
			false,
		},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			ms1 := membershipSetFromMap(tc.set1)
			ms2 := membershipSetFromMap(tc.set2)
			ms1.UnionWith(ms2.AsCheckResultsMap())
			require.Equal(t, tc.expected, ms1.membersByID)
			require.Equal(t, tc.hasDeterminedMember, ms1.HasDeterminedMember())
			require.Equal(t, tc.isEmpty, ms1.IsEmpty())
		})
	}
}

func TestMembershipSetIntersectWith(t *testing.T) {
	tcs := []struct {
		name                string
		set1                map[string]*core.CaveatExpression
		set2                map[string]*core.CaveatExpression
		expected            map[string]*core.CaveatExpression
		hasDeterminedMember bool
		isEmpty             bool
	}{
		{
			"empty with empty",
			nil,
			nil,
			map[string]*core.CaveatExpression{},
			false,
			true,
		},
		{
			"set with empty",
			map[string]*core.CaveatExpression{
				"somedoc": nil,
			},
			nil,
			map[string]*core.CaveatExpression{},
			false,
			true,
		},
		{
			"empty with set",
			nil,
			map[string]*core.CaveatExpression{
				"somedoc": nil,
			},
			map[string]*core.CaveatExpression{},
			false,
			true,
		},
		{
			"basic set with set",
			map[string]*core.CaveatExpression{
				"somedoc": nil,
			},
			map[string]*core.CaveatExpression{
				"somedoc": nil,
			},
			map[string]*core.CaveatExpression{
				"somedoc": nil,
			},
			true,
			false,
		},
		{
			"non-overlapping set with set",
			map[string]*core.CaveatExpression{
				"somedoc": nil,
			},
			map[string]*core.CaveatExpression{
				"anotherdoc": nil,
			},
			map[string]*core.CaveatExpression{},
			false,
			true,
		},
		{
			"partially overlapping set with set",
			map[string]*core.CaveatExpression{
				"somedoc":    nil,
				"anotherdoc": nil,
			},
			map[string]*core.CaveatExpression{
				"anotherdoc": nil,
			},
			map[string]*core.CaveatExpression{
				"anotherdoc": nil,
			},
			true,
			false,
		},
		{
			"set with partially overlapping set",
			map[string]*core.CaveatExpression{
				"anotherdoc": nil,
			},
			map[string]*core.CaveatExpression{
				"somedoc":    nil,
				"anotherdoc": nil,
			},
			map[string]*core.CaveatExpression{
				"anotherdoc": nil,
			},
			true,
			false,
		},
		{
			"partially overlapping sets with one caveat",
			map[string]*core.CaveatExpression{
				"anotherdoc": nil,
			},
			map[string]*core.CaveatExpression{
				"somedoc":    nil,
				"anotherdoc": caveat("c2", nil),
			},
			map[string]*core.CaveatExpression{
				"anotherdoc": caveat("c2", nil),
			},
			false,
			false,
		},
		{
			"partially overlapping sets with one caveat (other side)",
			map[string]*core.CaveatExpression{
				"anotherdoc": caveat("c1", nil),
			},
			map[string]*core.CaveatExpression{
				"somedoc":    nil,
				"anotherdoc": nil,
			},
			map[string]*core.CaveatExpression{
				"anotherdoc": caveat("c1", nil),
			},
			false,
			false,
		},
		{
			"partially overlapping sets with caveats",
			map[string]*core.CaveatExpression{
				"anotherdoc": caveat("c1", nil),
			},
			map[string]*core.CaveatExpression{
				"somedoc":    nil,
				"anotherdoc": caveat("c2", nil),
			},
			map[string]*core.CaveatExpression{
				"anotherdoc": caveatAnd(
					caveat("c1", nil),
					caveat("c2", nil),
				),
			},
			false,
			false,
		},
		{
			"overlapping sets with caveats and a determined member",
			map[string]*core.CaveatExpression{
				"somedoc":    nil,
				"thirddoc":   nil,
				"anotherdoc": caveat("c1", nil),
			},
			map[string]*core.CaveatExpression{
				"somedoc":    nil,
				"anotherdoc": caveat("c2", nil),
			},
			map[string]*core.CaveatExpression{
				"somedoc": nil,
				"anotherdoc": caveatAnd(
					caveat("c1", nil),
					caveat("c2", nil),
				),
			},
			true,
			false,
		},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			ms1 := membershipSetFromMap(tc.set1)
			ms2 := membershipSetFromMap(tc.set2)
			ms1.IntersectWith(ms2.AsCheckResultsMap())
			require.Equal(t, tc.expected, ms1.membersByID)
			require.Equal(t, tc.hasDeterminedMember, ms1.HasDeterminedMember())
			require.Equal(t, tc.isEmpty, ms1.IsEmpty())
		})
	}
}

func TestMembershipSetSubtract(t *testing.T) {
	tcs := []struct {
		name                string
		set1                map[string]*core.CaveatExpression
		set2                map[string]*core.CaveatExpression
		expected            map[string]*core.CaveatExpression
		hasDeterminedMember bool
		isEmpty             bool
	}{
		{
			"empty with empty",
			nil,
			nil,
			map[string]*core.CaveatExpression{},
			false,
			true,
		},
		{
			"empty with set",
			nil,
			map[string]*core.CaveatExpression{
				"somedoc": nil,
			},
			map[string]*core.CaveatExpression{},
			false,
			true,
		},
		{
			"set with empty",
			map[string]*core.CaveatExpression{
				"somedoc": nil,
			},
			nil,
			map[string]*core.CaveatExpression{
				"somedoc": nil,
			},
			true,
			false,
		},
		{
			"non overlapping sets",
			map[string]*core.CaveatExpression{
				"somedoc": nil,
			},
			map[string]*core.CaveatExpression{
				"anotherdoc": nil,
			},
			map[string]*core.CaveatExpression{
				"somedoc": nil,
			},
			true,
			false,
		},
		{
			"overlapping sets with no caveats",
			map[string]*core.CaveatExpression{
				"somedoc": nil,
			},
			map[string]*core.CaveatExpression{
				"somedoc": nil,
			},
			map[string]*core.CaveatExpression{},
			false,
			true,
		},
		{
			"overlapping sets with first having a caveat",
			map[string]*core.CaveatExpression{
				"somedoc": caveat("c1", nil),
			},
			map[string]*core.CaveatExpression{
				"somedoc": nil,
			},
			map[string]*core.CaveatExpression{},
			false,
			true,
		},
		{
			"overlapping sets with second having a caveat",
			map[string]*core.CaveatExpression{
				"somedoc": nil,
			},
			map[string]*core.CaveatExpression{
				"somedoc": caveat("c2", nil),
			},
			map[string]*core.CaveatExpression{
				"somedoc": invert(caveat("c2", nil)),
			},
			false,
			false,
		},
		{
			"overlapping sets with both having caveats",
			map[string]*core.CaveatExpression{
				"somedoc": caveat("c1", nil),
			},
			map[string]*core.CaveatExpression{
				"somedoc": caveat("c2", nil),
			},
			map[string]*core.CaveatExpression{
				"somedoc": caveatAnd(
					caveat("c1", nil),
					invert(caveat("c2", nil)),
				),
			},
			false,
			false,
		},
		{
			"overlapping sets with both having caveats and determined member",
			map[string]*core.CaveatExpression{
				"somedoc":    caveat("c1", nil),
				"anotherdoc": nil,
			},
			map[string]*core.CaveatExpression{
				"somedoc": caveat("c2", nil),
			},
			map[string]*core.CaveatExpression{
				"anotherdoc": nil,
				"somedoc": caveatAnd(
					caveat("c1", nil),
					invert(caveat("c2", nil)),
				),
			},
			true,
			false,
		},
		{
			"overlapping sets with both having caveats and determined members",
			map[string]*core.CaveatExpression{
				"somedoc":    caveat("c1", nil),
				"anotherdoc": nil,
			},
			map[string]*core.CaveatExpression{
				"somedoc":    caveat("c2", nil),
				"anotherdoc": nil,
			},
			map[string]*core.CaveatExpression{
				"somedoc": caveatAnd(
					caveat("c1", nil),
					invert(caveat("c2", nil)),
				),
			},
			false,
			false,
		},
		{
			"non overlapping",
			map[string]*core.CaveatExpression{
				"resource1": nil,
				"resource2": nil,
			},
			map[string]*core.CaveatExpression{
				"resource2": nil,
			},
			map[string]*core.CaveatExpression{
				"resource1": nil,
			},
			true,
			false,
		},
		{
			"non overlapping reversed",
			map[string]*core.CaveatExpression{
				"resource2": nil,
			},
			map[string]*core.CaveatExpression{
				"resource1": nil,
				"resource2": nil,
			},
			map[string]*core.CaveatExpression{},
			false,
			true,
		},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			ms1 := membershipSetFromMap(tc.set1)
			ms2 := membershipSetFromMap(tc.set2)
			ms1.Subtract(ms2.AsCheckResultsMap())
			require.Equal(t, tc.expected, ms1.membersByID)
			require.Equal(t, tc.hasDeterminedMember, ms1.HasDeterminedMember())
			require.Equal(t, tc.isEmpty, ms1.IsEmpty())
		})
	}
}

func TestMembershipSetUnionWithNonMemberEntries(t *testing.T) {
	ms := NewMembershipSet()
	ms.addMember("resource1", nil)
	ms.addMember("resource2", nil)

	ms.UnionWith(CheckResultsMap{
		"resource3": &v1.ResourceCheckResult{
			Membership: v1.ResourceCheckResult_NOT_MEMBER,
		},
	})

	keys := maps.Keys(ms.membersByID)
	sort.Strings(keys)

	require.Equal(t, 2, ms.Size())
	require.True(t, ms.HasDeterminedMember())
	require.Equal(t, []string{"resource1", "resource2"}, keys)
}

func TestMembershipSetIntersectWithNonMemberEntries(t *testing.T) {
	ms := NewMembershipSet()
	ms.addMember("resource1", nil)
	ms.addMember("resource2", nil)

	ms.IntersectWith(CheckResultsMap{
		"resource1": &v1.ResourceCheckResult{
			Membership: v1.ResourceCheckResult_NOT_MEMBER,
		},
		"resource2": &v1.ResourceCheckResult{
			Membership: v1.ResourceCheckResult_MEMBER,
		},
	})

	require.Equal(t, 1, ms.Size())
	require.True(t, ms.HasDeterminedMember())
	require.Equal(t, []string{"resource2"}, maps.Keys(ms.membersByID))
}

func TestMembershipSetSubtractWithNonMemberEntries(t *testing.T) {
	ms := NewMembershipSet()
	ms.addMember("resource1", nil)
	ms.addMember("resource2", nil)

	// Subtracting a set with a non-member entry should not change the set.
	ms.Subtract(CheckResultsMap{
		"resource1": &v1.ResourceCheckResult{
			Membership: v1.ResourceCheckResult_NOT_MEMBER,
		},
		"resource2": &v1.ResourceCheckResult{
			Membership: v1.ResourceCheckResult_MEMBER,
		},
	})

	require.Equal(t, 1, ms.Size())
	require.True(t, ms.HasDeterminedMember())
	require.Equal(t, []string{"resource1"}, maps.Keys(ms.membersByID))
}

func unwrapCaveat(ce *core.CaveatExpression) *core.ContextualizedCaveat {
	if ce == nil {
		return nil
	}
	return ce.GetCaveat()
}

func withCaveat(tple tuple.Relationship, ce *core.CaveatExpression) tuple.Relationship {
	tple.OptionalCaveat = unwrapCaveat(ce)
	return tple
}
