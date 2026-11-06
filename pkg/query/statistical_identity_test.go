package query

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/query/signals/signal"
)

func TestStatisticalIdentityIncludesCaveatChildren(t *testing.T) {
	caveat := func(child Outline) Outline {
		return Outline{
			Type:        CaveatIteratorType,
			Args:        &IteratorArgs{Caveat: &core.ContextualizedCaveat{CaveatName: "condition"}},
			SubOutlines: []Outline{child},
		}
	}
	a := caveat(Outline{Type: NullIteratorType})
	b := caveat(Outline{Type: FixedIteratorType})
	require.Equal(t, a.Serialize(), b.Serialize())
	aa := UniqueOutlineOccurrences(CanonicalOutline{Root: a})
	bb := UniqueOutlineOccurrences(CanonicalOutline{Root: b})
	require.NotEqual(t, DescribeOutline(aa.Root)[aa.Root.ID].Identity, DescribeOutline(bb.Root)[bb.Root.ID].Identity)
	require.Equal(t, []string{"condition"}, DescribeOutline(aa.Root)[aa.Root.ID].Footprint.Caveats)
}

func TestDuplicateOccurrenceIdentityAndFootprint(t *testing.T) {
	co, _ := CanonicalizeOutline(
		Outline{
			Type: UnionIteratorType,
			SubOutlines: []Outline{
				{
					Type:        AliasIteratorType,
					Args:        &IteratorArgs{DefinitionName: "doc", RelationName: "read"},
					SubOutlines: []Outline{{Type: NullIteratorType}},
				},
				{
					Type:        AliasIteratorType,
					Args:        &IteratorArgs{DefinitionName: "doc", RelationName: "read"},
					SubOutlines: []Outline{{Type: NullIteratorType}},
				},
			},
		},
	)
	unique := UniqueOutlineOccurrences(co)
	facts := DescribeOutline(unique.Root)
	require.NotEqual(t, unique.Root.SubOutlines[0].ID, unique.Root.SubOutlines[1].ID)
	require.Equal(t, facts[unique.Root.SubOutlines[0].ID].Identity, facts[unique.Root.SubOutlines[1].ID].Identity)
	require.Len(t, facts[unique.Root.ID].Footprint.Elements, 1)
}

func TestIdentityIncludesFixedEndpointsAndCaveatContext(t *testing.T) {
	identity := func(n Outline) string {
		co := UniqueOutlineOccurrences(CanonicalOutline{Root: n})
		return DescribeOutline(co.Root)[co.Root.ID].Identity
	}
	a := Outline{Type: FixedIteratorType, Args: &IteratorArgs{FixedPaths: []Path{*MustPathFromString("doc:d#read@user:a")}}}
	b := a
	b.Args = &IteratorArgs{FixedPaths: []Path{*MustPathFromString("doc:d#read@user:b")}}
	require.NotEqual(t, identity(a), identity(b))
	x, _ := structpb.NewStruct(map[string]any{"field": true})
	y, _ := structpb.NewStruct(map[string]any{"field": false})
	a = Outline{Type: CaveatIteratorType, Args: &IteratorArgs{Caveat: &core.ContextualizedCaveat{CaveatName: "condition", Context: x}}}
	b = Outline{Type: CaveatIteratorType, Args: &IteratorArgs{Caveat: &core.ContextualizedCaveat{CaveatName: "condition", Context: y}}}
	require.NotEqual(t, identity(a), identity(b))
}

func TestRecursiveAndCollapsedAliasFootprints(t *testing.T) {
	co := UniqueOutlineOccurrences(
		CanonicalOutline{
			Root: Outline{
				Type: RecursiveIteratorType,
				Args: &IteratorArgs{DefinitionName: "doc", RelationName: "read"},
				SubOutlines: []Outline{
					{
						Type: UnionIteratorType,
						SubOutlines: []Outline{
							{
								Type: RecursiveSentinelIteratorType,
								Args: &IteratorArgs{DefinitionName: "doc", RelationName: "read"},
							},
							{
								Type: AliasIteratorType,
								Args: &IteratorArgs{
									DefinitionName: "doc",
									RelationName:   "viewer",
									AliasedAs:      []string{"view", "access"},
								},
								SubOutlines: []Outline{{Type: NullIteratorType}},
							},
						},
					},
				},
			},
		},
	)
	facts := DescribeOutline(co.Root)
	sentinel := co.Root.SubOutlines[0].SubOutlines[0]
	require.ElementsMatch(
		t,
		[]signal.Element{
			{Namespace: "doc", Relation: "read"},
			{Namespace: "doc", Relation: "viewer"},
			{Namespace: "doc", Relation: "view"},
			{Namespace: "doc", Relation: "access"},
		},
		facts[sentinel.ID].Footprint.Elements,
	)
}

func TestFingerprintDoesNotMutateArguments(t *testing.T) {
	caveat := &core.ContextualizedCaveat{CaveatName: "condition"}
	before := caveat.CloneVT()
	co := UniqueOutlineOccurrences(CanonicalOutline{Root: Outline{Type: CaveatIteratorType, Args: &IteratorArgs{Caveat: caveat}}})
	DescribeOutline(co.Root)
	require.Equal(t, before, caveat)
}
