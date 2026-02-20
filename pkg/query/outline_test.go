package query

import (
	"testing"

	"github.com/stretchr/testify/require"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/schema/v2"
	"github.com/authzed/spicedb/pkg/tuple"
)

func TestOutline_Compile(t *testing.T) {
	t.Parallel()

	t.Run("NullIteratorType", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		outline := Outline{Type: NullIteratorType}
		canonical, err := CanonicalizeOutline(outline)
		require.NoError(err)

		it, err := canonical.Compile()
		require.NoError(err)

		// Should produce a FixedIterator with no paths
		require.IsType((*FixedIterator)(nil), it)
		fixed := it.(*FixedIterator)
		require.Empty(fixed.paths)
	})

	t.Run("DatastoreIteratorType", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		rel := schema.NewTestBaseRelation("document", "viewer", "user", tuple.Ellipsis)
		outline := Outline{
			Type: DatastoreIteratorType,
			Args: &IteratorArgs{
				Relation: rel,
			},
		}

		canonical, err := CanonicalizeOutline(outline)
		require.NoError(err)

		it, err := canonical.Compile()
		require.NoError(err)

		require.IsType((*DatastoreIterator)(nil), it)
		ds := it.(*DatastoreIterator)
		require.Equal(rel, ds.base)
	})

	t.Run("DatastoreIteratorType_MissingRelation", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		outline := Outline{
			Type: DatastoreIteratorType,
			Args: nil,
		}

		canonical, err := CanonicalizeOutline(outline)
		require.NoError(err)

		_, err = canonical.Compile()
		require.Error(err)
		require.Contains(err.Error(), "DatastoreIterator requires Relation")
	})

	t.Run("UnionIteratorType", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		rel1 := schema.NewTestBaseRelation("doc", "viewer", "user", tuple.Ellipsis)
		rel2 := schema.NewTestBaseRelation("doc", "editor", "user", tuple.Ellipsis)

		outline := Outline{
			Type: UnionIteratorType,
			SubOutlines: []Outline{
				{Type: DatastoreIteratorType, Args: &IteratorArgs{Relation: rel1}},
				{Type: DatastoreIteratorType, Args: &IteratorArgs{Relation: rel2}},
			},
		}

		canonical, err := CanonicalizeOutline(outline)
		require.NoError(err)

		it, err := canonical.Compile()
		require.NoError(err)

		require.IsType((*UnionIterator)(nil), it)
		union := it.(*UnionIterator)
		require.Len(union.Subiterators(), 2)
	})

	t.Run("IntersectionIteratorType", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		rel1 := schema.NewTestBaseRelation("doc", "viewer", "user", tuple.Ellipsis)
		rel2 := schema.NewTestBaseRelation("doc", "editor", "user", tuple.Ellipsis)

		outline := Outline{
			Type: IntersectionIteratorType,
			SubOutlines: []Outline{
				{Type: DatastoreIteratorType, Args: &IteratorArgs{Relation: rel1}},
				{Type: DatastoreIteratorType, Args: &IteratorArgs{Relation: rel2}},
			},
		}

		canonical, err := CanonicalizeOutline(outline)
		require.NoError(err)

		it, err := canonical.Compile()
		require.NoError(err)

		require.IsType((*IntersectionIterator)(nil), it)
		intersection := it.(*IntersectionIterator)
		require.Len(intersection.Subiterators(), 2)
	})

	t.Run("FixedIteratorType", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		path1 := MustPathFromString("document:doc1#viewer@user:alice")
		path2 := MustPathFromString("document:doc2#editor@user:bob")

		outline := Outline{
			Type: FixedIteratorType,
			Args: &IteratorArgs{
				FixedPaths: []Path{path1, path2},
			},
		}

		canonical, err := CanonicalizeOutline(outline)
		require.NoError(err)

		it, err := canonical.Compile()
		require.NoError(err)

		require.IsType((*FixedIterator)(nil), it)
		fixed := it.(*FixedIterator)
		require.Len(fixed.paths, 2)
	})

	t.Run("FixedIteratorType_NoPaths", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		outline := Outline{
			Type: FixedIteratorType,
			Args: nil,
		}

		canonical, err := CanonicalizeOutline(outline)
		require.NoError(err)

		it, err := canonical.Compile()
		require.NoError(err)

		require.IsType((*FixedIterator)(nil), it)
		fixed := it.(*FixedIterator)
		require.Empty(fixed.paths)
	})

	t.Run("ArrowIteratorType", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		outline := Outline{
			Type: ArrowIteratorType,
			SubOutlines: []Outline{
				{Type: NullIteratorType},
				{Type: NullIteratorType},
			},
		}

		canonical, err := CanonicalizeOutline(outline)
		require.NoError(err)

		it, err := canonical.Compile()
		require.NoError(err)

		require.IsType((*ArrowIterator)(nil), it)
		arrow := it.(*ArrowIterator)
		require.Len(arrow.Subiterators(), 2)
	})

	t.Run("ArrowIteratorType_WrongSubiteratorCount", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		outline := Outline{
			Type: ArrowIteratorType,
			SubOutlines: []Outline{
				{Type: NullIteratorType},
			},
		}

		canonical, err := CanonicalizeOutline(outline)
		require.NoError(err)

		_, err = canonical.Compile()
		require.Error(err)
		require.Contains(err.Error(), "ArrowIterator requires exactly 2 subiterators")
	})

	t.Run("ExclusionIteratorType", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		outline := Outline{
			Type: ExclusionIteratorType,
			SubOutlines: []Outline{
				{Type: NullIteratorType},
				{Type: NullIteratorType},
			},
		}

		canonical, err := CanonicalizeOutline(outline)
		require.NoError(err)

		it, err := canonical.Compile()
		require.NoError(err)

		require.IsType((*ExclusionIterator)(nil), it)
		exclusion := it.(*ExclusionIterator)
		require.Len(exclusion.Subiterators(), 2)
	})

	t.Run("ExclusionIteratorType_WrongSubiteratorCount", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		outline := Outline{
			Type: ExclusionIteratorType,
			SubOutlines: []Outline{
				{Type: NullIteratorType},
			},
		}

		canonical, err := CanonicalizeOutline(outline)
		require.NoError(err)

		_, err = canonical.Compile()
		require.Error(err)
		require.Contains(err.Error(), "ExclusionIterator requires exactly 2 subiterators")
	})

	t.Run("CaveatIteratorType", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		caveat := &core.ContextualizedCaveat{CaveatName: "test_caveat"}
		outline := Outline{
			Type: CaveatIteratorType,
			Args: &IteratorArgs{
				Caveat: caveat,
			},
			SubOutlines: []Outline{
				{Type: NullIteratorType},
			},
		}

		canonical, err := CanonicalizeOutline(outline)
		require.NoError(err)

		it, err := canonical.Compile()
		require.NoError(err)

		require.IsType((*CaveatIterator)(nil), it)
		cav := it.(*CaveatIterator)
		require.Equal(caveat, cav.caveat)
	})

	// Note: CaveatIteratorType error tests removed because canonicalization
	// handles these cases before compilation (extracts caveats, normalizes structure)

	t.Run("AliasIteratorType", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		outline := Outline{
			Type: AliasIteratorType,
			Args: &IteratorArgs{
				RelationName: "viewer",
			},
			SubOutlines: []Outline{
				{Type: NullIteratorType},
			},
		}

		canonical, err := CanonicalizeOutline(outline)
		require.NoError(err)

		it, err := canonical.Compile()
		require.NoError(err)

		require.IsType((*AliasIterator)(nil), it)
		alias := it.(*AliasIterator)
		require.Equal("viewer", alias.relation)
	})

	t.Run("AliasIteratorType_MissingRelationName", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		outline := Outline{
			Type: AliasIteratorType,
			Args: nil,
			SubOutlines: []Outline{
				{Type: NullIteratorType},
			},
		}

		canonical, err := CanonicalizeOutline(outline)
		require.NoError(err)

		_, err = canonical.Compile()
		require.Error(err)
		require.Contains(err.Error(), "AliasIterator requires RelationName")
	})

	t.Run("RecursiveIteratorType", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		outline := Outline{
			Type: RecursiveIteratorType,
			Args: &IteratorArgs{
				DefinitionName: "document",
				RelationName:   "parent",
			},
			SubOutlines: []Outline{
				{Type: NullIteratorType},
			},
		}

		canonical, err := CanonicalizeOutline(outline)
		require.NoError(err)

		it, err := canonical.Compile()
		require.NoError(err)

		require.IsType((*RecursiveIterator)(nil), it)
		rec := it.(*RecursiveIterator)
		require.Equal("document", rec.definitionName)
		require.Equal("parent", rec.relationName)
	})

	t.Run("RecursiveIteratorType_MissingDefinitionName", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		outline := Outline{
			Type: RecursiveIteratorType,
			Args: &IteratorArgs{
				RelationName: "parent",
			},
			SubOutlines: []Outline{
				{Type: NullIteratorType},
			},
		}

		canonical, err := CanonicalizeOutline(outline)
		require.NoError(err)

		_, err = canonical.Compile()
		require.Error(err)
		require.Contains(err.Error(), "RecursiveIterator requires DefinitionName and RelationName")
	})

	t.Run("RecursiveSentinelIteratorType", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		outline := Outline{
			Type: RecursiveSentinelIteratorType,
			Args: &IteratorArgs{
				DefinitionName: "document",
				RelationName:   "parent",
			},
		}

		canonical, err := CanonicalizeOutline(outline)
		require.NoError(err)

		it, err := canonical.Compile()
		require.NoError(err)

		require.IsType((*RecursiveSentinelIterator)(nil), it)
		sentinel := it.(*RecursiveSentinelIterator)
		require.Equal("document", sentinel.definitionName)
		require.Equal("parent", sentinel.relationName)
	})

	t.Run("RecursiveSentinelIteratorType_MissingArgs", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		outline := Outline{
			Type: RecursiveSentinelIteratorType,
			Args: nil,
		}

		canonical, err := CanonicalizeOutline(outline)
		require.NoError(err)

		_, err = canonical.Compile()
		require.Error(err)
		require.Contains(err.Error(), "RecursiveSentinelIterator requires DefinitionName and RelationName")
	})

	t.Run("IntersectionArrowIteratorType", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		outline := Outline{
			Type: IntersectionArrowIteratorType,
			SubOutlines: []Outline{
				{Type: NullIteratorType},
				{Type: NullIteratorType},
			},
		}

		canonical, err := CanonicalizeOutline(outline)
		require.NoError(err)

		it, err := canonical.Compile()
		require.NoError(err)

		require.IsType((*IntersectionArrowIterator)(nil), it)
		intArrow := it.(*IntersectionArrowIterator)
		require.Len(intArrow.Subiterators(), 2)
	})

	t.Run("SelfIteratorType", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		outline := Outline{
			Type: SelfIteratorType,
			Args: &IteratorArgs{
				RelationName:   "viewer",
				DefinitionName: "document",
			},
		}

		canonical, err := CanonicalizeOutline(outline)
		require.NoError(err)

		it, err := canonical.Compile()
		require.NoError(err)

		require.IsType((*SelfIterator)(nil), it)
		self := it.(*SelfIterator)
		require.Equal("viewer", self.relation)
		require.Equal("document", self.typeName)
	})

	t.Run("SelfIteratorType_MissingArgs", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		outline := Outline{
			Type: SelfIteratorType,
			Args: &IteratorArgs{
				RelationName: "viewer",
			},
		}

		canonical, err := CanonicalizeOutline(outline)
		require.NoError(err)

		_, err = canonical.Compile()
		require.Error(err)
		require.Contains(err.Error(), "SelfIterator requires RelationName and DefinitionName")
	})

	t.Run("UnknownIteratorType", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		outline := Outline{
			Type: IteratorType('Z'), // Unknown type
		}

		canonical, err := CanonicalizeOutline(outline)
		require.NoError(err)

		_, err = canonical.Compile()
		require.Error(err)
		require.Contains(err.Error(), "unknown iterator type")
	})
}

func TestOutline_Decompile(t *testing.T) {
	t.Parallel()

	t.Run("NilIterator", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		outline, err := Decompile(nil)
		require.NoError(err)
		require.Equal(NullIteratorType, outline.Type)
	})

	t.Run("DatastoreIterator", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		rel := schema.NewTestBaseRelation("document", "viewer", "user", tuple.Ellipsis)
		ds := NewDatastoreIterator(rel)

		outline, err := Decompile(ds)
		require.NoError(err)
		require.Equal(DatastoreIteratorType, outline.Type)
		require.NotNil(outline.Args)
		require.Equal(rel, outline.Args.Relation)
	})

	t.Run("UnionIterator", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		union := NewUnionIterator(
			NewFixedIterator(),
			NewFixedIterator(),
		)

		outline, err := Decompile(union)
		require.NoError(err)
		require.Equal(UnionIteratorType, outline.Type)
		require.Len(outline.SubOutlines, 2)
	})

	t.Run("IntersectionIterator", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		intersection := NewIntersectionIterator(
			NewFixedIterator(),
			NewFixedIterator(),
		)

		outline, err := Decompile(intersection)
		require.NoError(err)
		require.Equal(IntersectionIteratorType, outline.Type)
		require.Len(outline.SubOutlines, 2)
	})

	t.Run("FixedIterator", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		path1 := MustPathFromString("document:doc1#viewer@user:alice")
		path2 := MustPathFromString("document:doc2#editor@user:bob")
		fixed := NewFixedIterator(path1, path2)

		outline, err := Decompile(fixed)
		require.NoError(err)
		require.Equal(FixedIteratorType, outline.Type)
		require.NotNil(outline.Args)
		require.Len(outline.Args.FixedPaths, 2)
	})

	t.Run("ArrowIterator", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		arrow := NewArrowIterator(NewFixedIterator(), NewFixedIterator())

		outline, err := Decompile(arrow)
		require.NoError(err)
		require.Equal(ArrowIteratorType, outline.Type)
		require.Len(outline.SubOutlines, 2)
	})

	t.Run("ExclusionIterator", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		exclusion := NewExclusionIterator(NewFixedIterator(), NewFixedIterator())

		outline, err := Decompile(exclusion)
		require.NoError(err)
		require.Equal(ExclusionIteratorType, outline.Type)
		require.Len(outline.SubOutlines, 2)
	})

	t.Run("CaveatIterator", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		caveat := &core.ContextualizedCaveat{CaveatName: "test_caveat"}
		cav := NewCaveatIterator(NewFixedIterator(), caveat)

		outline, err := Decompile(cav)
		require.NoError(err)
		require.Equal(CaveatIteratorType, outline.Type)
		require.NotNil(outline.Args)
		require.Equal(caveat, outline.Args.Caveat)
		require.Len(outline.SubOutlines, 1)
	})

	t.Run("AliasIterator", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		alias := NewAliasIterator("viewer", NewFixedIterator())

		outline, err := Decompile(alias)
		require.NoError(err)
		require.Equal(AliasIteratorType, outline.Type)
		require.NotNil(outline.Args)
		require.Equal("viewer", outline.Args.RelationName)
	})

	t.Run("RecursiveIterator", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		rec := NewRecursiveIterator(NewFixedIterator(), "document", "parent")

		outline, err := Decompile(rec)
		require.NoError(err)
		require.Equal(RecursiveIteratorType, outline.Type)
		require.NotNil(outline.Args)
		require.Equal("document", outline.Args.DefinitionName)
		require.Equal("parent", outline.Args.RelationName)
	})

	t.Run("RecursiveSentinelIterator", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		sentinel := NewRecursiveSentinelIterator("document", "parent", false)

		outline, err := Decompile(sentinel)
		require.NoError(err)
		require.Equal(RecursiveSentinelIteratorType, outline.Type)
		require.NotNil(outline.Args)
		require.Equal("document", outline.Args.DefinitionName)
		require.Equal("parent", outline.Args.RelationName)
	})

	t.Run("IntersectionArrowIterator", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		intArrow := NewIntersectionArrowIterator(NewFixedIterator(), NewFixedIterator())

		outline, err := Decompile(intArrow)
		require.NoError(err)
		require.Equal(IntersectionArrowIteratorType, outline.Type)
		require.Len(outline.SubOutlines, 2)
	})

	t.Run("SelfIterator", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		self := NewSelfIterator("viewer", "document")

		outline, err := Decompile(self)
		require.NoError(err)
		require.Equal(SelfIteratorType, outline.Type)
		require.NotNil(outline.Args)
		require.Equal("viewer", outline.Args.RelationName)
		require.Equal("document", outline.Args.DefinitionName)
	})
}

func TestOutline_CompileDecompileRoundtrip(t *testing.T) {
	t.Parallel()

	t.Run("DatastoreIterator", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		rel := schema.NewTestBaseRelation("document", "viewer", "user", tuple.Ellipsis)
		original := Outline{
			Type: DatastoreIteratorType,
			Args: &IteratorArgs{
				Relation: rel,
			},
		}

		// Canonicalize first (required before compilation)
		canonical, err := CanonicalizeOutline(original)
		require.NoError(err)

		// Compile to iterator
		it, err := canonical.Compile()
		require.NoError(err)

		// Decompile back to outline
		roundtrip, err := Decompile(it)
		require.NoError(err)

		// Should be equal to canonical form
		require.True(canonical.Equals(roundtrip))
	})

	t.Run("ComplexNestedStructure", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		// Create a complex nested structure
		// Union( Null, Intersection( Null, Null ) )
		original := Outline{
			Type: UnionIteratorType,
			SubOutlines: []Outline{
				{Type: NullIteratorType},
				{
					Type: IntersectionIteratorType,
					SubOutlines: []Outline{
						{Type: NullIteratorType},
						{Type: NullIteratorType},
					},
				},
			},
		}

		// Canonicalize first (required before compilation)
		canonical, err := CanonicalizeOutline(original)
		require.NoError(err)

		// Compile to iterator
		it, err := canonical.Compile()
		require.NoError(err)

		// Decompile back to outline
		roundtrip, err := Decompile(it)
		require.NoError(err)

		// Should be equal to canonical form
		require.True(canonical.Equals(roundtrip))
	})

	t.Run("ArrowWithDatastore", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		rel1 := schema.NewTestBaseRelation("document", "viewer", "user", tuple.Ellipsis)
		rel2 := schema.NewTestBaseRelation("user", "member", "user", tuple.Ellipsis)

		original := Outline{
			Type: ArrowIteratorType,
			SubOutlines: []Outline{
				{
					Type: DatastoreIteratorType,
					Args: &IteratorArgs{Relation: rel1},
				},
				{
					Type: DatastoreIteratorType,
					Args: &IteratorArgs{Relation: rel2},
				},
			},
		}

		// Canonicalize first (required before compilation)
		canonical, err := CanonicalizeOutline(original)
		require.NoError(err)

		// Compile to iterator
		it, err := canonical.Compile()
		require.NoError(err)

		// Decompile back to outline
		roundtrip, err := Decompile(it)
		require.NoError(err)

		// Should be equal to canonical form
		require.True(canonical.Equals(roundtrip))
	})

	t.Run("CaveatIterator", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		caveat := &core.ContextualizedCaveat{CaveatName: "test_caveat"}
		original := Outline{
			Type: CaveatIteratorType,
			Args: &IteratorArgs{Caveat: caveat},
			SubOutlines: []Outline{
				{Type: NullIteratorType},
			},
		}

		// Canonicalize first (required before compilation)
		canonical, err := CanonicalizeOutline(original)
		require.NoError(err)

		// Compile to iterator
		it, err := canonical.Compile()
		require.NoError(err)

		// Decompile back to outline
		roundtrip, err := Decompile(it)
		require.NoError(err)

		// Should be equal to canonical form
		require.True(canonical.Equals(roundtrip))
	})
}

func TestOutline_Equals(t *testing.T) {
	t.Parallel()

	t.Run("IdenticalOutlines", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		o1 := Outline{Type: NullIteratorType}
		o2 := Outline{Type: NullIteratorType}

		require.True(o1.Equals(o2))
		require.True(o2.Equals(o1))
	})

	t.Run("DifferentTypes", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		o1 := Outline{Type: NullIteratorType}
		o2 := Outline{Type: FixedIteratorType}

		require.False(o1.Equals(o2))
	})

	t.Run("DifferentArgs", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		rel1 := schema.NewTestBaseRelation("document", "viewer", "user", tuple.Ellipsis)
		rel2 := schema.NewTestBaseRelation("document", "editor", "user", tuple.Ellipsis)

		o1 := Outline{
			Type: DatastoreIteratorType,
			Args: &IteratorArgs{Relation: rel1},
		}
		o2 := Outline{
			Type: DatastoreIteratorType,
			Args: &IteratorArgs{Relation: rel2},
		}

		require.False(o1.Equals(o2))
	})

	t.Run("DifferentSubiteratorCount", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		o1 := Outline{
			Type: UnionIteratorType,
			SubOutlines: []Outline{
				{Type: NullIteratorType},
			},
		}
		o2 := Outline{
			Type: UnionIteratorType,
			SubOutlines: []Outline{
				{Type: NullIteratorType},
				{Type: NullIteratorType},
			},
		}

		require.False(o1.Equals(o2))
	})

	t.Run("DifferentSubiterators", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		o1 := Outline{
			Type: UnionIteratorType,
			SubOutlines: []Outline{
				{Type: NullIteratorType},
			},
		}
		o2 := Outline{
			Type: UnionIteratorType,
			SubOutlines: []Outline{
				{Type: FixedIteratorType},
			},
		}

		require.False(o1.Equals(o2))
	})

	t.Run("NilArgs", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		o1 := Outline{Type: NullIteratorType, Args: nil}
		o2 := Outline{Type: NullIteratorType, Args: nil}

		require.True(o1.Equals(o2))
	})

	t.Run("OneNilArgs", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		o1 := Outline{Type: FixedIteratorType, Args: nil}
		o2 := Outline{
			Type: FixedIteratorType,
			Args: &IteratorArgs{},
		}

		require.False(o1.Equals(o2))
	})
}

func TestOutlineCompare(t *testing.T) {
	t.Parallel()

	t.Run("EqualOutlines", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		o1 := Outline{Type: NullIteratorType}
		o2 := Outline{Type: NullIteratorType}

		require.Equal(0, OutlineCompare(o1, o2))
	})

	t.Run("DifferentTypes", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		o1 := Outline{Type: DatastoreIteratorType}
		o2 := Outline{Type: UnionIteratorType}

		// DatastoreIteratorType ('D') < UnionIteratorType ('|')
		require.Equal(-1, OutlineCompare(o1, o2))
		require.Equal(1, OutlineCompare(o2, o1))
	})

	t.Run("SameTypeDifferentArgs", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		o1 := Outline{
			Type: AliasIteratorType,
			Args: &IteratorArgs{RelationName: "viewer"},
		}
		o2 := Outline{
			Type: AliasIteratorType,
			Args: &IteratorArgs{RelationName: "editor"},
		}

		// "editor" < "viewer" lexicographically
		require.Equal(1, OutlineCompare(o1, o2))
		require.Equal(-1, OutlineCompare(o2, o1))
	})

	t.Run("DifferentSubiteratorCount", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		o1 := Outline{
			Type: UnionIteratorType,
			SubOutlines: []Outline{
				{Type: NullIteratorType},
			},
		}
		o2 := Outline{
			Type: UnionIteratorType,
			SubOutlines: []Outline{
				{Type: NullIteratorType},
				{Type: NullIteratorType},
			},
		}

		require.Equal(-1, OutlineCompare(o1, o2))
		require.Equal(1, OutlineCompare(o2, o1))
	})

	t.Run("DifferentSubiterators", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		o1 := Outline{
			Type: UnionIteratorType,
			SubOutlines: []Outline{
				{Type: DatastoreIteratorType},
			},
		}
		o2 := Outline{
			Type: UnionIteratorType,
			SubOutlines: []Outline{
				{Type: UnionIteratorType},
			},
		}

		// DatastoreIteratorType ('D') < UnionIteratorType ('|')
		require.Equal(-1, OutlineCompare(o1, o2))
		require.Equal(1, OutlineCompare(o2, o1))
	})

	t.Run("NilArgs", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		o1 := Outline{Type: FixedIteratorType, Args: nil}
		o2 := Outline{Type: FixedIteratorType, Args: &IteratorArgs{}}

		// nil < non-nil
		require.Equal(-1, OutlineCompare(o1, o2))
		require.Equal(1, OutlineCompare(o2, o1))
	})
}

func TestArgsCompare(t *testing.T) {
	t.Parallel()

	t.Run("BothNil", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		result := argsCompare(nil, nil)
		require.Equal(0, result)
	})

	t.Run("FirstNil", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		result := argsCompare(nil, &IteratorArgs{})
		require.Equal(-1, result)
	})

	t.Run("SecondNil", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		result := argsCompare(&IteratorArgs{}, nil)
		require.Equal(1, result)
	})

	t.Run("DifferentDefinitionName", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		a1 := &IteratorArgs{DefinitionName: "document"}
		a2 := &IteratorArgs{DefinitionName: "folder"}

		// "document" < "folder"
		require.Equal(-1, argsCompare(a1, a2))
		require.Equal(1, argsCompare(a2, a1))
	})

	t.Run("DifferentRelationName", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		a1 := &IteratorArgs{
			DefinitionName: "document",
			RelationName:   "viewer",
		}
		a2 := &IteratorArgs{
			DefinitionName: "document",
			RelationName:   "editor",
		}

		// "editor" < "viewer"
		require.Equal(1, argsCompare(a1, a2))
		require.Equal(-1, argsCompare(a2, a1))
	})

	t.Run("DifferentRelation", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		rel1 := schema.NewTestBaseRelation("document", "viewer", "user", tuple.Ellipsis)
		rel2 := schema.NewTestBaseRelation("document", "editor", "user", tuple.Ellipsis)

		a1 := &IteratorArgs{Relation: rel1}
		a2 := &IteratorArgs{Relation: rel2}

		// Compare should use BaseRelation.Compare
		result := argsCompare(a1, a2)
		require.NotEqual(0, result)
	})

	t.Run("DifferentCaveat", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		caveat1 := &core.ContextualizedCaveat{CaveatName: "caveat1"}
		caveat2 := &core.ContextualizedCaveat{CaveatName: "caveat2"}

		a1 := &IteratorArgs{Caveat: caveat1}
		a2 := &IteratorArgs{Caveat: caveat2}

		result := argsCompare(a1, a2)
		require.NotEqual(0, result)
	})

	t.Run("DifferentFixedPathsLength", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		path1 := MustPathFromString("document:doc1#viewer@user:alice")

		a1 := &IteratorArgs{FixedPaths: []Path{path1}}
		a2 := &IteratorArgs{FixedPaths: []Path{path1, path1}}

		require.Equal(-1, argsCompare(a1, a2))
		require.Equal(1, argsCompare(a2, a1))
	})

	t.Run("DifferentFixedPaths", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		path1 := MustPathFromString("document:doc1#viewer@user:alice")
		path2 := MustPathFromString("document:doc2#viewer@user:alice")

		a1 := &IteratorArgs{FixedPaths: []Path{path1}}
		a2 := &IteratorArgs{FixedPaths: []Path{path2}}

		result := argsCompare(a1, a2)
		require.NotEqual(0, result)
	})

	t.Run("IdenticalArgs", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		rel := schema.NewTestBaseRelation("document", "viewer", "user", tuple.Ellipsis)
		caveat := &core.ContextualizedCaveat{CaveatName: "test_caveat"}
		path := MustPathFromString("document:doc1#viewer@user:alice")

		a1 := &IteratorArgs{
			Relation:       rel,
			DefinitionName: "document",
			RelationName:   "viewer",
			Caveat:         caveat,
			FixedPaths:     []Path{path},
		}
		a2 := &IteratorArgs{
			Relation:       rel,
			DefinitionName: "document",
			RelationName:   "viewer",
			Caveat:         caveat,
			FixedPaths:     []Path{path},
		}

		require.Equal(0, argsCompare(a1, a2))
	})
}

func TestCaveatCompare(t *testing.T) {
	t.Parallel()

	t.Run("BothNil", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		result := caveatCompare(nil, nil)
		require.Equal(0, result)
	})

	t.Run("FirstNil", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		caveat := &core.ContextualizedCaveat{CaveatName: "test_caveat"}
		result := caveatCompare(nil, caveat)
		require.Equal(-1, result)
	})

	t.Run("SecondNil", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		caveat := &core.ContextualizedCaveat{CaveatName: "test_caveat"}
		result := caveatCompare(caveat, nil)
		require.Equal(1, result)
	})

	t.Run("DifferentCaveatNames", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		caveat1 := &core.ContextualizedCaveat{CaveatName: "caveat1"}
		caveat2 := &core.ContextualizedCaveat{CaveatName: "caveat2"}

		// "caveat1" < "caveat2"
		require.Equal(-1, caveatCompare(caveat1, caveat2))
		require.Equal(1, caveatCompare(caveat2, caveat1))
	})

	t.Run("SameNameSameCaveat", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		caveat1 := &core.ContextualizedCaveat{CaveatName: "test_caveat"}
		caveat2 := &core.ContextualizedCaveat{CaveatName: "test_caveat"}

		result := caveatCompare(caveat1, caveat2)
		require.Equal(0, result)
	})

	t.Run("SameNameIdenticalCaveat", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		caveat1 := &core.ContextualizedCaveat{
			CaveatName: "test_caveat",
		}
		caveat2 := &core.ContextualizedCaveat{
			CaveatName: "test_caveat",
		}

		result := caveatCompare(caveat1, caveat2)
		// Should be equal since both have same name and no context
		require.Equal(0, result)
	})
}

func TestCanonicalKey_Type(t *testing.T) {
	t.Parallel()

	t.Run("String method", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		key := CanonicalKey("test_key")
		require.Equal("test_key", key.String())
	})

	t.Run("IsEmpty returns true for empty key", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		key := CanonicalKey("")
		require.True(key.IsEmpty())
	})

	t.Run("IsEmpty returns false for non-empty key", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		key := CanonicalKey("test_key")
		require.False(key.IsEmpty())
	})

	t.Run("Hash produces consistent value", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		key := CanonicalKey("test_key")
		hash1 := key.Hash()
		hash2 := key.Hash()
		require.Equal(hash1, hash2)
	})

	t.Run("Hash produces different values for different keys", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		key1 := CanonicalKey("key1")
		key2 := CanonicalKey("key2")
		hash1 := key1.Hash()
		hash2 := key2.Hash()
		require.NotEqual(hash1, hash2)
	})
}

func TestSerializeOutline(t *testing.T) {
	t.Parallel()

	t.Run("NullIteratorType", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		outline := Outline{Type: NullIteratorType}
		key := outline.Serialize()
		require.Equal("0", key.String())
	})

	t.Run("DatastoreIteratorType with Relation", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		rel := schema.NewTestBaseRelation("document", "viewer", "user", tuple.Ellipsis)
		outline := Outline{
			Type: DatastoreIteratorType,
			Args: &IteratorArgs{
				Relation: rel,
			},
		}

		key := outline.Serialize()
		// Should contain type 'D', base relation serialization
		require.Contains(key.String(), "D(")
		require.Contains(key.String(), "base:")
		require.Contains(key.String(), "document/viewer/user")
	})

	t.Run("UnionIteratorType with subiterators", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		outline := Outline{
			Type: UnionIteratorType,
			SubOutlines: []Outline{
				{Type: NullIteratorType},
				{Type: NullIteratorType},
			},
		}

		key := outline.Serialize()
		require.Equal("|[0,0]", key.String())
	})

	t.Run("IntersectionIteratorType with subiterators", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		outline := Outline{
			Type: IntersectionIteratorType,
			SubOutlines: []Outline{
				{Type: NullIteratorType},
				{Type: NullIteratorType},
			},
		}

		key := outline.Serialize()
		require.Equal("&[0,0]", key.String())
	})

	t.Run("Nested structure", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		// Union( Null, Intersection( Null, Null ) )
		outline := Outline{
			Type: UnionIteratorType,
			SubOutlines: []Outline{
				{Type: NullIteratorType},
				{
					Type: IntersectionIteratorType,
					SubOutlines: []Outline{
						{Type: NullIteratorType},
						{Type: NullIteratorType},
					},
				},
			},
		}

		key := outline.Serialize()
		require.Equal("|[0,&[0,0]]", key.String())
	})

	t.Run("CaveatIteratorType", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		caveat := &core.ContextualizedCaveat{CaveatName: "age_check"}
		outline := Outline{
			Type: CaveatIteratorType,
			Args: &IteratorArgs{
				Caveat: caveat,
			},
			SubOutlines: []Outline{
				{Type: NullIteratorType},
			},
		}

		key := outline.Serialize()
		require.Equal("C(cav:age_check)[0]", key.String())
	})

	t.Run("AliasIteratorType", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		outline := Outline{
			Type: AliasIteratorType,
			Args: &IteratorArgs{
				RelationName: "viewer",
			},
			SubOutlines: []Outline{
				{Type: NullIteratorType},
			},
		}

		key := outline.Serialize()
		require.Equal("@(rel:viewer)[0]", key.String())
	})

	t.Run("RecursiveIteratorType", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		outline := Outline{
			Type: RecursiveIteratorType,
			Args: &IteratorArgs{
				DefinitionName: "document",
				RelationName:   "parent",
			},
			SubOutlines: []Outline{
				{Type: NullIteratorType},
			},
		}

		key := outline.Serialize()
		require.Equal("R(def:document,rel:parent)[0]", key.String())
	})

	t.Run("FixedIteratorType with paths", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		path1 := MustPathFromString("document:doc1#viewer@user:alice")
		path2 := MustPathFromString("document:doc2#viewer@user:bob")

		outline := Outline{
			Type: FixedIteratorType,
			Args: &IteratorArgs{
				FixedPaths: []Path{path1, path2},
			},
		}

		key := outline.Serialize()
		require.Equal("F(paths:2)", key.String())
	})

	t.Run("Empty Args", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		outline := Outline{
			Type: UnionIteratorType,
			Args: &IteratorArgs{}, // Empty but non-nil
		}

		key := outline.Serialize()
		// Should not include parentheses for empty args
		require.Equal("|", key.String())
	})

	t.Run("Nil Args", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		outline := Outline{
			Type: IntersectionIteratorType,
			Args: nil,
		}

		key := outline.Serialize()
		require.Equal("&", key.String())
	})
}

func TestSerializeOutline_Deterministic(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	rel := schema.NewTestBaseRelation("document", "viewer", "user", tuple.Ellipsis)
	outline := Outline{
		Type: UnionIteratorType,
		SubOutlines: []Outline{
			{
				Type: DatastoreIteratorType,
				Args: &IteratorArgs{Relation: rel},
			},
			{Type: NullIteratorType},
		},
	}

	// Serialize multiple times
	key1 := outline.Serialize()
	key2 := outline.Serialize()
	key3 := outline.Serialize()

	// Should all be identical
	require.Equal(key1.String(), key2.String())
	require.Equal(key2.String(), key3.String())
}

func TestSerializeOutline_Args(t *testing.T) {
	t.Parallel()

	t.Run("DefinitionName only", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		outline := Outline{
			Type: RecursiveSentinelIteratorType,
			Args: &IteratorArgs{
				DefinitionName: "document",
			},
		}

		key := outline.Serialize()
		require.Contains(key.String(), "def:document")
	})

	t.Run("RelationName only", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		outline := Outline{
			Type: AliasIteratorType,
			Args: &IteratorArgs{
				RelationName: "viewer",
			},
		}

		key := outline.Serialize()
		require.Contains(key.String(), "rel:viewer")
	})

	t.Run("Multiple Args fields", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		outline := Outline{
			Type: RecursiveIteratorType,
			Args: &IteratorArgs{
				DefinitionName: "document",
				RelationName:   "parent",
			},
		}

		key := outline.Serialize()
		require.Contains(key.String(), "def:document")
		require.Contains(key.String(), "rel:parent")
	})
}

func TestSerializeOutline_IgnoresCanonicalKey(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	// Create two identical outlines with different CanonicalKeys
	outline1 := Outline{
		Type:         NullIteratorType,
		CanonicalKey: CanonicalKey("key1"),
	}

	outline2 := Outline{
		Type:         NullIteratorType,
		CanonicalKey: CanonicalKey("key2"),
	}

	// Serialization should be identical (ignoring CanonicalKey field)
	key1 := outline1.Serialize()
	key2 := outline2.Serialize()
	require.Equal(key1.String(), key2.String())
}

func TestCanonicalKey_Hash(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	// Create different keys
	key1 := CanonicalKey("|[0,0]")
	key2 := CanonicalKey("&[0,0]")
	key3 := CanonicalKey("|[0,0]") // Same as key1

	hash1 := key1.Hash()
	hash2 := key2.Hash()
	hash3 := key3.Hash()

	// Same keys produce same hash
	require.Equal(hash1, hash3)

	// Different keys produce different hashes
	require.NotEqual(hash1, hash2)
}
