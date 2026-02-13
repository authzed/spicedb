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
		it, err := outline.Compile()
		require.NoError(err)

		// Should produce a FixedIterator with no paths
		fixed, ok := it.(*FixedIterator)
		require.True(ok, "should be a FixedIterator")
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

		it, err := outline.Compile()
		require.NoError(err)

		ds, ok := it.(*DatastoreIterator)
		require.True(ok, "should be a DatastoreIterator")
		require.Equal(rel, ds.base)
	})

	t.Run("DatastoreIteratorType_MissingRelation", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		outline := Outline{
			Type: DatastoreIteratorType,
			Args: nil,
		}

		_, err := outline.Compile()
		require.Error(err)
		require.Contains(err.Error(), "DatastoreIterator requires Relation")
	})

	t.Run("UnionIteratorType", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		outline := Outline{
			Type: UnionIteratorType,
			Subiterators: []Outline{
				{Type: NullIteratorType},
				{Type: NullIteratorType},
			},
		}

		it, err := outline.Compile()
		require.NoError(err)

		union, ok := it.(*UnionIterator)
		require.True(ok, "should be a UnionIterator")
		require.Len(union.Subiterators(), 2)
	})

	t.Run("IntersectionIteratorType", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		outline := Outline{
			Type: IntersectionIteratorType,
			Subiterators: []Outline{
				{Type: NullIteratorType},
				{Type: NullIteratorType},
			},
		}

		it, err := outline.Compile()
		require.NoError(err)

		intersection, ok := it.(*IntersectionIterator)
		require.True(ok, "should be an IntersectionIterator")
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

		it, err := outline.Compile()
		require.NoError(err)

		fixed, ok := it.(*FixedIterator)
		require.True(ok, "should be a FixedIterator")
		require.Len(fixed.paths, 2)
	})

	t.Run("FixedIteratorType_NoPaths", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		outline := Outline{
			Type: FixedIteratorType,
			Args: nil,
		}

		it, err := outline.Compile()
		require.NoError(err)

		fixed, ok := it.(*FixedIterator)
		require.True(ok, "should be a FixedIterator")
		require.Empty(fixed.paths)
	})

	t.Run("ArrowIteratorType", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		outline := Outline{
			Type: ArrowIteratorType,
			Subiterators: []Outline{
				{Type: NullIteratorType},
				{Type: NullIteratorType},
			},
		}

		it, err := outline.Compile()
		require.NoError(err)

		arrow, ok := it.(*ArrowIterator)
		require.True(ok, "should be an ArrowIterator")
		require.Len(arrow.Subiterators(), 2)
	})

	t.Run("ArrowIteratorType_WrongSubiteratorCount", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		outline := Outline{
			Type: ArrowIteratorType,
			Subiterators: []Outline{
				{Type: NullIteratorType},
			},
		}

		_, err := outline.Compile()
		require.Error(err)
		require.Contains(err.Error(), "ArrowIterator requires exactly 2 subiterators")
	})

	t.Run("ExclusionIteratorType", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		outline := Outline{
			Type: ExclusionIteratorType,
			Subiterators: []Outline{
				{Type: NullIteratorType},
				{Type: NullIteratorType},
			},
		}

		it, err := outline.Compile()
		require.NoError(err)

		exclusion, ok := it.(*ExclusionIterator)
		require.True(ok, "should be an ExclusionIterator")
		require.Len(exclusion.Subiterators(), 2)
	})

	t.Run("ExclusionIteratorType_WrongSubiteratorCount", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		outline := Outline{
			Type: ExclusionIteratorType,
			Subiterators: []Outline{
				{Type: NullIteratorType},
			},
		}

		_, err := outline.Compile()
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
			Subiterators: []Outline{
				{Type: NullIteratorType},
			},
		}

		it, err := outline.Compile()
		require.NoError(err)

		cav, ok := it.(*CaveatIterator)
		require.True(ok, "should be a CaveatIterator")
		require.Equal(caveat, cav.caveat)
	})

	t.Run("CaveatIteratorType_MissingCaveat", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		outline := Outline{
			Type: CaveatIteratorType,
			Args: nil,
			Subiterators: []Outline{
				{Type: NullIteratorType},
			},
		}

		_, err := outline.Compile()
		require.Error(err)
		require.Contains(err.Error(), "CaveatIterator requires Caveat")
	})

	t.Run("CaveatIteratorType_WrongSubiteratorCount", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		caveat := &core.ContextualizedCaveat{CaveatName: "test_caveat"}
		outline := Outline{
			Type: CaveatIteratorType,
			Args: &IteratorArgs{
				Caveat: caveat,
			},
			Subiterators: []Outline{},
		}

		_, err := outline.Compile()
		require.Error(err)
		require.Contains(err.Error(), "CaveatIterator requires exactly 1 subiterator")
	})

	t.Run("AliasIteratorType", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		outline := Outline{
			Type: AliasIteratorType,
			Args: &IteratorArgs{
				RelationName: "viewer",
			},
			Subiterators: []Outline{
				{Type: NullIteratorType},
			},
		}

		it, err := outline.Compile()
		require.NoError(err)

		alias, ok := it.(*AliasIterator)
		require.True(ok, "should be an AliasIterator")
		require.Equal("viewer", alias.relation)
	})

	t.Run("AliasIteratorType_MissingRelationName", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		outline := Outline{
			Type: AliasIteratorType,
			Args: nil,
			Subiterators: []Outline{
				{Type: NullIteratorType},
			},
		}

		_, err := outline.Compile()
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
			Subiterators: []Outline{
				{Type: NullIteratorType},
			},
		}

		it, err := outline.Compile()
		require.NoError(err)

		rec, ok := it.(*RecursiveIterator)
		require.True(ok, "should be a RecursiveIterator")
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
			Subiterators: []Outline{
				{Type: NullIteratorType},
			},
		}

		_, err := outline.Compile()
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

		it, err := outline.Compile()
		require.NoError(err)

		sentinel, ok := it.(*RecursiveSentinelIterator)
		require.True(ok, "should be a RecursiveSentinelIterator")
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

		_, err := outline.Compile()
		require.Error(err)
		require.Contains(err.Error(), "RecursiveSentinelIterator requires DefinitionName and RelationName")
	})

	t.Run("IntersectionArrowIteratorType", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		outline := Outline{
			Type: IntersectionArrowIteratorType,
			Subiterators: []Outline{
				{Type: NullIteratorType},
				{Type: NullIteratorType},
			},
		}

		it, err := outline.Compile()
		require.NoError(err)

		intArrow, ok := it.(*IntersectionArrowIterator)
		require.True(ok, "should be an IntersectionArrowIterator")
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

		it, err := outline.Compile()
		require.NoError(err)

		self, ok := it.(*SelfIterator)
		require.True(ok, "should be a SelfIterator")
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

		_, err := outline.Compile()
		require.Error(err)
		require.Contains(err.Error(), "SelfIterator requires RelationName and DefinitionName")
	})

	t.Run("UnknownIteratorType", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		outline := Outline{
			Type: IteratorType('Z'), // Unknown type
		}

		_, err := outline.Compile()
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
		require.Len(outline.Subiterators, 2)
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
		require.Len(outline.Subiterators, 2)
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
		require.Len(outline.Subiterators, 2)
	})

	t.Run("ExclusionIterator", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		exclusion := NewExclusionIterator(NewFixedIterator(), NewFixedIterator())

		outline, err := Decompile(exclusion)
		require.NoError(err)
		require.Equal(ExclusionIteratorType, outline.Type)
		require.Len(outline.Subiterators, 2)
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
		require.Len(outline.Subiterators, 1)
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
		require.Len(outline.Subiterators, 2)
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

		// Compile to iterator
		it, err := original.Compile()
		require.NoError(err)

		// Decompile back to outline
		roundtrip, err := Decompile(it)
		require.NoError(err)

		// Should be equal
		require.True(original.Equals(roundtrip))
	})

	t.Run("ComplexNestedStructure", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		// Create a complex nested structure
		// Union( Fixed, Intersection( Fixed, Fixed ) )
		original := Outline{
			Type: UnionIteratorType,
			Subiterators: []Outline{
				{Type: FixedIteratorType},
				{
					Type: IntersectionIteratorType,
					Subiterators: []Outline{
						{Type: FixedIteratorType},
						{Type: FixedIteratorType},
					},
				},
			},
		}

		// Compile to iterator
		it, err := original.Compile()
		require.NoError(err)

		// Decompile back to outline
		roundtrip, err := Decompile(it)
		require.NoError(err)

		// Should be equal
		require.True(original.Equals(roundtrip))
	})

	t.Run("ArrowWithDatastore", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		rel1 := schema.NewTestBaseRelation("document", "viewer", "user", tuple.Ellipsis)
		rel2 := schema.NewTestBaseRelation("user", "member", "user", tuple.Ellipsis)

		original := Outline{
			Type: ArrowIteratorType,
			Subiterators: []Outline{
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

		// Compile to iterator
		it, err := original.Compile()
		require.NoError(err)

		// Decompile back to outline
		roundtrip, err := Decompile(it)
		require.NoError(err)

		// Should be equal
		require.True(original.Equals(roundtrip))
	})

	t.Run("CaveatIterator", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		caveat := &core.ContextualizedCaveat{CaveatName: "test_caveat"}
		original := Outline{
			Type: CaveatIteratorType,
			Args: &IteratorArgs{Caveat: caveat},
			Subiterators: []Outline{
				{Type: FixedIteratorType},
			},
		}

		// Compile to iterator
		it, err := original.Compile()
		require.NoError(err)

		// Decompile back to outline
		roundtrip, err := Decompile(it)
		require.NoError(err)

		// Should be equal
		require.True(original.Equals(roundtrip))
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
			Subiterators: []Outline{
				{Type: NullIteratorType},
			},
		}
		o2 := Outline{
			Type: UnionIteratorType,
			Subiterators: []Outline{
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
			Subiterators: []Outline{
				{Type: NullIteratorType},
			},
		}
		o2 := Outline{
			Type: UnionIteratorType,
			Subiterators: []Outline{
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
			Subiterators: []Outline{
				{Type: NullIteratorType},
			},
		}
		o2 := Outline{
			Type: UnionIteratorType,
			Subiterators: []Outline{
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
			Subiterators: []Outline{
				{Type: DatastoreIteratorType},
			},
		}
		o2 := Outline{
			Type: UnionIteratorType,
			Subiterators: []Outline{
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
