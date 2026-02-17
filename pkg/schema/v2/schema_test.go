package schema

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
)

func TestGetTypeDefinition(t *testing.T) {
	t.Parallel()

	schemaText := `
		definition user {}

		definition organization {
			relation member: user
		}

		definition resource {
			relation org: organization
			relation viewer: user
			permission view = org->member + viewer
		}
	`

	compiled, err := compiler.Compile(compiler.InputSchema{
		Source:       input.Source("test"),
		SchemaString: schemaText,
	}, compiler.AllowUnprefixedObjectType())
	require.NoError(t, err)

	schema, err := BuildSchemaFromCompiledSchema(*compiled)
	require.NoError(t, err)

	t.Run("existing type definition", func(t *testing.T) {
		t.Parallel()
		def, ok := schema.GetTypeDefinition("user")
		require.True(t, ok)
		require.NotNil(t, def)
		require.Equal(t, "user", def.Name())
	})

	t.Run("another existing type definition", func(t *testing.T) {
		t.Parallel()
		def, ok := schema.GetTypeDefinition("organization")
		require.True(t, ok)
		require.NotNil(t, def)
		require.Equal(t, "organization", def.Name())
	})

	t.Run("non-existent type definition", func(t *testing.T) {
		t.Parallel()
		def, ok := schema.GetTypeDefinition("nonexistent")
		require.False(t, ok)
		require.Nil(t, def)
	})

	t.Run("empty string type definition", func(t *testing.T) {
		t.Parallel()
		def, ok := schema.GetTypeDefinition("")
		require.False(t, ok)
		require.Nil(t, def)
	})
}

func TestGetRelation(t *testing.T) {
	t.Parallel()

	schemaText := `
		definition user {}

		definition organization {
			relation member: user
			relation admin: user
		}

		definition resource {
			relation org: organization
			relation viewer: user
			permission view = org->member + viewer
		}
	`

	compiled, err := compiler.Compile(compiler.InputSchema{
		Source:       input.Source("test"),
		SchemaString: schemaText,
	}, compiler.AllowUnprefixedObjectType())
	require.NoError(t, err)

	schema, err := BuildSchemaFromCompiledSchema(*compiled)
	require.NoError(t, err)

	orgDef, ok := schema.GetTypeDefinition("organization")
	require.True(t, ok)
	require.NotNil(t, orgDef)

	t.Run("existing relation", func(t *testing.T) {
		t.Parallel()
		rel, ok := orgDef.GetRelation("member")
		require.True(t, ok)
		require.NotNil(t, rel)
		require.Equal(t, "member", rel.Name())
	})

	t.Run("another existing relation", func(t *testing.T) {
		t.Parallel()
		rel, ok := orgDef.GetRelation("admin")
		require.True(t, ok)
		require.NotNil(t, rel)
		require.Equal(t, "admin", rel.Name())
	})

	t.Run("non-existent relation", func(t *testing.T) {
		t.Parallel()
		rel, ok := orgDef.GetRelation("nonexistent")
		require.False(t, ok)
		require.Nil(t, rel)
	})

	t.Run("empty string relation", func(t *testing.T) {
		t.Parallel()
		rel, ok := orgDef.GetRelation("")
		require.False(t, ok)
		require.Nil(t, rel)
	})

	t.Run("definition with no relations", func(t *testing.T) {
		t.Parallel()
		userDef, ok := schema.GetTypeDefinition("user")
		require.True(t, ok)
		require.NotNil(t, userDef)

		rel, ok := userDef.GetRelation("anything")
		require.False(t, ok)
		require.Nil(t, rel)
	})
}

func TestGetPermission(t *testing.T) {
	t.Parallel()

	schemaText := `
		definition user {}

		definition organization {
			relation member: user
		}

		definition resource {
			relation org: organization
			relation viewer: user
			relation editor: user
			permission view = org->member + viewer
			permission edit = org->member + editor
		}
	`

	compiled, err := compiler.Compile(compiler.InputSchema{
		Source:       input.Source("test"),
		SchemaString: schemaText,
	}, compiler.AllowUnprefixedObjectType())
	require.NoError(t, err)

	schema, err := BuildSchemaFromCompiledSchema(*compiled)
	require.NoError(t, err)

	resourceDef, ok := schema.GetTypeDefinition("resource")
	require.True(t, ok)
	require.NotNil(t, resourceDef)

	t.Run("existing permission", func(t *testing.T) {
		t.Parallel()
		perm, ok := resourceDef.GetPermission("view")
		require.True(t, ok)
		require.NotNil(t, perm)
		require.Equal(t, "view", perm.Name())
	})

	t.Run("another existing permission", func(t *testing.T) {
		t.Parallel()
		perm, ok := resourceDef.GetPermission("edit")
		require.True(t, ok)
		require.NotNil(t, perm)
		require.Equal(t, "edit", perm.Name())
	})

	t.Run("non-existent permission", func(t *testing.T) {
		t.Parallel()
		perm, ok := resourceDef.GetPermission("nonexistent")
		require.False(t, ok)
		require.Nil(t, perm)
	})

	t.Run("empty string permission", func(t *testing.T) {
		t.Parallel()
		perm, ok := resourceDef.GetPermission("")
		require.False(t, ok)
		require.Nil(t, perm)
	})

	t.Run("definition with no permissions", func(t *testing.T) {
		t.Parallel()
		userDef, ok := schema.GetTypeDefinition("user")
		require.True(t, ok)
		require.NotNil(t, userDef)

		perm, ok := userDef.GetPermission("anything")
		require.False(t, ok)
		require.Nil(t, perm)
	})
}

func TestFindParent(t *testing.T) {
	// Build a schema with nested structure
	s := NewSchemaBuilder().
		AddDefinition("document").
		AddRelation("owner").
		AllowedDirectRelation("user").
		Done().
		AddPermission("view").
		Union(
			RelRef("owner"),
			ArrowRef("parent", "view"),
		).
		Done().
		Done().
		Build()

	def, _ := s.GetTypeDefinition("document")
	viewPerm, _ := def.GetPermission("view")
	unionOp := viewPerm.Operation()
	firstChild := unionOp.(*UnionOperation).Children()[0]

	// Test finding Permission parent
	perm := FindParent[*Permission](firstChild)
	require.NotNil(t, perm, "should find permission parent")
	require.Equal(t, "view", perm.Name(), "should find the correct permission")

	// Test finding Definition parent
	definition := FindParent[*Definition](firstChild)
	require.NotNil(t, definition, "should find definition parent")
	require.Equal(t, "document", definition.Name(), "should find the correct definition")

	// Test finding Schema parent
	schema := FindParent[*Schema](firstChild)
	require.NotNil(t, schema, "should find schema parent")
	require.Equal(t, s, schema, "should find the same schema instance")
}

func TestFindParent_NotFound(t *testing.T) {
	s := NewSchemaBuilder().
		AddDefinition("document").
		AddRelation("owner").
		AllowedDirectRelation("user").
		Done().
		Done().
		Build()

	def, _ := s.GetTypeDefinition("document")
	rel, _ := def.GetRelation("owner")

	// Try to find a Permission parent from a Relation (should not exist)
	perm := FindParent[*Permission](rel)
	require.Nil(t, perm, "should not find permission parent for a relation")
}

func TestFindParent_NilInput(t *testing.T) {
	// Test with nil input
	perm := FindParent[*Permission](nil)
	require.Nil(t, perm, "should return nil for nil input")
}

func TestFindParent_ImmediateParent(t *testing.T) {
	s := NewSchemaBuilder().
		AddDefinition("document").
		AddPermission("view").
		RelationRef("owner").
		Done().
		Done().
		Build()

	def, _ := s.GetTypeDefinition("document")
	viewPerm, _ := def.GetPermission("view")
	op := viewPerm.Operation()

	// Test that FindParent works for immediate parents (no traversal needed).
	// The operation's parent is the permission itself, so FindParent should return it directly.
	perm := FindParent[*Permission](op)
	require.NotNil(t, perm, "should find permission parent")
	require.Equal(t, viewPerm, perm, "should find the immediate parent permission")
}

func TestFindParent_SkipIntermediateTypes(t *testing.T) {
	// Build a deeply nested operation tree
	s := NewSchemaBuilder().
		AddDefinition("document").
		AddPermission("complex").
		Intersection(
			Union(
				RelRef("a"),
				RelRef("b"),
			),
			RelRef("c"),
		).
		Done().
		Done().
		Build()

	def, _ := s.GetTypeDefinition("document")
	complexPerm, _ := def.GetPermission("complex")
	intersectionOp := complexPerm.Operation().(*IntersectionOperation)
	unionOp := intersectionOp.Children()[0].(*UnionOperation)
	leafOp := unionOp.Children()[0] // This is a RelationReference

	// From the leaf operation, find the permission (skipping union and intersection)
	perm := FindParent[*Permission](leafOp)
	require.NotNil(t, perm, "should find permission parent")
	require.Equal(t, "complex", perm.Name(), "should find the correct permission")

	// Can also find intermediate operation types
	union := FindParent[*UnionOperation](leafOp)
	require.NotNil(t, union, "should find union parent")
	require.Equal(t, unionOp, union, "should find the correct union operation")

	intersection := FindParent[*IntersectionOperation](leafOp)
	require.NotNil(t, intersection, "should find intersection parent")
	require.Equal(t, intersectionOp, intersection, "should find the correct intersection operation")
}

func TestBaseRelationCompare(t *testing.T) {
	t.Parallel()

	getBase := func(t *testing.T, s *Schema, defName, relName string, idx int) *BaseRelation {
		t.Helper()
		def, ok := s.GetTypeDefinition(defName)
		require.True(t, ok)
		rel, ok := def.GetRelation(relName)
		require.True(t, ok)
		return rel.BaseRelations()[idx]
	}

	t.Run("both nil", func(t *testing.T) {
		t.Parallel()
		var a, b *BaseRelation
		require.Equal(t, 0, a.Compare(b))
	})

	t.Run("left nil", func(t *testing.T) {
		t.Parallel()
		s := NewSchemaBuilder().
			AddDefinition("user").Done().
			AddDefinition("doc").AddRelation("viewer").AllowedDirectRelation("user").Done().Done().
			Build()
		b := getBase(t, s, "doc", "viewer", 0)
		var a *BaseRelation
		require.Equal(t, -1, a.Compare(b))
	})

	t.Run("right nil", func(t *testing.T) {
		t.Parallel()
		s := NewSchemaBuilder().
			AddDefinition("user").Done().
			AddDefinition("doc").AddRelation("viewer").AllowedDirectRelation("user").Done().Done().
			Build()
		a := getBase(t, s, "doc", "viewer", 0)
		require.Equal(t, 1, a.Compare(nil))
	})

	t.Run("equal", func(t *testing.T) {
		t.Parallel()
		s := NewSchemaBuilder().
			AddDefinition("user").Done().
			AddDefinition("doc").AddRelation("viewer").
			AllowedDirectRelation("user").
			AllowedDirectRelation("user").
			Done().Done().
			Build()
		a := getBase(t, s, "doc", "viewer", 0)
		b := getBase(t, s, "doc", "viewer", 1)
		require.Equal(t, 0, a.Compare(b))
	})

	t.Run("different definition name", func(t *testing.T) {
		t.Parallel()
		s := NewSchemaBuilder().
			AddDefinition("user").Done().
			AddDefinition("aaa").AddRelation("viewer").AllowedDirectRelation("user").Done().Done().
			AddDefinition("zzz").AddRelation("viewer").AllowedDirectRelation("user").Done().Done().
			Build()
		a := getBase(t, s, "aaa", "viewer", 0)
		b := getBase(t, s, "zzz", "viewer", 0)
		require.Equal(t, -1, a.Compare(b))
		require.Equal(t, 1, b.Compare(a))
	})

	t.Run("different relation name", func(t *testing.T) {
		t.Parallel()
		s := NewSchemaBuilder().
			AddDefinition("user").Done().
			AddDefinition("doc").
			AddRelation("admin").AllowedDirectRelation("user").Done().
			AddRelation("viewer").AllowedDirectRelation("user").Done().
			Done().
			Build()
		a := getBase(t, s, "doc", "admin", 0)
		b := getBase(t, s, "doc", "viewer", 0)
		require.Equal(t, -1, a.Compare(b))
		require.Equal(t, 1, b.Compare(a))
	})

	t.Run("different subject type", func(t *testing.T) {
		t.Parallel()
		s := NewSchemaBuilder().
			AddDefinition("org").Done().
			AddDefinition("user").Done().
			AddDefinition("doc").AddRelation("viewer").
			AllowedDirectRelation("org").
			AllowedDirectRelation("user").
			Done().Done().
			Build()
		a := getBase(t, s, "doc", "viewer", 0) // type = "org"
		b := getBase(t, s, "doc", "viewer", 1) // type = "user"
		require.Equal(t, -1, a.Compare(b))
		require.Equal(t, 1, b.Compare(a))
	})

	t.Run("different subrelation", func(t *testing.T) {
		t.Parallel()
		s := NewSchemaBuilder().
			AddDefinition("org").Done().
			AddDefinition("doc").AddRelation("viewer").
			AllowedRelation("org", "admin").
			AllowedRelation("org", "member").
			Done().Done().
			Build()
		a := getBase(t, s, "doc", "viewer", 0) // subrelation = "admin"
		b := getBase(t, s, "doc", "viewer", 1) // subrelation = "member"
		require.Equal(t, -1, a.Compare(b))
		require.Equal(t, 1, b.Compare(a))
	})

	t.Run("different caveat", func(t *testing.T) {
		t.Parallel()
		s := NewSchemaBuilder().
			AddDefinition("user").Done().
			AddDefinition("doc").AddRelation("viewer").
			AllowedDirectRelationWithCaveat("user", "alpha_caveat").
			AllowedDirectRelationWithCaveat("user", "beta_caveat").
			Done().Done().
			Build()
		a := getBase(t, s, "doc", "viewer", 0) // caveat = "alpha_caveat"
		b := getBase(t, s, "doc", "viewer", 1) // caveat = "beta_caveat"
		require.Equal(t, -1, a.Compare(b))
		require.Equal(t, 1, b.Compare(a))
	})

	t.Run("different wildcard", func(t *testing.T) {
		t.Parallel()
		// Use AllowedRelation with empty subrelation and AllowedWildcard (also empty subrelation)
		// so comparison reaches the wildcard field.
		s := NewSchemaBuilder().
			AddDefinition("user").Done().
			AddDefinition("doc").AddRelation("viewer").
			AllowedRelation("user", "").
			AllowedWildcard("user").
			Done().Done().
			Build()
		a := getBase(t, s, "doc", "viewer", 0) // wildcard = false
		b := getBase(t, s, "doc", "viewer", 1) // wildcard = true
		require.Equal(t, -1, a.Compare(b))
		require.Equal(t, 1, b.Compare(a))
	})

	t.Run("different expiration", func(t *testing.T) {
		t.Parallel()
		s := NewSchemaBuilder().
			AddDefinition("user").Done().
			AddDefinition("doc").AddRelation("viewer").
			AllowedRelation("user", "member").
			AllowedRelationWithExpiration("user", "member").
			Done().Done().
			Build()
		a := getBase(t, s, "doc", "viewer", 0) // expiration = false
		b := getBase(t, s, "doc", "viewer", 1) // expiration = true
		require.Equal(t, -1, a.Compare(b))
		require.Equal(t, 1, b.Compare(a))
	})
}

func TestFindParent_FromDifferentElements(t *testing.T) {
	s := NewSchemaBuilder().
		AddDefinition("document").
		AddRelation("owner").
		AllowedDirectRelation("user").
		Done().
		AddPermission("view").
		RelationRef("owner").
		Done().
		Done().
		Build()

	def, _ := s.GetTypeDefinition("document")

	// Test from Relation
	rel, _ := def.GetRelation("owner")
	defFromRel := FindParent[*Definition](rel)
	require.NotNil(t, defFromRel)
	require.Equal(t, "document", defFromRel.Name())

	// Test from Permission
	perm, _ := def.GetPermission("view")
	defFromPerm := FindParent[*Definition](perm)
	require.NotNil(t, defFromPerm)
	require.Equal(t, "document", defFromPerm.Name())

	// Test from BaseRelation
	baseRel := rel.BaseRelations()[0]
	relFromBase := FindParent[*Relation](baseRel)
	require.NotNil(t, relFromBase)
	require.Equal(t, "owner", relFromBase.Name())
}
