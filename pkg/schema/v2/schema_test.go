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
