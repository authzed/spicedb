package queryopttest_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/queryopttest"
	"github.com/authzed/spicedb/pkg/query"
)

func TestBuiltOutlinePreservesSubjects(t *testing.T) {
	// The exclusion removes bob from the union, leaving only alice.
	co := queryopttest.Canonical(t, queryopttest.Exclusion(
		queryopttest.Union(
			queryopttest.Fixed(t, "doc:d#read@user:alice"),
			queryopttest.Fixed(t, "doc:d#read@user:bob"),
		),
		queryopttest.Fixed(t, "doc:d#read@user:bob"),
	))
	iterator, err := co.Compile()
	require.NoError(t, err)
	ctx := query.NewLocalContext(t.Context())
	alice, err := ctx.Check(iterator, query.NewObject("doc", "d"), query.NewObject("user", "alice").WithEllipses())
	require.NoError(t, err)
	require.NotNil(t, alice)
	bob, err := ctx.Check(iterator, query.NewObject("doc", "d"), query.NewObject("user", "bob").WithEllipses())
	require.NoError(t, err)
	require.Nil(t, bob)
}

func TestOutlineFromSchema(t *testing.T) {
	// The helper uses the real schema planner, preserving relation identities.
	co := queryopttest.FromSchema(t, `
 definition user {}
 definition doc {
  relation viewer: user
  relation editor: user
  permission read = viewer + editor
 }`, "doc", "read")
	require.Equal(t, query.AliasIteratorType, co.Root.Type)
	require.Equal(t, "read", co.Root.Args.RelationName)
	require.Len(t, co.Root.SubOutlines, 1)
	require.Equal(t, query.UnionIteratorType, co.Root.SubOutlines[0].Type)
	require.Len(t, co.Root.SubOutlines[0].SubOutlines, 2)
	_, err := co.Compile()
	require.NoError(t, err)
}
