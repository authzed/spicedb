package query

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNamedAliasScopesItsDefinition(t *testing.T) {
	// Example schema:
	//   definition user {}
	//   definition group { relation access: user }
	//   definition team { relation access: user }
	//   definition document {
	//     relation parent: group | team
	//     permission view = parent.all(access)
	//   }
	//
	// The arrow's shared right side contains group#access and team#access branches.
	// Here we isolate group#access with an empty body. It must reject team:shared,
	// even when team:shared#access would otherwise trigger its reflexive identity
	// rule. Its own group:shared#access identity must still grant.
	alias := NewAliasIterator("group", "access", NewEmptyFixedIterator())
	ctx := NewTestContext(t)
	foreign := NewObject("team", "shared")
	subject := foreign.WithRelation("access")

	path, err := ctx.Check(alias, foreign, subject)
	require.NoError(t, err)
	require.Nil(t, path)

	paths, err := ctx.CheckManyResources(alias, []Object{foreign, NewObject("group", "shared")}, subject)
	require.NoError(t, err)
	require.Equal(t, []*Path{nil, nil}, paths)

	paths, err = ctx.CheckManySubjects(alias, foreign, []ObjectAndRelation{subject})
	require.NoError(t, err)
	require.Equal(t, []*Path{nil}, paths)

	seq, err := ctx.IterResources(alias, subject, NoObjectFilter())
	require.NoError(t, err)
	found, err := CollectAll(seq)
	require.NoError(t, err)
	require.Empty(t, found)

	own := NewObject("group", "shared")
	path, err = ctx.Check(alias, own, own.WithRelation("access"))
	require.NoError(t, err)
	require.NotNil(t, path)
}
