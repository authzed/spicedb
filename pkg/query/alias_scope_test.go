package query

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNamedAliasScopesItsDefinition(t *testing.T) {
	// The right side of a heterogeneous arrow contains named branches for each
	// possible intermediate type. A branch must reject another type even when
	// its body or reflexive identity would otherwise grant that object.
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
