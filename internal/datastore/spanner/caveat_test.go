package spanner

import (
	"testing"

	"cloud.google.com/go/spanner"
	"github.com/stretchr/testify/require"
)

func TestContextualizedCaveatFrom(t *testing.T) {
	res, err := ContextualizedCaveatFrom(spanner.NullString{StringVal: "", Valid: false}, spanner.NullJSON{Value: nil, Valid: true})
	require.NoError(t, err)
	require.Nil(t, res)

	res, err = ContextualizedCaveatFrom(spanner.NullString{StringVal: "", Valid: true}, spanner.NullJSON{Value: nil, Valid: false})
	require.NoError(t, err)
	require.Nil(t, res)

	res, err = ContextualizedCaveatFrom(spanner.NullString{StringVal: "", Valid: true}, spanner.NullJSON{Value: nil, Valid: true})
	require.NoError(t, err)
	require.Nil(t, res)

	res, err = ContextualizedCaveatFrom(spanner.NullString{StringVal: "test", Valid: true}, spanner.NullJSON{Value: nil, Valid: true})
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Equal(t, "test", res.GetCaveatName())
	require.NotNil(t, res.GetContext())
	require.Empty(t, res.GetContext().GetFields())

	res, err = ContextualizedCaveatFrom(
		spanner.NullString{StringVal: "test", Valid: true},
		spanner.NullJSON{Value: map[string]any{"key": "val"}, Valid: true})
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Equal(t, "test", res.GetCaveatName())
	require.NotNil(t, res.GetContext())
	require.Len(t, res.GetContext().GetFields(), 1)
	require.Equal(t, "val", res.GetContext().GetFields()["key"].GetStringValue())
}
