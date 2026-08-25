package crdb

import (
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"
)

// Under QueryExecModeExec pgx infers parameter types from Go types rather than
// asking the server, and rejects arguments it cannot type unambiguously. A caveat
// context is written as a bare map[string]any, so without an explicit default
// type every caveated write would fail to encode.
func TestRegisterTypesMapsCaveatContextToJSONB(t *testing.T) {
	m := pgtype.NewMap()

	_, ok := m.TypeForValue(map[string]any{})
	require.False(t, ok, "precondition: pgx should not type map[string]any on its own")

	RegisterTypes(m)

	typ, ok := m.TypeForValue(map[string]any{})
	require.True(t, ok, "caveat context must have a registered default type")
	require.Equal(t, "jsonb", typ.Name)
}

// The encode path is what actually runs during a write, so exercise it in the
// text format that exec mode uses rather than trusting the type lookup alone.
func TestCaveatContextEncodesUnderExecMode(t *testing.T) {
	m := pgtype.NewMap()
	RegisterTypes(m)

	typ, ok := m.TypeForValue(map[string]any{})
	require.True(t, ok)

	ctx := map[string]any{"expired": true, "n": float64(42)}
	plan := m.PlanEncode(typ.OID, pgx.TextFormatCode, ctx)
	require.NotNil(t, plan, "no encode plan for caveat context in text format")

	buf, err := plan.Encode(ctx, nil)
	require.NoError(t, err)
	require.JSONEq(t, `{"expired":true,"n":42}`, string(buf))
}
