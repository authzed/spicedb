package caveats

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConditionUnconditional(t *testing.T) {
	require.True(t, Unconditional().IsUnconditional())
	require.Equal(t, "true", Unconditional().String())

	// A nil expression is unconditional.
	require.True(t, FromExpression(nil).IsUnconditional())

	// A single caveat is conditional.
	c1 := FromExpression(CaveatExprForTesting("cav1"))
	require.False(t, c1.IsUnconditional())
	require.Equal(t, "cav1", c1.String())
	require.Equal(t, 1, c1.Disjuncts())
}

func TestConditionAndIsIdempotent(t *testing.T) {
	// c1 ∧ c2 ∧ c1 must collapse to the two-atom conjunct {c1, c2}.
	expr := And(And(CaveatExprForTesting("cav1"), CaveatExprForTesting("cav2")), CaveatExprForTesting("cav1"))
	c := FromExpression(expr)
	require.Equal(t, "cav1 & cav2", c.String())
	require.Equal(t, 1, c.Disjuncts())
}

func TestConditionAndIsCommutative(t *testing.T) {
	a := FromExpression(And(CaveatExprForTesting("cav1"), CaveatExprForTesting("cav2")))
	b := FromExpression(And(CaveatExprForTesting("cav2"), CaveatExprForTesting("cav1")))
	require.Equal(t, a.String(), b.String())
}

func TestConditionAndWithUnconditionalIsIdentity(t *testing.T) {
	c1 := FromExpression(CaveatExprForTesting("cav1"))
	require.Equal(t, "cav1", Unconditional().And(c1).String())
	require.Equal(t, "cav1", c1.And(Unconditional()).String())
}

func TestConditionAndDistributesOverOr(t *testing.T) {
	// (cav1 | cav2) & cav3 == (cav1 & cav3) | (cav2 & cav3)
	expr := And(Or(CaveatExprForTesting("cav1"), CaveatExprForTesting("cav2")), CaveatExprForTesting("cav3"))
	require.Equal(t, "cav1 & cav3 | cav2 & cav3", FromExpression(expr).String())
}

func TestConditionOrUnions(t *testing.T) {
	c1 := FromExpression(CaveatExprForTesting("cav1"))
	c2 := FromExpression(CaveatExprForTesting("cav2"))
	res, changed := c1.Or(c2)
	require.True(t, changed)
	require.Equal(t, "cav1 | cav2", res.String())
}

func TestConditionOrIdempotentReportsUnchanged(t *testing.T) {
	c1 := FromExpression(CaveatExprForTesting("cav1"))
	res, changed := c1.Or(FromExpression(CaveatExprForTesting("cav1")))
	require.False(t, changed, "OR-ing an identical condition must not report a change")
	require.Equal(t, "cav1", res.String())
}

func TestConditionOrAbsorbsUnconditional(t *testing.T) {
	c1 := FromExpression(CaveatExprForTesting("cav1"))

	// A conditional weakened by an unconditional path becomes unconditional (a change).
	res, changed := c1.Or(Unconditional())
	require.True(t, res.IsUnconditional())
	require.True(t, changed)

	// An unconditional path OR anything stays unconditional (no change).
	res2, changed2 := Unconditional().Or(c1)
	require.True(t, res2.IsUnconditional())
	require.False(t, changed2)
}

func TestConditionOrSubsumesSuperset(t *testing.T) {
	// cav1 ∨ (cav1 ∧ cav2): the two-atom conjunct is redundant (implies cav1), so
	// the result is just cav1.
	c1 := FromExpression(CaveatExprForTesting("cav1"))
	c1and2 := FromExpression(And(CaveatExprForTesting("cav1"), CaveatExprForTesting("cav2")))

	res, changed := c1.Or(c1and2)
	require.Equal(t, "cav1", res.String())
	require.False(t, changed, "adding a subsumed conjunct does not weaken the condition")

	// The other direction: starting from the superset and adding cav1 weakens it.
	res2, changed2 := c1and2.Or(c1)
	require.Equal(t, "cav1", res2.String())
	require.True(t, changed2)
}

func TestConditionContextDistinguishesAtoms(t *testing.T) {
	// Same caveat name, different context => distinct atoms.
	a := FromExpression(MustCaveatExprForTestingWithContext("cav1", map[string]any{"x": 1}))
	b := FromExpression(MustCaveatExprForTestingWithContext("cav1", map[string]any{"x": 2}))
	res, changed := a.Or(b)
	require.True(t, changed)
	require.Equal(t, 2, res.Disjuncts(), "different context must not be deduped")
}

func TestConditionExpressionRoundTrip(t *testing.T) {
	require.Nil(t, Unconditional().Expression())

	orig := FromExpression(And(CaveatExprForTesting("cav1"), CaveatExprForTesting("cav2")))
	rebuilt := FromExpression(orig.Expression())
	require.Equal(t, orig.String(), rebuilt.String())
}
