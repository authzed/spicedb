package caveats

import (
	"sort"
	"strings"

	"google.golang.org/protobuf/proto"

	pkgcaveats "github.com/authzed/spicedb/pkg/caveats"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

// Condition is a canonical, order-independent representation of a caveat
// expression as a disjunctive normal form (DNF): an OR of conjuncts, where each
// conjunct is an AND of atoms. It exists so that recursive traversals can decide
// whether a newly-discovered path *weakens* the condition under which an object
// is reachable — the termination guarantee of the semi-naive fixpoint.
//
// Two properties make that guarantee hold:
//   - AND is idempotent and commutative: c1 ∧ c2 ∧ c1 collapses to {c1, c2}, so
//     the set of distinct conjuncts is finite.
//   - ⊤ (unconditional) is absorbing under OR: once an object is reachable
//     unconditionally, no further path can weaken it.
//
// The zero value is the "false" condition (no disjuncts); use Top() for the
// unconditional condition and FromExpression to derive one from a caveat.
type Condition struct {
	top       bool
	disjuncts [][]atom // OR of conjuncts; each conjunct sorted+deduped by atom key, non-empty
}

// atom is a single indivisible leaf of a condition: a contextualized caveat, or
// an opaque (e.g. negated) subexpression. key is the canonical identity used for
// dedup, sorting and comparison; expr is retained so the original expression can
// be rebuilt, since the key alone cannot recover a caveat's context.
type atom struct {
	key  string
	expr *core.CaveatExpression
}

// Top returns the unconditional (always-true) condition.
func Top() Condition {
	return Condition{top: true}
}

// FromExpression converts a caveat expression into its canonical DNF. A nil
// expression is unconditional and yields Top().
func FromExpression(expr *core.CaveatExpression) Condition {
	if expr == nil {
		return Top()
	}
	return fromExpr(expr)
}

func fromExpr(expr *core.CaveatExpression) Condition {
	if leaf := expr.GetCaveat(); leaf != nil {
		return singleAtom(atom{key: caveatAtomKey(leaf), expr: expr})
	}

	op := expr.GetOperation()
	if op == nil {
		return singleAtom(atom{key: opaqueAtomKey(expr), expr: expr})
	}

	switch op.GetOp() {
	case core.CaveatOperation_AND:
		result := Top()
		for _, child := range op.GetChildren() {
			result = result.And(fromExpr(child))
		}
		return result

	case core.CaveatOperation_OR:
		var result Condition // the zero value is the "false" condition
		for _, child := range op.GetChildren() {
			result, _ = result.Or(fromExpr(child))
		}
		return result

	default:
		// NOT (and anything unrecognized) is treated as an opaque atom: we do not
		// distribute negation, which keeps the atom set finite and the result sound
		// (conservative — it may report "changed" when logically unchanged).
		return singleAtom(atom{key: opaqueAtomKey(expr), expr: expr})
	}
}

// IsTop reports whether the condition is unconditional.
func (c Condition) IsTop() bool {
	return c.top
}

// Disjuncts returns the number of DNF disjuncts (0 for Top or false).
func (c Condition) Disjuncts() int {
	return len(c.disjuncts)
}

// And returns the canonical conjunction of the two conditions.
func (c Condition) And(other Condition) Condition {
	if c.top {
		return other
	}
	if other.top {
		return c
	}
	// false AND x == false
	if len(c.disjuncts) == 0 || len(other.disjuncts) == 0 {
		return Condition{}
	}

	// Distribute: (A1 ∨ A2) ∧ (B1 ∨ B2) == (A1∧B1) ∨ (A1∧B2) ∨ (A2∧B1) ∨ (A2∧B2).
	product := make([][]atom, 0, len(c.disjuncts)*len(other.disjuncts))
	for _, a := range c.disjuncts {
		for _, b := range other.disjuncts {
			combined := make([]atom, 0, len(a)+len(b))
			combined = append(combined, a...)
			combined = append(combined, b...)
			product = append(product, combined)
		}
	}
	return normalizeCondition(product)
}

// Or returns the canonical disjunction of the two conditions, and reports whether
// the result differs from the receiver — i.e. whether other *weakened* c.
func (c Condition) Or(other Condition) (Condition, bool) {
	if c.top {
		return c, false
	}
	if other.top {
		return Top(), true
	}

	combined := make([][]atom, 0, len(c.disjuncts)+len(other.disjuncts))
	combined = append(combined, c.disjuncts...)
	combined = append(combined, other.disjuncts...)
	result := normalizeCondition(combined)
	return result, !result.equalTo(c)
}

// Expression rebuilds a caveat expression equivalent to this condition. Top()
// (and the degenerate false condition) yield nil.
func (c Condition) Expression() *core.CaveatExpression {
	if c.top || len(c.disjuncts) == 0 {
		return nil
	}

	var result *core.CaveatExpression
	for _, conjunct := range c.disjuncts {
		var conj *core.CaveatExpression
		for _, a := range conjunct {
			conj = And(conj, a.expr)
		}
		result = Or(result, conj)
	}
	return result
}

// String returns a deterministic, human-readable rendering of the DNF.
func (c Condition) String() string {
	if c.top {
		return "true"
	}
	if len(c.disjuncts) == 0 {
		return "false"
	}
	parts := make([]string, len(c.disjuncts))
	for i, conjunct := range c.disjuncts {
		keys := make([]string, len(conjunct))
		for j, a := range conjunct {
			keys[j] = a.key
		}
		parts[i] = strings.Join(keys, " & ")
	}
	return strings.Join(parts, " | ")
}

func (c Condition) equalTo(other Condition) bool {
	if c.top != other.top || len(c.disjuncts) != len(other.disjuncts) {
		return false
	}
	for i := range c.disjuncts {
		if len(c.disjuncts[i]) != len(other.disjuncts[i]) {
			return false
		}
		for j := range c.disjuncts[i] {
			if c.disjuncts[i][j].key != other.disjuncts[i][j].key {
				return false
			}
		}
	}
	return true
}

func singleAtom(a atom) Condition {
	return normalizeCondition([][]atom{{a}})
}

// normalizeCondition sorts and dedupes atoms within each conjunct, drops any
// conjunct subsumed by a smaller one, dedupes identical conjuncts, and sorts the
// disjuncts — yielding the canonical form. An empty conjunct means "true" and
// collapses the whole condition to Top.
func normalizeCondition(disjuncts [][]atom) Condition {
	reduced := make([][]atom, 0, len(disjuncts))
	for _, conjunct := range disjuncts {
		conjunct = sortDedupeAtoms(conjunct)
		if len(conjunct) == 0 {
			return Top()
		}
		reduced = append(reduced, conjunct)
	}

	reduced = subsumeAndDedupe(reduced)
	sort.Slice(reduced, func(i, j int) bool {
		return conjunctKey(reduced[i]) < conjunctKey(reduced[j])
	})
	return Condition{disjuncts: reduced}
}

func sortDedupeAtoms(atoms []atom) []atom {
	if len(atoms) <= 1 {
		return atoms
	}
	cp := make([]atom, len(atoms))
	copy(cp, atoms)
	sort.Slice(cp, func(i, j int) bool { return cp[i].key < cp[j].key })

	out := cp[:1]
	for _, a := range cp[1:] {
		if a.key != out[len(out)-1].key {
			out = append(out, a)
		}
	}
	return out
}

// subsumeAndDedupe removes duplicate conjuncts and any conjunct that is a strict
// superset of another (a superset is a stronger, redundant constraint in an OR).
func subsumeAndDedupe(disjuncts [][]atom) [][]atom {
	seen := make(map[string]bool, len(disjuncts))
	unique := make([][]atom, 0, len(disjuncts))
	for _, conjunct := range disjuncts {
		key := conjunctKey(conjunct)
		if !seen[key] {
			seen[key] = true
			unique = append(unique, conjunct)
		}
	}

	keep := make([][]atom, 0, len(unique))
	for i, a := range unique {
		subsumed := false
		for j, b := range unique {
			if i != j && len(b) < len(a) && isAtomSubset(b, a) {
				subsumed = true
				break
			}
		}
		if !subsumed {
			keep = append(keep, a)
		}
	}
	return keep
}

// isAtomSubset reports whether every atom in b (sorted by key) appears in a
// (sorted by key).
func isAtomSubset(b, a []atom) bool {
	i := 0
	for _, x := range b {
		for i < len(a) && a[i].key < x.key {
			i++
		}
		if i >= len(a) || a[i].key != x.key {
			return false
		}
		i++
	}
	return true
}

func conjunctKey(conjunct []atom) string {
	keys := make([]string, len(conjunct))
	for i, a := range conjunct {
		keys[i] = a.key
	}
	return strings.Join(keys, "\x00")
}

func caveatAtomKey(c *core.ContextualizedCaveat) string {
	name := c.GetCaveatName()
	ctx := c.GetContext()
	if ctx == nil || len(ctx.GetFields()) == 0 {
		return name
	}
	stable, err := pkgcaveats.StableContextStringForHashing(ctx)
	if err != nil {
		return name + "@" + ctx.String()
	}
	return name + "@" + stable
}

func opaqueAtomKey(expr *core.CaveatExpression) string {
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(expr)
	if err != nil {
		return "opaque:" + expr.String()
	}
	return "opaque:" + string(encoded)
}
