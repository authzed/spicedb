// Package oracle computes the expected permissionship for a validation file without using
// any of SpiceDB's own evaluation code. The schema and relationships are translated into an
// answer set program, which is solved by the clingo binary.
//
// Objects are encoded as o(Type, ID), subjects as s(Object, Relation) (with "..." as the
// relation of a terminal subject) and wildcards as w(Type). Every caveat world is solved in
// the same program: the program derives has(World, Object, RelationOrPermission, Subject)
// for every membership that holds in that world.
package oracle

import (
	"fmt"
	"slices"
	"strconv"
	"strings"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

// rules holds the parts of the program that do not depend on the schema.
const rules = `
% A relationship grants its subject, which is either a terminal subject or a userset.
has(W,O,R,S) :- live(W,O,R,S), S = s(_,_).

% A userset grants every member of the userset.
has(W,O,R,S) :- live(W,O,R,s(O2,R2)), R2 != "...", has(W,O2,R2,S).

% A wildcard grants every concrete object of its type.
has(W,O,R,s(o(T,I),"...")) :- live(W,O,R,w(T)), concrete(o(T,I)).

% Every userset contains itself.
has(W,O,R,s(O,R)) :- world(W), obj(O), O = o(T,_), defined(T,R).

% Candidate subjects, used where a rule must quantify over subjects.
cand(W,s(O,"...")) :- world(W), obj(O).
cand(W,S) :- live(W,_,_,S), S = s(_,_).

#show.
#show out(W,T,I,R,ST,SI,SR) : has(W,o(T,I),R,s(o(ST,SI),SR)).
`

// translator builds the schema-dependent rules of the program.
type translator struct {
	sb      strings.Builder
	counter int
}

// translateSchema returns the rules for the given namespace definitions.
func translateSchema(defs []*core.NamespaceDefinition) (string, error) {
	tr := &translator{}
	for _, def := range defs {
		for _, rel := range def.Relation {
			fmt.Fprintf(&tr.sb, "defined(%s,%s).\n", quote(def.Name), quote(rel.Name))
			if rel.UsersetRewrite == nil {
				// Relations are covered by the generic rules over live/4.
				continue
			}

			pred, err := tr.rewrite(def.Name, rel.UsersetRewrite)
			if err != nil {
				return "", fmt.Errorf("%s#%s: %w", def.Name, rel.Name, err)
			}
			fmt.Fprintf(&tr.sb, "has(W,o(%s,I),%s,S) :- %s(W,o(%s,I),S).\n",
				quote(def.Name), quote(rel.Name), pred, quote(def.Name))
		}
	}
	return rules + tr.sb.String(), nil
}

func (tr *translator) fresh() string {
	tr.counter++
	pred := fmt.Sprintf("e%d", tr.counter)
	// Declare every predicate, so that ones without rules (e.g. nil) are valid.
	fmt.Fprintf(&tr.sb, "#defined %s/3.\n", pred)
	return pred
}

// rewrite emits the rules for a rewrite and returns the predicate holding its result.
func (tr *translator) rewrite(typ string, rw *core.UsersetRewrite) (string, error) {
	var children []*core.SetOperation_Child
	switch op := rw.RewriteOperation.(type) {
	case *core.UsersetRewrite_Union:
		children = op.Union.Child
	case *core.UsersetRewrite_Intersection:
		children = op.Intersection.Child
	case *core.UsersetRewrite_Exclusion:
		children = op.Exclusion.Child
	default:
		return "", fmt.Errorf("unknown rewrite operation %T", op)
	}

	childPreds := make([]string, 0, len(children))
	for _, child := range children {
		pred, err := tr.child(typ, child)
		if err != nil {
			return "", err
		}
		childPreds = append(childPreds, pred)
	}

	pred := tr.fresh()
	switch rw.RewriteOperation.(type) {
	case *core.UsersetRewrite_Union:
		for _, c := range childPreds {
			fmt.Fprintf(&tr.sb, "%s(W,O,S) :- %s(W,O,S).\n", pred, c)
		}

	case *core.UsersetRewrite_Intersection:
		body := make([]string, 0, len(childPreds))
		for _, c := range childPreds {
			body = append(body, c+"(W,O,S)")
		}
		fmt.Fprintf(&tr.sb, "%s(W,O,S) :- %s.\n", pred, strings.Join(body, ", "))

	case *core.UsersetRewrite_Exclusion:
		// The first child, minus every other child.
		body := []string{childPreds[0] + "(W,O,S)"}
		for _, c := range childPreds[1:] {
			body = append(body, "not "+c+"(W,O,S)")
		}
		fmt.Fprintf(&tr.sb, "%s(W,O,S) :- %s.\n", pred, strings.Join(body, ", "))
	}
	return pred, nil
}

func (tr *translator) child(typ string, child *core.SetOperation_Child) (string, error) {
	onType := fmt.Sprintf("O = o(%s,_)", quote(typ))

	switch c := child.ChildType.(type) {
	case *core.SetOperation_Child_UsersetRewrite:
		return tr.rewrite(typ, c.UsersetRewrite)

	case *core.SetOperation_Child_ComputedUserset:
		pred := tr.fresh()
		fmt.Fprintf(&tr.sb, "%s(W,O,S) :- has(W,O,%s,S), %s.\n", pred, quote(c.ComputedUserset.Relation), onType)
		return pred, nil

	case *core.SetOperation_Child_TupleToUserset:
		pred := tr.fresh()
		tr.arrowAny(pred, onType, c.TupleToUserset.Tupleset.Relation, c.TupleToUserset.ComputedUserset.Relation)
		return pred, nil

	case *core.SetOperation_Child_FunctionedTupleToUserset:
		pred := tr.fresh()
		tupleset := c.FunctionedTupleToUserset.Tupleset.Relation
		computed := c.FunctionedTupleToUserset.ComputedUserset.Relation
		switch c.FunctionedTupleToUserset.Function {
		case core.FunctionedTupleToUserset_FUNCTION_ANY:
			tr.arrowAny(pred, onType, tupleset, computed)

		case core.FunctionedTupleToUserset_FUNCTION_ALL:
			// The subject is granted if there is at least one left-hand object, and no
			// left-hand object is missing the subject.
			missing := tr.fresh()
			fmt.Fprintf(&tr.sb, "%s(W,O,S) :- live(W,O,%s,s(X,_)), cand(W,S), not has(W,X,%s,S), %s.\n",
				missing, quote(tupleset), quote(computed), onType)
			fmt.Fprintf(&tr.sb, "%s(W,O,S) :- live(W,O,%s,s(_,_)), cand(W,S), not %s(W,O,S), %s.\n",
				pred, quote(tupleset), missing, onType)

		default:
			return "", fmt.Errorf("unknown arrow function %v", c.FunctionedTupleToUserset.Function)
		}
		return pred, nil

	case *core.SetOperation_Child_XNil:
		return tr.fresh(), nil

	case *core.SetOperation_Child_XSelf:
		pred := tr.fresh()
		fmt.Fprintf(&tr.sb, "%s(W,O,s(O,\"...\")) :- world(W), obj(O), %s.\n", pred, onType)
		return pred, nil

	default:
		return "", fmt.Errorf("unsupported rewrite child %T", c)
	}
}

func (tr *translator) arrowAny(pred, onType, tupleset, computed string) {
	fmt.Fprintf(&tr.sb, "%s(W,O,S) :- live(W,O,%s,s(X,_)), has(W,X,%s,S), %s.\n",
		pred, quote(tupleset), quote(computed), onType)
}

// translateFacts returns the facts shared by every world: the objects under test, and the
// concrete objects that wildcards expand to.
func translateFacts(objects, concrete []tuple.ObjectAndRelation) string {
	lines := make([]string, 0, len(objects)+len(concrete))
	for _, o := range objects {
		lines = append(lines, fmt.Sprintf("obj(%s).", object(o)))
	}
	for _, o := range concrete {
		lines = append(lines, fmt.Sprintf("concrete(%s).", object(o)))
	}
	slices.Sort(lines)
	return strings.Join(slices.Compact(lines), "\n") + "\n"
}

// translateWorld returns the facts for a single world: the relationships live in it.
func translateWorld(world int, live []tuple.Relationship) string {
	w := quote(strconv.Itoa(world))
	lines := []string{fmt.Sprintf("world(%s).", w)}
	for _, rel := range live {
		var subject string
		if rel.Subject.ObjectID == tuple.PublicWildcard {
			subject = fmt.Sprintf("w(%s)", quote(rel.Subject.ObjectType))
		} else {
			subject = fmt.Sprintf("s(%s,%s)", object(rel.Subject), quote(rel.Subject.Relation))
		}
		lines = append(lines, fmt.Sprintf("live(%s,%s,%s,%s).", w, object(rel.Resource), quote(rel.Resource.Relation), subject))
	}
	slices.Sort(lines)
	return strings.Join(slices.Compact(lines), "\n") + "\n"
}

func object(onr tuple.ObjectAndRelation) string {
	return fmt.Sprintf("o(%s,%s)", quote(onr.ObjectType), quote(onr.ObjectID))
}

func quote(s string) string {
	return `"` + strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`).Replace(s) + `"`
}
