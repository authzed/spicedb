// Package oracle computes the expected permissionship for a validation file without using
// any of SpiceDB's own evaluation code. The schema and relationships are translated into an
// answer set program, which is solved by the clingo binary.
//
// Objects are encoded as object(Type, ID), subjects as subject(Object, Relation) (with
// "..." as the relation of a terminal subject) and wildcards as wildcard(Type). Every
// caveat world is solved in the same program: the program derives
// member(World, Object, RelationOrPermission, Subject) for every membership that holds in
// that world, and shows each one as an answer/7 atom of plain strings.
//
// Each set operation in a rewrite gets its own helper predicate, named after the
// definition, the relation and the operation, e.g. document_view_union_3/3.
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
member(World,Resource,Relation,Subject) :-
    relationship(World,Resource,Relation,Subject), Subject = subject(_,_).

% A userset grants every member of the userset.
member(World,Resource,Relation,Subject) :-
    relationship(World,Resource,Relation,subject(Userset,UsersetRelation)),
    UsersetRelation != "...",
    member(World,Userset,UsersetRelation,Subject).

% A wildcard grants every concrete object of its type.
member(World,Resource,Relation,subject(object(Type,ID),"...")) :-
    relationship(World,Resource,Relation,wildcard(Type)), concrete(object(Type,ID)).

% Every userset contains itself.
member(World,Resource,Relation,subject(Resource,Relation)) :-
    world(World), queried(Resource), Resource = object(Type,_), defined(Type,Relation).

% Candidate subjects, used where a rule must quantify over subjects.
candidate(World,subject(Object,"...")) :- world(World), queried(Object).
candidate(World,Subject) :- relationship(World,_,_,Subject), Subject = subject(_,_).

#show.
#show answer(World,Type,ID,Relation,SubjectType,SubjectID,SubjectRelation) :
    member(World,object(Type,ID),Relation,subject(object(SubjectType,SubjectID),SubjectRelation)).
`

// translator builds the schema-dependent rules of the program.
type translator struct {
	sb      strings.Builder
	counter int

	// prefix names the helper predicates of the relation being translated.
	prefix string
}

// translateSchema returns the rules for the given namespace definitions.
func translateSchema(defs []*core.NamespaceDefinition) (string, error) {
	tr := &translator{}
	for _, def := range defs {
		for _, rel := range def.Relation {
			if rel.UsersetRewrite != nil {
				fmt.Fprintf(&tr.sb, "\n%% %s#%s\n", def.Name, rel.Name)
			}
			fmt.Fprintf(&tr.sb, "defined(%s,%s).\n", quote(def.Name), quote(rel.Name))
			if rel.UsersetRewrite == nil {
				// Relations are covered by the generic rules over relationship/4.
				continue
			}

			tr.prefix = identifier(def.Name) + "_" + identifier(rel.Name)
			pred, err := tr.rewrite(def.Name, rel.UsersetRewrite)
			if err != nil {
				return "", fmt.Errorf("%s#%s: %w", def.Name, rel.Name, err)
			}
			fmt.Fprintf(&tr.sb, "member(World,object(%s,ID),%s,Subject) :- %s(World,object(%s,ID),Subject).\n",
				quote(def.Name), quote(rel.Name), pred, quote(def.Name))
		}
	}
	return rules + tr.sb.String(), nil
}

// fresh returns a new helper predicate for an operation of the relation being translated.
// The counter keeps names unique even where sanitized names collide.
func (tr *translator) fresh(kind string) string {
	tr.counter++
	pred := fmt.Sprintf("%s_%s_%d", tr.prefix, kind, tr.counter)
	// Declare every predicate, so that ones without rules (e.g. nil) are valid.
	fmt.Fprintf(&tr.sb, "#defined %s/3.\n", pred)
	return pred
}

// identifier turns a schema name into one usable in a predicate name. Schema names are
// already lowercase and start with a letter; only the namespace separator needs replacing.
func identifier(name string) string {
	return strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '_' {
			return r
		}
		return '_'
	}, name)
}

// rewrite emits the rules for a rewrite and returns the predicate holding its result.
func (tr *translator) rewrite(typ string, rw *core.UsersetRewrite) (string, error) {
	var children []*core.SetOperation_Child
	var kind string
	switch op := rw.RewriteOperation.(type) {
	case *core.UsersetRewrite_Union:
		children, kind = op.Union.Child, "union"
	case *core.UsersetRewrite_Intersection:
		children, kind = op.Intersection.Child, "intersection"
	case *core.UsersetRewrite_Exclusion:
		children, kind = op.Exclusion.Child, "exclusion"
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

	pred := tr.fresh(kind)
	switch rw.RewriteOperation.(type) {
	case *core.UsersetRewrite_Union:
		for _, c := range childPreds {
			fmt.Fprintf(&tr.sb, "%s(World,Resource,Subject) :- %s(World,Resource,Subject).\n", pred, c)
		}

	case *core.UsersetRewrite_Intersection:
		body := make([]string, 0, len(childPreds))
		for _, c := range childPreds {
			body = append(body, c+"(World,Resource,Subject)")
		}
		fmt.Fprintf(&tr.sb, "%s(World,Resource,Subject) :- %s.\n", pred, strings.Join(body, ", "))

	case *core.UsersetRewrite_Exclusion:
		// The first child, minus every other child.
		body := []string{childPreds[0] + "(World,Resource,Subject)"}
		for _, c := range childPreds[1:] {
			body = append(body, "not "+c+"(World,Resource,Subject)")
		}
		fmt.Fprintf(&tr.sb, "%s(World,Resource,Subject) :- %s.\n", pred, strings.Join(body, ", "))
	}
	return pred, nil
}

func (tr *translator) child(typ string, child *core.SetOperation_Child) (string, error) {
	onType := fmt.Sprintf("Resource = object(%s,_)", quote(typ))

	switch c := child.ChildType.(type) {
	case *core.SetOperation_Child_UsersetRewrite:
		return tr.rewrite(typ, c.UsersetRewrite)

	case *core.SetOperation_Child_ComputedUserset:
		relation := c.ComputedUserset.Relation
		pred := tr.fresh("computed_" + identifier(relation))
		fmt.Fprintf(&tr.sb, "%s(World,Resource,Subject) :- member(World,Resource,%s,Subject), %s.\n",
			pred, quote(relation), onType)
		return pred, nil

	case *core.SetOperation_Child_TupleToUserset:
		tupleset := c.TupleToUserset.Tupleset.Relation
		computed := c.TupleToUserset.ComputedUserset.Relation
		pred := tr.fresh("arrow_" + identifier(tupleset) + "_" + identifier(computed))
		tr.arrowAny(pred, onType, tupleset, computed)
		return pred, nil

	case *core.SetOperation_Child_FunctionedTupleToUserset:
		tupleset := c.FunctionedTupleToUserset.Tupleset.Relation
		computed := c.FunctionedTupleToUserset.ComputedUserset.Relation
		arrow := identifier(tupleset) + "_" + identifier(computed)
		switch c.FunctionedTupleToUserset.Function {
		case core.FunctionedTupleToUserset_FUNCTION_ANY:
			pred := tr.fresh("any_" + arrow)
			tr.arrowAny(pred, onType, tupleset, computed)
			return pred, nil

		case core.FunctionedTupleToUserset_FUNCTION_ALL:
			// The subject is granted if there is at least one left-hand object, and no
			// left-hand object is missing the subject.
			missing := tr.fresh("all_" + arrow + "_missing")
			fmt.Fprintf(&tr.sb, "%s(World,Resource,Subject) :- relationship(World,Resource,%s,subject(Target,_)), "+
				"candidate(World,Subject), not member(World,Target,%s,Subject), %s.\n",
				missing, quote(tupleset), quote(computed), onType)
			pred := tr.fresh("all_" + arrow)
			fmt.Fprintf(&tr.sb, "%s(World,Resource,Subject) :- relationship(World,Resource,%s,subject(_,_)), "+
				"candidate(World,Subject), not %s(World,Resource,Subject), %s.\n",
				pred, quote(tupleset), missing, onType)
			return pred, nil

		default:
			return "", fmt.Errorf("unknown arrow function %v", c.FunctionedTupleToUserset.Function)
		}

	case *core.SetOperation_Child_XNil:
		return tr.fresh("nil"), nil

	case *core.SetOperation_Child_XSelf:
		pred := tr.fresh("self")
		fmt.Fprintf(&tr.sb, "%s(World,Resource,subject(Resource,\"...\")) :- world(World), queried(Resource), %s.\n",
			pred, onType)
		return pred, nil

	default:
		return "", fmt.Errorf("unsupported rewrite child %T", c)
	}
}

func (tr *translator) arrowAny(pred, onType, tupleset, computed string) {
	fmt.Fprintf(&tr.sb, "%s(World,Resource,Subject) :- relationship(World,Resource,%s,subject(Target,_)), "+
		"member(World,Target,%s,Subject), %s.\n",
		pred, quote(tupleset), quote(computed), onType)
}

// translateFacts returns the facts shared by every world: the objects under test, and the
// concrete objects that wildcards expand to.
func translateFacts(objects, concrete []tuple.ObjectAndRelation) string {
	lines := make([]string, 0, len(objects)+len(concrete))
	for _, o := range objects {
		lines = append(lines, fmt.Sprintf("queried(%s).", object(o)))
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
			subject = fmt.Sprintf("wildcard(%s)", quote(rel.Subject.ObjectType))
		} else {
			subject = fmt.Sprintf("subject(%s,%s)", object(rel.Subject), quote(rel.Subject.Relation))
		}
		lines = append(lines, fmt.Sprintf("relationship(%s,%s,%s,%s).", w, object(rel.Resource), quote(rel.Resource.Relation), subject))
	}
	slices.Sort(lines)
	return strings.Join(slices.Compact(lines), "\n") + "\n"
}

func object(onr tuple.ObjectAndRelation) string {
	return fmt.Sprintf("object(%s,%s)", quote(onr.ObjectType), quote(onr.ObjectID))
}

func quote(s string) string {
	return `"` + strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`).Replace(s) + `"`
}
