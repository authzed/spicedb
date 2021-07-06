package generator

import (
	"github.com/authzed/spicedb/pkg/graph"
	v0 "github.com/authzed/spicedb/pkg/proto/authzed/api/v0"
)

// Ellipsis is the relation name for terminal subjects.
const Ellipsis = "..."

// GenerateSource generates a DSL view of the given namespace definition.
func GenerateSource(namespace *v0.NamespaceDefinition) (string, bool) {
	generator := &sourceGenerator{
		indentationLevel: 0,
		hasNewline:       true,
	}

	generator.emitNamespace(namespace)
	return generator.buf.String(), !generator.hasIssue
}

func (sg *sourceGenerator) emitNamespace(namespace *v0.NamespaceDefinition) {
	sg.append("definition ")
	sg.append(namespace.Name)

	if len(namespace.Relation) == 0 {
		sg.append(" {}")
		return
	}

	sg.append(" {")
	sg.appendLine()
	sg.indent()

	for _, relation := range namespace.Relation {
		sg.emitRelation(relation)
	}

	sg.dedent()
	sg.append("}")
}

func (sg *sourceGenerator) emitRelation(relation *v0.Relation) {
	hasThis := graph.HasThis(relation.UsersetRewrite)
	isPermission := relation.UsersetRewrite != nil && !hasThis

	if isPermission {
		sg.append("permission ")
	} else {
		sg.append("relation ")
	}

	sg.append(relation.Name)

	if !isPermission {
		sg.append(": ")
		if relation.TypeInformation == nil || relation.TypeInformation.AllowedDirectRelations == nil || len(relation.TypeInformation.AllowedDirectRelations) == 0 {
			sg.appendIssue("missing allowed types")
		} else {
			for index, relationRef := range relation.TypeInformation.AllowedDirectRelations {
				if index > 0 {
					sg.append(" | ")
				}

				sg.emitRelationReference(relationRef)
			}
		}
	}

	if relation.UsersetRewrite != nil {
		sg.append(" = ")
		sg.emitRewrite(relation.UsersetRewrite)
	}

	sg.appendLine()
}

func (sg *sourceGenerator) emitRelationReference(relationReference *v0.RelationReference) {
	sg.append(relationReference.Namespace)
	if relationReference.Relation != Ellipsis {
		sg.append("#")
		sg.append(relationReference.Relation)
	}
}

func (sg *sourceGenerator) emitRewrite(rewrite *v0.UsersetRewrite) {
	switch rw := rewrite.RewriteOperation.(type) {
	case *v0.UsersetRewrite_Union:
		sg.emitRewriteOps(rw.Union, "+")
	case *v0.UsersetRewrite_Intersection:
		sg.emitRewriteOps(rw.Intersection, "&")
	case *v0.UsersetRewrite_Exclusion:
		sg.emitRewriteOps(rw.Exclusion, "-")
	}
}

func (sg *sourceGenerator) emitRewriteOps(setOp *v0.SetOperation, op string) {
	for index, child := range setOp.Child {
		if index > 0 {
			sg.append(" " + op + " ")
		}

		sg.emitSetOpChild(child)
	}
}

func (sg *sourceGenerator) emitSetOpChild(setOpChild *v0.SetOperation_Child) {
	switch child := setOpChild.ChildType.(type) {
	case *v0.SetOperation_Child_UsersetRewrite:
		sg.append("(")
		sg.emitRewrite(child.UsersetRewrite)
		sg.append(")")

	case *v0.SetOperation_Child_XThis:
		sg.appendIssue("_this unsupported here. Please rewrite into a relation and permission")

	case *v0.SetOperation_Child_ComputedUserset:
		sg.append(child.ComputedUserset.Relation)

	case *v0.SetOperation_Child_TupleToUserset:
		sg.append(child.TupleToUserset.Tupleset.Relation)
		sg.append("->")
		sg.append(child.TupleToUserset.ComputedUserset.Relation)
	}
}
