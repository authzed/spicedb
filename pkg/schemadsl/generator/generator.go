package generator

import (
	"strings"

	pb "github.com/authzed/spicedb/pkg/REDACTEDapi/api"
	"github.com/authzed/spicedb/pkg/graph"
)

const Ellipsis = "..."

// generateSource generates a DSL view of the given namespace definition.
func generateSource(namespace *pb.NamespaceDefinition) []byte {
	generator := &sourceGenerator{
		indentationLevel: 0,
		hasNewline:       true,
	}

	generator.emitNamespace(namespace)
	return generator.buf.Bytes()
}

func (sg *sourceGenerator) emitNamespace(namespace *pb.NamespaceDefinition) {
	parts := strings.Split(namespace.Name, "/")
	namespaceName := parts[len(parts)-1]

	sg.append("definition ")
	sg.append(namespaceName)

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

func (sg *sourceGenerator) emitRelation(relation *pb.Relation) {
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
			sg.append("/* missing allowed types */")
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

func (sg *sourceGenerator) emitRelationReference(relationReference *pb.RelationReference) {
	parts := strings.Split(relationReference.Namespace, "/")
	namespaceName := parts[len(parts)-1]

	sg.append(namespaceName)
	if relationReference.Relation != Ellipsis {
		sg.append("#")
		sg.append(relationReference.Relation)
	}
}

func (sg *sourceGenerator) emitRewrite(rewrite *pb.UsersetRewrite) {
	switch rw := rewrite.RewriteOperation.(type) {
	case *pb.UsersetRewrite_Union:
		sg.emitRewriteOps(rw.Union, "+")
	case *pb.UsersetRewrite_Intersection:
		sg.emitRewriteOps(rw.Intersection, "&")
	case *pb.UsersetRewrite_Exclusion:
		sg.emitRewriteOps(rw.Exclusion, "-")
	}

}

func (sg *sourceGenerator) emitRewriteOps(setOp *pb.SetOperation, op string) {
	for index, child := range setOp.Child {
		if index > 0 {
			sg.append(" " + op + " ")
		}

		sg.emitSetOpChild(child)
	}
}

func (sg *sourceGenerator) emitSetOpChild(setOpChild *pb.SetOperation_Child) {
	switch child := setOpChild.ChildType.(type) {
	case *pb.SetOperation_Child_UsersetRewrite:
		sg.append("(")
		sg.emitRewrite(child.UsersetRewrite)
		sg.append(")")

	case *pb.SetOperation_Child_XThis:
		sg.append("(")
		sg.append("/* _this unsupported here. Please rewrite into a relation and permission */")
		sg.append(")")

	case *pb.SetOperation_Child_ComputedUserset:
		sg.append(child.ComputedUserset.Relation)

	case *pb.SetOperation_Child_TupleToUserset:
		sg.append(child.TupleToUserset.Tupleset.Relation)
		sg.append("->")
		sg.append(child.TupleToUserset.ComputedUserset.Relation)
	}
}
