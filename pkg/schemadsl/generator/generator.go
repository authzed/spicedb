package generator

import (
	"bufio"
	"strings"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"

	"github.com/authzed/spicedb/pkg/graph"
	"github.com/authzed/spicedb/pkg/namespace"
)

// Ellipsis is the relation name for terminal subjects.
const Ellipsis = "..."

// MaxSingleLineCommentLength sets the maximum length for a comment to made single line.
const MaxSingleLineCommentLength = 70 // 80 - the comment parts and some padding

// GenerateSource generates a DSL view of the given namespace definition.
func GenerateSource(namespace *core.NamespaceDefinition) (string, bool) {
	generator := &sourceGenerator{
		indentationLevel: 0,
		hasNewline:       true,
		hasBlankline:     true,
		hasNewScope:      true,
	}

	generator.emitNamespace(namespace)
	return generator.buf.String(), !generator.hasIssue
}

func (sg *sourceGenerator) emitNamespace(namespace *core.NamespaceDefinition) {
	sg.emitComments(namespace.Metadata)
	sg.append("definition ")
	sg.append(namespace.Name)

	if len(namespace.Relation) == 0 {
		sg.append(" {}")
		return
	}

	sg.append(" {")
	sg.appendLine()
	sg.indent()
	sg.markNewScope()

	for _, relation := range namespace.Relation {
		sg.emitRelation(relation)
	}

	sg.dedent()
	sg.append("}")
}

func (sg *sourceGenerator) emitRelation(relation *core.Relation) {
	hasThis := graph.HasThis(relation.UsersetRewrite)
	isPermission := relation.UsersetRewrite != nil && !hasThis

	sg.emitComments(relation.Metadata)
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
			for index, allowedRelation := range relation.TypeInformation.AllowedDirectRelations {
				if index > 0 {
					sg.append(" | ")
				}

				sg.emitAllowedRelation(allowedRelation)
			}
		}
	}

	if relation.UsersetRewrite != nil {
		sg.append(" = ")
		sg.emitRewrite(relation.UsersetRewrite)
	}

	sg.appendLine()
}

func (sg *sourceGenerator) emitAllowedRelation(allowedRelation *core.AllowedRelation) {
	sg.append(allowedRelation.Namespace)
	if allowedRelation.GetRelation() != "" && allowedRelation.GetRelation() != Ellipsis {
		sg.append("#")
		sg.append(allowedRelation.GetRelation())
	}
	if allowedRelation.GetPublicWildcard() != nil {
		sg.append(":*")
	}
}

func (sg *sourceGenerator) emitRewrite(rewrite *core.UsersetRewrite) {
	switch rw := rewrite.RewriteOperation.(type) {
	case *core.UsersetRewrite_Union:
		sg.emitRewriteOps(rw.Union, "+")
	case *core.UsersetRewrite_Intersection:
		sg.emitRewriteOps(rw.Intersection, "&")
	case *core.UsersetRewrite_Exclusion:
		sg.emitRewriteOps(rw.Exclusion, "-")
	}
}

func (sg *sourceGenerator) emitRewriteOps(setOp *core.SetOperation, op string) {
	for index, child := range setOp.Child {
		if index > 0 {
			sg.append(" " + op + " ")
		}

		sg.emitSetOpChild(child)
	}
}

func (sg *sourceGenerator) isAllUnion(rewrite *core.UsersetRewrite) bool {
	switch rw := rewrite.RewriteOperation.(type) {
	case *core.UsersetRewrite_Union:
		for _, setOpChild := range rw.Union.Child {
			switch child := setOpChild.ChildType.(type) {
			case *core.SetOperation_Child_UsersetRewrite:
				if !sg.isAllUnion(child.UsersetRewrite) {
					return false
				}
			default:
				continue
			}
		}
		return true
	default:
		return false
	}
}

func (sg *sourceGenerator) emitSetOpChild(setOpChild *core.SetOperation_Child) {
	switch child := setOpChild.ChildType.(type) {
	case *core.SetOperation_Child_UsersetRewrite:
		if sg.isAllUnion(child.UsersetRewrite) {
			sg.emitRewrite(child.UsersetRewrite)
			break
		}

		sg.append("(")
		sg.emitRewrite(child.UsersetRewrite)
		sg.append(")")

	case *core.SetOperation_Child_XThis:
		sg.appendIssue("_this unsupported here. Please rewrite into a relation and permission")

	case *core.SetOperation_Child_XNil:
		sg.append("nil")

	case *core.SetOperation_Child_ComputedUserset:
		sg.append(child.ComputedUserset.Relation)

	case *core.SetOperation_Child_TupleToUserset:
		sg.append(child.TupleToUserset.Tupleset.Relation)
		sg.append("->")
		sg.append(child.TupleToUserset.ComputedUserset.Relation)
	}
}

func (sg *sourceGenerator) emitComments(metadata *core.Metadata) {
	if len(namespace.GetComments(metadata)) > 0 {
		sg.ensureBlankLineOrNewScope()
	}

	for _, comment := range namespace.GetComments(metadata) {
		sg.appendComment(comment)
	}
}

func (sg *sourceGenerator) appendComment(comment string) {
	switch {
	case strings.HasPrefix(comment, "/*"):
		stripped := strings.TrimSpace(comment)

		if strings.HasPrefix(stripped, "/**") {
			stripped = strings.TrimPrefix(stripped, "/**")
			sg.append("/**")
		} else {
			stripped = strings.TrimPrefix(stripped, "/*")
			sg.append("/*")
		}

		stripped = strings.TrimSuffix(stripped, "*/")
		stripped = strings.TrimSpace(stripped)

		requireMultiline := len(stripped) > MaxSingleLineCommentLength || strings.ContainsRune(stripped, '\n')

		if requireMultiline {
			sg.appendLine()
			scanner := bufio.NewScanner(strings.NewReader(stripped))
			for scanner.Scan() {
				sg.append(" * ")
				sg.append(strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(scanner.Text()), "*")))
				sg.appendLine()
			}
			sg.append(" */")
			sg.appendLine()
		} else {
			sg.append(" ")
			sg.append(strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(stripped), "*")))
			sg.append(" */")
			sg.appendLine()
		}

	case strings.HasPrefix(comment, "//"):
		sg.append("// ")
		sg.append(strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(comment), "//")))
		sg.appendLine()
	}
}
