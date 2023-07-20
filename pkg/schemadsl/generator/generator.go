package generator

import (
	"bufio"
	"fmt"
	"sort"
	"strings"

	"golang.org/x/exp/maps"

	"github.com/authzed/spicedb/pkg/caveats"
	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
	"github.com/authzed/spicedb/pkg/graph"
	"github.com/authzed/spicedb/pkg/namespace"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

// Ellipsis is the relation name for terminal subjects.
const Ellipsis = "..."

// MaxSingleLineCommentLength sets the maximum length for a comment to made single line.
const MaxSingleLineCommentLength = 70 // 80 - the comment parts and some padding

// GenerateSchema generates a DSL view of the given schema.
func GenerateSchema(definitions []compiler.SchemaDefinition) (string, bool, error) {
	generated := make([]string, 0, len(definitions))
	result := true
	for _, definition := range definitions {
		switch def := definition.(type) {
		case *core.CaveatDefinition:
			generatedCaveat, ok, err := GenerateCaveatSource(def)
			if err != nil {
				return "", false, err
			}

			result = result && ok
			generated = append(generated, generatedCaveat)

		case *core.NamespaceDefinition:
			generatedSchema, ok, err := GenerateSource(def)
			if err != nil {
				return "", false, err
			}

			result = result && ok
			generated = append(generated, generatedSchema)

		default:
			return "", false, spiceerrors.MustBugf("unknown type of definition %T in GenerateSchema", def)
		}
	}

	return strings.Join(generated, "\n\n"), result, nil
}

// GenerateCaveatSource generates a DSL view of the given caveat definition.
func GenerateCaveatSource(caveat *core.CaveatDefinition) (string, bool, error) {
	generator := &sourceGenerator{
		indentationLevel: 0,
		hasNewline:       true,
		hasBlankline:     true,
		hasNewScope:      true,
	}

	err := generator.emitCaveat(caveat)
	if err != nil {
		return "", false, err
	}

	return generator.buf.String(), !generator.hasIssue, nil
}

// GenerateSource generates a DSL view of the given namespace definition.
func GenerateSource(namespace *core.NamespaceDefinition) (string, bool, error) {
	generator := &sourceGenerator{
		indentationLevel: 0,
		hasNewline:       true,
		hasBlankline:     true,
		hasNewScope:      true,
	}

	err := generator.emitNamespace(namespace)
	if err != nil {
		return "", false, err
	}

	return generator.buf.String(), !generator.hasIssue, nil
}

func (sg *sourceGenerator) emitCaveat(caveat *core.CaveatDefinition) error {
	sg.emitComments(caveat.Metadata)
	sg.append("caveat ")
	sg.append(caveat.Name)
	sg.append("(")

	parameterNames := maps.Keys(caveat.ParameterTypes)
	sort.Strings(parameterNames)

	for index, paramName := range parameterNames {
		if index > 0 {
			sg.append(", ")
		}

		decoded, err := caveattypes.DecodeParameterType(caveat.ParameterTypes[paramName])
		if err != nil {
			return fmt.Errorf("invalid parameter type on caveat: %w", err)
		}

		sg.append(paramName)
		sg.append(" ")
		sg.append(decoded.String())
	}

	sg.append(")")

	sg.append(" {")
	sg.appendLine()
	sg.indent()
	sg.markNewScope()

	parameterTypes, err := caveattypes.DecodeParameterTypes(caveat.ParameterTypes)
	if err != nil {
		return fmt.Errorf("invalid caveat parameters: %w", err)
	}

	deserializedExpression, err := caveats.DeserializeCaveat(caveat.SerializedExpression, parameterTypes)
	if err != nil {
		return fmt.Errorf("invalid caveat expression bytes: %w", err)
	}

	exprString, err := deserializedExpression.ExprString()
	if err != nil {
		return fmt.Errorf("invalid caveat expression: %w", err)
	}

	sg.append(strings.TrimSpace(exprString))
	sg.appendLine()

	sg.dedent()
	sg.append("}")
	return nil
}

func (sg *sourceGenerator) emitNamespace(namespace *core.NamespaceDefinition) error {
	sg.emitComments(namespace.Metadata)
	sg.append("definition ")
	sg.append(namespace.Name)

	if len(namespace.Relation) == 0 {
		sg.append(" {}")
		return nil
	}

	sg.append(" {")
	sg.appendLine()
	sg.indent()
	sg.markNewScope()

	for _, relation := range namespace.Relation {
		err := sg.emitRelation(relation)
		if err != nil {
			return err
		}
	}

	sg.dedent()
	sg.append("}")
	return nil
}

func (sg *sourceGenerator) emitRelation(relation *core.Relation) error {
	hasThis, err := graph.HasThis(relation.UsersetRewrite)
	if err != nil {
		return err
	}

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
	return nil
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
	if allowedRelation.GetRequiredCaveat() != nil {
		sg.append(" with ")
		sg.append(allowedRelation.RequiredCaveat.CaveatName)
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
