package development

import (
	"context"
	"fmt"
	"strings"

	corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"
	devinterface "github.com/authzed/spicedb/pkg/proto/developer/v1"
	"github.com/authzed/spicedb/pkg/schema"
	"github.com/authzed/spicedb/pkg/tuple"
)

var lintRelationReferencesParentType = relationCheck{
	"relation-name-references-parent",
	func(
		ctx context.Context,
		relation *corev1.Relation,
		def *schema.Definition,
	) (*devinterface.DeveloperWarning, error) {
		parentDef := def.Namespace()
		if strings.HasSuffix(relation.Name, parentDef.Name) {
			if def.IsPermission(relation.Name) {
				return warningForMetadata(
					"relation-name-references-parent",
					fmt.Sprintf("Permission %q references parent type %q in its name; it is recommended to drop the suffix", relation.Name, parentDef.Name),
					relation.Name,
					relation,
				), nil
			}

			return warningForMetadata(
				"relation-name-references-parent",
				fmt.Sprintf("Relation %q references parent type %q in its name; it is recommended to drop the suffix", relation.Name, parentDef.Name),
				relation.Name,
				relation,
			), nil
		}

		return nil, nil
	},
}

var lintPermissionReferencingItself = computedUsersetCheck{
	"permission-references-itself",
	func(
		ctx context.Context,
		computedUserset *corev1.ComputedUserset,
		sourcePosition *corev1.SourcePosition,
		def *schema.Definition,
	) (*devinterface.DeveloperWarning, error) {
		parentRelation := ctx.Value(relationKey).(*corev1.Relation)
		permName := parentRelation.Name
		if computedUserset.GetRelation() == permName {
			return warningForPosition(
				"permission-references-itself",
				fmt.Sprintf("Permission %q references itself, which will cause an error to be raised due to infinite recursion", permName),
				permName,
				sourcePosition,
			), nil
		}

		return nil, nil
	},
}

var lintArrowReferencingUnreachable = ttuCheck{
	"arrow-references-unreachable-relation",
	func(
		ctx context.Context,
		ttu ttu,
		sourcePosition *corev1.SourcePosition,
		def *schema.Definition,
	) (*devinterface.DeveloperWarning, error) {
		parentRelation := ctx.Value(relationKey).(*corev1.Relation)

		referencedRelation, ok := def.GetRelation(ttu.GetTupleset().GetRelation())
		if !ok {
			return nil, nil
		}

		allowedSubjectTypes, err := def.AllowedSubjectRelations(referencedRelation.Name)
		if err != nil {
			return nil, err
		}

		wasFound := false
		for _, subjectType := range allowedSubjectTypes {
			nts, err := def.TypeSystem().GetDefinition(ctx, subjectType.Namespace)
			if err != nil {
				return nil, err
			}

			_, ok := nts.GetRelation(ttu.GetComputedUserset().GetRelation())
			if ok {
				wasFound = true
			}
		}

		if !wasFound {
			arrowString, err := ttu.GetArrowString()
			if err != nil {
				return nil, err
			}

			return warningForPosition(
				"arrow-references-unreachable-relation",
				fmt.Sprintf(
					"Arrow `%s` under permission %q references relation/permission %q that does not exist on any subject types of relation %q",
					arrowString,
					parentRelation.Name,
					ttu.GetComputedUserset().GetRelation(),
					ttu.GetTupleset().GetRelation(),
				),
				arrowString,
				sourcePosition,
			), nil
		}

		return nil, nil
	},
}

var lintArrowOverSubRelation = ttuCheck{
	"arrow-walks-subject-relation",
	func(
		ctx context.Context,
		ttu ttu,
		sourcePosition *corev1.SourcePosition,
		def *schema.Definition,
	) (*devinterface.DeveloperWarning, error) {
		parentRelation := ctx.Value(relationKey).(*corev1.Relation)

		referencedRelation, ok := def.GetRelation(ttu.GetTupleset().GetRelation())
		if !ok {
			return nil, nil
		}

		allowedSubjectTypes, err := def.AllowedSubjectRelations(referencedRelation.Name)
		if err != nil {
			return nil, err
		}

		arrowString, err := ttu.GetArrowString()
		if err != nil {
			return nil, err
		}

		for _, subjectType := range allowedSubjectTypes {
			if subjectType.GetRelation() != tuple.Ellipsis {
				return warningForPosition(
					"arrow-walks-subject-relation",
					fmt.Sprintf(
						"Arrow `%s` under permission %q references relation %q that has relation %q on subject %q: *the subject relation will be ignored for the arrow*",
						arrowString,
						parentRelation.Name,
						ttu.GetTupleset().GetRelation(),
						subjectType.GetRelation(),
						subjectType.Namespace,
					),
					arrowString,
					sourcePosition,
				), nil
			}
		}

		return nil, nil
	},
}

var lintArrowReferencingRelation = ttuCheck{
	"arrow-references-relation",
	func(
		ctx context.Context,
		ttu ttu,
		sourcePosition *corev1.SourcePosition,
		def *schema.Definition,
	) (*devinterface.DeveloperWarning, error) {
		parentRelation := ctx.Value(relationKey).(*corev1.Relation)

		referencedRelation, ok := def.GetRelation(ttu.GetTupleset().GetRelation())
		if !ok {
			return nil, nil
		}

		// For each subject type of the referenced relation, check if the referenced permission
		// is, in fact, a relation.
		allowedSubjectTypes, err := def.AllowedSubjectRelations(referencedRelation.Name)
		if err != nil {
			return nil, err
		}

		arrowString, err := ttu.GetArrowString()
		if err != nil {
			return nil, err
		}

		for _, subjectType := range allowedSubjectTypes {
			// Skip for arrow referencing relations in the same namespace.
			if subjectType.Namespace == def.Namespace().Name {
				continue
			}

			nts, err := def.TypeSystem().GetDefinition(ctx, subjectType.Namespace)
			if err != nil {
				return nil, err
			}

			targetRelation, ok := nts.GetRelation(ttu.GetComputedUserset().GetRelation())
			if !ok {
				continue
			}

			if !nts.IsPermission(targetRelation.Name) {
				return warningForPosition(
					"arrow-references-relation",
					fmt.Sprintf(
						"Arrow `%s` under permission %q references relation %q on definition %q; it is recommended to point to a permission",
						arrowString,
						parentRelation.Name,
						targetRelation.Name,
						subjectType.Namespace,
					),
					arrowString,
					sourcePosition,
				), nil
			}
		}

		return nil, nil
	},
}

// CheckExpressionForMixedOperators checks a permission expression for mixed operators
// at the same parenthetical scope. This is a source-scanning check that detects
// cases where users mix +, -, and & operators without explicit parentheses.
//
// For example:
//   - "foo + bar" - OK (single operator type)
//   - "foo - bar & baz" - WARN (mixed - and & at same scope)
//   - "(foo - bar) & baz" - OK (operators in different scopes)
//   - "(foo + bar) - (baz & qux)" - OK (operators in different scopes)
//   - "(foo + bar - baz)" - WARN (mixed inside parentheses)
func CheckExpressionForMixedOperators(
	permissionName string,
	expressionText string,
	sourcePosition *corev1.SourcePosition,
) *devinterface.DeveloperWarning {
	// Use a stack-based approach to track operators in each parenthetical scope.
	// Each scope is a map of operators seen at that level.
	type scope struct {
		operators map[rune]bool
	}

	// Helper to check if a scope has mixed operators and return warning if so
	checkScope := func(s scope) *devinterface.DeveloperWarning {
		if len(s.operators) > 1 {
			// Build a human-readable list of the mixed operators
			var opList []string
			if s.operators['+'] {
				opList = append(opList, "union (+)")
			}
			if s.operators['-'] {
				opList = append(opList, "exclusion (-)")
			}
			if s.operators['&'] {
				opList = append(opList, "intersection (&)")
			}

			return warningForPosition(
				"mixed-operators-without-parentheses",
				fmt.Sprintf(
					"Permission %q mixes %s at the same level of nesting; consider adding parentheses to clarify precedence",
					permissionName,
					strings.Join(opList, " and "),
				),
				permissionName,
				sourcePosition,
			)
		}
		return nil
	}

	// Stack of scopes - start with the root scope
	scopeStack := []scope{{operators: make(map[rune]bool)}}

	runes := []rune(expressionText)
	for i := 0; i < len(runes); i++ {
		r := runes[i]

		// Skip whitespace
		if isWhitespace(r) {
			continue
		}

		// Enter a new scope on open parenthesis
		if r == '(' {
			scopeStack = append(scopeStack, scope{operators: make(map[rune]bool)})
			continue
		}

		// Exit scope on close parenthesis - check for mixed operators before popping
		if r == ')' {
			if len(scopeStack) > 1 {
				// Check the scope we're about to pop for mixed operators
				if warning := checkScope(scopeStack[len(scopeStack)-1]); warning != nil {
					return warning
				}
				scopeStack = scopeStack[:len(scopeStack)-1]
			}
			continue
		}

		// Skip arrow operator (->)
		if r == '-' && i+1 < len(runes) && runes[i+1] == '>' {
			i++ // Skip the '>'
			continue
		}

		// Skip dot notation for function calls (e.g., parent.all)
		if r == '.' {
			continue
		}

		// Check for operators: +, -, &
		// Only count operators that are not part of an identifier
		if r == '+' || r == '-' || r == '&' {
			// Look back to see if the previous non-whitespace char was an identifier char
			// If so, this might be part of a different construct, but in SpiceDB schema
			// these operators should always be surrounded by whitespace or parens
			currentScope := &scopeStack[len(scopeStack)-1]
			currentScope.operators[r] = true
			continue
		}
	}

	// Check remaining scopes (including root) for mixed operators
	for _, s := range scopeStack {
		if warning := checkScope(s); warning != nil {
			return warning
		}
	}

	return nil
}

func isWhitespace(r rune) bool {
	return r == ' ' || r == '\t' || r == '\n' || r == '\r'
}
