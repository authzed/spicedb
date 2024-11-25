//go:generate go run golang.org/x/tools/cmd/stringer -type=NodeType -output zz_generated.nodetype_string.go

// Package dslshape defines the types representing the structure of schema DSL.
package dslshape

// NodeType identifies the type of AST node.
type NodeType int

const (
	// Top-level
	NodeTypeError   NodeType = iota // error occurred; value is text of error
	NodeTypeFile                    // The file root node
	NodeTypeComment                 // A single or multiline comment

	NodeTypeDefinition       // A definition.
	NodeTypeCaveatDefinition // A caveat definition.

	NodeTypeCaveatParameter  // A caveat parameter.
	NodeTypeCaveatExpression // A caveat expression.

	NodeTypeRelation   // A relation
	NodeTypePermission // A permission

	NodeTypeTypeReference         // A type reference
	NodeTypeSpecificTypeReference // A reference to a specific type.
	NodeTypeCaveatReference       // A caveat reference under a type.

	NodeTypeUnionExpression
	NodeTypeIntersectExpression
	NodeTypeExclusionExpression

	NodeTypeArrowExpression // A TTU in arrow form.

	NodeTypeIdentifier    // An identifier under an expression.
	NodeTypeNilExpression // A nil keyword

	NodeTypeCaveatTypeReference // A type reference for a caveat parameter.

	NodeTypeImport

	// A balanced brace-enclosed expression. Represents both caveat CELs and caveat JSON context.
	NodeTypeOpaqueBraceExpression

	NodeTypeTest
	NodeTypeTestRelations
	NodeTypeTestRelation
	NodeTypeTestObject
	NodeTypeTestPermission
	NodeTypeTestNegativePermission
	NodeTypeTestAssertions
	NodeTypeTestAssertion
	NodeTypeTestExpectedRelations
	NodeTypeTestExpectedRelation
	NodeTypeTestExpectedRelationSource
)

const (
	//
	// All nodes
	//
	// The source of this node.
	NodePredicateSource = "input-source"

	// The rune position in the input string at which this node begins.
	NodePredicateStartRune = "start-rune"

	// The rune position in the input string at which this node ends.
	NodePredicateEndRune = "end-rune"

	// A direct child of this node. Implementations should handle the ordering
	// automatically for this predicate.
	NodePredicateChild = "child-node"

	//
	// NodeTypeError
	//

	// The message for the parsing error.
	NodePredicateErrorMessage = "error-message"

	// The (optional) source to highlight for the parsing error.
	NodePredicateErrorSource = "error-source"

	//
	// NodeTypeComment
	//

	// The value of the comment, including its delimeter(s)
	NodeCommentPredicateValue = "comment-value"

	//
	// NodeTypeDefinition
	//

	// The name of the definition
	NodeDefinitionPredicateName = "definition-name"

	//
	// NodeTypeCaveatDefinition
	//

	// The name of the definition
	NodeCaveatDefinitionPredicateName = "caveat-definition-name"

	// The parameters for the definition.
	NodeCaveatDefinitionPredicateParameters = "parameters"

	// The link to the expression for the definition.
	NodeCaveatDefinitionPredicateExpession = "caveat-definition-expression"

	//
	// NodeTypeCaveatExpression
	//

	// The raw CEL expression, in string form.
	NodeCaveatExpressionPredicateExpression = "caveat-expression-expressionstr"

	//
	// NodeTypeCaveatParameter
	//

	// The name of the parameter
	NodeCaveatParameterPredicateName = "caveat-parameter-name"

	// The defined type of the caveat parameter.
	NodeCaveatParameterPredicateType = "caveat-parameter-type"

	//
	// NodeTypeCaveatTypeReference
	//

	// The type for the caveat type reference.
	NodeCaveatTypeReferencePredicateType = "type-name"

	// The child type(s) for the type reference.
	NodeCaveatTypeReferencePredicateChildTypes = "child-types"

	//
	// NodeTypeRelation + NodeTypePermission
	//

	// The name of the relation/permission
	NodePredicateName = "relation-name"

	//
	// NodeTypeRelation
	//

	// The allowed types for the relation.
	NodeRelationPredicateAllowedTypes = "allowed-types"

	//
	// NodeTypeTypeReference
	//

	// A type under a type reference.
	NodeTypeReferencePredicateType = "type-ref-type"

	//
	// NodeTypeSpecificTypeReference
	//

	// A type under a type reference.
	NodeSpecificReferencePredicateType = "type-name"

	// A relation under a type reference.
	NodeSpecificReferencePredicateRelation = "relation-name"

	// A wildcard under a type reference.
	NodeSpecificReferencePredicateWildcard = "type-wildcard"

	// A caveat under a type reference.
	NodeSpecificReferencePredicateCaveat = "caveat"

	//
	// NodeTypeCaveatReference
	//

	// The caveat name under the caveat.
	NodeCaveatPredicateCaveat = "caveat-name"

	//
	// NodeTypePermission
	//

	// The expression to compute the permission.
	NodePermissionPredicateComputeExpression = "compute-expression"

	//
	// NodeTypeArrowExpression
	//

	// The name of the function in the arrow expression.
	NodeArrowExpressionFunctionName = "function-name"

	//
	// NodeTypeIdentifer
	//

	// The value of the identifier.
	NodeIdentiferPredicateValue = "identifier-value"

	//
	// NodeTypeUnionExpression + NodeTypeIntersectExpression + NodeTypeExclusionExpression + NodeTypeArrowExpression
	//
	NodeExpressionPredicateLeftExpr  = "left-expr"
	NodeExpressionPredicateRightExpr = "right-expr"

	//
	// NodeTypeImport
	//
	// TODO: still need to figure out what form this should take - full path? relative path?
	NodeImportPredicateSource         = "import-source"
	NodeImportPredicatePathSegment    = "path-segment"
	NodeImportPredicateDefinitionName = "imported-definition"

	NodeTestPredicateName = "test-name"

	NodeTestPredicateSubject  = "subject"
	NodeTestPredicateObject   = "object"
	NodeTestPredicateRelation = "relation"
	NodeTestPredicatePermission = "permission"
	NodeTestPredicateAssertionType = "assertion-type"
	NodeTestPredicateCaveatName    = "caveat-name"
	NodeTestPredicateCaveatContext = "caveat-context"

	NodeTestObjectPredicateObjectType = "object-type"
	NodeTestObjectPredicateObjectID   = "object-id"
	// Used for both positive and negative permissions
	NodeTestRelationPermissionPredicateName = "permission-name"

	NodeTestExpectedRelationSourcePredicateCaveatAnnotation = "caveat-annotation"

	NodeOpaqueBraceExpressionPredicateExpression = "expression"
)
