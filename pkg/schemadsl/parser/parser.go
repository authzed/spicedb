// parser package defines the parser for the Authzed Schema DSL.
package parser

import (
	"strings"

	"golang.org/x/exp/maps"

	"github.com/authzed/spicedb/pkg/schemadsl/dslshape"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
	"github.com/authzed/spicedb/pkg/schemadsl/lexer"
)

// Parse parses the given Schema DSL source into a parse tree.
func Parse(builder NodeBuilder, source input.Source, input string) AstNode {
	lx := lexer.Lex(source, input)
	parser := buildParser(lx, builder, source, input)
	defer parser.close()
	return parser.consumeTopLevel()
}

// ignoredTokenTypes are those tokens ignored when parsing.
var ignoredTokenTypes = map[lexer.TokenType]bool{
	lexer.TokenTypeWhitespace:        true,
	lexer.TokenTypeNewline:           true,
	lexer.TokenTypeSinglelineComment: true,
	lexer.TokenTypeMultilineComment:  true,
}

// consumeTopLevel attempts to consume the top-level definitions.
func (p *sourceParser) consumeTopLevel() AstNode {
	rootNode := p.startNode(dslshape.NodeTypeFile)
	defer p.mustFinishNode()

	// Start at the first token.
	p.consumeToken()

	if p.currentToken.Kind == lexer.TokenTypeError {
		p.emitErrorf("%s", p.currentToken.Value)
		return rootNode
	}

	hasSeenDefinition := false

Loop:
	for {
		if p.isToken(lexer.TokenTypeEOF) {
			break Loop
		}

		// Consume a statement terminator if one was found.
		p.tryConsumeStatementTerminator()

		if p.isToken(lexer.TokenTypeEOF) {
			break Loop
		}

		// The top level of the DSL is a set of definitions and caveats:
		// definition foobar { ... }
		// caveat somecaveat (...) { ... }

		switch {
		case p.isIdentifier("use"):
			rootNode.Connect(dslshape.NodePredicateChild, p.consumeUseFlag(hasSeenDefinition))

		case p.isKeyword("definition"):
			hasSeenDefinition = true
			rootNode.Connect(dslshape.NodePredicateChild, p.consumeDefinition())

		case p.isKeyword("caveat"):
			hasSeenDefinition = true
			rootNode.Connect(dslshape.NodePredicateChild, p.consumeCaveat())

		default:
			p.emitErrorf("Unexpected token at root level: %v", p.currentToken.Kind)
			break Loop
		}
	}

	return rootNode
}

// consumeCaveat attempts to consume a single caveat definition.
// ```caveat somecaveat(param1 type, param2 type) { ... }```
func (p *sourceParser) consumeCaveat() AstNode {
	defNode := p.startNode(dslshape.NodeTypeCaveatDefinition)
	defer p.mustFinishNode()

	// caveat ...
	p.consumeKeyword("caveat")
	caveatName, ok := p.consumeTypePath()
	if !ok {
		return defNode
	}

	defNode.MustDecorate(dslshape.NodeCaveatDefinitionPredicateName, caveatName)

	// Parameters:
	// (
	_, ok = p.consume(lexer.TokenTypeLeftParen)
	if !ok {
		return defNode
	}

	for {
		paramNode, ok := p.consumeCaveatParameter()
		if !ok {
			return defNode
		}

		defNode.Connect(dslshape.NodeCaveatDefinitionPredicateParameters, paramNode)
		if _, ok := p.tryConsume(lexer.TokenTypeComma); !ok {
			break
		}
	}

	// )
	_, ok = p.consume(lexer.TokenTypeRightParen)
	if !ok {
		return defNode
	}

	// {
	_, ok = p.consume(lexer.TokenTypeLeftBrace)
	if !ok {
		return defNode
	}

	exprNode, ok := p.consumeCaveatExpression()
	if !ok {
		return defNode
	}

	defNode.Connect(dslshape.NodeCaveatDefinitionPredicateExpession, exprNode)

	// }
	_, ok = p.consume(lexer.TokenTypeRightBrace)
	if !ok {
		return defNode
	}

	return defNode
}

func (p *sourceParser) consumeCaveatExpression() (AstNode, bool) {
	exprNode := p.startNode(dslshape.NodeTypeCaveatExpression)
	defer p.mustFinishNode()

	// Special Logic Note: Since CEL is its own language, we consume here until we have a matching
	// close brace, and then pass ALL the found tokens to CEL's own parser to attach the expression
	// here.
	braceDepth := 1 // Starting at 1 from the open brace above
	var startToken *commentedLexeme
	var endToken *commentedLexeme
consumer:
	for {
		currentToken := p.currentToken

		switch currentToken.Kind {
		case lexer.TokenTypeLeftBrace:
			braceDepth++

		case lexer.TokenTypeRightBrace:
			if braceDepth == 1 {
				break consumer
			}

			braceDepth--

		case lexer.TokenTypeError:
			break consumer

		case lexer.TokenTypeEOF:
			break consumer
		}

		if startToken == nil {
			startToken = &currentToken
		}

		endToken = &currentToken
		p.consumeToken()
	}

	if startToken == nil {
		p.emitErrorf("missing caveat expression")
		return exprNode, false
	}

	caveatExpression := p.input[startToken.Position : int(endToken.Position)+len(endToken.Value)]
	exprNode.MustDecorate(dslshape.NodeCaveatExpressionPredicateExpression, caveatExpression)
	return exprNode, true
}

// consumeCaveatParameter attempts to consume a caveat parameter.
// ```(paramName paramtype)```
func (p *sourceParser) consumeCaveatParameter() (AstNode, bool) {
	paramNode := p.startNode(dslshape.NodeTypeCaveatParameter)
	defer p.mustFinishNode()

	name, ok := p.consumeIdentifier()
	if !ok {
		return paramNode, false
	}

	paramNode.MustDecorate(dslshape.NodeCaveatParameterPredicateName, name)
	paramNode.Connect(dslshape.NodeCaveatParameterPredicateType, p.consumeCaveatTypeReference())
	return paramNode, true
}

// consumeCaveatTypeReference attempts to consume a caveat type reference.
// ```typeName<childType>```
func (p *sourceParser) consumeCaveatTypeReference() AstNode {
	typeRefNode := p.startNode(dslshape.NodeTypeCaveatTypeReference)
	defer p.mustFinishNode()

	name, ok := p.consumeIdentifier()
	if !ok {
		return typeRefNode
	}

	typeRefNode.MustDecorate(dslshape.NodeCaveatTypeReferencePredicateType, name)

	// Check for child type(s).
	// <
	if _, ok := p.tryConsume(lexer.TokenTypeLessThan); !ok {
		return typeRefNode
	}

	for {
		childTypeRef := p.consumeCaveatTypeReference()
		typeRefNode.Connect(dslshape.NodeCaveatTypeReferencePredicateChildTypes, childTypeRef)
		if _, ok := p.tryConsume(lexer.TokenTypeComma); !ok {
			break
		}
	}

	// >
	p.consume(lexer.TokenTypeGreaterThan)
	return typeRefNode
}

// consumeUseFlag attempts to consume a use flag.
// ``` use flagname ```
func (p *sourceParser) consumeUseFlag(afterDefinition bool) AstNode {
	useNode := p.startNode(dslshape.NodeTypeUseFlag)
	defer p.mustFinishNode()

	// consume the `use`
	p.consumeIdentifier()

	var useFlag string
	if p.isToken(lexer.TokenTypeIdentifier) {
		useFlag, _ = p.consumeIdentifier()
	} else {
		useName, ok := p.consumeVariableKeyword()
		if !ok {
			return useNode
		}
		useFlag = useName
	}

	if _, ok := lexer.Flags[useFlag]; !ok {
		p.emitErrorf("Unknown use flag: `%s`. Options are: %s", useFlag, strings.Join(maps.Keys(lexer.Flags), ", "))
		return useNode
	}

	useNode.MustDecorate(dslshape.NodeUseFlagPredicateName, useFlag)

	// NOTE: we conduct this check in `consumeFlag` rather than at
	// the callsite to keep the callsite clean.
	// We also do the check after consumption to ensure that the parser continues
	// moving past the use expression.
	if afterDefinition {
		p.emitErrorf("`use` expressions must be declared before any definition")
		return useNode
	}

	return useNode
}

// consumeDefinition attempts to consume a single schema definition.
// ```definition somedef { ... }```
func (p *sourceParser) consumeDefinition() AstNode {
	defNode := p.startNode(dslshape.NodeTypeDefinition)
	defer p.mustFinishNode()

	// definition ...
	p.consumeKeyword("definition")
	definitionName, ok := p.consumeTypePath()
	if !ok {
		return defNode
	}

	defNode.MustDecorate(dslshape.NodeDefinitionPredicateName, definitionName)

	// {
	_, ok = p.consume(lexer.TokenTypeLeftBrace)
	if !ok {
		return defNode
	}

	// Relations and permissions.
	for {
		// }
		if _, ok := p.tryConsume(lexer.TokenTypeRightBrace); ok {
			break
		}

		// relation ...
		// permission ...
		switch {
		case p.isKeyword("relation"):
			defNode.Connect(dslshape.NodePredicateChild, p.consumeRelation())

		case p.isKeyword("permission"):
			defNode.Connect(dslshape.NodePredicateChild, p.consumePermission())
		}

		ok := p.consumeStatementTerminator()
		if !ok {
			break
		}
	}

	return defNode
}

// consumeRelation consumes a relation.
// ```relation foo: sometype```
func (p *sourceParser) consumeRelation() AstNode {
	relNode := p.startNode(dslshape.NodeTypeRelation)
	defer p.mustFinishNode()

	// relation ...
	p.consumeKeyword("relation")
	relationName, ok := p.consumeIdentifier()
	if !ok {
		return relNode
	}

	relNode.MustDecorate(dslshape.NodePredicateName, relationName)

	// :
	_, ok = p.consume(lexer.TokenTypeColon)
	if !ok {
		return relNode
	}

	// Relation allowed type(s).
	relNode.Connect(dslshape.NodeRelationPredicateAllowedTypes, p.consumeTypeReference())

	return relNode
}

// consumeTypeReference consumes a reference to a type or types of relations.
// ```sometype | anothertype | anothertype:* ```
func (p *sourceParser) consumeTypeReference() AstNode {
	refNode := p.startNode(dslshape.NodeTypeTypeReference)
	defer p.mustFinishNode()

	for {
		refNode.Connect(dslshape.NodeTypeReferencePredicateType, p.consumeSpecificTypeWithCaveat())
		if _, ok := p.tryConsume(lexer.TokenTypePipe); !ok {
			break
		}
	}

	return refNode
}

// tryConsumeWithCaveat tries to consume a caveat `with` expression.
func (p *sourceParser) tryConsumeWithCaveat() (AstNode, bool) {
	caveatNode := p.startNode(dslshape.NodeTypeCaveatReference)
	defer p.mustFinishNode()

	consumed, ok := p.consumeTypePath()
	if !ok {
		return caveatNode, true
	}

	caveatNode.MustDecorate(dslshape.NodeCaveatPredicateCaveat, consumed)
	return caveatNode, true
}

// consumeSpecificTypeWithCaveat consumes an identifier as a specific type reference, with optional caveat.
func (p *sourceParser) consumeSpecificTypeWithCaveat() AstNode {
	specificNode := p.consumeSpecificTypeWithoutFinish()
	defer p.mustFinishNode()

	// Check for a caveat and/or supported trait.
	if !p.isKeyword("with") {
		return specificNode
	}

	p.consumeKeyword("with")

	if !p.isKeyword("expiration") {
		caveatNode, ok := p.tryConsumeWithCaveat()
		if ok {
			specificNode.Connect(dslshape.NodeSpecificReferencePredicateCaveat, caveatNode)
		}

		if !p.tryConsumeKeyword("and") {
			return specificNode
		}
	}

	if p.isKeyword("expiration") {
		// Check for expiration trait.
		traitNode := p.consumeExpirationTrait()

		// Decorate with the expiration trait.
		specificNode.Connect(dslshape.NodeSpecificReferencePredicateTrait, traitNode)
	}

	return specificNode
}

// consumeExpirationTrait consumes an expiration trait.
func (p *sourceParser) consumeExpirationTrait() AstNode {
	expirationTraitNode := p.startNode(dslshape.NodeTypeTraitReference)
	p.consumeKeyword("expiration")

	expirationTraitNode.MustDecorate(dslshape.NodeTraitPredicateTrait, "expiration")
	defer p.mustFinishNode()

	return expirationTraitNode
}

// consumeSpecificTypeOpen consumes an identifier as a specific type reference.
func (p *sourceParser) consumeSpecificTypeWithoutFinish() AstNode {
	specificNode := p.startNode(dslshape.NodeTypeSpecificTypeReference)

	typeName, ok := p.consumeTypePath()
	if !ok {
		return specificNode
	}

	specificNode.MustDecorate(dslshape.NodeSpecificReferencePredicateType, typeName)

	// Check for a wildcard
	if _, ok := p.tryConsume(lexer.TokenTypeColon); ok {
		_, ok := p.consume(lexer.TokenTypeStar)
		if !ok {
			return specificNode
		}

		specificNode.MustDecorate(dslshape.NodeSpecificReferencePredicateWildcard, "true")
		return specificNode
	}

	// Check for a relation specified.
	if _, ok := p.tryConsume(lexer.TokenTypeHash); !ok {
		return specificNode
	}

	// Consume an identifier or an ellipsis.
	consumed, ok := p.consume(lexer.TokenTypeIdentifier, lexer.TokenTypeEllipsis)
	if !ok {
		return specificNode
	}

	specificNode.MustDecorate(dslshape.NodeSpecificReferencePredicateRelation, consumed.Value)
	return specificNode
}

func (p *sourceParser) consumeTypePath() (string, bool) {
	var segments []string

	for {
		segment, ok := p.consumeIdentifier()
		if !ok {
			return "", false
		}

		segments = append(segments, segment)

		_, ok = p.tryConsume(lexer.TokenTypeDiv)
		if !ok {
			break
		}
	}

	return strings.Join(segments, "/"), true
}

// consumePermission consumes a permission.
// ```permission foo = bar + baz```
func (p *sourceParser) consumePermission() AstNode {
	permNode := p.startNode(dslshape.NodeTypePermission)
	defer p.mustFinishNode()

	// permission ...
	p.consumeKeyword("permission")
	permissionName, ok := p.consumeIdentifier()
	if !ok {
		return permNode
	}

	permNode.MustDecorate(dslshape.NodePredicateName, permissionName)

	// =
	_, ok = p.consume(lexer.TokenTypeEquals)
	if !ok {
		return permNode
	}

	permNode.Connect(dslshape.NodePermissionPredicateComputeExpression, p.consumeComputeExpression())
	return permNode
}

// ComputeExpressionOperators defines the binary operators in precedence order.
var ComputeExpressionOperators = []binaryOpDefinition{
	{lexer.TokenTypeMinus, dslshape.NodeTypeExclusionExpression},
	{lexer.TokenTypeAnd, dslshape.NodeTypeIntersectExpression},
	{lexer.TokenTypePlus, dslshape.NodeTypeUnionExpression},
}

// consumeComputeExpression consumes an expression for computing a permission.
func (p *sourceParser) consumeComputeExpression() AstNode {
	// Compute expressions consist of a set of binary operators, so build a tree with proper
	// precedence.
	binaryParser := p.buildBinaryOperatorExpressionFnTree(ComputeExpressionOperators)
	found, ok := binaryParser()
	if !ok {
		return p.createErrorNodef("Expected compute expression for permission")
	}
	return found
}

// tryConsumeComputeExpression attempts to consume a nested compute expression.
func (p *sourceParser) tryConsumeComputeExpression(subTryExprFn tryParserFn, binaryTokenType lexer.TokenType, nodeType dslshape.NodeType) (AstNode, bool) {
	rightNodeBuilder := func(leftNode AstNode, operatorToken lexer.Lexeme) (AstNode, bool) {
		rightNode, ok := subTryExprFn()
		if !ok {
			return nil, false
		}

		// Create the expression node representing the binary expression.
		exprNode := p.createNode(nodeType)
		exprNode.Connect(dslshape.NodeExpressionPredicateLeftExpr, leftNode)
		exprNode.Connect(dslshape.NodeExpressionPredicateRightExpr, rightNode)
		return exprNode, true
	}
	return p.performLeftRecursiveParsing(subTryExprFn, rightNodeBuilder, nil, binaryTokenType)
}

// tryConsumeArrowExpression attempts to consume an arrow expression.
// ```foo->bar->baz->meh```
func (p *sourceParser) tryConsumeArrowExpression() (AstNode, bool) {
	rightNodeBuilder := func(leftNode AstNode, operatorToken lexer.Lexeme) (AstNode, bool) {
		// Check for an arrow function.
		if operatorToken.Kind == lexer.TokenTypePeriod {
			functionName, ok := p.consumeIdentifier()
			if !ok {
				return nil, false
			}

			// TODO(jschorr): Change to keywords in schema v2.
			if functionName != "any" && functionName != "all" {
				p.emitErrorf("Expected 'any' or 'all' for arrow function, found: %s", functionName)
				return nil, false
			}

			if _, ok := p.consume(lexer.TokenTypeLeftParen); !ok {
				return nil, false
			}

			rightNode, ok := p.tryConsumeIdentifierLiteral()
			if !ok {
				return nil, false
			}

			if _, ok := p.consume(lexer.TokenTypeRightParen); !ok {
				return nil, false
			}

			exprNode := p.createNode(dslshape.NodeTypeArrowExpression)
			exprNode.Connect(dslshape.NodeExpressionPredicateLeftExpr, leftNode)
			exprNode.Connect(dslshape.NodeExpressionPredicateRightExpr, rightNode)
			exprNode.MustDecorate(dslshape.NodeArrowExpressionFunctionName, functionName)
			return exprNode, true
		}

		rightNode, ok := p.tryConsumeIdentifierLiteral()
		if !ok {
			return nil, false
		}

		// Create the expression node representing the binary expression.
		exprNode := p.createNode(dslshape.NodeTypeArrowExpression)
		exprNode.Connect(dslshape.NodeExpressionPredicateLeftExpr, leftNode)
		exprNode.Connect(dslshape.NodeExpressionPredicateRightExpr, rightNode)
		return exprNode, true
	}
	return p.performLeftRecursiveParsing(p.tryConsumeIdentifierLiteral, rightNodeBuilder, nil, lexer.TokenTypeRightArrow, lexer.TokenTypePeriod)
}

// tryConsumeBaseExpression attempts to consume base compute expressions (identifiers, parenthesis).
// ```(foo + bar)```
// ```(foo)```
// ```foo```
// ```nil```
func (p *sourceParser) tryConsumeBaseExpression() (AstNode, bool) {
	switch {
	// Nested expression.
	case p.isToken(lexer.TokenTypeLeftParen):
		comments := p.currentToken.comments

		p.consume(lexer.TokenTypeLeftParen)
		exprNode := p.consumeComputeExpression()
		p.consume(lexer.TokenTypeRightParen)

		// Attach any comments found to the consumed expression.
		p.decorateComments(exprNode, comments)

		return exprNode, true

	// Nil expression.
	case p.isKeyword("nil"):
		return p.tryConsumeNilExpression()

	// Identifier.
	case p.isToken(lexer.TokenTypeIdentifier):
		return p.tryConsumeIdentifierLiteral()
	}

	return nil, false
}

// tryConsumeIdentifierLiteral attempts to consume an identifier as a literal
// expression.
//
// ```foo```
func (p *sourceParser) tryConsumeIdentifierLiteral() (AstNode, bool) {
	if !p.isToken(lexer.TokenTypeIdentifier) {
		return nil, false
	}

	identNode := p.startNode(dslshape.NodeTypeIdentifier)
	defer p.mustFinishNode()

	identifier, _ := p.consumeIdentifier()
	identNode.MustDecorate(dslshape.NodeIdentiferPredicateValue, identifier)
	return identNode, true
}

func (p *sourceParser) tryConsumeNilExpression() (AstNode, bool) {
	if !p.isKeyword("nil") {
		return nil, false
	}

	node := p.startNode(dslshape.NodeTypeNilExpression)
	p.consumeKeyword("nil")
	defer p.mustFinishNode()
	return node, true
}
