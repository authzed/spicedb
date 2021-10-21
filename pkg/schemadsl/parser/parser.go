// parser package defines the parser for the Authzed Schema DSL.
package parser

import (
	"fmt"

	"github.com/authzed/spicedb/pkg/schemadsl/dslshape"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
	"github.com/authzed/spicedb/pkg/schemadsl/lexer"
)

// Parse parses the given Schema DSL source into a parse tree.
func Parse(builder NodeBuilder, source input.InputSource, input string) AstNode {
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
	defer p.finishNode()

	// Start at the first token.
	p.consumeToken()

	if p.currentToken.Kind == lexer.TokenTypeError {
		p.emitError("%s", p.currentToken.Value)
		return rootNode
	}

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

		// The top level of the DSL is a set of definitions:
		// definition foobar { ... }

		switch {
		case p.isKeyword("definition"):
			rootNode.Connect(dslshape.NodePredicateChild, p.consumeDefinition())

		default:
			p.emitError("Unexpected token at root level: %v", p.currentToken.Kind)
			break Loop
		}
	}

	return rootNode
}

// consumeDefinition attempts to consume a single schema definition.
// ```definition somedef { ... }````
func (p *sourceParser) consumeDefinition() AstNode {
	defNode := p.startNode(dslshape.NodeTypeDefinition)
	defer p.finishNode()

	// definition ...
	p.consumeKeyword("definition")
	definitionName, ok := p.consumeTypePath()
	if !ok {
		return defNode
	}

	defNode.Decorate(dslshape.NodeDefinitionPredicateName, definitionName)

	// {
	_, ok = p.consume(lexer.TokenTypeLeftBrace)
	if !ok {
		return defNode
	}

	// Relations, permissions, and decorators
	for {
		// }
		if _, ok := p.tryConsume(lexer.TokenTypeRightBrace); ok {
			break
		}

		// @decorator(...)
		decoratorNode, hasDecorator := p.tryConsumeDecorator()
		if hasDecorator {
			if ok := p.consumeStatementTerminator(); !ok {
				break
			}
		}
		decorate := func(node AstNode) {
			if !hasDecorator {
				return
			}
			node.Connect(dslshape.NodePredicateDecorator, decoratorNode)
		}

		// relation ...
		// permission ...
		switch {
		case p.isKeyword("relation"):
			relNode := p.consumeRelation()
			defNode.Connect(dslshape.NodePredicateChild, relNode)
			decorate(relNode)
		case p.isKeyword("permission"):
			permNode := p.consumePermission()
			defNode.Connect(dslshape.NodePredicateChild, permNode)
			decorate(permNode)
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
	defer p.finishNode()

	// relation ...
	p.consumeKeyword("relation")
	relationName, ok := p.consumeIdentifier()
	if !ok {
		return relNode
	}

	relNode.Decorate(dslshape.NodePredicateName, relationName)

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
// ```somerel | anotherrel```
func (p *sourceParser) consumeTypeReference() AstNode {
	refNode := p.startNode(dslshape.NodeTypeTypeReference)
	defer p.finishNode()

	for {
		refNode.Connect(dslshape.NodeTypeReferencePredicateType, p.consumeSpecificType())
		if _, ok := p.tryConsume(lexer.TokenTypePipe); !ok {
			break
		}
	}

	return refNode
}

// consumeSpecificType consumes an identifier as a specific type reference.
func (p *sourceParser) consumeSpecificType() AstNode {
	specificNode := p.startNode(dslshape.NodeTypeSpecificTypeReference)
	defer p.finishNode()

	typeName, ok := p.consumeTypePath()
	if !ok {
		return specificNode
	}

	specificNode.Decorate(dslshape.NodeSpecificReferencePredicateType, typeName)

	// Check for a relation specified.
	if _, ok := p.tryConsume(lexer.TokenTypeHash); !ok {
		return specificNode
	}

	// Consume an identifier or an ellipsis.
	consumed, ok := p.consume(lexer.TokenTypeIdentifier, lexer.TokenTypeEllipsis)
	if !ok {
		return specificNode
	}

	specificNode.Decorate(dslshape.NodeSpecificReferencePredicateRelation, consumed.Value)

	return specificNode
}

func (p *sourceParser) consumeTypePath() (string, bool) {
	typeNameOrNamespace, ok := p.consumeIdentifier()
	if !ok {
		return "", false
	}

	_, ok = p.tryConsume(lexer.TokenTypeDiv)
	if !ok {
		return typeNameOrNamespace, true
	}

	typeName, ok := p.consumeIdentifier()
	if !ok {
		return "", false
	}

	return fmt.Sprintf("%s/%s", typeNameOrNamespace, typeName), true
}

// consumePermission consumes a permission.
// ```permission foo = bar + baz```
func (p *sourceParser) consumePermission() AstNode {
	permNode := p.startNode(dslshape.NodeTypePermission)
	defer p.finishNode()

	// permission ...
	p.consumeKeyword("permission")
	permissionName, ok := p.consumeIdentifier()
	if !ok {
		return permNode
	}

	permNode.Decorate(dslshape.NodePredicateName, permissionName)

	// =
	_, ok = p.consume(lexer.TokenTypeEquals)
	if !ok {
		return permNode
	}

	permNode.Connect(dslshape.NodePermissionPredicateComputeExpression, p.consumeComputeExpression())
	return permNode
}

// consumeDecorator consumes a decorator
// ```@decoratorName(opt1,(opt2))```
func (p *sourceParser) tryConsumeDecorator() (AstNode, bool) {
	decNode := p.startNode(dslshape.NodeTypeDecorator)
	defer p.finishNode()

	if _, ok := p.tryConsume(lexer.TokenTypeAt); !ok {
		return nil, false
	}
	decoratorName, ok := p.consumeIdentifier()
	if !ok {
		return nil, false
	}
	decNode.Decorate(dslshape.NodeDecoratorName, decoratorName)
	if p.isToken(lexer.TokenTypeLeftParen) {
		decNode.Connect(dslshape.NodeDecoratorOptions, p.consumeDecoratorOptions())
	}
	return decNode, true
}

// consumeDecoratorOptions consumes a decorator's options (or sub-options)
// ```(opt1,(opt2, opt3))```
func (p *sourceParser) consumeDecoratorOptions() AstNode {
	optNode := p.startNode(dslshape.NodeTypeDecoratorOptions)
	defer p.finishNode()

	p.consume(lexer.TokenTypeLeftParen)

OPTS:
	for {
		switch {
		case p.isToken(lexer.TokenTypeLeftParen):
			optNode.Connect(dslshape.NodeDecoratorOptions, p.consumeDecoratorOptions())
		case p.isToken(lexer.TokenTypeIdentifier):
			optValue, ok := p.consumeIdentifier()
			if !ok {
				return p.createErrorNode("Expected options for decorator")
			}
			optValueNode := p.startNode(dslshape.NodeTypeDecoratorOptions)
			optValueNode.Decorate(dslshape.NodeDecoratorOptionValue, optValue)
			p.finishNode()
			optNode.Connect(dslshape.NodeDecoratorOptions, optValueNode)
		case p.isToken(lexer.TokenTypeComma):
			p.consume(lexer.TokenTypeComma)
		case p.isToken(lexer.TokenTypeRightParen):
			break OPTS
		}
	}

	p.consume(lexer.TokenTypeRightParen)

	return optNode
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
		return p.createErrorNode("Expected compute expression for permission")
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
		rightNode, ok := p.tryConsumeBaseExpression()
		if !ok {
			return nil, false
		}

		// Create the expression node representing the binary expression.
		exprNode := p.createNode(dslshape.NodeTypeArrowExpression)
		exprNode.Connect(dslshape.NodeExpressionPredicateLeftExpr, leftNode)
		exprNode.Connect(dslshape.NodeExpressionPredicateRightExpr, rightNode)
		return exprNode, true
	}
	return p.performLeftRecursiveParsing(p.tryConsumeIdentifierLiteral, rightNodeBuilder, nil, lexer.TokenTypeRightArrow)
}

// tryConsumeBaseExpression attempts to consume base compute expressions (identifiers, parenthesis).
// ```(foo + bar)```
// ```(foo)```
// ```foo```
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

		// Identifier.
	case p.isToken(lexer.TokenTypeIdentifier):
		return p.tryConsumeIdentifierLiteral()

	}

	return nil, false
}

// tryConsumeIdentifierLiteral attempts to consume an identifer as a literal expression.
/// ```foo```
func (p *sourceParser) tryConsumeIdentifierLiteral() (AstNode, bool) {
	if !p.isToken(lexer.TokenTypeIdentifier) {
		return nil, false
	}

	identNode := p.startNode(dslshape.NodeTypeIdentifier)
	defer p.finishNode()

	identifier, _ := p.consumeIdentifier()
	identNode.Decorate(dslshape.NodeIdentiferPredicateValue, identifier)
	return identNode, true
}
