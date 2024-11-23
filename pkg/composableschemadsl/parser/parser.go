// parser package defines the parser for the Authzed Schema DSL.
package parser

import (
	"strings"

	"github.com/authzed/spicedb/pkg/composableschemadsl/dslshape"
	"github.com/authzed/spicedb/pkg/composableschemadsl/input"
	"github.com/authzed/spicedb/pkg/composableschemadsl/lexer"
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
		case p.isKeyword("definition"):
			rootNode.Connect(dslshape.NodePredicateChild, p.consumeDefinition())

		case p.isKeyword("caveat"):
			rootNode.Connect(dslshape.NodePredicateChild, p.consumeCaveat())

		case p.isKeyword("from"):
			rootNode.Connect(dslshape.NodePredicateChild, p.consumeImport())

		case p.isKeyword("test"):
			rootNode.Connect(dslshape.NodePredicateChild, p.consumeTest())

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

	exprNode, ok := p.consumeOpaqueBraceExpression()
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

// Consume from an opening brace to a closing brace, returning all tokens as a single string.
// Used for caveat CEL expressions and test JSON expressions.
// Keeps track of brace balance.
// Special Logic Note: Since CEL is its own language, we consume here until we have a matching
// close brace, and then pass ALL the found tokens to CEL's own parser to attach the expression
// here.
func (p *sourceParser) consumeOpaqueBraceExpression() (AstNode, bool) {
	exprNode := p.startNode(dslshape.NodeTypeOpaqueBraceExpression)
	defer p.mustFinishNode()

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
	exprNode.MustDecorate(dslshape.NodeOpaqueBraceExpressionPredicateExpression, caveatExpression)
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

	name, ok := p.consumeCaveatTypeIdentifier()
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

// "any" is both a keyword and a valid caveat type, so a caveat type identifier
// can either be a keyword or an identifier. This wraps around that.
func (p *sourceParser) consumeCaveatTypeIdentifier() (string, bool) {
	if ok := p.tryConsumeKeyword("any"); ok {
		return "any", true
	}

	identifier, ok := p.tryConsume(lexer.TokenTypeIdentifier)
	if !ok {
		p.emitErrorf("Expected keyword \"any\" or a valid identifier, found token %v", p.currentToken.Kind)
		return "", false
	}
	return identifier.Value, true
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
	if !p.isKeyword("with") {
		return nil, false
	}

	caveatNode := p.startNode(dslshape.NodeTypeCaveatReference)
	defer p.mustFinishNode()

	if ok := p.consumeKeyword("with"); !ok {
		return nil, ok
	}

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

	caveatNode, ok := p.tryConsumeWithCaveat()
	if ok {
		specificNode.Connect(dslshape.NodeSpecificReferencePredicateCaveat, caveatNode)
	}

	return specificNode
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
			functionName, ok := p.consumeKeywords("any", "all")
			if !ok {
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

// consumeIdentifierLiteral is similar to the above, but attempts and errors
// rather than checking the token type beforehand
func (p *sourceParser) consumeIdentifierLiteral() (AstNode, bool) {
	identNode := p.startNode(dslshape.NodeTypeIdentifier)
	defer p.mustFinishNode()

	identifier, ok := p.consumeIdentifier()
	if !ok {
		return identNode, false
	}
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

func (p *sourceParser) consumeImport() AstNode {
	importNode := p.startNode(dslshape.NodeTypeImport)
	defer p.mustFinishNode()

	// from ...
	// NOTE: error handling isn't necessary here because this function is only
	// invoked if the `from` keyword is found in the function above.
	p.consumeKeyword("from")

	// Consume alternating periods and identifiers
	for {
		if _, ok := p.consume(lexer.TokenTypePeriod); !ok {
			return importNode
		}

		segmentNode, ok := p.consumeIdentifierLiteral()
		// We connect the node so that the error information is retained, then break the loop
		// so that we aren't continuing to attempt to consume.
		importNode.Connect(dslshape.NodeImportPredicatePathSegment, segmentNode)
		if !ok {
			break
		}

		if !p.isToken(lexer.TokenTypePeriod) {
			// If we don't have a period as our next token, we move
			// to the next step of parsing.
			break
		}
	}

	if ok := p.consumeKeyword("import"); !ok {
		return importNode
	}

	// Consume alternating identifiers and commas until we reach the end of the import statement
	for {
		definitionNode, ok := p.consumeIdentifierLiteral()
		// We connect the node so that the error information is retained, then break the loop
		// so that we aren't continuing to attempt to consume.
		importNode.Connect(dslshape.NodeImportPredicateDefinitionName, definitionNode)
		if !ok {
			break
		}

		if _, ok := p.tryConsumeStatementTerminator(); ok {
			break
		}
		if _, ok := p.consume(lexer.TokenTypeComma); !ok {
			return importNode
		}
	}

	return importNode
}

func (p *sourceParser) consumeTest() AstNode {
	testNode := p.startNode(dslshape.NodeTypeImport)
	defer p.mustFinishNode()

	// These are how we enforce that there is at most one of
	// each of these three
	var consumedRelations, consumedAssertions, consumedExpected bool

	// test ...
	p.consumeKeyword("test")

	testName, ok := p.consumeIdentifier()
	if !ok {
		return testNode
	}
	testNode.MustDecorate(dslshape.NodeTestPredicateName, testName)

	// {
	_, ok = p.consume(lexer.TokenTypeLeftBrace)
	if !ok {
		return testNode
	}

	// top-levels for test
	for {
		// }
		if _, ok := p.tryConsume(lexer.TokenTypeRightBrace); ok {
			break
		}
		// relations ...
		// assertions ...
		// expected ...
		switch {
		case p.isKeyword("relations"):
			if consumedRelations {
				p.emitErrorf("%s", "at most one relations block is permitted")
				return testNode
			}
			testNode.Connect(dslshape.NodePredicateChild, p.consumeTestRelations())
			consumedRelations = true
		case p.isKeyword("assertions"):
			if consumedAssertions {
				p.emitErrorf("%s", "at most one assertions block is permitted")
				return testNode
			}
			testNode.Connect(dslshape.NodePredicateChild, p.consumeTestAssertions())
			consumedAssertions = true
		case p.isKeyword("expected"):
			if consumedExpected {
				p.emitErrorf("%s", "at most one expected block is permitted")
				return testNode
			}
			testNode.Connect(dslshape.NodePredicateChild, p.consumeTestExpected())
			consumedExpected = true
		}

		ok := p.consumeStatementTerminator()
		if !ok {
			break
		}
	}

	return testNode
}

func (p *sourceParser) consumeTestRelations() AstNode {
	relationsNode := p.startNode(dslshape.NodeTypeImport)
	defer p.mustFinishNode()

	// relations ...
	p.consumeKeyword("relations")

	// {
	if _, ok := p.consume(lexer.TokenTypeLeftBrace); !ok {
		return relationsNode
	}

	for {
		// }
		if _, ok := p.tryConsume(lexer.TokenTypeRightBrace); ok {
			break
		}

		relationsNode.Connect(dslshape.NodePredicateChild, p.consumeTestRelation())

		ok := p.consumeStatementTerminator()
		if !ok {
			break
		}
	}

	return relationsNode
}

func (p *sourceParser) consumeTestRelation() AstNode {
	relationNode := p.startNode(dslshape.NodeTypeImport)
	defer p.mustFinishNode()

	// A relation looks like:
	// object:foo relation subject:bar
	// object consumption
	objectNode, ok := p.consumeTestObject()
	if !ok {
		return relationNode
	}
	relationNode.Connect(dslshape.NodeTestRelationPredicateObject, objectNode)

	// relation consumption
	relation, ok := p.consumeIdentifier()
	if !ok {
		return relationNode
	}
	relationNode.MustDecorate(dslshape.NodeTestRelationPredicateRelation, relation)

	// subject consumption
	subjectNode, ok := p.consumeTestObject()
	if !ok {
		return relationNode
	}
	relationNode.Connect(dslshape.NodeTestRelationPredicateSubject, subjectNode)

	// optional caveat consumption
	if p.tryConsumeKeyword("with") {
		caveatName, ok := p.consumeIdentifier()
		if !ok {
			return relationNode
		}
		relationNode.MustDecorate(dslshape.NodeTestRelationPredicateCaveatName, caveatName)

		// optional caveat context
		if _, ok := p.tryConsume(lexer.TokenTypeLeftBrace); ok {
			caveatContextNode, ok := p.consumeOpaqueBraceExpression()
			if !ok {
				return relationNode
			}
			relationNode.Connect(dslshape.NodeTestRelationPredicateCaveatContext, caveatContextNode)
		}
	}

	return relationNode
}

// Consumes an objectType:objectId pair and returns a test object node with
// object type and ID
func (p *sourceParser) consumeTestObject() (AstNode, bool) {
	objectNode := p.startNode(dslshape.NodeTypeTestObject)
	defer p.mustFinishNode()

	objectType, ok := p.consumeIdentifier()
	if !ok {
		return objectNode, false
	}
	objectNode.MustDecorate(dslshape.NodeTestObjectPredicateObjectType, objectType)

	_, ok = p.consume(lexer.TokenTypeColon)
	if !ok {
		return objectNode, false
	}

	objectID, ok := p.consumeIdentifier()
	if !ok {
		return objectNode, false
	}
	objectNode.MustDecorate(dslshape.NodeTestObjectPredicateObjectID, objectID)
	return objectNode, true
}

func (p *sourceParser) consumeTestAssertions() AstNode {
	assertionsNode := p.startNode(dslshape.NodeTypeImport)
	defer p.mustFinishNode()

	// relations ...
	p.consumeKeyword("relations")

	// {
	if _, ok := p.consume(lexer.TokenTypeLeftBrace); !ok {
		return assertionsNode
	}

	for {
		// }
		if _, ok := p.tryConsume(lexer.TokenTypeRightBrace); ok {
			break
		}

		assertionsNode.Connect(dslshape.NodePredicateChild, p.consumeTestAssertion())

		ok := p.consumeStatementTerminator()
		if !ok {
			break
		}
	}

	return assertionsNode
}

func (p *sourceParser) consumeTestAssertion() AstNode {
	assertionNode := p.startNode(dslshape.NodeTypeImport)
	defer p.mustFinishNode()

	return assertionNode
}

func (p *sourceParser) consumeTestExpected() AstNode {
	expectedNode := p.startNode(dslshape.NodeTypeImport)
	defer p.mustFinishNode()

	return expectedNode
}
