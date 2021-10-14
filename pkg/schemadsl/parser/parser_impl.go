package parser

import (
	"fmt"

	"github.com/authzed/spicedb/pkg/schemadsl/dslshape"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
	"github.com/authzed/spicedb/pkg/schemadsl/lexer"
)

// AstNode defines an interface for working with nodes created by this parser.
type AstNode interface {
	// Connect connects this AstNode to another AstNode with the given predicate,
	// and returns the same AstNode.
	Connect(predicate string, other AstNode) AstNode

	// Decorate decorates this AstNode with the given property and string value,
	// and returns the same AstNode.
	Decorate(property string, value string) AstNode

	// Decorate decorates this AstNode with the given property and int value,
	// and returns the same AstNode.
	DecorateWithInt(property string, value int) AstNode
}

// NodeBuilder is a function for building AST nodes.
type NodeBuilder func(source input.InputSource, kind dslshape.NodeType) AstNode

// tryParserFn is a function that attempts to build an AST node.
type tryParserFn func() (AstNode, bool)

// lookaheadParserFn is a function that performs lookahead.
type lookaheadParserFn func(currentToken lexer.Lexeme) bool

// rightNodeConstructor is a function which takes in a left expr node and the
// token consumed for a left-recursive operator, and returns a newly constructed
// operator expression if a right expression could be found.
type rightNodeConstructor func(AstNode, lexer.Lexeme) (AstNode, bool)

// commentedLexeme is a lexer.Lexeme with comments attached.
type commentedLexeme struct {
	lexer.Lexeme
	comments []string
}

// sourceParser holds the state of the parser.
type sourceParser struct {
	source        input.InputSource    // the name of the input; used only for error reports
	lex           *lexer.PeekableLexer // a reference to the lexer used for tokenization
	builder       NodeBuilder          // the builder function for creating AstNode instances
	nodes         *nodeStack           // the stack of the current nodes
	currentToken  commentedLexeme      // the current token
	previousToken commentedLexeme      // the previous token
}

// buildParser returns a new sourceParser instance.
func buildParser(lx *lexer.Lexer, builder NodeBuilder, source input.InputSource, input string) *sourceParser {
	l := lexer.NewPeekableLexer(lx)
	return &sourceParser{
		source:        source,
		lex:           l,
		builder:       builder,
		nodes:         &nodeStack{},
		currentToken:  commentedLexeme{lexer.Lexeme{Kind: lexer.TokenTypeEOF}, make([]string, 0)},
		previousToken: commentedLexeme{lexer.Lexeme{Kind: lexer.TokenTypeEOF}, make([]string, 0)},
	}
}

func (p *sourceParser) close() {
	p.lex.Close()
}

// createNode creates a new AstNode and returns it.
func (p *sourceParser) createNode(kind dslshape.NodeType) AstNode {
	return p.builder(p.source, kind)
}

// createErrorNode creates a new error node and returns it.
func (p *sourceParser) createErrorNode(format string, args ...interface{}) AstNode {
	message := fmt.Sprintf(format, args...)
	node := p.startNode(dslshape.NodeTypeError).Decorate(dslshape.NodePredicateErrorMessage, message)
	p.finishNode()
	return node
}

// startNode creates a new node of the given type, decorates it with the current token's
// position as its start position, and pushes it onto the nodes stack.
func (p *sourceParser) startNode(kind dslshape.NodeType) AstNode {
	node := p.createNode(kind)
	p.decorateStartRuneAndComments(node, p.currentToken)
	p.nodes.push(node)
	return node
}

// decorateStartRuneAndComments decorates the given node with the location of the given token as its
// starting rune, as well as any comments attached to the token.
func (p *sourceParser) decorateStartRuneAndComments(node AstNode, token commentedLexeme) {
	node.Decorate(dslshape.NodePredicateSource, string(p.source))
	node.DecorateWithInt(dslshape.NodePredicateStartRune, int(token.Position))
	p.decorateComments(node, token.comments)
}

// decorateComments decorates the given node with the specified comments.
func (p *sourceParser) decorateComments(node AstNode, comments []string) {
	for _, comment := range comments {
		commentNode := p.createNode(dslshape.NodeTypeComment)
		commentNode.Decorate(dslshape.NodeCommentPredicateValue, comment)
		node.Connect(dslshape.NodePredicateChild, commentNode)
	}
}

// decorateEndRune decorates the given node with the location of the given token as its
// ending rune.
func (p *sourceParser) decorateEndRune(node AstNode, token commentedLexeme) {
	position := int(token.Position) + len(token.Value) - 1
	node.DecorateWithInt(dslshape.NodePredicateEndRune, position)
}

// currentNode returns the node at the top of the stack.
func (p *sourceParser) currentNode() AstNode {
	return p.nodes.topValue()
}

// finishNode pops the current node from the top of the stack and decorates it with
// the current token's end position as its end position.
func (p *sourceParser) finishNode() {
	if p.currentNode() == nil {
		panic(fmt.Sprintf("No current node on stack. Token: %s", p.currentToken.Value))
	}

	p.decorateEndRune(p.currentNode(), p.previousToken)
	p.nodes.pop()
}

// consumeToken advances the lexer forward, returning the next token.
func (p *sourceParser) consumeToken() commentedLexeme {
	comments := make([]string, 0)

	for {
		token := p.lex.NextToken()

		if token.Kind == lexer.TokenTypeSinglelineComment || token.Kind == lexer.TokenTypeMultilineComment {
			comments = append(comments, token.Value)
		}

		if _, ok := ignoredTokenTypes[token.Kind]; !ok {
			p.previousToken = p.currentToken
			p.currentToken = commentedLexeme{token, comments}
			return p.currentToken
		}
	}
}

// isToken returns true if the current token matches one of the types given.
func (p *sourceParser) isToken(types ...lexer.TokenType) bool {
	for _, kind := range types {
		if p.currentToken.Kind == kind {
			return true
		}
	}

	return false
}

// isKeyword returns true if the current token is a keyword matching that given.
func (p *sourceParser) isKeyword(keyword string) bool {
	return p.isToken(lexer.TokenTypeKeyword) && p.currentToken.Value == keyword
}

// emitError creates a new error node and attachs it as a child of the current
// node.
func (p *sourceParser) emitError(format string, args ...interface{}) {
	errorNode := p.createErrorNode(format, args...)
	p.currentNode().Connect(dslshape.NodePredicateChild, errorNode)
}

// consumeKeyword consumes an expected keyword token or adds an error node.
func (p *sourceParser) consumeKeyword(keyword string) bool {
	if !p.tryConsumeKeyword(keyword) {
		p.emitError("Expected keyword %s, found token %v", keyword, p.currentToken.Kind)
		return false
	}
	return true
}

// tryConsumeKeyword attempts to consume an expected keyword token.
func (p *sourceParser) tryConsumeKeyword(keyword string) bool {
	if !p.isKeyword(keyword) {
		return false
	}

	p.consumeToken()
	return true
}

// cosumeIdentifier consumes an expected identifier token or adds an error node.
func (p *sourceParser) consumeIdentifier() (string, bool) {
	token, ok := p.tryConsume(lexer.TokenTypeIdentifier)
	if !ok {
		p.emitError("Expected identifier, found token %v", p.currentToken.Kind)
		return "", false
	}
	return token.Value, true
}

// consume performs consumption of the next token if it matches any of the given
// types and returns it. If no matching type is found, adds an error node.
func (p *sourceParser) consume(types ...lexer.TokenType) (lexer.Lexeme, bool) {
	token, ok := p.tryConsume(types...)
	if !ok {
		p.emitError("Expected one of: %v, found: %v", types, p.currentToken.Kind)
	}
	return token, ok
}

// tryConsume performs consumption of the next token if it matches any of the given
// types and returns it.
func (p *sourceParser) tryConsume(types ...lexer.TokenType) (lexer.Lexeme, bool) {
	token, found := p.tryConsumeWithComments(types...)
	return token.Lexeme, found
}

// tryConsume performs consumption of the next token if it matches any of the given
// types and returns it.
func (p *sourceParser) tryConsumeWithComments(types ...lexer.TokenType) (commentedLexeme, bool) {
	if p.isToken(types...) {
		token := p.currentToken
		p.consumeToken()
		return token, true
	}

	return commentedLexeme{lexer.Lexeme{
		Kind: lexer.TokenTypeError,
	}, make([]string, 0)}, false
}

// performLeftRecursiveParsing performs left-recursive parsing of a set of operators. This method
// first performs the parsing via the subTryExprFn and then checks for one of the left-recursive
// operator token types found. If none found, the left expression is returned. Otherwise, the
// rightNodeBuilder is called to attempt to construct an operator expression. This method also
// properly handles decoration of the nodes with their proper start and end run locations and
// comments.
func (p *sourceParser) performLeftRecursiveParsing(subTryExprFn tryParserFn, rightNodeBuilder rightNodeConstructor, rightTokenTester lookaheadParserFn, operatorTokens ...lexer.TokenType) (AstNode, bool) {
	var currentLeftToken commentedLexeme
	currentLeftToken = p.currentToken

	// Consume the left side of the expression.
	leftNode, ok := subTryExprFn()
	if !ok {
		return nil, false
	}

	// Check for an operator token. If none found, then we've found just the left side of the
	// expression and so we return that node.
	if !p.isToken(operatorTokens...) {
		return leftNode, true
	}

	// Keep consuming pairs of operators and child expressions until such
	// time as no more can be consumed. We use this loop+custom build rather than recursion
	// because these operators are *left* recursive, not right.
	var currentLeftNode AstNode
	currentLeftNode = leftNode

	for {
		// Check for an operator.
		if !p.isToken(operatorTokens...) {
			break
		}

		// If a lookahead function is defined, check the lookahead for the matched token.
		if rightTokenTester != nil && !rightTokenTester(p.currentToken.Lexeme) {
			break
		}

		// Consume the operator.
		operatorToken, ok := p.tryConsumeWithComments(operatorTokens...)
		if !ok {
			break
		}

		// Consume the right hand expression and build an expression node (if applicable).
		exprNode, ok := rightNodeBuilder(currentLeftNode, operatorToken.Lexeme)
		if !ok {
			p.emitError("Expected right hand expression, found: %v", p.currentToken.Kind)
			return currentLeftNode, true
		}

		p.decorateStartRuneAndComments(exprNode, currentLeftToken)
		p.decorateEndRune(exprNode, p.previousToken)

		currentLeftNode = exprNode
		currentLeftToken = operatorToken
	}

	return currentLeftNode, true
}

// tryConsumeStatementTerminator tries to consume a statement terminator.
func (p *sourceParser) tryConsumeStatementTerminator() (lexer.Lexeme, bool) {
	return p.tryConsume(lexer.TokenTypeSyntheticSemicolon, lexer.TokenTypeSemicolon, lexer.TokenTypeEOF)
}

// consumeStatementTerminator consume a statement terminator.
func (p *sourceParser) consumeStatementTerminator() bool {
	_, ok := p.tryConsumeStatementTerminator()
	if ok {
		return true
	}

	p.emitError("Expected end of statement or definition, found: %s", p.currentToken.Kind)
	return false
}

// binaryOpDefinition represents information a binary operator token and its associated node type.
type binaryOpDefinition struct {
	// The token representing the binary expression's operator.
	BinaryOperatorToken lexer.TokenType

	// The type of node to create for this expression.
	BinaryExpressionNodeType dslshape.NodeType
}

// buildBinaryOperatorExpressionFnTree builds a tree of functions to try to consume a set of binary
// operator expressions.
func (p *sourceParser) buildBinaryOperatorExpressionFnTree(ops []binaryOpDefinition) tryParserFn {
	// Start with a base expression function.
	var currentParseFn tryParserFn
	currentParseFn = func() (AstNode, bool) {
		arrowExpr, ok := p.tryConsumeArrowExpression()
		if !ok {
			return p.tryConsumeBaseExpression()
		}

		return arrowExpr, true
	}

	for i := range ops {
		// Note: We have to reverse this to ensure we have proper precedence.
		currentParseFn = func(operatorInfo binaryOpDefinition, currentFn tryParserFn) tryParserFn {
			return (func() (AstNode, bool) {
				return p.tryConsumeComputeExpression(currentFn, operatorInfo.BinaryOperatorToken, operatorInfo.BinaryExpressionNodeType)
			})
		}(ops[len(ops)-i-1], currentParseFn)
	}

	return currentParseFn
}
