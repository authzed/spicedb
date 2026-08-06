package parser

import (
	"github.com/authzed/spicedb/pkg/schemadsl/dslshape"
	"github.com/authzed/spicedb/pkg/schemadsl/lexer"
)

// Syntactic kinds recorded on a decorator parameter value. These mirror
// decorators.ValueKind; the compiler coerces to the declared parameter type.
const (
	decoratorValueKindString     = "string"
	decoratorValueKindIdentifier = "identifier"
)

// tryConsumeDecorators consumes a run of decorators, if any are present, returning the
// nodes for the caller to attach to whatever declaration follows, along with every
// comment found across the whole run (including any comment preceding the first
// decorator, and any comment between two stacked decorators), in source order.
//
// ```@first @second``` and ```@first\n@second``` are equivalent: a newline after a
// decorator produces a synthetic semicolon, which is absorbed here.
func (p *sourceParser) tryConsumeDecorators() ([]AstNode, []string) {
	var decorators []AstNode
	var comments []string
	for p.isToken(lexer.TokenTypeAt) {
		comments = append(comments, p.currentToken.comments...)
		decorators = append(decorators, p.consumeDecorator())
		p.tryConsumeStatementTerminator()
	}
	return decorators, comments
}

// consumeDecorator consumes a single decorator.
// ```@somename``` or ```@somename(param: value, other: "value")```
func (p *sourceParser) consumeDecorator() AstNode {
	decoratorNode := p.startNodeWithoutComments(dslshape.NodeTypeDecorator)
	defer p.mustFinishNode()

	// @
	if _, ok := p.consume(lexer.TokenTypeAt); !ok {
		return decoratorNode
	}

	name, ok := p.consumeIdentifier()
	if !ok {
		return decoratorNode
	}

	decoratorNode.MustDecorate(dslshape.NodeDecoratorPredicateName, name)

	// Parameters are optional.
	// (
	if _, ok := p.tryConsume(lexer.TokenTypeLeftParen); !ok {
		return decoratorNode
	}

	if p.isToken(lexer.TokenTypeRightParen) {
		p.emitErrorf("Decorator `@%s` has an empty parameter list; write `@%s` instead", name, name)
		// Consume the stray `)` so the caller can recover and continue parsing the
		// declaration that follows, rather than getting stuck on it.
		p.tryConsume(lexer.TokenTypeRightParen)
		return decoratorNode
	}

	for {
		paramNode, ok := p.consumeDecoratorParameter()

		// Connect paramNode unconditionally: even on failure, it carries the specific
		// error node for whatever went wrong (e.g. a missing `:`), and that error is
		// only reachable via root.FindAll(NodeTypeError) once it is linked into the
		// tree. Returning before this Connect would silently drop the real diagnostic.
		decoratorNode.Connect(dslshape.NodeDecoratorPredicateParameters, paramNode)

		if !ok {
			// Recover by skipping ahead to the decorator's closing `)`, the same way
			// the empty-parameter-list case above does. Without this, the token
			// cursor is left wherever the malformed parameter choked (e.g. right
			// before the `)`, or on a stray value token), the caller's switch on
			// whatever construct is supposed to follow the decorator fails to match
			// anything, and the decorator - along with the real error node just
			// connected above - is discarded wholesale via the generic "expected
			// definition/caveat/partial after decorator" path. That is the same
			// orphaning mechanism fixed for decorators in commit a694626, just
			// reached from inside the parameter list instead of from an empty one.
			p.skipToDecoratorParametersClose()
			return decoratorNode
		}

		if _, ok := p.tryConsume(lexer.TokenTypeComma); !ok {
			break
		}
	}

	// )
	p.consume(lexer.TokenTypeRightParen)
	return decoratorNode
}

// skipToDecoratorParametersClose advances past whatever malformed token(s) remain in a
// decorator's parameter list after an unrecoverable parse error, consuming up to and
// including the list's own closing `)` if one is found. This lets parsing of whatever
// follows the decorator resume normally instead of leaving the cursor stuck
// mid-decorator (see consumeDecorator's call site for why that matters).
//
// Depth is tracked (mirroring the brace-depth tracking in consumeCaveatExpression),
// starting at 1 for the decorator's own already-consumed opening `(`, so that a stray
// `(` inside a malformed value (e.g. `@name(p: (1))`) does not make the *inner* `)`
// look like the decorator's own close. Stopping at that inner `)` would leave the
// decorator's real `)` dangling as the next token and reintroduce the exact orphaning
// this function exists to avoid (the outer switch would see a stray `)` where a
// declaration should be, and discard the decorator - and its real error - all over
// again).
//
// Termination is unconditional, not dependent on well-formed input, mirroring
// consumeCaveatExpression's own hard stops: TokenTypeError and TokenTypeEOF always
// stop the scan. This matters because after a lex error the lexer's goroutine exits
// and closes its token channel; every subsequent read from a closed channel returns
// the zero Lexeme, whose Kind is TokenTypeError (iota 0) - so failing to treat that
// as a hard stop would spin forever re-observing the same synthetic token. The
// statement-terminator and brace kinds are additional hard stops because none of them
// can legitimately appear inside a decorator's parameter list, so treating them as
// "just another token to skip" could run the scan into unrelated, distant source
// instead of admitting defeat.
//
// Progress argument: each loop iteration either returns (via the labeled break) or
// falls through to an unconditional p.consumeToken(), which always advances to the
// next token in the (finite, for any input) token stream. Combined with the
// unconditional EOF/error stops above, the loop is bounded by the number of tokens
// remaining before EOF or a lex error and cannot run forever on any input.
func (p *sourceParser) skipToDecoratorParametersClose() {
	depth := 1
skipLoop:
	for {
		switch p.currentToken.Kind {
		case lexer.TokenTypeLeftParen:
			depth++

		case lexer.TokenTypeRightParen:
			depth--
			if depth == 0 {
				p.consumeToken()
				break skipLoop
			}

		case lexer.TokenTypeError,
			lexer.TokenTypeEOF,
			lexer.TokenTypeSyntheticSemicolon,
			lexer.TokenTypeSemicolon,
			lexer.TokenTypeLeftBrace,
			lexer.TokenTypeRightBrace:
			break skipLoop
		}

		p.consumeToken()
	}
}

// consumeDecoratorParameter consumes a single named decorator parameter.
// ```paramname: value```
func (p *sourceParser) consumeDecoratorParameter() (AstNode, bool) {
	paramNode := p.startNode(dslshape.NodeTypeDecoratorParameter)
	defer p.mustFinishNode()

	name, ok := p.consumeIdentifier()
	if !ok {
		return paramNode, false
	}

	paramNode.MustDecorate(dslshape.NodeDecoratorParameterPredicateName, name)

	// :
	if _, ok := p.consume(lexer.TokenTypeColon); !ok {
		return paramNode, false
	}

	kind, value, ok := p.consumeDecoratorValue()
	if !ok {
		return paramNode, false
	}

	paramNode.MustDecorate(dslshape.NodeDecoratorParameterPredicateKind, kind)
	paramNode.MustDecorate(dslshape.NodeDecoratorParameterPredicateValue, value)
	return paramNode, true
}

// consumeDecoratorValue consumes a decorator parameter value, returning its syntactic
// kind and its text.
//
// NOTE: numbers arrive as identifiers, because the lexer never emits TokenTypeNumber.
// The compiler coerces based on the declared parameter type.
func (p *sourceParser) consumeDecoratorValue() (string, string, bool) {
	if value, ok := p.tryConsumeStringLiteral(); ok {
		return decoratorValueKindString, value, true
	}

	negated := false
	if _, ok := p.tryConsume(lexer.TokenTypeMinus); ok {
		negated = true
	}

	token, ok := p.tryConsume(lexer.TokenTypeIdentifier)
	if !ok {
		p.emitErrorf("Expected a decorator parameter value, found token %v", p.currentToken.Kind)
		return "", "", false
	}

	if negated {
		return decoratorValueKindIdentifier, "-" + token.Value, true
	}

	return decoratorValueKindIdentifier, token.Value, true
}

// attachDecorators connects the given decorators to the decorated node and replays the
// comments captured across the whole decorator run onto it, so that a doc comment
// written above (or between) decorators documents the declaration rather than the
// decorator(s).
//
// If there were no decorators, this is a no-op: the declaration's own startNode call
// already claimed its comments normally, and replaying here would double-attach them.
func (p *sourceParser) attachDecorators(node AstNode, decorators []AstNode, comments []string) {
	if len(decorators) == 0 {
		return
	}

	for _, decorator := range decorators {
		node.Connect(dslshape.NodePredicateDecorator, decorator)
	}

	p.decorateComments(node, comments)
}
