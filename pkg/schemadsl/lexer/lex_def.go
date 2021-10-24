//go:generate stringer -type=TokenType

package lexer

import (
	"unicode"

	"github.com/authzed/spicedb/pkg/schemadsl/input"
)

// Lex creates a new scanner for the input string.
func Lex(source input.InputSource, input string) *Lexer {
	return createLexer(source, input)
}

// TokenType identifies the type of lexer lexemes.
type TokenType int

const (
	TokenTypeError TokenType = iota // error occurred; value is text of error

	// Synthetic semicolon
	TokenTypeSyntheticSemicolon

	TokenTypeEOF
	TokenTypeWhitespace
	TokenTypeSinglelineComment
	TokenTypeMultilineComment
	TokenTypeNewline

	TokenTypeKeyword    // interface
	TokenTypeIdentifier // helloworld
	TokenTypeNumber     // 123

	TokenTypeLeftBrace  // {
	TokenTypeRightBrace // }
	TokenTypeLeftParen  // (
	TokenTypeRightParen // )

	TokenTypePipe  // |
	TokenTypePlus  // +
	TokenTypeMinus // -
	TokenTypeAnd   // &
	TokenTypeDiv   // /

	TokenTypeEquals     // =
	TokenTypeColon      // :
	TokenTypeSemicolon  // ;
	TokenTypeRightArrow // ->
	TokenTypeHash       // #
	TokenTypeEllipsis   // ...
	TokenTypeAt         // @
	TokenTypeComma      // ,
)

// keywords contains the full set of keywords supported.
var keywords = map[string]bool{
	"definition": true,
	"relation":   true,
	"permission": true,
}

// syntheticPredecessors contains the full set of token types after which, if a newline is found,
// we emit a synthetic semicolon rather than a normal newline token.
var syntheticPredecessors = map[TokenType]bool{
	TokenTypeIdentifier: true,
	TokenTypeKeyword:    true,

	TokenTypeRightBrace: true,
	TokenTypeRightParen: true,
}

// lexerEntrypoint scans until EOFRUNE
func lexerEntrypoint(l *Lexer) stateFn {
Loop:
	for {
		switch r := l.next(); {
		case r == EOFRUNE:
			break Loop

		case r == '{':
			l.emit(TokenTypeLeftBrace)

		case r == '}':
			l.emit(TokenTypeRightBrace)

		case r == '(':
			l.emit(TokenTypeLeftParen)

		case r == ')':
			l.emit(TokenTypeRightParen)

		case r == '+':
			l.emit(TokenTypePlus)

		case r == '|':
			l.emit(TokenTypePipe)

		case r == '&':
			l.emit(TokenTypeAnd)

		case r == '=':
			l.emit(TokenTypeEquals)

		case r == ':':
			l.emit(TokenTypeColon)

		case r == ';':
			l.emit(TokenTypeSemicolon)

		case r == '#':
			l.emit(TokenTypeHash)

		case r == '@':
			l.emit(TokenTypeAt)

		case r == ',':
			l.emit(TokenTypeComma)

		case r == '.':
			if l.acceptString("..") {
				l.emit(TokenTypeEllipsis)
			} else {
				return l.errorf("unrecognized character at this location: %#U", r)
			}

		case r == '-':
			if l.accept(">") {
				l.emit(TokenTypeRightArrow)
			} else {
				l.emit(TokenTypeMinus)
			}

		case isSpace(r):
			l.emit(TokenTypeWhitespace)

		case isNewline(r):
			// If the previous token matches the synthetic semicolon list,
			// we emit a synthetic semicolon instead of a simple newline.
			if _, ok := syntheticPredecessors[l.lastNonIgnoredToken.Kind]; ok {
				l.emit(TokenTypeSyntheticSemicolon)
			} else {
				l.emit(TokenTypeNewline)
			}

		case isAlphaNumeric(r):
			l.backup()
			return lexIdentifierOrKeyword

		case r == '/':
			// Check for comments.
			if l.peekValue("/") {
				l.backup()
				return lexSinglelineComment
			}

			if l.peekValue("*") {
				l.backup()
				return lexMultilineComment
			}

			l.emit(TokenTypeDiv)
		default:
			return l.errorf("unrecognized character at this location: %#U", r)
		}
	}

	l.emit(TokenTypeEOF)
	return nil
}

// lexSinglelineComment scans until newline or EOFRUNE
func lexSinglelineComment(l *Lexer) stateFn {
	checker := func(r rune) (bool, error) {
		result := r == EOFRUNE || isNewline(r)
		return !result, nil
	}

	l.acceptString("//")
	return buildLexUntil(TokenTypeSinglelineComment, checker)
}

// lexMultilineComment scans until the close of the multiline comment or EOFRUNE
func lexMultilineComment(l *Lexer) stateFn {
	l.acceptString("/*")
	for {
		// Check for the end of the multiline comment.
		if l.peekValue("*/") {
			l.acceptString("*/")
			l.emit(TokenTypeMultilineComment)
			return lexSource
		}

		// Otherwise, consume until we hit EOFRUNE.
		r := l.next()
		if r == EOFRUNE {
			return l.errorf("Unterminated multiline comment")
		}
	}
}

// lexIdentifierOrKeyword searches for a keyword or literal identifier.
func lexIdentifierOrKeyword(l *Lexer) stateFn {
	for {
		if !isAlphaNumeric(l.peek()) {
			break
		}

		l.next()
	}

	_, is_keyword := keywords[l.value()]

	switch {
	case is_keyword:
		l.emit(TokenTypeKeyword)

	default:
		l.emit(TokenTypeIdentifier)
	}

	return lexSource
}

// isSpace reports whether r is a space character.
func isSpace(r rune) bool {
	return r == ' ' || r == '\t'
}

// isNewline reports whether r is a newline character.
func isNewline(r rune) bool {
	return r == '\r' || r == '\n'
}

// isAlphaNumeric reports whether r is an alphabetic, digit, or underscore.
func isAlphaNumeric(r rune) bool {
	return r == '_' || unicode.IsLetter(r) || unicode.IsDigit(r)
}
