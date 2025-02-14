package lexer

import (
	"slices"
	"testing"

	"github.com/authzed/spicedb/pkg/composableschemadsl/input"
)

var flaggableLexerTests = []lexerTest{
	{"use expiration", "use expiration", []Lexeme{
		{TokenTypeKeyword, 0, "use", ""},
		{TokenTypeWhitespace, 0, " ", ""},
		{TokenTypeKeyword, 0, "expiration", ""},
		tEOF,
	}},
	{"use expiration and", "use expiration and", []Lexeme{
		{TokenTypeKeyword, 0, "use", ""},
		{TokenTypeWhitespace, 0, " ", ""},
		{TokenTypeKeyword, 0, "expiration", ""},
		{TokenTypeWhitespace, 0, " ", ""},
		{TokenTypeKeyword, 0, "and", ""},
		tEOF,
	}},
	{"expiration as non-keyword", "foo expiration", []Lexeme{
		{TokenTypeIdentifier, 0, "foo", ""},
		{TokenTypeWhitespace, 0, " ", ""},
		{TokenTypeKeyword, 0, "expiration", ""},
		tEOF,
	}},
	{"and as non-keyword", "foo and", []Lexeme{
		{TokenTypeIdentifier, 0, "foo", ""},
		{TokenTypeWhitespace, 0, " ", ""},
		{TokenTypeKeyword, 0, "and", ""},
		tEOF,
	}},
	{"invalid use flag", "use foobar", []Lexeme{
		{TokenTypeKeyword, 0, "use", ""},
		{TokenTypeWhitespace, 0, " ", ""},
		{TokenTypeIdentifier, 0, "foobar", ""},
		tEOF,
	}},
	{"use flag after definition", "definition use expiration", []Lexeme{
		{TokenTypeKeyword, 0, "definition", ""},
		{TokenTypeWhitespace, 0, " ", ""},
		{TokenTypeKeyword, 0, "use", ""},
		{TokenTypeWhitespace, 0, " ", ""},
		{TokenTypeKeyword, 0, "expiration", ""},
		tEOF,
	}},
}

func TestFlaggableLexer(t *testing.T) {
	for _, test := range append(slices.Clone(lexerTests), flaggableLexerTests...) {
		t.Run(test.name, func(t *testing.T) {
			tokens := performFlaggedLex(&test)
			if !equal(tokens, test.tokens) {
				t.Errorf("%s: got\n\t%+v\nexpected\n\t%v", test.name, tokens, test.tokens)
			}
		})
	}
}

func performFlaggedLex(t *lexerTest) (tokens []Lexeme) {
	lexer := NewFlaggableLexer(Lex(input.Source(t.name), t.input))
	for {
		token := lexer.NextToken()
		tokens = append(tokens, token)
		if token.Kind == TokenTypeEOF || token.Kind == TokenTypeError {
			break
		}
	}
	return
}
