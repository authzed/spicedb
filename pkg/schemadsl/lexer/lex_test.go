package lexer

import (
	"testing"

	"github.com/authzed/spicedb/pkg/schemadsl/input"
)

type lexerTest struct {
	name   string
	input  string
	tokens []Lexeme
}

var (
	tEOF        = Lexeme{TokenTypeEOF, 0, "", ""}
	tWhitespace = Lexeme{TokenTypeWhitespace, 0, " ", ""}
)

var lexerTests = []lexerTest{
	// Simple tests.
	{"empty", "", []Lexeme{tEOF}},

	{"single whitespace", " ", []Lexeme{tWhitespace, tEOF}},
	{"single tab", "\t", []Lexeme{{TokenTypeWhitespace, 0, "\t", ""}, tEOF}},
	{"multiple whitespace", "   ", []Lexeme{tWhitespace, tWhitespace, tWhitespace, tEOF}},

	{"newline r", "\r", []Lexeme{{TokenTypeNewline, 0, "\r", ""}, tEOF}},
	{"newline n", "\n", []Lexeme{{TokenTypeNewline, 0, "\n", ""}, tEOF}},
	{"newline rn", "\r\n", []Lexeme{{TokenTypeNewline, 0, "\r", ""}, {TokenTypeNewline, 0, "\n", ""}, tEOF}},

	{"comment", "// a comment", []Lexeme{{TokenTypeSinglelineComment, 0, "// a comment", ""}, tEOF}},
	{"multiline comment", "/* a comment\n foo*/", []Lexeme{{TokenTypeMultilineComment, 0, "/* a comment\n foo*/", ""}, tEOF}},

	{"left brace", "{", []Lexeme{{TokenTypeLeftBrace, 0, "{", ""}, tEOF}},
	{"right brace", "}", []Lexeme{{TokenTypeRightBrace, 0, "}", ""}, tEOF}},

	{"left paren", "(", []Lexeme{{TokenTypeLeftParen, 0, "(", ""}, tEOF}},
	{"right paren", ")", []Lexeme{{TokenTypeRightParen, 0, ")", ""}, tEOF}},

	{"semicolon", ";", []Lexeme{{TokenTypeSemicolon, 0, ";", ""}, tEOF}},
	{"star", "*", []Lexeme{{TokenTypeStar, 0, "*", ""}, tEOF}},

	{"right arrow", "->", []Lexeme{{TokenTypeRightArrow, 0, "->", ""}, tEOF}},

	{"hash", "#", []Lexeme{{TokenTypeHash, 0, "#", ""}, tEOF}},
	{"ellipsis", "...", []Lexeme{{TokenTypeEllipsis, 0, "...", ""}, tEOF}},

	{"relation reference", "foo#...", []Lexeme{
		{TokenTypeIdentifier, 0, "foo", ""},
		{TokenTypeHash, 0, "#", ""},
		{TokenTypeEllipsis, 0, "...", ""},
		tEOF,
	}},

	{"plus", "+", []Lexeme{{TokenTypePlus, 0, "+", ""}, tEOF}},
	{"minus", "-", []Lexeme{{TokenTypeMinus, 0, "-", ""}, tEOF}},

	{"keyword", "definition", []Lexeme{{TokenTypeKeyword, 0, "definition", ""}, tEOF}},
	{"keyword", "nil", []Lexeme{{TokenTypeKeyword, 0, "nil", ""}, tEOF}},
	{"identifier", "define", []Lexeme{{TokenTypeIdentifier, 0, "define", ""}, tEOF}},
	{"typepath", "foo/bar", []Lexeme{
		{TokenTypeIdentifier, 0, "foo", ""},
		{TokenTypeDiv, 0, "/", ""},
		{TokenTypeIdentifier, 0, "bar", ""},
		tEOF,
	}},

	{"type star", "foo:*", []Lexeme{
		{TokenTypeIdentifier, 0, "foo", ""},
		{TokenTypeColon, 0, ":", ""},
		{TokenTypeStar, 0, "*", ""},
		tEOF,
	}},

	{"expression", "foo->bar", []Lexeme{
		{TokenTypeIdentifier, 0, "foo", ""},
		{TokenTypeRightArrow, 0, "->", ""},
		{TokenTypeIdentifier, 0, "bar", ""},
		tEOF,
	}},

	{"relation", "/* foo */relation parent: namespace | organization\n", []Lexeme{
		{TokenTypeMultilineComment, 0, "/* foo */", ""},
		{TokenTypeKeyword, 0, "relation", ""},
		tWhitespace,
		{TokenTypeIdentifier, 0, "parent", ""},
		{TokenTypeColon, 0, ":", ""},
		tWhitespace,
		{TokenTypeIdentifier, 0, "namespace", ""},
		tWhitespace,
		{TokenTypePipe, 0, "|", ""},
		tWhitespace,
		{TokenTypeIdentifier, 0, "organization", ""},
		{TokenTypeSyntheticSemicolon, 0, "\n", ""},
		tEOF,
	}},

	{"relation", "/* foo */relation parent: namespace | organization;", []Lexeme{
		{TokenTypeMultilineComment, 0, "/* foo */", ""},
		{TokenTypeKeyword, 0, "relation", ""},
		tWhitespace,
		{TokenTypeIdentifier, 0, "parent", ""},
		{TokenTypeColon, 0, ":", ""},
		tWhitespace,
		{TokenTypeIdentifier, 0, "namespace", ""},
		tWhitespace,
		{TokenTypePipe, 0, "|", ""},
		tWhitespace,
		{TokenTypeIdentifier, 0, "organization", ""},
		{TokenTypeSemicolon, 0, ";", ""},
		tEOF,
	}},

	{"relation", "/* foo */relation parent: namespace:*\n", []Lexeme{
		{TokenTypeMultilineComment, 0, "/* foo */", ""},
		{TokenTypeKeyword, 0, "relation", ""},
		tWhitespace,
		{TokenTypeIdentifier, 0, "parent", ""},
		{TokenTypeColon, 0, ":", ""},
		tWhitespace,
		{TokenTypeIdentifier, 0, "namespace", ""},
		{TokenTypeColon, 0, ":", ""},
		{TokenTypeStar, 0, "*", ""},
		{TokenTypeSyntheticSemicolon, 0, "\n", ""},
		tEOF,
	}},

	{"expression with parens", "(foo->bar)\n", []Lexeme{
		{TokenTypeLeftParen, 0, "(", ""},
		{TokenTypeIdentifier, 0, "foo", ""},
		{TokenTypeRightArrow, 0, "->", ""},
		{TokenTypeIdentifier, 0, "bar", ""},
		{TokenTypeRightParen, 0, ")", ""},
		{TokenTypeSyntheticSemicolon, 0, "\n", ""},
		tEOF,
	}},
}

func TestLexer(t *testing.T) {
	for _, test := range lexerTests {
		t.Run(test.name, func(t *testing.T) {
			test := test // Close over test and not the pointer that is reused.
			tokens := performLex(&test)
			if !equal(tokens, test.tokens) {
				t.Errorf("%s: got\n\t%+v\nexpected\n\t%v", test.name, tokens, test.tokens)
			}
		})
	}
}

func performLex(t *lexerTest) (tokens []Lexeme) {
	l := Lex(input.Source(t.name), t.input)
	for {
		token := l.nextToken()
		tokens = append(tokens, token)
		if token.Kind == TokenTypeEOF || token.Kind == TokenTypeError {
			break
		}
	}
	return
}

func equal(found, expected []Lexeme) bool {
	if len(found) != len(expected) {
		return false
	}
	for k := range found {
		if found[k].Kind != expected[k].Kind {
			return false
		}
		if found[k].Value != expected[k].Value {
			return false
		}
	}
	return true
}
