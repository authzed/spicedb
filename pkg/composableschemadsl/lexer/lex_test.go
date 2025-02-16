package lexer

import (
	"testing"

	"github.com/authzed/spicedb/pkg/composableschemadsl/input"
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
	{"keyword", "caveat", []Lexeme{{TokenTypeKeyword, 0, "caveat", ""}, tEOF}},
	{"keyword", "relation", []Lexeme{{TokenTypeKeyword, 0, "relation", ""}, tEOF}},
	{"keyword", "permission", []Lexeme{{TokenTypeKeyword, 0, "permission", ""}, tEOF}},
	{"keyword", "nil", []Lexeme{{TokenTypeKeyword, 0, "nil", ""}, tEOF}},
	{"keyword", "with", []Lexeme{{TokenTypeKeyword, 0, "with", ""}, tEOF}},
	{"keyword", "import", []Lexeme{{TokenTypeKeyword, 0, "import", ""}, tEOF}},
	{"keyword", "all", []Lexeme{{TokenTypeKeyword, 0, "all", ""}, tEOF}},
	{"keyword", "nil", []Lexeme{{TokenTypeKeyword, 0, "nil", ""}, tEOF}},
	{"keyword", "partial", []Lexeme{{TokenTypeKeyword, 0, "partial", ""}, tEOF}},
	{"keyword", "use", []Lexeme{{TokenTypeKeyword, 0, "use", ""}, tEOF}},
	{"keyword", "expiration", []Lexeme{{TokenTypeKeyword, 0, "expiration", ""}, tEOF}},
	{"keyword", "and", []Lexeme{{TokenTypeKeyword, 0, "and", ""}, tEOF}},
	{"keyword", "or", []Lexeme{{TokenTypeKeyword, 0, "or", ""}, tEOF}},
	{"keyword", "not", []Lexeme{{TokenTypeKeyword, 0, "not", ""}, tEOF}},
	{"keyword", "under", []Lexeme{{TokenTypeKeyword, 0, "under", ""}, tEOF}},
	{"keyword", "static", []Lexeme{{TokenTypeKeyword, 0, "static", ""}, tEOF}},
	{"keyword", "if", []Lexeme{{TokenTypeKeyword, 0, "if", ""}, tEOF}},
	{"keyword", "where", []Lexeme{{TokenTypeKeyword, 0, "where", ""}, tEOF}},
	{"keyword", "private", []Lexeme{{TokenTypeKeyword, 0, "private", ""}, tEOF}},
	{"keyword", "public", []Lexeme{{TokenTypeKeyword, 0, "public", ""}, tEOF}},

	{"identifier", "define", []Lexeme{{TokenTypeIdentifier, 0, "define", ""}, tEOF}},
	{"typepath", "foo/bar", []Lexeme{
		{TokenTypeIdentifier, 0, "foo", ""},
		{TokenTypeDiv, 0, "/", ""},
		{TokenTypeIdentifier, 0, "bar", ""},
		tEOF,
	}},

	{"multiple slash path", "foo/bar/baz/bang/zoom", []Lexeme{
		{TokenTypeIdentifier, 0, "foo", ""},
		{TokenTypeDiv, 0, "/", ""},
		{TokenTypeIdentifier, 0, "bar", ""},
		{TokenTypeDiv, 0, "/", ""},
		{TokenTypeIdentifier, 0, "baz", ""},
		{TokenTypeDiv, 0, "/", ""},
		{TokenTypeIdentifier, 0, "bang", ""},
		{TokenTypeDiv, 0, "/", ""},
		{TokenTypeIdentifier, 0, "zoom", ""},
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

	{"relation with caveat", "/* foo */relation viewer: user with somecaveat\n", []Lexeme{
		{TokenTypeMultilineComment, 0, "/* foo */", ""},
		{TokenTypeKeyword, 0, "relation", ""},
		tWhitespace,
		{TokenTypeIdentifier, 0, "viewer", ""},
		{TokenTypeColon, 0, ":", ""},
		tWhitespace,
		{TokenTypeIdentifier, 0, "user", ""},
		tWhitespace,
		{TokenTypeKeyword, 0, "with", ""},
		tWhitespace,
		{TokenTypeIdentifier, 0, "somecaveat", ""},
		{TokenTypeSyntheticSemicolon, 0, "\n", ""},
		tEOF,
	}},

	{"relation with wildcard caveat", "/* foo */relation viewer: user:* with somecaveat\n", []Lexeme{
		{TokenTypeMultilineComment, 0, "/* foo */", ""},
		{TokenTypeKeyword, 0, "relation", ""},
		tWhitespace,
		{TokenTypeIdentifier, 0, "viewer", ""},
		{TokenTypeColon, 0, ":", ""},
		tWhitespace,
		{TokenTypeIdentifier, 0, "user", ""},
		{TokenTypeColon, 0, ":", ""},
		{TokenTypeStar, 0, "*", ""},
		tWhitespace,
		{TokenTypeKeyword, 0, "with", ""},
		tWhitespace,
		{TokenTypeIdentifier, 0, "somecaveat", ""},
		{TokenTypeSyntheticSemicolon, 0, "\n", ""},
		tEOF,
	}},

	{"relation with invalid caveat", "/* foo */relation viewer: user with with\n", []Lexeme{
		{TokenTypeMultilineComment, 0, "/* foo */", ""},
		{TokenTypeKeyword, 0, "relation", ""},
		tWhitespace,
		{TokenTypeIdentifier, 0, "viewer", ""},
		{TokenTypeColon, 0, ":", ""},
		tWhitespace,
		{TokenTypeIdentifier, 0, "user", ""},
		tWhitespace,
		{TokenTypeKeyword, 0, "with", ""},
		tWhitespace,
		{TokenTypeKeyword, 0, "with", ""},
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
	{
		"cel lexemes", "[a<=b]",
		[]Lexeme{
			{TokenTypeLeftBracket, 0, "[", ""},
			{TokenTypeIdentifier, 0, "a", ""},
			{TokenTypeLessThanOrEqual, 0, "<=", ""},
			{TokenTypeIdentifier, 0, "b", ""},
			{TokenTypeRightBracket, 0, "]", ""},
			tEOF,
		},
	},
	{
		"more cel lexemes", "[a>=b.?]",
		[]Lexeme{
			{TokenTypeLeftBracket, 0, "[", ""},
			{TokenTypeIdentifier, 0, "a", ""},
			{TokenTypeGreaterThanOrEqual, 0, ">=", ""},
			{TokenTypeIdentifier, 0, "b", ""},
			{TokenTypePeriod, 0, ".", ""},
			{TokenTypeQuestionMark, 0, "?", ""},
			{TokenTypeRightBracket, 0, "]", ""},
			tEOF,
		},
	},
	{
		"cel string literal", `"hi there"`,
		[]Lexeme{
			{TokenTypeString, 0, `"hi there"`, ""},
			tEOF,
		},
	},
	{
		"cel string literal with terminators", `"""hi "there" """`,
		[]Lexeme{
			{TokenTypeString, 0, `"""hi "there" """`, ""},
			tEOF,
		},
	},
	{
		"unterminated cel string literal", "\"hi\nthere\"",
		[]Lexeme{
			{
				Kind:     TokenTypeError,
				Position: 0,
				Value:    "\n",
				Error:    "Unterminated string",
			},
		},
	},

	{"dot access", "foo.all(something)", []Lexeme{
		{TokenTypeIdentifier, 0, "foo", ""},
		{TokenTypePeriod, 0, ".", ""},
		{TokenTypeKeyword, 0, "all", ""},
		{TokenTypeLeftParen, 0, "(", ""},
		{TokenTypeIdentifier, 0, "something", ""},
		{TokenTypeRightParen, 0, ")", ""},
		tEOF,
	}},
}

func TestLexer(t *testing.T) {
	for _, test := range lexerTests {
		test := test
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
