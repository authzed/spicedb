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
	tEOF        = Lexeme{TokenTypeEOF, 0, ""}
	tWhitespace = Lexeme{TokenTypeWhitespace, 0, " "}
)

var lexerTests = []lexerTest{
	// Simple tests.
	{"empty", "", []Lexeme{tEOF}},

	{"single whitespace", " ", []Lexeme{tWhitespace, tEOF}},
	{"single tab", "\t", []Lexeme{Lexeme{TokenTypeWhitespace, 0, "\t"}, tEOF}},
	{"multiple whitespace", "   ", []Lexeme{tWhitespace, tWhitespace, tWhitespace, tEOF}},

	{"newline r", "\r", []Lexeme{Lexeme{TokenTypeNewline, 0, "\r"}, tEOF}},
	{"newline n", "\n", []Lexeme{Lexeme{TokenTypeNewline, 0, "\n"}, tEOF}},
	{"newline rn", "\r\n", []Lexeme{Lexeme{TokenTypeNewline, 0, "\r"}, Lexeme{TokenTypeNewline, 0, "\n"}, tEOF}},

	{"comment", "// a comment", []Lexeme{Lexeme{TokenTypeSinglelineComment, 0, "// a comment"}, tEOF}},
	{"multiline comment", "/* a comment\n foo*/", []Lexeme{Lexeme{TokenTypeMultilineComment, 0, "/* a comment\n foo*/"}, tEOF}},

	{"left brace", "{", []Lexeme{Lexeme{TokenTypeLeftBrace, 0, "{"}, tEOF}},
	{"right brace", "}", []Lexeme{Lexeme{TokenTypeRightBrace, 0, "}"}, tEOF}},

	{"left paren", "(", []Lexeme{Lexeme{TokenTypeLeftParen, 0, "("}, tEOF}},
	{"right paren", ")", []Lexeme{Lexeme{TokenTypeRightParen, 0, ")"}, tEOF}},

	{"semicolon", ";", []Lexeme{Lexeme{TokenTypeSemicolon, 0, ";"}, tEOF}},

	{"right arrow", "->", []Lexeme{Lexeme{TokenTypeRightArrow, 0, "->"}, tEOF}},

	{"hash", "#", []Lexeme{Lexeme{TokenTypeHash, 0, "#"}, tEOF}},
	{"ellipsis", "...", []Lexeme{Lexeme{TokenTypeEllipsis, 0, "..."}, tEOF}},

	{"relation reference", "foo#...", []Lexeme{
		Lexeme{TokenTypeIdentifier, 0, "foo"},
		Lexeme{TokenTypeHash, 0, "#"},
		Lexeme{TokenTypeEllipsis, 0, "..."},
		tEOF,
	}},

	{"plus", "+", []Lexeme{Lexeme{TokenTypePlus, 0, "+"}, tEOF}},
	{"minus", "-", []Lexeme{Lexeme{TokenTypeMinus, 0, "-"}, tEOF}},

	{"keyword", "definition", []Lexeme{Lexeme{TokenTypeKeyword, 0, "definition"}, tEOF}},
	{"identifier", "define", []Lexeme{Lexeme{TokenTypeIdentifier, 0, "define"}, tEOF}},
	{"typepath", "foo/bar", []Lexeme{
		Lexeme{TokenTypeIdentifier, 0, "foo"},
		Lexeme{TokenTypeDiv, 0, "/"},
		Lexeme{TokenTypeIdentifier, 0, "bar"},
		tEOF,
	}},

	{"expression", "foo->bar", []Lexeme{
		Lexeme{TokenTypeIdentifier, 0, "foo"},
		Lexeme{TokenTypeRightArrow, 0, "->"},
		Lexeme{TokenTypeIdentifier, 0, "bar"},
		tEOF,
	}},

	{"relation", "/* foo */relation parent: namespace | organization\n", []Lexeme{
		Lexeme{TokenTypeMultilineComment, 0, "/* foo */"},
		Lexeme{TokenTypeKeyword, 0, "relation"},
		tWhitespace,
		Lexeme{TokenTypeIdentifier, 0, "parent"},
		Lexeme{TokenTypeColon, 0, ":"},
		tWhitespace,
		Lexeme{TokenTypeIdentifier, 0, "namespace"},
		tWhitespace,
		Lexeme{TokenTypePipe, 0, "|"},
		tWhitespace,
		Lexeme{TokenTypeIdentifier, 0, "organization"},
		Lexeme{TokenTypeSyntheticSemicolon, 0, "\n"},
		tEOF,
	}},

	{"relation", "/* foo */relation parent: namespace | organization;", []Lexeme{
		Lexeme{TokenTypeMultilineComment, 0, "/* foo */"},
		Lexeme{TokenTypeKeyword, 0, "relation"},
		tWhitespace,
		Lexeme{TokenTypeIdentifier, 0, "parent"},
		Lexeme{TokenTypeColon, 0, ":"},
		tWhitespace,
		Lexeme{TokenTypeIdentifier, 0, "namespace"},
		tWhitespace,
		Lexeme{TokenTypePipe, 0, "|"},
		tWhitespace,
		Lexeme{TokenTypeIdentifier, 0, "organization"},
		Lexeme{TokenTypeSemicolon, 0, ";"},
		tEOF,
	}},
}

func TestLexer(t *testing.T) {
	for _, test := range lexerTests {
		t.Run(test.name, func(t *testing.T) {
			tokens := performLex(&test)
			if !equal(tokens, test.tokens) {
				t.Errorf("%s: got\n\t%+v\nexpected\n\t%v", test.name, tokens, test.tokens)
			}
		})
	}
}

func performLex(t *lexerTest) (tokens []Lexeme) {
	l := Lex(input.InputSource(t.name), t.input)
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
