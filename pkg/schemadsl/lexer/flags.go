package lexer

import (
	"maps"
	"slices"
)

const (
	// FlagExpiration indicates that `expiration` is supported as a first-class
	// feature in the schema.
	FlagExpiration = "expiration"

	// FlagTypeChecking indicates that `typechecking` is supported as a first-class
	// feature in the schema.
	FlagTypeChecking = "typechecking"

	// FlagSelf indicates that `self` is supported as a first-class
	// feature in the schema.
	FlagSelf = "self"

	// FlagPartials indicates that partials are supported in the schema.
	FlagPartials = "partial"

	// FlagImports indicates that imports are supported in the schema.
	FlagImports = "import"
)

var AllUseFlags []string

func init() {
	AllUseFlags = slices.Collect(maps.Keys(Flags))
	slices.Sort(AllUseFlags)
}

type transformer func(lexeme Lexeme) (Lexeme, bool)

// Flags is a map of flag names to their corresponding transformers.
var Flags = map[string]transformer{
	FlagExpiration: func(lexeme Lexeme) (Lexeme, bool) {
		// `expiration` becomes a keyword.
		if lexeme.Kind == TokenTypeIdentifier && lexeme.Value == "expiration" {
			lexeme.Kind = TokenTypeKeyword
			return lexeme, true
		}

		// `and` becomes a keyword (for "caveat and expiration")
		if lexeme.Kind == TokenTypeIdentifier && lexeme.Value == "and" {
			lexeme.Kind = TokenTypeKeyword
			return lexeme, true
		}

		return lexeme, false
	},
	FlagTypeChecking: func(lexeme Lexeme) (Lexeme, bool) {
		// `typechecking` becomes a keyword.
		if lexeme.Kind == TokenTypeIdentifier && lexeme.Value == "typechecking" {
			lexeme.Kind = TokenTypeKeyword
			return lexeme, true
		}

		return lexeme, false
	},
	FlagSelf: func(lexeme Lexeme) (Lexeme, bool) {
		// `self` becomes a keyword.
		if lexeme.Kind == TokenTypeIdentifier && lexeme.Value == "self" {
			lexeme.Kind = TokenTypeKeyword
			return lexeme, true
		}

		return lexeme, false
	},
	FlagPartials: func(lexeme Lexeme) (Lexeme, bool) {
		// `partial` becomes a keyword.
		if lexeme.Kind == TokenTypeIdentifier && lexeme.Value == "partial" {
			lexeme.Kind = TokenTypeKeyword
			return lexeme, true
		}

		return lexeme, false
	},
	FlagImports: func(lexeme Lexeme) (Lexeme, bool) {
		// `import` becomes a keyword.
		if lexeme.Kind == TokenTypeIdentifier && lexeme.Value == "import" {
			lexeme.Kind = TokenTypeKeyword
			return lexeme, true
		}

		return lexeme, false
	},
}
