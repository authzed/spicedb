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

		// `and` becomes a keyword.
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
}
