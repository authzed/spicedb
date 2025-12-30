package lexer

// FlagExpiration indicates that `expiration` is supported as a first-class
// feature in the schema.
const FlagExpiration = "expiration"

// FlagSelf indicates that `self` is supported as a first-class
// feature in the schema.
const FlagSelf = "self"

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
	// TODO add typechecking flag?
	FlagSelf: func(lexeme Lexeme) (Lexeme, bool) {
		// `self` becomes a keyword.
		if lexeme.Kind == TokenTypeIdentifier && lexeme.Value == "self" {
			lexeme.Kind = TokenTypeKeyword
			return lexeme, true
		}

		return lexeme, false
	},
}
