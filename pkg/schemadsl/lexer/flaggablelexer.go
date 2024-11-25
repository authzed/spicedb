package lexer

// FlaggableLexler wraps a lexer, automatically translating tokens based on flags, if any.
type FlaggableLexler struct {
	lex                *Lexer                 // a reference to the lexer used for tokenization
	enabledFlags       map[string]transformer // flags that are enabled
	seenDefinition     bool
	afterUseIdentifier bool
}

// NewFlaggableLexler returns a new FlaggableLexler for the given lexer.
func NewFlaggableLexler(lex *Lexer) *FlaggableLexler {
	return &FlaggableLexler{
		lex:          lex,
		enabledFlags: map[string]transformer{},
	}
}

// Close stops the lexer from running.
func (l *FlaggableLexler) Close() {
	l.lex.Close()
}

// NextToken returns the next token found in the lexer.
func (l *FlaggableLexler) NextToken() Lexeme {
	nextToken := l.lex.nextToken()

	// Look for `use somefeature`
	if nextToken.Kind == TokenTypeIdentifier {
		// Only allowed until we've seen a definition of some kind.
		if !l.seenDefinition {
			if l.afterUseIdentifier {
				if transformer, ok := Flags[nextToken.Value]; ok {
					l.enabledFlags[nextToken.Value] = transformer
				}

				l.afterUseIdentifier = false
			} else {
				l.afterUseIdentifier = nextToken.Value == "use"
			}
		}
	}

	if nextToken.Kind == TokenTypeKeyword && nextToken.Value == "definition" {
		l.seenDefinition = true
	}
	if nextToken.Kind == TokenTypeKeyword && nextToken.Value == "caveat" {
		l.seenDefinition = true
	}

	for _, handler := range l.enabledFlags {
		updated, ok := handler(nextToken)
		if ok {
			return updated
		}
	}

	return nextToken
}
