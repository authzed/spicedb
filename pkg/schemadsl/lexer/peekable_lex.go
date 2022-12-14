package lexer

import (
	"container/list"
)

// PeekableLexer wraps a lexer and provides the ability to peek forward without
// losing state.
type PeekableLexer struct {
	lex        *Lexer     // a reference to the lexer used for tokenization
	readTokens *list.List // tokens already read from the lexer during a lookahead.
}

// NewPeekableLexer returns a new PeekableLexer for the given lexer.
func NewPeekableLexer(lex *Lexer) *PeekableLexer {
	return &PeekableLexer{
		lex:        lex,
		readTokens: list.New(),
	}
}

// Close stops the lexer from running.
func (l *PeekableLexer) Close() {
	l.lex.Close()
}

// NextToken returns the next token found in the lexer.
func (l *PeekableLexer) NextToken() Lexeme {
	frontElement := l.readTokens.Front()
	if frontElement != nil {
		return l.readTokens.Remove(frontElement).(Lexeme)
	}

	return l.lex.nextToken()
}
