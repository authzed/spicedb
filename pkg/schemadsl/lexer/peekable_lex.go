package lexer

import (
	"container/list"
	"fmt"
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

// PeekToken performs lookahead of the given count on the token stream.
func (l *PeekableLexer) PeekToken(count int) Lexeme {
	if count < 1 {
		panic(fmt.Sprintf("Expected count > 1, received: %v", count))
	}

	// Ensure that the readTokens has at least the requested number of tokens.
	if l.readTokens.Len() < count {
		for {
			l.readTokens.PushBack(l.lex.nextToken())

			if l.readTokens.Len() == count {
				break
			}
		}
	}

	// Retrieve the count-th token from the list.
	var element *list.Element
	element = l.readTokens.Front()

	for i := 1; i < count; i++ {
		element = element.Next()
	}

	return element.Value.(Lexeme)
}
