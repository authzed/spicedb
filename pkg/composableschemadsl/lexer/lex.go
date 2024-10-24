// Based on design first introduced in: http://blog.golang.org/two-go-talks-lexical-scanning-in-go-and
// Portions copied and modified from: https://github.com/golang/go/blob/master/src/text/template/parse/lex.go

package lexer

import (
	"fmt"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/authzed/spicedb/pkg/composableschemadsl/input"
)

const EOFRUNE = -1

// createLexer creates a new scanner for the input string.
func createLexer(source input.Source, input string) *Lexer {
	l := &Lexer{
		source: source,
		input:  input,
		tokens: make(chan Lexeme),
		closed: make(chan struct{}),
	}
	go l.run()
	return l
}

// run runs the state machine for the lexer.
func (l *Lexer) run() {
	defer func() {
		close(l.tokens)
	}()
	l.withLock(func() {
		l.state = lexSource
	})
	var state stateFn
	for {
		l.withRLock(func() {
			state = l.state
		})
		if state == nil {
			break
		}
		next := state(l)
		l.withLock(func() {
			l.state = next
		})
	}
}

// Close stops the lexer from running.
func (l *Lexer) Close() {
	close(l.closed)
	l.withLock(func() {
		l.state = nil
	})
}

// withLock runs f protected by l's lock
func (l *Lexer) withLock(f func()) {
	l.Lock()
	defer l.Unlock()
	f()
}

// withRLock runs f protected by l's read lock
func (l *Lexer) withRLock(f func()) {
	l.RLock()
	defer l.RUnlock()
	f()
}

// Lexeme represents a token returned from scanning the contents of a file.
type Lexeme struct {
	Kind     TokenType          // The type of this lexeme.
	Position input.BytePosition // The starting position of this token in the input string.
	Value    string             // The textual value of this token.
	Error    string             // The error associated with the lexeme, if any.
}

// stateFn represents the state of the scanner as a function that returns the next state.
type stateFn func(*Lexer) stateFn

// Lexer holds the state of the scanner.
type Lexer struct {
	sync.RWMutex
	source              input.Source       // the name of the input; used only for error reports
	input               string             // the string being scanned
	state               stateFn            // the next lexing function to enter
	pos                 input.BytePosition // current position in the input
	start               input.BytePosition // start position of this token
	width               input.BytePosition // width of last rune read from input
	lastPos             input.BytePosition // position of most recent token returned by nextToken
	tokens              chan Lexeme        // channel of scanned lexemes
	currentToken        Lexeme             // The current token if any
	lastNonIgnoredToken Lexeme             // The last token returned that is non-whitespace and non-comment
	closed              chan struct{}      // Holds the closed channel
}

// nextToken returns the next token from the input.
func (l *Lexer) nextToken() Lexeme {
	token := <-l.tokens
	l.lastPos = token.Position
	return token
}

// next returns the next rune in the input.
func (l *Lexer) next() rune {
	if int(l.pos) >= len(l.input) {
		l.width = 0
		return EOFRUNE
	}
	r, w := utf8.DecodeRuneInString(l.input[l.pos:])
	l.width = input.BytePosition(w)
	l.pos += l.width
	return r
}

// peek returns but does not consume the next rune in the input.
func (l *Lexer) peek() rune {
	r := l.next()
	l.backup()
	return r
}

// backup steps back one rune. Can only be called once per call of next.
func (l *Lexer) backup() {
	l.pos -= l.width
}

// value returns the current value of the token in the lexer.
func (l *Lexer) value() string {
	return l.input[l.start:l.pos]
}

// emit passes an token back to the client.
func (l *Lexer) emit(t TokenType) {
	currentToken := Lexeme{t, l.start, l.value(), ""}

	if t != TokenTypeWhitespace && t != TokenTypeMultilineComment && t != TokenTypeSinglelineComment {
		l.lastNonIgnoredToken = currentToken
	}

	select {
	case l.tokens <- currentToken:
		l.currentToken = currentToken
		l.start = l.pos

	case <-l.closed:
		return
	}
}

// errorf returns an error token and terminates the scan by passing
// back a nil pointer that will be the next state, terminating l.nexttoken.
func (l *Lexer) errorf(currentRune rune, format string, args ...interface{}) stateFn {
	l.tokens <- Lexeme{TokenTypeError, l.start, string(currentRune), fmt.Sprintf(format, args...)}
	return nil
}

// peekValue looks forward for the given value string. If found, returns true.
func (l *Lexer) peekValue(value string) bool {
	for index, runeValue := range value {
		r := l.next()
		if r != runeValue {
			for j := 0; j <= index; j++ {
				l.backup()
			}
			return false
		}
	}

	for i := 0; i < len(value); i++ {
		l.backup()
	}

	return true
}

// accept consumes the next rune if it's from the valid set.
func (l *Lexer) accept(valid string) bool {
	if nextRune := l.next(); strings.ContainsRune(valid, nextRune) {
		return true
	}
	l.backup()
	return false
}

// acceptString consumes the full given string, if the next tokens in the stream.
func (l *Lexer) acceptString(value string) bool {
	for index, runeValue := range value {
		if l.next() != runeValue {
			for i := 0; i <= index; i++ {
				l.backup()
			}

			return false
		}
	}

	return true
}

// lexSource scans until EOFRUNE
func lexSource(l *Lexer) stateFn {
	return lexerEntrypoint(l)
}

// checkFn returns whether a rune matches for continue looping.
type checkFn func(r rune) (bool, error)

func buildLexUntil(findType TokenType, checker checkFn) stateFn {
	return func(l *Lexer) stateFn {
		for {
			r := l.next()
			isValid, err := checker(r)
			if err != nil {
				return l.errorf(r, "%v", err)
			}
			if !isValid {
				l.backup()
				break
			}
		}

		l.emit(findType)
		return lexSource
	}
}
