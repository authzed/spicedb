package parser

import (
	"testing"
	"time"

	"github.com/authzed/spicedb/pkg/schemadsl/input"
)

// TestDecoratorMalformedParameterRecoveryTerminates guards against a regression in
// skipToDecoratorParametersClose (consumeDecorator's malformed-parameter recovery path):
// a lex error (or an unterminated string, which the lexer also reports as a lex error)
// inside a decorator's parameter list closes the lexer's token channel, after which
// every further read returns the zero Lexeme - Kind TokenTypeError (iota 0). A recovery
// loop that does not treat TokenTypeError as a hard stop will busy-loop forever
// re-observing that same synthetic token instead of returning, hanging whatever called
// Parse (and, transitively, WriteSchema/compiler.Compile - decorator syntax parses
// unconditionally, with no `use` flag required to reach it).
//
// Each case runs Parse on its own goroutine and requires it to return well within the
// test timeout, so a regression shows up as a fast, specific test failure instead of a
// CI job hanging until the suite-level timeout kills it.
func TestDecoratorMalformedParameterRecoveryTerminates(t *testing.T) {
	tcs := []struct {
		name  string
		input string
	}{
		{
			"lex error inside parameter list",
			"use testdecorators\n\n@testall(needed: $)\ndefinition user {}",
		},
		{
			"unterminated string inside parameter list",
			"use testdecorators\n\n@testall(needed: 1, label: \"unterminated)\ndefinition user {}",
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			done := make(chan struct{})
			go func() {
				defer close(done)
				Parse(createAstNode, input.Source(tc.name), tc.input)
			}()

			select {
			case <-done:
				// Parse returned: the recovery loop terminated as required. Note we
				// deliberately do not assert anything about the resulting tree here -
				// this test exists solely to prove termination.
			case <-time.After(5 * time.Second):
				t.Fatalf("Parse(%q) did not return within 5s; the malformed-parameter "+
					"recovery loop in skipToDecoratorParametersClose likely regressed "+
					"into an infinite loop on a lex error", tc.name)
			}
		})
	}
}
