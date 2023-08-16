package lsp

import (
	"testing"

	"github.com/sourcegraph/go-lsp"
	"github.com/stretchr/testify/require"
)

func TestInitialize(t *testing.T) {
	tester := newLSPTester(t)
	tester.initialize()
}

func TestAnotherCommandBeforeInitialize(t *testing.T) {
	tester := newLSPTester(t)

	lerr, serverState := sendAndExpectError(tester, "textDocument/formatting", lsp.FormattingOptions{})
	require.Equal(t, serverStateNotInitialized, serverState)
	require.Equal(t, codeUninitialized, lerr.Code)
}

func TestDocumentChange(t *testing.T) {
	tester := newLSPTester(t)
	tester.initialize()

	tester.setFileContents("file:///test", "test")

	contents, ok := tester.server.files.Get("file:///test")
	require.True(t, ok)
	require.Equal(t, "test", contents)

	tester.setFileContents("file:///test", "test2")

	contents, ok = tester.server.files.Get("file:///test")
	require.True(t, ok)
	require.Equal(t, "test2", contents)
}

func TestDocumentNoDiagnostics(t *testing.T) {
	tester := newLSPTester(t)
	tester.initialize()

	tester.setFileContents("file:///test", "definition user{}")

	resp, _ := sendAndReceive[FullDocumentDiagnosticReport](tester, "textDocument/diagnostic",
		TextDocumentDiagnosticParams{
			TextDocument: TextDocument{URI: "file:///test"},
		})
	require.Equal(t, "full", resp.Kind)
	require.Len(t, resp.Items, 0)
}

func TestDocumentDiagnostics(t *testing.T) {
	tester := newLSPTester(t)
	tester.initialize()

	tester.setFileContents("file:///test", "test")

	resp, _ := sendAndReceive[FullDocumentDiagnosticReport](tester, "textDocument/diagnostic",
		TextDocumentDiagnosticParams{
			TextDocument: TextDocument{URI: "file:///test"},
		})
	require.Equal(t, "full", resp.Kind)
	require.Len(t, resp.Items, 1)
	require.Equal(t, lsp.Error, resp.Items[0].Severity)
	require.Equal(t, "Unexpected token at root level: TokenTypeIdentifier", resp.Items[0].Message)
	require.Equal(t, lsp.Range{
		Start: lsp.Position{Line: 0, Character: 0},
		End:   lsp.Position{Line: 0, Character: 0},
	}, resp.Items[0].Range)

	tester.setFileContents("file:///test", "definition user{}")

	resp, _ = sendAndReceive[FullDocumentDiagnosticReport](tester, "textDocument/diagnostic",
		TextDocumentDiagnosticParams{
			TextDocument: TextDocument{URI: "file:///test"},
		})
	require.Equal(t, "full", resp.Kind)
	require.Len(t, resp.Items, 0)
}

func TestDocumentDiagnosticsForTypeError(t *testing.T) {
	tester := newLSPTester(t)
	tester.initialize()

	tester.setFileContents("file:///test", `definition user {}

	definition resource {
		relation foo: something
	}
`)

	resp, _ := sendAndReceive[FullDocumentDiagnosticReport](tester, "textDocument/diagnostic",
		TextDocumentDiagnosticParams{
			TextDocument: TextDocument{URI: "file:///test"},
		})
	require.Equal(t, "full", resp.Kind)
	require.Len(t, resp.Items, 1)
	require.Equal(t, lsp.Error, resp.Items[0].Severity)
	require.Contains(t, resp.Items[0].Message, "object definition `something` not found")
	require.Equal(t, lsp.Range{
		Start: lsp.Position{Line: 3, Character: 16},
		End:   lsp.Position{Line: 3, Character: 16},
	}, resp.Items[0].Range)
}

func TestDocumentFormat(t *testing.T) {
	tester := newLSPTester(t)
	tester.initialize()

	tester.setFileContents("file:///test", "definition user{}")

	resp, _ := sendAndReceive[[]lsp.TextEdit](tester, "textDocument/formatting",
		lsp.DocumentFormattingParams{
			TextDocument: lsp.TextDocumentIdentifier{URI: "file:///test"},
		})
	require.Len(t, resp, 1)
	require.Equal(t, lsp.Range{
		Start: lsp.Position{Line: 0, Character: 0},
		End:   lsp.Position{Line: 10000000, Character: 100000000},
	}, resp[0].Range)
	require.Equal(t, "definition user {}", resp[0].NewText)
}

func TestDocumentOpenedClosed(t *testing.T) {
	tester := newLSPTester(t)
	tester.initialize()

	sendAndReceive[any](tester, "textDocument/didOpen", lsp.DidOpenTextDocumentParams{
		TextDocument: lsp.TextDocumentItem{
			URI:        lsp.DocumentURI("file:///test"),
			LanguageID: "test",
			Version:    1,
			Text:       "definition user{}",
		},
	})

	contents, ok := tester.server.files.Get(lsp.DocumentURI("file:///test"))
	require.True(t, ok)
	require.Equal(t, "definition user{}", contents)

	sendAndReceive[any](tester, "textDocument/didClose", lsp.DidCloseTextDocumentParams{
		TextDocument: lsp.TextDocumentIdentifier{
			URI: lsp.DocumentURI("file:///test"),
		},
	})

	_, ok = tester.server.files.Get(lsp.DocumentURI("file:///test"))
	require.False(t, ok)
}
