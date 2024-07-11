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
	require.Equal(t, "test", contents.contents)

	tester.setFileContents("file:///test", "test2")

	contents, ok = tester.server.files.Get("file:///test")
	require.True(t, ok)
	require.Equal(t, "test2", contents.contents)
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

func TestDocumentErrorDiagnostics(t *testing.T) {
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

func TestDocumentWarningDiagnostics(t *testing.T) {
	tester := newLSPTester(t)
	tester.initialize()

	tester.setFileContents("file:///test", `
		definition user {}

		definition resource {
			relation viewer: user
			permission view_resource = viewer
		}
	`)

	resp, _ := sendAndReceive[FullDocumentDiagnosticReport](tester, "textDocument/diagnostic",
		TextDocumentDiagnosticParams{
			TextDocument: TextDocument{URI: "file:///test"},
		})
	require.Equal(t, "full", resp.Kind)
	require.Len(t, resp.Items, 1)
	require.Equal(t, lsp.DiagnosticSeverity(lsp.Warning), resp.Items[0].Severity)
	require.Equal(t, `Permission "view_resource" references parent type "resource" in its name; it is recommended to drop the suffix (relation-name-references-parent)`, resp.Items[0].Message)
	require.Equal(t, lsp.Range{
		Start: lsp.Position{Line: 5, Character: 3},
		End:   lsp.Position{Line: 5, Character: 3},
	}, resp.Items[0].Range)
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

	// test formatting malformed content without panicing
	tester.setFileContents("file:///test", "dfinition user{}")
	err, _ := sendAndExpectError(tester, "textDocument/formatting",
		lsp.DocumentFormattingParams{
			TextDocument: lsp.TextDocumentIdentifier{URI: "file:///test"},
		})
	require.Error(t, err)
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
	require.Equal(t, "definition user{}", contents.contents)

	sendAndReceive[any](tester, "textDocument/didClose", lsp.DidCloseTextDocumentParams{
		TextDocument: lsp.TextDocumentIdentifier{
			URI: lsp.DocumentURI("file:///test"),
		},
	})

	_, ok = tester.server.files.Get(lsp.DocumentURI("file:///test"))
	require.False(t, ok)
}

func TestDocumentHover(t *testing.T) {
	tester := newLSPTester(t)
	tester.initialize()

	sendAndReceive[any](tester, "textDocument/didOpen", lsp.DidOpenTextDocumentParams{
		TextDocument: lsp.TextDocumentItem{
			URI:        lsp.DocumentURI("file:///test"),
			LanguageID: "test",
			Version:    1,
			Text: `definition user {}

definition resource {
	relation viewer: user
}
`,
		},
	})

	resp, _ := sendAndReceive[Hover](tester, "textDocument/hover", lsp.TextDocumentPositionParams{
		TextDocument: lsp.TextDocumentIdentifier{
			URI: lsp.DocumentURI("file:///test"),
		},
		Position: lsp.Position{Line: 3, Character: 18},
	})

	require.Equal(t, "definition user {}", resp.Contents.Value)
	require.Equal(t, "spicedb", resp.Contents.Language)

	// test hovering malformed content without panicing
	sendAndReceive[any](tester, "textDocument/didOpen", lsp.DidOpenTextDocumentParams{
		TextDocument: lsp.TextDocumentItem{
			URI:        lsp.DocumentURI("file:///test"),
			LanguageID: "test",
			Version:    1,
			Text: `definition user {}

dfinition resource {
	relation viewer: user
}
`,
		},
	})

	err, _ := sendAndExpectError(tester, "textDocument/hover", lsp.TextDocumentPositionParams{
		TextDocument: lsp.TextDocumentIdentifier{
			URI: lsp.DocumentURI("file:///test"),
		},
		Position: lsp.Position{Line: 3, Character: 18},
	})
	require.Error(t, err)
}
