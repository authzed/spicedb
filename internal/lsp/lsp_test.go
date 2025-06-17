package lsp

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/sourcegraph/go-lsp"
	"github.com/sourcegraph/jsonrpc2"
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
	require.Empty(t, resp.Items)
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
	require.Empty(t, resp.Items)
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

func TestTextDocDidSave(t *testing.T) {
	tester := newLSPTester(t)
	tester.initialize()

	tester.setFileContents("file:///test", "definition user{}")

	_, serverState := sendAndReceive[any](tester, "textDocument/didSave", lsp.DidSaveTextDocumentParams{
		TextDocument: lsp.TextDocumentIdentifier{URI: "file:///test"},
	})
	require.Equal(t, serverStateInitialized, serverState)
}

func TestInitialized(t *testing.T) {
	tester := newLSPTester(t)
	tester.initialize()

	_, serverState := sendAndReceive[any](tester, "initialized", struct{}{})
	require.Equal(t, serverStateInitialized, serverState)
}

func TestInitializedBeforeInitialize(t *testing.T) {
	tester := newLSPTester(t)

	lerr, serverState := sendAndExpectError(tester, "initialized", struct{}{})
	require.Equal(t, serverStateNotInitialized, serverState)
	require.Equal(t, codeUninitialized, lerr.Code)
}

func TestShutdown(t *testing.T) {
	tester := newLSPTester(t)
	tester.initialize()

	_, serverState := sendAndReceive[any](tester, "shutdown", struct{}{})
	require.Equal(t, serverStateShuttingDown, serverState)
}

func TestRequestAfterShutdown(t *testing.T) {
	tester := newLSPTester(t)
	tester.initialize()

	sendAndReceive[any](tester, "shutdown", struct{}{})

	_, serverState := sendAndReceive[any](tester, "textDocument/formatting", lsp.FormattingOptions{})
	require.Equal(t, serverStateShuttingDown, serverState)
}

func TestDiagnosticsRefreshSupport(t *testing.T) {
	tester := newLSPTester(t)

	// Initialize with diagnostic refresh support enabled
	resp, serverState := sendAndReceive[lsp.InitializeResult](tester, "initialize", InitializeParams{
		Capabilities: ClientCapabilities{
			Diagnostics: DiagnosticWorkspaceClientCapabilities{
				RefreshSupport: true,
			},
		},
	})
	require.Equal(t, serverStateInitialized, serverState)
	require.True(t, resp.Capabilities.DocumentFormattingProvider)
	require.True(t, tester.server.requestsDiagnostics)

	// Initialize without diagnostic refresh support
	tester2 := newLSPTester(t)
	resp2, serverState2 := sendAndReceive[lsp.InitializeResult](tester2, "initialize", InitializeParams{
		Capabilities: ClientCapabilities{
			Diagnostics: DiagnosticWorkspaceClientCapabilities{
				RefreshSupport: false,
			},
		},
	})
	require.Equal(t, serverStateInitialized, serverState2)
	require.True(t, resp2.Capabilities.DocumentFormattingProvider)
	require.False(t, tester2.server.requestsDiagnostics)
}

func TestLogJSONPtr(t *testing.T) {
	require.Equal(t, "nil", logJSONPtr(nil))

	msg := json.RawMessage(`{"test": "value"}`)
	require.JSONEq(t, `{"test": "value"}`, logJSONPtr(&msg))
}

func TestServerRunInvalidArgs(t *testing.T) {
	server := NewServer()
	ctx := t.Context()

	err := server.Run(ctx, "-", false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot use stdin with stdio disabled")
}

func TestUnmarshalParamsErrors(t *testing.T) {
	// Test with nil params
	r := &jsonrpc2.Request{Method: "test", Params: nil}
	_, err := unmarshalParams[struct{}](r)
	require.Error(t, err)
	require.IsType(t, &jsonrpc2.Error{}, err)
	require.Equal(t, int64(jsonrpc2.CodeInvalidParams), func() *jsonrpc2.Error {
		target := &jsonrpc2.Error{}
		_ = errors.As(err, &target)
		return target
	}().Code)

	// Test with invalid JSON
	invalidJSON := json.RawMessage(`{"invalid": json}`)
	r = &jsonrpc2.Request{Method: "test", Params: &invalidJSON}
	_, err = unmarshalParams[struct{ Valid bool }](r)
	require.Error(t, err)
	require.IsType(t, &jsonrpc2.Error{}, err)
	require.Equal(t, int64(jsonrpc2.CodeInvalidParams), func() *jsonrpc2.Error {
		target := &jsonrpc2.Error{}
		_ = errors.As(err, &target)
		return target
	}().Code)
}

func TestInvalidParams(t *testing.T) {
	err := invalidParams(errors.New("test error"))
	require.Equal(t, int64(jsonrpc2.CodeInvalidParams), err.Code)
	require.Equal(t, "test error", err.Message)
}

func TestInvalidRequest(t *testing.T) {
	err := invalidRequest(errors.New("test error"))
	require.Equal(t, int64(jsonrpc2.CodeInvalidRequest), err.Code)
	require.Equal(t, "test error", err.Message)
}

func TestUnsupportedMethod(t *testing.T) {
	tester := newLSPTester(t)
	tester.initialize()

	// Test unsupported method - should return nil without error
	_, serverState := sendAndReceive[any](tester, "unsupported/method", struct{}{})
	require.Equal(t, serverStateInitialized, serverState)
}

func TestHoverNoReferenceFound(t *testing.T) {
	tester := newLSPTester(t)
	tester.initialize()

	sendAndReceive[any](tester, "textDocument/didOpen", lsp.DidOpenTextDocumentParams{
		TextDocument: lsp.TextDocumentItem{
			URI:        lsp.DocumentURI("file:///test"),
			LanguageID: "test",
			Version:    1,
			Text:       "definition user {}",
		},
	})

	// Test hover at position where no reference exists
	resp, _ := sendAndReceive[*Hover](tester, "textDocument/hover", lsp.TextDocumentPositionParams{
		TextDocument: lsp.TextDocumentIdentifier{
			URI: lsp.DocumentURI("file:///test"),
		},
		Position: lsp.Position{Line: 0, Character: 0},
	})

	require.Nil(t, resp)
}

func TestGetCompiledContentsFileNotFound(t *testing.T) {
	tester := newLSPTester(t)
	tester.initialize()

	// Test hover on non-existent file
	err, _ := sendAndExpectError(tester, "textDocument/hover", lsp.TextDocumentPositionParams{
		TextDocument: lsp.TextDocumentIdentifier{
			URI: lsp.DocumentURI("file:///nonexistent"),
		},
		Position: lsp.Position{Line: 0, Character: 0},
	})
	require.Error(t, err)
	require.Equal(t, int64(jsonrpc2.CodeInternalError), err.Code)
}
