package lsp

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/sourcegraph/go-lsp"
	"github.com/sourcegraph/jsonrpc2"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/development"
	developerv1 "github.com/authzed/spicedb/pkg/proto/developer/v1"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
)

func TestNewLspDiagnostic(t *testing.T) {
	diag := newLspDiagnostic(&developerv1.DeveloperError{
		Line:    5,
		Column:  10,
		Message: "something went wrong",
	}, lsp.Error)

	require.Equal(t, lsp.Error, diag.Severity)
	require.Equal(t, "something went wrong", diag.Message)
	require.Equal(t, lsp.Position{Line: 4, Character: 9}, diag.Range.Start)
	require.Equal(t, lsp.Position{Line: 4, Character: 9}, diag.Range.End)
}

func TestNewLspRange(t *testing.T) {
	ref := &development.SchemaReference{
		Text: "viewer",
		TargetPosition: &input.Position{
			LineNumber:     3,
			ColumnPosition: 1,
		},
		TargetNamePositionOffset: len("relation "),
	}

	r := newLspRange(ref, schemaOffset{})
	// Start character = ColumnPosition (1) + offset (9) = 10
	require.Equal(t, lsp.Position{Line: 3, Character: 10}, r.Start)
	// End character = Start (10) + len("viewer") (6) = 16
	require.Equal(t, lsp.Position{Line: 3, Character: 16}, r.End)
}

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
	// Initialize with textDocument diagnostic support enabled
	tester := newLSPTester(t)
	resp, serverState := sendAndReceive[lsp.InitializeResult](tester, "initialize", InitializeParams{
		Capabilities: ClientCapabilities{
			TextDocument: &TextDocumentClientCapabilities{
				Diagnostic: &DiagnosticClientCapabilities{},
			},
		},
	})
	require.Equal(t, serverStateInitialized, serverState)
	require.True(t, resp.Capabilities.DocumentFormattingProvider)
	require.True(t, tester.server.requestsDiagnostics)

	// Initialize with only workspace diagnostic refresh support (not pull diagnostics)
	tester2 := newLSPTester(t)
	resp2, serverState2 := sendAndReceive[lsp.InitializeResult](tester2, "initialize", InitializeParams{
		Capabilities: ClientCapabilities{
			Workspace: &WorkspaceClientCapabilities{
				Diagnostics: &DiagnosticWorkspaceClientCapabilities{
					RefreshSupport: true,
				},
			},
		},
	})
	require.Equal(t, serverStateInitialized, serverState2)
	require.True(t, resp2.Capabilities.DocumentFormattingProvider)
	require.False(t, tester2.server.requestsDiagnostics)

	// Initialize without any diagnostic support
	tester3 := newLSPTester(t)
	resp3, serverState3 := sendAndReceive[lsp.InitializeResult](tester3, "initialize", InitializeParams{
		Capabilities: ClientCapabilities{},
	})
	require.Equal(t, serverStateInitialized, serverState3)
	require.True(t, resp3.Capabilities.DocumentFormattingProvider)
	require.False(t, tester3.server.requestsDiagnostics)
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
	var jError *jsonrpc2.Error
	require.ErrorAs(t, err, &jError)
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
	require.ErrorAs(t, err, &jError)
	require.Equal(t, int64(jsonrpc2.CodeInvalidParams), func() *jsonrpc2.Error {
		target := &jsonrpc2.Error{}
		_ = errors.As(err, &target)
		return target
	}().Code)
}

func TestMultiFileNoDiagnostics(t *testing.T) {
	tester := newLSPTester(t)
	tester.initialize()

	tester.setFileContents("file:///testdir/users.zed", "definition user {}")
	tester.setFileContents("file:///testdir/root.zed", `use import

import "users.zed"

definition resource {
	relation viewer: user
	permission view = viewer
}
`)

	resp, _ := sendAndReceive[FullDocumentDiagnosticReport](tester, "textDocument/diagnostic",
		TextDocumentDiagnosticParams{
			TextDocument: TextDocument{URI: "file:///testdir/root.zed"},
		})
	require.Equal(t, "full", resp.Kind)
	require.Empty(t, resp.Items)
}

func TestMultiFileUndefinedDefinitionDiagnostics(t *testing.T) {
	tester := newLSPTester(t)
	tester.initialize()

	tester.setFileContents("file:///testdir/broken1.zed", `
definition resource {
	relation viewer: organization
}`)
	tester.setFileContents("file:///testdir/broken2.zed", `
use partial
partial secret {
	relation viewer: organization
}`)
	tester.setFileContents("file:///testdir/root.zed", `use import

import "broken1.zed"
import "broken2.zed"
`)

	// root.zed is fine, no errors
	resp, _ := sendAndReceive[FullDocumentDiagnosticReport](tester, "textDocument/diagnostic",
		TextDocumentDiagnosticParams{
			TextDocument: TextDocument{URI: "file:///testdir/root.zed"},
		})
	require.Equal(t, "full", resp.Kind)
	require.Empty(t, resp.Items)

	// broken1.zed has one error
	resp, _ = sendAndReceive[FullDocumentDiagnosticReport](tester, "textDocument/diagnostic",
		TextDocumentDiagnosticParams{
			TextDocument: TextDocument{URI: "file:///testdir/broken1.zed"},
		})
	require.Equal(t, "full", resp.Kind)
	require.Len(t, resp.Items, 1)
	require.Equal(t, lsp.Error, resp.Items[0].Severity)
	require.Contains(t, resp.Items[0].Message, "could not lookup definition `organization` for relation `viewer`: object definition `organization` not found")

	// TODO this doesn't pass
	//// broken2.zed has one error
	// resp, _ = sendAndReceive[FullDocumentDiagnosticReport](tester, "textDocument/diagnostic",
	//	TextDocumentDiagnosticParams{
	//		TextDocument: TextDocument{URI: "file:///testdir/broken2.zed"},
	//	})
	// require.Equal(t, "full", resp.Kind)
	// require.Len(t, resp.Items, 1)
	// require.Equal(t, lsp.Error, resp.Items[0].Severity)
	// require.Contains(t, resp.Items[0].Message, "could not lookup definition `organization` for relation `viewer`: object definition `organization` not found")}
}

func TestMultiFileBrokenImportDiagnostics(t *testing.T) {
	tester := newLSPTester(t)
	tester.initialize()

	tester.setFileContents("file:///testdir/root.zed", `use import
import "unknown.zed"
`)

	resp, _ := sendAndReceive[FullDocumentDiagnosticReport](tester, "textDocument/diagnostic",
		TextDocumentDiagnosticParams{
			TextDocument: TextDocument{URI: "file:///testdir/root.zed"},
		})
	require.Equal(t, "full", resp.Kind)
	require.Len(t, resp.Items, 1)
	require.Equal(t, lsp.Error, resp.Items[0].Severity)
	require.Contains(t, resp.Items[0].Message, "failed to read import \"unknown.zed\": open unknown.zed: no such file or director")
}

func TestDefinitionSameFileTypeReference(t *testing.T) {
	tester := newLSPTester(t)
	tester.initialize()

	tester.setFileContents("file:///test", `definition user {}

definition resource {
	relation viewer: user
	permission view = viewer
}
`)

	// Click on "user" in "relation viewer: user" (line 3, character 18)
	resp, _ := sendAndReceive[lsp.Location](tester, "textDocument/definition",
		lsp.TextDocumentPositionParams{
			TextDocument: lsp.TextDocumentIdentifier{URI: "file:///test"},
			Position:     lsp.Position{Line: 3, Character: 18},
		})
	require.Equal(t, lsp.DocumentURI("file:///test"), resp.URI)
	require.Equal(t, 0, resp.Range.Start.Line)
	require.Equal(t, len("definition "), resp.Range.Start.Character)
}

func TestDefinitionSameFileRelationReference(t *testing.T) {
	tester := newLSPTester(t)
	tester.initialize()

	tester.setFileContents("file:///test", `definition user {}

definition resource {
	relation viewer: user
	permission view = viewer
}
`)

	// Click on "viewer" in "permission view = viewer" (line 4, character 19)
	resp, _ := sendAndReceive[lsp.Location](tester, "textDocument/definition",
		lsp.TextDocumentPositionParams{
			TextDocument: lsp.TextDocumentIdentifier{URI: "file:///test"},
			Position:     lsp.Position{Line: 4, Character: 19},
		})
	require.Equal(t, lsp.DocumentURI("file:///test"), resp.URI)
	require.Equal(t, 3, resp.Range.Start.Line)
	require.Equal(t, len("\trelation "), resp.Range.Start.Character)
}

func TestDefinitionCrossFileTypeReference(t *testing.T) {
	tester := newLSPTester(t)
	tester.initialize()

	tester.setFileContents("file:///testdir/users.zed", "definition user {}")
	tester.setFileContents("file:///testdir/root.zed", `use import

import "users.zed"

definition resource {
	relation viewer: user
	permission view = viewer
}
`)

	// Click on "user" in "relation viewer: user" (line 5, character 18)
	// It should point to "users.zed"
	resp, _ := sendAndReceive[lsp.Location](tester, "textDocument/definition",
		lsp.TextDocumentPositionParams{
			TextDocument: lsp.TextDocumentIdentifier{URI: "file:///testdir/root.zed"},
			Position:     lsp.Position{Line: 5, Character: 18},
		})
	require.Equal(t, lsp.DocumentURI("file:///testdir/users.zed"), resp.URI)
	require.Equal(t, 0, resp.Range.Start.Line)
	require.Equal(t, len("definition "), resp.Range.Start.Character)
}

func TestDefinitionImportReference(t *testing.T) {
	tester := newLSPTester(t)
	tester.initialize()

	tester.setFileContents("file:///testdir/users.zed", "definition user {}")
	tester.setFileContents("file:///testdir/root.zed", `use import

import "users.zed"

definition resource {
	relation viewer: user
	permission view = viewer
}
`)

	// Click on import "users.zed" (line 2, character 10)
	// It should point on the very begginning of "users.zed"
	resp, _ := sendAndReceive[lsp.Location](tester, "textDocument/definition",
		lsp.TextDocumentPositionParams{
			TextDocument: lsp.TextDocumentIdentifier{URI: "file:///testdir/root.zed"},
			Position:     lsp.Position{Line: 2, Character: 10},
		})
	require.Equal(t, lsp.DocumentURI("file:///testdir/users.zed"), resp.URI)
	require.Equal(t, 0, resp.Range.Start.Line)
	require.Equal(t, 0, resp.Range.Start.Character)
}

func TestDefinitionCrossFileCaveatReference(t *testing.T) {
	tester := newLSPTester(t)
	tester.initialize()

	tester.setFileContents("file:///testdir/caveats.zed", `caveat some_caveat(some_param int) {
	some_param < 100
}`)
	tester.setFileContents("file:///testdir/root.zed", `use import

import "caveats.zed"

definition user {}

definition resource {
	relation viewer: user with some_caveat
}
`)

	// Click on "some_caveat" in "relation viewer: user with some_caveat" (line 7, character 30)
	resp, _ := sendAndReceive[lsp.Location](tester, "textDocument/definition",
		lsp.TextDocumentPositionParams{
			TextDocument: lsp.TextDocumentIdentifier{URI: "file:///testdir/root.zed"},
			Position:     lsp.Position{Line: 7, Character: 30},
		})
	require.Equal(t, lsp.DocumentURI("file:///testdir/caveats.zed"), resp.URI)
	require.Equal(t, 0, resp.Range.Start.Line)
	require.Equal(t, len("caveat "), resp.Range.Start.Character)
}

func TestDefinitionCrossFilePartialReference(t *testing.T) {
	tester := newLSPTester(t)
	tester.initialize()

	tester.setFileContents("file:///testdir/imported.zed", `use partial
definition user {}

partial secret {
	relation secretview: user
}`)
	tester.setFileContents("file:///testdir/root.zed", `use import
use partial

import "imported.zed"

definition resource {
	...secret
	permission secret = secretview
}
`)

	// Click on "secretview" in "permission secret = secretview"
	resp, _ := sendAndReceive[lsp.Location](tester, "textDocument/definition",
		lsp.TextDocumentPositionParams{
			TextDocument: lsp.TextDocumentIdentifier{URI: "file:///testdir/root.zed"},
			Position:     lsp.Position{Line: 7, Character: 26},
		})
	// require.Equal(t, lsp.DocumentURI("file:///testdir/imported.zed"), resp.URI) // TODO this fails
	require.Equal(t, 4, resp.Range.Start.Line)
	require.Equal(t, len("\trelation "), resp.Range.Start.Character)
}

func TestDefinitionNoReference(t *testing.T) {
	tester := newLSPTester(t)
	tester.initialize()

	tester.setFileContents("file:///test", "definition user {}")

	// Click on whitespace / keyword where no reference exists
	resp, _ := sendAndReceive[*lsp.Location](tester, "textDocument/definition",
		lsp.TextDocumentPositionParams{
			TextDocument: lsp.TextDocumentIdentifier{URI: "file:///test"},
			Position:     lsp.Position{Line: 0, Character: 0},
		})
	require.Nil(t, resp)
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

func TestHoverDuplicateDefinitionAcrossFiles(t *testing.T) {
	tester := newLSPTester(t)
	tester.initialize()

	tester.setFileContents("file:///testdir/users.zed", "definition user {}")
	tester.setFileContents("file:///testdir/root.zed", `use import

import "users.zed"

definition user {}`)

	// Hover over "definition user" in root.zed (line 4, character 14)
	err, _ := sendAndExpectError(tester, "textDocument/hover", lsp.TextDocumentPositionParams{
		TextDocument: lsp.TextDocumentIdentifier{
			URI: lsp.DocumentURI("file:///testdir/root.zed"),
		},
		Position: lsp.Position{Line: 4, Character: 14},
	})
	require.Contains(t, err.Message, "found name reused between multiple definitions and/or caveats: user")
}

func TestYAMLDiagnosticsInlineSchemaError(t *testing.T) {
	tester := newLSPTester(t)
	tester.initialize()

	// The schema has an error on the 3rd line: "relation viewer: nonexistent".
	// Schema line 3 + YAML node offset 1 = adjusted line 4 (1-indexed) → LSP line 3 (0-indexed).
	tester.setFileContents("file:///testdir/test.yaml", `schema: |
  definition user {}
  definition resource {
    relation viewer: nonexistent
  }
relationships: ""
`)

	resp, _ := sendAndReceive[FullDocumentDiagnosticReport](tester, "textDocument/diagnostic",
		TextDocumentDiagnosticParams{
			TextDocument: TextDocument{URI: "file:///testdir/test.yaml"},
		})
	require.Equal(t, "full", resp.Kind)
	require.Len(t, resp.Items, 1)
	require.Equal(t, lsp.Error, resp.Items[0].Severity)
	require.Contains(t, resp.Items[0].Message, "object definition `nonexistent` not found")
	// Schema line 3 + YAML offset 1 = line 4 (1-indexed) → LSP line 3 (0-indexed)
	// Schema column 19 (0-indexed, after "  relation viewer: ") + YAML indent 2 = 21
	require.Equal(t, 3, resp.Items[0].Range.Start.Line)
	require.Equal(t, 21, resp.Items[0].Range.Start.Character)
}

func TestYAMLDiagnosticsSchemaFileRelationshipError(t *testing.T) {
	tester := newLSPTester(t)
	tester.initialize()

	tester.setFileContents("file:///testdir/schema.zed", `definition user {}
definition document {
  relation viewer: user
}
`)
	tester.setFileContents("file:///testdir/test.yaml", `schemaFile: schema.zed
relationships: |
  document:doc1#nonexistent@user:alice
`)

	resp, _ := sendAndReceive[FullDocumentDiagnosticReport](tester, "textDocument/diagnostic",
		TextDocumentDiagnosticParams{
			TextDocument: TextDocument{URI: "file:///testdir/test.yaml"},
		})
	require.Equal(t, "full", resp.Kind)
	require.Len(t, resp.Items, 1)
	require.Equal(t, lsp.Error, resp.Items[0].Severity)
	require.Contains(t, resp.Items[0].Message, "nonexistent")
	// Relationship is on YAML line 3 (1-indexed) → LSP line 2 (0-indexed)
	// "nonexistent" is the relation name: document:doc1#nonexistent@user:alice
	// 2-space indent + len("document:doc1#") = 2 + 14 = 16 (start)
	// 16 + len("nonexistent") = 27 (end)
	require.Equal(t, 2, resp.Items[0].Range.Start.Line)
	require.Equal(t, 16, resp.Items[0].Range.Start.Character)
	require.Equal(t, 27, resp.Items[0].Range.End.Character)
}

func TestYAMLDiagnosticsUnknownObjectType(t *testing.T) {
	tester := newLSPTester(t)
	tester.initialize()

	tester.setFileContents("file:///testdir/test.yaml", `---
schema: |-
  definition platform {
      relation administrator: user
      permission super_admin = administrator
  }

  definition organization {
    // The platform is generally a singleton pointing to the same
    // platform object, on which the superuser is in turn granted
    // access.
      relation platform: platform
      permission admin = platform->super_admin
  }

  definition resource {
      relation owner: user | organization
      permission admin = owner + owner->admin
  }

  definition user {}
relationships: |-
  platform:evilempire#administrator@user:drevil
  organization:virtucon#platform@unknown:evilempire
  resource:lasers#owner@organization:virtucon
assertions:
  assertTrue:
    - "resource:lasers#admin@user:drevil"
`)

	resp, _ := sendAndReceive[FullDocumentDiagnosticReport](tester, "textDocument/diagnostic",
		TextDocumentDiagnosticParams{
			TextDocument: TextDocument{URI: "file:///testdir/test.yaml"},
		})
	require.Equal(t, "full", resp.Kind)
	require.Len(t, resp.Items, 1)
	require.Equal(t, lsp.Error, resp.Items[0].Severity)
	require.Contains(t, resp.Items[0].Message, "unknown")
	// Error is on YAML line 24 (1-indexed) → LSP line 23 (0-indexed)
	require.Equal(t, 23, resp.Items[0].Range.Start.Line)
	// "unknown" is the subject type: organization:virtucon#platform@unknown:evilempire
	// 2-space indent + len("organization:virtucon#platform@") = 2 + 31 = 33 (start)
	// 33 + len("unknown") = 40 (end)
	require.Equal(t, 33, resp.Items[0].Range.Start.Character)
	require.Equal(t, 40, resp.Items[0].Range.End.Character)
}

func TestYAMLDiagnosticsUnknownResourceType(t *testing.T) {
	tester := newLSPTester(t)
	tester.initialize()

	tester.setFileContents("file:///testdir/test.yaml", `---
schema: |-
  definition platform {
      relation administrator: user
      permission super_admin = administrator
  }

  definition organization {
      relation platform: platform
      permission admin = platform->super_admin
  }

  definition resource {
      relation owner: user | organization
      permission admin = owner + owner->admin
  }

  definition user {}
relationships: |-
  platform:evilempire#administrator@user:drevil
  aaaaa:virtucon#platform@platform:evilempire
  resource:lasers#owner@organization:virtucon
assertions:
  assertTrue:
    - "resource:lasers#admin@user:drevil"
`)

	resp, _ := sendAndReceive[FullDocumentDiagnosticReport](tester, "textDocument/diagnostic",
		TextDocumentDiagnosticParams{
			TextDocument: TextDocument{URI: "file:///testdir/test.yaml"},
		})
	require.Equal(t, "full", resp.Kind)
	require.Len(t, resp.Items, 1)
	require.Equal(t, lsp.Error, resp.Items[0].Severity)
	require.Contains(t, resp.Items[0].Message, "aaaaa")
	// Error is on YAML line 21 (1-indexed) → LSP line 20 (0-indexed)
	require.Equal(t, 20, resp.Items[0].Range.Start.Line)
	// "aaaaa" is the resource type: aaaaa:virtucon#platform@platform:evilempire
	// 2-space indent + 0 = 2 (start), 2 + len("aaaaa") = 7 (end)
	require.Equal(t, 2, resp.Items[0].Range.Start.Character)
	require.Equal(t, 7, resp.Items[0].Range.End.Character)
}

func TestYAMLDiagnosticsInvalidSubjectType(t *testing.T) {
	tester := newLSPTester(t)
	tester.initialize()

	tester.setFileContents("file:///testdir/test.yaml", `schema: |
  definition user {}
  definition organization {}
  definition document {
    relation viewer: user
  }
relationships: |
  document:doc1#viewer@organization:foo
`)

	resp, _ := sendAndReceive[FullDocumentDiagnosticReport](tester, "textDocument/diagnostic",
		TextDocumentDiagnosticParams{
			TextDocument: TextDocument{URI: "file:///testdir/test.yaml"},
		})
	require.Equal(t, "full", resp.Kind)
	require.Len(t, resp.Items, 1)
	require.Equal(t, lsp.Error, resp.Items[0].Severity)
	require.Contains(t, resp.Items[0].Message, "organization")
	// Relationship is on YAML line 8 (1-indexed) → LSP line 7 (0-indexed)
	require.Equal(t, 7, resp.Items[0].Range.Start.Line)
	// "organization" is the subject type: document:doc1#viewer@organization:foo
	// 2-space indent + len("document:doc1#viewer@") = 2 + 21 = 23 (start)
	// 23 + len("organization") = 35 (end)
	require.Equal(t, 23, resp.Items[0].Range.Start.Character)
	require.Equal(t, 35, resp.Items[0].Range.End.Character)
}

func TestYAMLDiagnosticsUndefinedType(t *testing.T) {
	tester := newLSPTester(t)
	tester.initialize()

	tester.setFileContents("file:///testdir/test.yaml", `---
schema: |-
  definition doc {
    relation vieww: user
  }
relationships: |-
  doc:1#vieww@user:maria
`)

	resp, _ := sendAndReceive[FullDocumentDiagnosticReport](tester, "textDocument/diagnostic",
		TextDocumentDiagnosticParams{
			TextDocument: TextDocument{URI: "file:///testdir/test.yaml"},
		})
	require.Equal(t, "full", resp.Kind)
	require.NotEmpty(t, resp.Items)
	require.Equal(t, lsp.Error, resp.Items[0].Severity)
	t.Log(resp.Items[0].Message)
	require.Contains(t, resp.Items[0].Message, "could not lookup definition `user` for relation `vieww`: object definition `user` not found")
}

func TestYAMLDiagnosticsSchemaFileNotFound(t *testing.T) {
	tester := newLSPTester(t)
	tester.initialize()

	tester.setFileContents("file:///testdir/test.yaml", `---
schemaFile: "witherror.zed"
`)

	lerr, serverState := sendAndExpectError(tester, "textDocument/diagnostic",
		TextDocumentDiagnosticParams{
			TextDocument: TextDocument{URI: "file:///testdir/test.yaml"},
		})
	require.Equal(t, serverStateInitialized, serverState)
	require.Contains(t, lerr.Message, "open witherror.zed: no such file or directory")
}

func TestYAMLDiagnosticsAssertionFailure(t *testing.T) {
	tester := newLSPTester(t)
	tester.initialize()

	tester.setFileContents("file:///testdir/test.yaml", `schema: |
  definition user {}
  definition document {
    relation viewer: user
    permission view = viewer
  }
relationships: |
  document:doc1#viewer@user:alice
assertions:
  assertTrue:
    - "document:doc1#view@user:bob"
`)

	resp, _ := sendAndReceive[FullDocumentDiagnosticReport](tester, "textDocument/diagnostic",
		TextDocumentDiagnosticParams{
			TextDocument: TextDocument{URI: "file:///testdir/test.yaml"},
		})
	require.Equal(t, "full", resp.Kind)
	require.Len(t, resp.Items, 1)
	require.Equal(t, lsp.Error, resp.Items[0].Severity)
	require.Contains(t, resp.Items[0].Message, "Expected relation or permission document:doc1#view@user:bob to exist")
	// Assertion is on YAML line 11 (1-indexed) → LSP line 10 (0-indexed)
	require.Equal(t, 10, resp.Items[0].Range.Start.Line)
	// Highlights the full line content: `    - "document:doc1#view@user:bob"`
	// Start at character 4 (first non-whitespace), end at 35 (end of content)
	require.Equal(t, 4, resp.Items[0].Range.Start.Character)
	require.Equal(t, len(`    - "document:doc1#view@user:bob"`), resp.Items[0].Range.End.Character)
}

func TestYAMLHoverInlineSchema(t *testing.T) {
	tester := newLSPTester(t)
	tester.initialize()

	sendAndReceive[any](tester, "textDocument/didOpen", lsp.DidOpenTextDocumentParams{
		TextDocument: lsp.TextDocumentItem{
			URI:        lsp.DocumentURI("file:///testdir/test.yaml"),
			LanguageID: "yaml",
			Version:    1,
			Text: `schema: |
  definition user {}
  definition resource {
    relation viewer: user
  }
relationships: ""
`,
		},
	})

	// Hover over "user" in "relation viewer: user" (YAML line 3, character 21)
	resp, _ := sendAndReceive[Hover](tester, "textDocument/hover", lsp.TextDocumentPositionParams{
		TextDocument: lsp.TextDocumentIdentifier{
			URI: lsp.DocumentURI("file:///testdir/test.yaml"),
		},
		Position: lsp.Position{Line: 3, Character: 21},
	})

	require.Equal(t, "definition user {}", resp.Contents.Value)
	require.Equal(t, "spicedb", resp.Contents.Language)
}

func TestYAMLHoverOutsideSchemaBlock(t *testing.T) {
	tester := newLSPTester(t)
	tester.initialize()

	sendAndReceive[any](tester, "textDocument/didOpen", lsp.DidOpenTextDocumentParams{
		TextDocument: lsp.TextDocumentItem{
			URI:        lsp.DocumentURI("file:///testdir/test.yaml"),
			LanguageID: "yaml",
			Version:    1,
			Text: `---
schema: |-
  definition user {}
  definition doc {
    relation view: user
    }

assertions:
  assertTrue:
    - "doc:1#admin@user:maria"

relationships: |-
  doc:1#view@user:maria
`,
		},
	})

	// Hover over the assertion line (line 9) — outside the schema block.
	// Should return nil, not an error.
	resp, _ := sendAndReceive[*Hover](tester, "textDocument/hover", lsp.TextDocumentPositionParams{
		TextDocument: lsp.TextDocumentIdentifier{
			URI: lsp.DocumentURI("file:///testdir/test.yaml"),
		},
		Position: lsp.Position{Line: 9, Character: 10},
	})
	require.Nil(t, resp)
}

func TestYAMLDiagnosticsSchemaFileSyntaxError(t *testing.T) {
	tester := newLSPTester(t)
	tester.initialize()

	tester.setFileContents("file:///testdir/schema.zed", `definition user {}
definition resource {
  this is invalid syntax
}
`)
	tester.setFileContents("file:///testdir/test.yaml", `schemaFile: schema.zed
relationships: ""
`)

	// Schema errors from an external schemaFile should NOT appear on the YAML file.
	yamlResp, _ := sendAndReceive[FullDocumentDiagnosticReport](tester, "textDocument/diagnostic",
		TextDocumentDiagnosticParams{
			TextDocument: TextDocument{URI: "file:///testdir/test.yaml"},
		})
	require.Equal(t, "full", yamlResp.Kind)
	require.Empty(t, yamlResp.Items)

	// The error should appear on the .zed file instead.
	zedResp, _ := sendAndReceive[FullDocumentDiagnosticReport](tester, "textDocument/diagnostic",
		TextDocumentDiagnosticParams{
			TextDocument: TextDocument{URI: "file:///testdir/schema.zed"},
		})
	require.Equal(t, "full", zedResp.Kind)
	require.Len(t, zedResp.Items, 1)
	require.Equal(t, lsp.Error, zedResp.Items[0].Severity)
	// Syntax error on line 3 of schema.zed (1-indexed) → LSP line 2 (0-indexed)
	require.Equal(t, 2, zedResp.Items[0].Range.Start.Line)
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

func TestYAMLDiagnosticsSchemaFileErrorNotShownOnYAML(t *testing.T) {
	tester := newLSPTester(t)
	tester.initialize()

	tester.setFileContents("file:///testdir/witherror.zed", `definition document {
  relation viewer: user
}
`)
	tester.setFileContents("file:///testdir/test.yaml", `schemaFile: "witherror.zed"
`)

	// Schema errors from an external schemaFile should NOT appear as
	// diagnostics on the YAML file — they belong to the .zed file.
	resp, _ := sendAndReceive[FullDocumentDiagnosticReport](tester, "textDocument/diagnostic",
		TextDocumentDiagnosticParams{
			TextDocument: TextDocument{URI: "file:///testdir/test.yaml"},
		})
	require.Equal(t, "full", resp.Kind)
	require.Empty(t, resp.Items)
}

func TestDidChangeWatchedFilesPushDiagnostics(t *testing.T) {
	tester := newLSPTester(t)

	// Initialize WITHOUT pull diagnostics so the push path is exercised.
	sendAndReceive[lsp.InitializeResult](tester, "initialize", InitializeParams{
		Capabilities: ClientCapabilities{},
	})
	require.False(t, tester.server.requestsDiagnostics)

	tester.setFileContents("file:///test.zed", "definition user{}")

	// sendAndReceive skips server notifications, so the publishDiagnostics
	// notification sent by the push path is consumed transparently.
	resp, serverState := sendAndReceive[any](tester, "workspace/didChangeWatchedFiles", lsp.DidChangeWatchedFilesParams{
		Changes: []lsp.FileEvent{{URI: "file:///test.zed", Type: 2}},
	})

	require.Nil(t, resp)
	require.Equal(t, serverStateInitialized, serverState)
}
