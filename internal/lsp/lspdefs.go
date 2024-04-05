package lsp

import (
	baselsp "github.com/sourcegraph/go-lsp"
)

type InitializeResult struct {
	Capabilities ServerCapabilities `json:"capabilities,omitempty"`
}

type ServerCapabilities struct {
	TextDocumentSync           *baselsp.TextDocumentSyncOptionsOrKind `json:"textDocumentSync,omitempty"`
	CompletionProvider         *baselsp.CompletionOptions             `json:"completionProvider,omitempty"`
	DocumentFormattingProvider bool                                   `json:"documentFormattingProvider,omitempty"`
	DiagnosticProvider         *DiagnosticOptions                     `json:"diagnosticProvider,omitempty"`
	HoverProvider              bool                                   `json:"hoverProvider,omitempty"`
}

type DiagnosticOptions struct {
	Identifier            string `json:"identifier"`
	InterFileDependencies bool   `json:"interFileDependencies"`
	WorkspaceDiagnostics  bool   `json:"workspaceDiagnostics"`
}

type TextDocumentDiagnosticParams struct {
	Identifier   string       `json:"identifier"`
	TextDocument TextDocument `json:"textDocument"`
}

type TextDocument struct {
	URI baselsp.DocumentURI `json:"uri"`
}

type FullDocumentDiagnosticReport struct {
	Kind     string               `json:"kind"`
	Items    []baselsp.Diagnostic `json:"items"`
	ResultID string               `json:"resultId,omitempty"`
}

type InitializeParams struct {
	ProcessID int `json:"processId,omitempty"`

	// RootPath is DEPRECATED in favor of the RootURI field.
	RootPath string `json:"rootPath,omitempty"`

	RootURI               baselsp.DocumentURI `json:"rootUri,omitempty"`
	ClientInfo            baselsp.ClientInfo  `json:"clientInfo,omitempty"`
	Trace                 baselsp.Trace       `json:"trace,omitempty"`
	InitializationOptions interface{}         `json:"initializationOptions,omitempty"`
	Capabilities          ClientCapabilities  `json:"capabilities"`

	WorkDoneToken string `json:"workDoneToken,omitempty"`
}

type ClientCapabilities struct {
	Diagnostics DiagnosticWorkspaceClientCapabilities `json:"diagnostics,omitempty"`
}

type DiagnosticWorkspaceClientCapabilities struct {
	// RefreshSupport indicates whether the client supports the new
	// `textDocument/diagnostic` request.
	RefreshSupport bool `json:"refreshSupport,omitempty"`
}

type Hover struct {
	Contents MarkupContent  `json:"contents"`
	Range    *baselsp.Range `json:"range,omitempty"`
}

type MarkupContent struct {
	Kind     string `json:"kind,omitempty"`
	Language string `json:"language,omitempty"`
	Value    string `json:"value"`
}
