package lsp

import baselsp "github.com/sourcegraph/go-lsp"

type InitializeResult struct {
	Capabilities ServerCapabilities `json:"capabilities,omitempty"`
}

type ServerCapabilities struct {
	TextDocumentSync           *baselsp.TextDocumentSyncOptionsOrKind `json:"textDocumentSync,omitempty"`
	CompletionProvider         *baselsp.CompletionOptions             `json:"completionProvider,omitempty"`
	DocumentFormattingProvider bool                                   `json:"documentFormattingProvider,omitempty"`
	DiagnosticProvider         *DiagnosticOptions                     `json:"diagnosticProvider,omitempty"`
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
