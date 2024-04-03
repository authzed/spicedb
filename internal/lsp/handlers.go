package lsp

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jzelinskie/persistent"
	"github.com/sourcegraph/go-lsp"
	"github.com/sourcegraph/jsonrpc2"

	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/development"
	developerv1 "github.com/authzed/spicedb/pkg/proto/developer/v1"
	"github.com/authzed/spicedb/pkg/schemadsl/generator"
)

func (s *Server) textDocDiagnostic(ctx context.Context, r *jsonrpc2.Request) (FullDocumentDiagnosticReport, error) {
	params, err := unmarshalParams[TextDocumentDiagnosticParams](r)
	if err != nil {
		return FullDocumentDiagnosticReport{}, err
	}

	log.Info().
		Str("method", "textDocument/diagnostic").
		Str("uri", string(params.TextDocument.URI)).
		Msg("textDocDiagnostic")

	diagnostics, err := s.computeDiagnostics(ctx, params.TextDocument.URI)
	if err != nil {
		return FullDocumentDiagnosticReport{}, err
	}

	log.Info().
		Str("uri", string(params.TextDocument.URI)).
		Int("diagnostics", len(diagnostics)).
		Msg("diagnostics complete")

	return FullDocumentDiagnosticReport{
		Kind:  "full",
		Items: diagnostics,
	}, nil
}

func (s *Server) computeDiagnostics(ctx context.Context, uri lsp.DocumentURI) ([]lsp.Diagnostic, error) {
	diagnostics := make([]lsp.Diagnostic, 0) // Important: must not be nil for the consumer on the client side
	if err := s.withFiles(func(files *persistent.Map[lsp.DocumentURI, string]) error {
		file, ok := files.Get(uri)
		if !ok {
			log.Warn().
				Str("uri", string(uri)).
				Msg("file not found for diagnostics")

			return &jsonrpc2.Error{Code: jsonrpc2.CodeInternalError, Message: "file not found"}
		}

		_, devErrs, err := development.NewDevContext(ctx, &developerv1.RequestContext{
			Schema:        file,
			Relationships: nil,
		})
		if err != nil {
			return err
		}

		for _, devErr := range devErrs.GetInputErrors() {
			diagnostics = append(diagnostics, lsp.Diagnostic{
				Severity: lsp.Error,
				Range: lsp.Range{
					Start: lsp.Position{Line: int(devErr.Line) - 1, Character: int(devErr.Column) - 1},
					End:   lsp.Position{Line: int(devErr.Line) - 1, Character: int(devErr.Column) - 1},
				},
				Message: devErr.Message,
			})
		}

		return nil
	}); err != nil {
		return nil, err
	}

	return diagnostics, nil
}

func (s *Server) textDocDidChange(ctx context.Context, r *jsonrpc2.Request, conn *jsonrpc2.Conn) (any, error) {
	params, err := unmarshalParams[lsp.DidChangeTextDocumentParams](r)
	if err != nil {
		return nil, err
	}

	s.files.Set(params.TextDocument.URI, params.ContentChanges[0].Text, nil)

	if err := s.publishDiagnosticsIfNecessary(ctx, conn, params.TextDocument.URI); err != nil {
		return nil, err
	}

	return nil, nil
}

func (s *Server) textDocDidClose(_ context.Context, r *jsonrpc2.Request) (any, error) {
	params, err := unmarshalParams[lsp.DidCloseTextDocumentParams](r)
	if err != nil {
		return nil, err
	}

	s.files.Delete(params.TextDocument.URI)
	return nil, nil
}

func (s *Server) textDocDidOpen(ctx context.Context, r *jsonrpc2.Request, conn *jsonrpc2.Conn) (any, error) {
	params, err := unmarshalParams[lsp.DidOpenTextDocumentParams](r)
	if err != nil {
		return nil, err
	}

	uri := params.TextDocument.URI
	contents := params.TextDocument.Text
	s.files.Set(uri, contents, nil)

	if err := s.publishDiagnosticsIfNecessary(ctx, conn, uri); err != nil {
		return nil, err
	}

	log.Debug().
		Str("uri", string(uri)).
		Str("path", strings.TrimPrefix(string(uri), "file://")).
		Msg("refreshed file")

	return nil, nil
}

func (s *Server) publishDiagnosticsIfNecessary(ctx context.Context, conn *jsonrpc2.Conn, uri lsp.DocumentURI) error {
	requestsDiagnostics := s.requestsDiagnostics
	if requestsDiagnostics {
		return nil
	}

	log.Debug().
		Str("uri", string(uri)).
		Msg("publishing diagnostics")

	diagnostics, err := s.computeDiagnostics(ctx, uri)
	if err != nil {
		return fmt.Errorf("failed to compute diagnostics: %w", err)
	}

	return conn.Notify(ctx, "textDocument/publishDiagnostics", lsp.PublishDiagnosticsParams{
		URI:         uri,
		Diagnostics: diagnostics,
	})
}

func (s *Server) textDocFormat(ctx context.Context, r *jsonrpc2.Request) ([]lsp.TextEdit, error) {
	params, err := unmarshalParams[lsp.DocumentFormattingParams](r)
	if err != nil {
		return nil, err
	}

	var formatted string
	err = s.withFiles(func(files *persistent.Map[lsp.DocumentURI, string]) error {
		file, ok := files.Get(params.TextDocument.URI)
		if !ok {
			log.Warn().
				Str("uri", string(params.TextDocument.URI)).
				Msg("file not found for formatting")

			return &jsonrpc2.Error{Code: jsonrpc2.CodeInternalError, Message: "file not found"}
		}

		dctx, devErrs, err := development.NewDevContext(ctx, &developerv1.RequestContext{
			Schema:        file,
			Relationships: nil,
		})
		if err != nil {
			return err
		}

		if len(devErrs.GetInputErrors()) > 0 {
			return nil
		}

		formattedSchema, _, err := generator.GenerateSchema(dctx.CompiledSchema.OrderedDefinitions)
		if err != nil {
			return err
		}

		formatted = formattedSchema
		return nil
	})
	if err != nil {
		return nil, err
	}

	if formatted == "" {
		return nil, nil
	}

	return []lsp.TextEdit{
		{
			Range: lsp.Range{
				Start: lsp.Position{Line: 0, Character: 0},
				End:   lsp.Position{Line: 10000000, Character: 100000000}, // Replace the schema entirely
			},
			NewText: formatted,
		},
	}, nil
}

func (s *Server) initialized(_ context.Context, _ *jsonrpc2.Request) (any, error) {
	if s.state != serverStateInitialized {
		return nil, invalidRequest(errors.New("server not initialized"))
	}
	return nil, nil
}

func (s *Server) initialize(_ context.Context, r *jsonrpc2.Request) (any, error) {
	ip, err := unmarshalParams[InitializeParams](r)
	if err != nil {
		return nil, err
	}

	s.requestsDiagnostics = ip.Capabilities.Diagnostics.RefreshSupport
	log.Debug().
		Bool("requestsDiagnostics", s.requestsDiagnostics).
		Msg("initialize")

	if s.state != serverStateNotInitialized {
		return nil, invalidRequest(errors.New("already initialized"))
	}

	syncKind := lsp.TDSKFull
	s.state = serverStateInitialized
	return InitializeResult{
		Capabilities: ServerCapabilities{
			TextDocumentSync:           &lsp.TextDocumentSyncOptionsOrKind{Kind: &syncKind},
			CompletionProvider:         &lsp.CompletionOptions{TriggerCharacters: []string{"."}},
			DocumentFormattingProvider: true,
			DiagnosticProvider:         &DiagnosticOptions{Identifier: "spicedb", InterFileDependencies: false, WorkspaceDiagnostics: false},
		},
	}, nil
}

func (s *Server) shutdown() error {
	s.state = serverStateShuttingDown
	log.Debug().
		Msg("shutting down LSP server")
	return nil
}

func (s *Server) withFiles(fn func(*persistent.Map[lsp.DocumentURI, string]) error) error {
	clone := s.files.Clone()
	defer clone.Destroy()
	return fn(clone)
}
