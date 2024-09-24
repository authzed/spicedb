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
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/generator"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
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
	if err := s.withFiles(func(files *persistent.Map[lsp.DocumentURI, trackedFile]) error {
		file, ok := files.Get(uri)
		if !ok {
			log.Warn().
				Str("uri", string(uri)).
				Msg("file not found for diagnostics")

			return &jsonrpc2.Error{Code: jsonrpc2.CodeInternalError, Message: "file not found"}
		}

		devCtx, devErrs, err := development.NewDevContext(ctx, &developerv1.RequestContext{
			Schema:        file.contents,
			Relationships: nil,
		})
		if err != nil {
			return err
		}

		// Get errors.
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

		// If there are no errors, we can also check for warnings.
		if len(diagnostics) == 0 {
			warnings, err := development.GetWarnings(ctx, devCtx)
			if err != nil {
				return err
			}

			for _, devWarning := range warnings {
				diagnostics = append(diagnostics, lsp.Diagnostic{
					Severity: lsp.Warning,
					Range: lsp.Range{
						Start: lsp.Position{Line: int(devWarning.Line) - 1, Character: int(devWarning.Column) - 1},
						End:   lsp.Position{Line: int(devWarning.Line) - 1, Character: int(devWarning.Column) - 1},
					},
					Message: devWarning.Message,
				})
			}
		}

		return nil
	}); err != nil {
		return nil, err
	}

	log.Info().Int("diagnostics", len(diagnostics)).Str("uri", string(uri)).Msg("computed diagnostics")
	return diagnostics, nil
}

func (s *Server) textDocDidSave(ctx context.Context, r *jsonrpc2.Request, conn *jsonrpc2.Conn) (any, error) {
	params, err := unmarshalParams[lsp.DidSaveTextDocumentParams](r)
	if err != nil {
		return nil, err
	}

	if err := s.publishDiagnosticsIfNecessary(ctx, conn, params.TextDocument.URI); err != nil {
		return nil, err
	}

	return nil, nil
}

func (s *Server) textDocDidChange(ctx context.Context, r *jsonrpc2.Request, conn *jsonrpc2.Conn) (any, error) {
	params, err := unmarshalParams[lsp.DidChangeTextDocumentParams](r)
	if err != nil {
		return nil, err
	}

	s.files.Set(params.TextDocument.URI, trackedFile{params.ContentChanges[0].Text, nil}, nil)

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
	s.files.Set(uri, trackedFile{contents, nil}, nil)

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

func (s *Server) getCompiledContents(path lsp.DocumentURI, files *persistent.Map[lsp.DocumentURI, trackedFile]) (*compiler.CompiledSchema, error) {
	file, ok := files.Get(path)
	if !ok {
		return nil, &jsonrpc2.Error{Code: jsonrpc2.CodeInternalError, Message: "file not found"}
	}

	compiled := file.parsed
	if compiled != nil {
		return compiled, nil
	}

	justCompiled, derr, err := development.CompileSchema(file.contents)
	if err != nil {
		return nil, err
	}
	if derr != nil {
		return nil, &jsonrpc2.Error{Code: jsonrpc2.CodeInternalError, Message: derr.String()}
	}

	files.Set(path, trackedFile{file.contents, justCompiled}, nil)
	return justCompiled, nil
}

func (s *Server) textDocHover(_ context.Context, r *jsonrpc2.Request) (*Hover, error) {
	params, err := unmarshalParams[lsp.TextDocumentPositionParams](r)
	if err != nil {
		return nil, err
	}

	var hoverContents *Hover
	err = s.withFiles(func(files *persistent.Map[lsp.DocumentURI, trackedFile]) error {
		compiled, err := s.getCompiledContents(params.TextDocument.URI, files)
		if err != nil {
			return err
		}

		resolver, err := development.NewResolver(compiled)
		if err != nil {
			return err
		}

		position := input.Position{
			LineNumber:     params.Position.Line,
			ColumnPosition: params.Position.Character,
		}

		resolved, err := resolver.ReferenceAtPosition(input.Source("schema"), position)
		if err != nil {
			return err
		}

		if resolved == nil {
			return nil
		}

		var lspRange *lsp.Range
		if resolved.TargetPosition != nil {
			lspRange = &lsp.Range{
				Start: lsp.Position{
					Line:      resolved.TargetPosition.LineNumber,
					Character: resolved.TargetPosition.ColumnPosition + resolved.TargetNamePositionOffset,
				},
				End: lsp.Position{
					Line:      resolved.TargetPosition.LineNumber,
					Character: resolved.TargetPosition.ColumnPosition + resolved.TargetNamePositionOffset + len(resolved.Text),
				},
			}
		}

		if resolved.TargetSourceCode != "" {
			hoverContents = &Hover{
				Contents: MarkupContent{
					Language: "spicedb",
					Value:    resolved.TargetSourceCode,
				},
				Range: lspRange,
			}
		} else {
			hoverContents = &Hover{
				Contents: MarkupContent{
					Kind:  "markdown",
					Value: resolved.ReferenceMarkdown,
				},
				Range: lspRange,
			}
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return hoverContents, nil
}

func (s *Server) textDocFormat(_ context.Context, r *jsonrpc2.Request) ([]lsp.TextEdit, error) {
	params, err := unmarshalParams[lsp.DocumentFormattingParams](r)
	if err != nil {
		return nil, err
	}

	var formatted string
	err = s.withFiles(func(files *persistent.Map[lsp.DocumentURI, trackedFile]) error {
		compiled, err := s.getCompiledContents(params.TextDocument.URI, files)
		if err != nil {
			return err
		}

		formattedSchema, _, err := generator.GenerateSchema(compiled.OrderedDefinitions)
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
			HoverProvider:              true,
		},
	}, nil
}

func (s *Server) shutdown() error {
	s.state = serverStateShuttingDown
	log.Debug().
		Msg("shutting down LSP server")
	return nil
}

type trackedFile struct {
	contents string
	parsed   *compiler.CompiledSchema
}

func (s *Server) withFiles(fn func(*persistent.Map[lsp.DocumentURI, trackedFile]) error) error {
	clone := s.files.Clone()
	defer clone.Destroy()
	return fn(clone)
}
