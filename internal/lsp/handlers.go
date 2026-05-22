package lsp

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"slices"
	"strings"

	"github.com/jzelinskie/persistent"
	"github.com/sourcegraph/go-lsp"
	"github.com/sourcegraph/jsonrpc2"

	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/development"
	"github.com/authzed/spicedb/pkg/genutil/slicez"
	developerv1 "github.com/authzed/spicedb/pkg/proto/developer/v1"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/generator"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/validationfile"
	"github.com/authzed/spicedb/pkg/validationfile/blocks"
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

		overlayFS := newLSPOverlayFS(uriToSourceDir(uri), files)

		if isYAMLFile(uri) {
			err := computeDiagnosticsForYaml(ctx, file, overlayFS, uri, &diagnostics)
			if err != nil {
				return err
			}
		} else {
			// We assume that the current uri that we are diagnosing is the root of a composable schema.
			devCtx, devErrs, err := development.NewDevContext(ctx, &developerv1.RequestContext{
				Schema:        file.contents,
				Relationships: nil,
			}, development.WithSourceFS(overlayFS), development.WithRootFileName(string(uri)))
			if err != nil {
				return err
			}
			// Get errors.
			// We filter out errors that are *not* specifically for URI.
			errors := devErrs.GetInputErrors()
			errorsForURI := slicez.Filter(errors, func(developerError *developerv1.DeveloperError) bool {
				return slices.Contains(developerError.Path, string(uri))
			})
			for _, devErr := range errorsForURI {
				diagnostics = append(diagnostics, newLspDiagnostic(devErr, lsp.Error))
			}

			// If there are no errors, we can also check for warnings.
			if len(errors) == 0 {
				warnings, err := development.GetWarnings(ctx, devCtx)
				if err != nil {
					return err
				}

				for _, devWarning := range warnings {
					diagnostics = append(diagnostics, newLspDiagnostic(devWarning, lsp.Warning))
				}
			}
		}

		return nil
	}); err != nil {
		return nil, err
	}

	return diagnostics, nil
}

func computeDiagnosticsForYaml(ctx context.Context, file trackedFile, overlayFS fs.FS, uri lsp.DocumentURI, diagnostics *[]lsp.Diagnostic) error {
	var off schemaOffset
	vf, verr := validationfile.DecodeValidationFile([]byte(file.contents))
	if verr != nil {
		return verr
	}
	usesSchemaFile := vf.SchemaFile != ""
	if !usesSchemaFile {
		// Compute offsets for inline schema position adjustment.
		// Schema error *lines* are already adjusted by NewDevContextForValidationFile,
		// but columns remain relative to the schema text and need the
		// YAML indent added.
		off = computeSchemaOffset(file.contents, vf.Schema.SourcePosition.LineNumber)
	}

	err := resolveSchemaFileIfPresent(overlayFS, vf)
	if err != nil {
		return err
	}

	vCtx, devErrs, err := development.NewDevContextForValidationFile(ctx, vf,
		development.WithSourceFS(overlayFS), development.WithRootFileName(string(uri)))
	if err != nil {
		return err
	}

	if devErrs != nil {
		for _, devErr := range devErrs.GetInputErrors() {
			if devErr.Source == developerv1.DeveloperError_SCHEMA && usesSchemaFile {
				// Schema errors from an external schemaFile belong to
				// that file, not the YAML file.
				continue
			}
			diag := newLspDiagnostic(devErr, lsp.Error)
			switch devErr.Source {
			case developerv1.DeveloperError_SCHEMA:
				diag.Range.Start.Character += off.colOff
				diag.Range.End.Character += off.colOff
			case developerv1.DeveloperError_RELATIONSHIP:
				adjustRelationshipDiagnostic(&diag, file.contents, devErr)
			}
			*diagnostics = append(*diagnostics, diag)
		}
	}

	if vCtx != nil {
		warnings, err := development.GetWarnings(ctx, vCtx.DevContext)
		if err != nil {
			return err
		}
		for _, devWarning := range warnings {
			diag := newLspDiagnostic(devWarning, lsp.Warning)
			adjustAssertionDiagnostic(&diag, file.contents)
			*diagnostics = append(*diagnostics, diag)
		}

		// Run assertions and surface failures as error diagnostics.
		assertionErrors, err := development.RunAllAssertions(vCtx.DevContext, &vCtx.Assertions)
		if err != nil {
			return err
		}
		for _, assertErr := range assertionErrors {
			diag := newLspDiagnostic(assertErr, lsp.Error)
			adjustAssertionDiagnostic(&diag, file.contents)
			*diagnostics = append(*diagnostics, diag)
		}
	}

	return nil
}

type DeveloperErrorWithPosition interface {
	// 1-indexed Line
	GetLine() uint32
	// 1-indexed Column
	GetColumn() uint32
	GetMessage() string
}

func newLspDiagnostic(devErr DeveloperErrorWithPosition, severity lsp.DiagnosticSeverity) lsp.Diagnostic {
	// lines and columns are 1-indexed
	return lsp.Diagnostic{
		Severity: severity,
		Range: lsp.Range{
			Start: lsp.Position{Line: int(devErr.GetLine()) - 1, Character: int(devErr.GetColumn()) - 1},
			End:   lsp.Position{Line: int(devErr.GetLine()) - 1, Character: int(devErr.GetColumn()) - 1},
		},
		Message: devErr.GetMessage(),
	}
}

func newLspRange(resolved *development.SchemaReference, schemaOff schemaOffset) *lsp.Range {
	// lines and columns are 0-indexed
	return &lsp.Range{
		Start: lsp.Position{
			Line:      resolved.TargetPosition.LineNumber + schemaOff.lineOff,
			Character: resolved.TargetPosition.ColumnPosition + resolved.TargetNamePositionOffset + schemaOff.colOff,
		},
		End: lsp.Position{
			Line:      resolved.TargetPosition.LineNumber + schemaOff.lineOff,
			Character: resolved.TargetPosition.ColumnPosition + resolved.TargetNamePositionOffset + len(resolved.Text) + schemaOff.colOff,
		},
	}
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

func (s *Server) didChangeWatchedFiles(ctx context.Context, r *jsonrpc2.Request, conn *jsonrpc2.Conn) (any, error) {
	params, err := unmarshalParams[lsp.DidChangeWatchedFilesParams](r)
	if err != nil {
		return nil, err
	}

	for _, change := range params.Changes {
		log.Info().
			Str("uri", string(change.URI)).
			Int("type", change.Type).
			Msg("watched file changed")
	}

	// Invalidate cached compiled schemas for every tracked file, since
	// any of them could import or reference the changed files.
	var uris []lsp.DocumentURI
	s.files.Range(func(uri lsp.DocumentURI, file trackedFile) {
		s.files.Set(uri, trackedFile{contents: file.contents}, nil)
		uris = append(uris, uri)
	})

	for _, uri := range uris {
		if err := s.publishDiagnosticsIfNecessary(ctx, conn, uri); err != nil {
			log.Warn().Err(err).Str("uri", string(uri)).Msg("failed to re-publish diagnostics after watched file change")
		}
	}

	return nil, nil
}

func (s *Server) publishDiagnosticsIfNecessary(ctx context.Context, conn *jsonrpc2.Conn, uri lsp.DocumentURI) error {
	if s.requestsDiagnostics {
		// Client uses pull diagnostics — ask it to re-request.
		return conn.Notify(ctx, "workspace/diagnostic/refresh", nil)
	}

	diagnostics, err := s.computeDiagnostics(ctx, uri)
	if err != nil {
		return fmt.Errorf("failed to compute diagnostics: %w", err)
	}

	log.Info().
		Str("uri", string(uri)).
		Int("diagnostics", len(diagnostics)).
		Msg("publishing diagnostics")

	return conn.Notify(ctx, "textDocument/publishDiagnostics", lsp.PublishDiagnosticsParams{
		URI:         uri,
		Diagnostics: diagnostics,
	})
}

func (s *Server) getCompiledContents(path lsp.DocumentURI, files *persistent.Map[lsp.DocumentURI, trackedFile]) (*compiler.CompiledSchema, schemaOffset, error) {
	file, ok := files.Get(path)
	if !ok {
		return nil, schemaOffset{}, &jsonrpc2.Error{Code: jsonrpc2.CodeInternalError, Message: "file not found"}
	}

	compiled := file.parsed
	if compiled != nil {
		return compiled, schemaOffset{}, nil
	}

	overlayFS := newLSPOverlayFS(uriToSourceDir(path), files)

	var off schemaOffset
	schemaText := file.contents
	if isYAMLFile(path) {
		vf, err := validationfile.DecodeValidationFile([]byte(file.contents))
		if err != nil {
			return nil, schemaOffset{}, &jsonrpc2.Error{Code: jsonrpc2.CodeInvalidRequest, Message: fmt.Sprintf("failed to decode YAML: %v", err)}
		}
		if vf.SchemaFile != "" {
			data, err := fs.ReadFile(overlayFS, vf.SchemaFile)
			if err != nil {
				return nil, schemaOffset{}, &jsonrpc2.Error{Code: jsonrpc2.CodeInvalidRequest, Message: fmt.Sprintf("failed to read schema file %q: %v", vf.SchemaFile, err)}
			}
			schemaText = string(data)
		} else {
			schemaText = vf.Schema.Schema
			off = computeSchemaOffset(file.contents, vf.Schema.SourcePosition.LineNumber)
		}
	}

	justCompiled, derr, err := development.CompileSchema(schemaText, development.WithSourceFS(overlayFS))
	if err != nil {
		return nil, schemaOffset{}, err
	}
	if derr != nil {
		return nil, schemaOffset{}, &jsonrpc2.Error{Code: jsonrpc2.CodeInternalError, Message: derr.String()}
	}

	files.Set(path, trackedFile{file.contents, justCompiled}, nil)
	return justCompiled, off, nil
}

func (s *Server) textDocHover(_ context.Context, r *jsonrpc2.Request) (*Hover, error) {
	params, err := unmarshalParams[lsp.TextDocumentPositionParams](r)
	if err != nil {
		return nil, err
	}

	var hoverContents *Hover
	err = s.withFiles(func(files *persistent.Map[lsp.DocumentURI, trackedFile]) error {
		compiled, schemaOff, err := s.getCompiledContents(params.TextDocument.URI, files)
		if err != nil {
			return err
		}

		resolver, err := development.NewSchemaPositionMapper(compiled)
		if err != nil {
			return err
		}

		// Translate the LSP position (relative to the file) into a schema-text
		// position by subtracting the YAML offset. For non-YAML files the offset
		// is zero so this is a no-op. Values are clamped to 0 so that hovering
		// outside the schema block doesn't produce negative indices.
		position := input.Position{
			LineNumber:     max(0, params.Position.Line-schemaOff.lineOff),
			ColumnPosition: max(0, params.Position.Character-schemaOff.colOff),
		}

		resolved, err := resolver.ReferenceAtPosition(input.Source("schema"), position)
		if err != nil {
			// For YAML files the cursor may be outside the schema block
			// (e.g. on assertions or relationships). The position maps to a
			// line that doesn't exist in the schema, so treat the error as
			// "no reference found" rather than a server error.
			if isYAMLFile(params.TextDocument.URI) {
				return nil
			}
			return err
		}

		if resolved == nil {
			return nil
		}

		var lspRange *lsp.Range
		if resolved.TargetPosition != nil {
			lspRange = newLspRange(resolved, schemaOff)
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

func (s *Server) textDocDefinition(_ context.Context, r *jsonrpc2.Request) (*lsp.Location, error) {
	params, err := unmarshalParams[lsp.TextDocumentPositionParams](r)
	if err != nil {
		return nil, err
	}

	var location *lsp.Location
	err = s.withFiles(func(files *persistent.Map[lsp.DocumentURI, trackedFile]) error {
		compiled, schemaOff, err := s.getCompiledContents(params.TextDocument.URI, files)
		if err != nil {
			return err
		}

		resolver, err := development.NewSchemaPositionMapper(compiled)
		if err != nil {
			return err
		}

		// Translate the LSP position (relative to the file) into a schema-text
		// position by subtracting the YAML offset. For non-YAML files the offset
		// is zero so this is a no-op. Values are clamped to 0 so that hovering
		// outside the schema block doesn't produce negative indices.
		position := input.Position{
			LineNumber:     max(0, params.Position.Line-schemaOff.lineOff),
			ColumnPosition: max(0, params.Position.Character-schemaOff.colOff),
		}

		resolved, err := resolver.ReferenceAtPosition(input.Source("schema"), position)
		if err != nil {
			if isYAMLFile(params.TextDocument.URI) {
				return nil
			}
			return err
		}

		if resolved == nil || resolved.TargetPosition == nil {
			return nil
		}

		// Determine the target file URI from TargetSource.
		targetURI := params.TextDocument.URI
		if resolved.TargetSource != nil && *resolved.TargetSource != "schema" {
			targetURI = resolveURI(params.TextDocument.URI, string(*resolved.TargetSource))
		}

		r := newLspRange(resolved, schemaOff)

		location = &lsp.Location{
			URI:   targetURI,
			Range: *r,
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return location, nil
}

func (s *Server) textDocFormat(ctx context.Context, r *jsonrpc2.Request) ([]lsp.TextEdit, error) {
	params, err := unmarshalParams[lsp.DocumentFormattingParams](r)
	if err != nil {
		return nil, err
	}

	if isYAMLFile(params.TextDocument.URI) {
		return nil, nil
	}

	var formatted string
	err = s.withFiles(func(files *persistent.Map[lsp.DocumentURI, trackedFile]) error {
		compiled, _, err := s.getCompiledContents(params.TextDocument.URI, files)
		if err != nil {
			return err
		}

		formattedSchema, _, err := generator.GenerateSchema(ctx, compiled.OrderedDefinitions)
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

	s.requestsDiagnostics = ip.Capabilities.SupportsPullDiagnostics()

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
			DefinitionProvider:         true,
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

func isYAMLFile(uri lsp.DocumentURI) bool {
	s := string(uri)
	return strings.HasSuffix(s, ".yaml") || strings.HasSuffix(s, ".yml")
}

// adjustAssertionDiagnostic adjusts an assertion diagnostic to highlight the
// full line content rather than a single character.
func adjustAssertionDiagnostic(diag *lsp.Diagnostic, contents string) {
	lines := strings.Split(contents, "\n")
	lineIdx := diag.Range.Start.Line // 0-indexed
	if lineIdx < 0 || lineIdx >= len(lines) {
		return
	}
	line := lines[lineIdx]
	trimmed := strings.TrimLeft(line, " \t")
	indent := len(line) - len(trimmed)
	diag.Range.Start.Character = indent
	diag.Range.End.Character = len(strings.TrimRight(line, " \t\r"))
}

// adjustRelationshipDiagnostic adjusts a relationship diagnostic to highlight
// the specific problematic token (e.g. the unknown relation or type) rather
// than pointing at the YAML block scalar indicator column.
func adjustRelationshipDiagnostic(diag *lsp.Diagnostic, contents string, devErr *developerv1.DeveloperError) {
	lines := strings.Split(contents, "\n")
	lineIdx := diag.Range.Start.Line // 0-indexed
	if lineIdx < 0 || lineIdx >= len(lines) {
		return
	}

	trimmed := strings.TrimLeft(lines[lineIdx], " \t")
	indent := len(lines[lineIdx]) - len(trimmed)

	start, end := relationshipErrorSpan(devErr.Context, devErr.Kind, devErr.Message)
	diag.Range.Start.Character = indent + start
	diag.Range.End.Character = indent + end
}

// relationshipErrorSpan returns the start (inclusive) and end (exclusive)
// byte offsets within a relationship string for the token that caused the error.
func relationshipErrorSpan(relStr string, kind developerv1.DeveloperError_ErrorKind, message string) (int, int) {
	switch kind {
	case developerv1.DeveloperError_UNKNOWN_RELATION:
		// Relation name is between '#' and '@': resource:id#relation@subject
		hashIdx := strings.Index(relStr, "#")
		atIdx := strings.Index(relStr, "@")
		if hashIdx >= 0 && atIdx > hashIdx {
			return hashIdx + 1, atIdx
		}
	case developerv1.DeveloperError_UNKNOWN_OBJECT_TYPE:
		// Extract the unknown type name from the error message (backtick-quoted)
		// and find it in the relationship string. This handles both unknown
		// resource types and unknown subject types.
		if name := extractBacktickQuoted(message); name != "" {
			if idx := strings.Index(relStr, name); idx >= 0 {
				return idx, idx + len(name)
			}
		}
	case developerv1.DeveloperError_INVALID_SUBJECT_TYPE:
		// Subject type is after '@' and before ':': ...@subjectType:id
		atIdx := strings.Index(relStr, "@")
		if atIdx >= 0 {
			rest := relStr[atIdx+1:]
			colonIdx := strings.Index(rest, ":")
			if colonIdx >= 0 {
				return atIdx + 1, atIdx + 1 + colonIdx
			}
		}
	}
	return 0, len(relStr)
}

func extractBacktickQuoted(s string) string {
	start := strings.Index(s, "`")
	if start < 0 {
		return ""
	}
	end := strings.Index(s[start+1:], "`")
	if end < 0 {
		return ""
	}
	return s[start+1 : start+1+end]
}

// schemaOffset holds the line and column offsets needed to translate between
// YAML file positions and schema text positions for inline YAML schemas.
// For non-YAML files and YAML files with schemaFile, both fields are 0.
type schemaOffset struct {
	lineOff int
	colOff  int
}

// computeSchemaOffset returns the offset of inline schema content within a YAML file.
// schemaNodeLine is the 1-indexed YAML node line for the schema key.
func computeSchemaOffset(yamlContent string, schemaNodeLine int) schemaOffset {
	lines := strings.Split(yamlContent, "\n")
	colOff := 0
	if schemaNodeLine < len(lines) {
		contentLine := lines[schemaNodeLine]
		colOff = len(contentLine) - len(strings.TrimLeft(contentLine, " "))
	}
	return schemaOffset{lineOff: schemaNodeLine, colOff: colOff}
}

// resolveSchemaFileIfPresent takes a ValidationFile and if the SchemaFile key is present,
// reads the schema from the given filesystem, populates the `Schema` key,
// and empties the `SchemaFile` key.
func resolveSchemaFileIfPresent(fsys fs.FS, validationFile *validationfile.ValidationFile) error {
	if validationFile.SchemaFile != "" {
		schemaBytes, err := fs.ReadFile(fsys, validationFile.SchemaFile)
		if err != nil {
			return err
		}
		validationFile.Schema = blocks.SchemaWithPosition{
			SourcePosition: spiceerrors.SourcePosition{LineNumber: 0, ColumnPosition: 1},
			Schema:         string(schemaBytes),
		}
		validationFile.SchemaFile = ""
	}
	return nil
}
