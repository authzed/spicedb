// Package lsp implements the Language Server Protocol for SpiceDB schema
// development.
package lsp

import (
	"context"
	"encoding/json"
	"fmt"
	"net"

	"github.com/jzelinskie/persistent"
	"github.com/sourcegraph/go-lsp"
	"github.com/sourcegraph/jsonrpc2"
	"golang.org/x/sync/errgroup"

	log "github.com/authzed/spicedb/internal/logging"
)

type serverState int

const (
	serverStateNotInitialized serverState = iota
	serverStateInitialized
	serverStateShuttingDown
)

// Server is a Language Server Protocol server for SpiceDB schema development.
type Server struct {
	files *persistent.Map[lsp.DocumentURI, trackedFile]
	state serverState

	requestsDiagnostics bool
}

// NewServer returns a new Server.
func NewServer() *Server {
	return &Server{
		state: serverStateNotInitialized,
		files: persistent.NewMap[lsp.DocumentURI, trackedFile](func(x, y lsp.DocumentURI) bool {
			return string(x) < string(y)
		}),
	}
}

func (s *Server) Handle(ctx context.Context, conn *jsonrpc2.Conn, r *jsonrpc2.Request) {
	jsonrpc2.HandlerWithError(s.handle).Handle(ctx, conn, r)
}

func logJSONPtr(msg *json.RawMessage) string {
	if msg == nil {
		return "nil"
	}
	return string(*msg)
}

func (s *Server) handle(ctx context.Context, conn *jsonrpc2.Conn, r *jsonrpc2.Request) (result any, err error) {
	log.Ctx(ctx).Debug().
		Stringer("id", r.ID).
		Str("method", r.Method).
		Str("params", logJSONPtr(r.Params)).
		Msg("received LSP request")

	if s.state == serverStateShuttingDown {
		log.Ctx(ctx).Warn().
			Str("method", r.Method).
			Msg("ignoring request during shutdown")
		return nil, nil
	}

	// Reference: https://microsoft.github.io/language-server-protocol/specifications/lsp/3.17/specification/#lifeCycleMessages
	if r.Method != "initialize" && s.state != serverStateInitialized {
		return nil, &jsonrpc2.Error{
			Code:    codeUninitialized,
			Message: "server not initialized",
		}
	}

	switch r.Method {
	case "initialize":
		result, err = s.initialize(ctx, r)
	case "initialized":
		result, err = s.initialized(ctx, r)
	case "shutdown":
		result, err = nil, s.shutdown()
	case "exit":
		result, err = nil, conn.Close()
	case "textDocument/didOpen":
		result, err = s.textDocDidOpen(ctx, r, conn)
	case "textDocument/didClose":
		result, err = s.textDocDidClose(ctx, r)
	case "textDocument/didChange":
		result, err = s.textDocDidChange(ctx, r, conn)
	case "textDocument/didSave":
		result, err = s.textDocDidSave(ctx, r, conn)
	case "textDocument/diagnostic":
		result, err = s.textDocDiagnostic(ctx, r)
	case "textDocument/formatting":
		result, err = s.textDocFormat(ctx, r)
	case "textDocument/hover":
		result, err = s.textDocHover(ctx, r)
	default:
		log.Ctx(ctx).Warn().
			Str("method", r.Method).
			Msg("unsupported LSP method")
		return nil, nil
	}
	log.Ctx(ctx).Info().
		Stringer("id", r.ID).
		Str("method", r.Method).
		Str("params", logJSONPtr(r.Params)).
		Interface("response", result).
		Msg("responded to LSP request")
	return result, err
}

func (s *Server) listenStdin(ctx context.Context) error {
	log.Ctx(ctx).Info().
		Msg("listening for LSP connections on stdin")

	var connOpts []jsonrpc2.ConnOpt
	stream := jsonrpc2.NewBufferedStream(stdrwc{}, jsonrpc2.VSCodeObjectCodec{})
	conn := jsonrpc2.NewConn(ctx, stream, jsonrpc2.AsyncHandler(s), connOpts...)
	defer conn.Close()

	select {
	case <-ctx.Done():
	case <-conn.DisconnectNotify():
	}
	return nil
}

func (s *Server) listenTCP(ctx context.Context, addr string) error {
	log.Ctx(ctx).Info().
		Str("addr", addr).
		Msg("listening for LSP connections")

	l, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	defer l.Close()

	var g errgroup.Group

serving:
	for {
		select {
		case <-ctx.Done():
			break serving
		default:
			conn, err := l.Accept()
			if err != nil {
				continue
			}

			g.Go(func() error {
				stream := jsonrpc2.NewBufferedStream(conn, jsonrpc2.VSCodeObjectCodec{})
				jconn := jsonrpc2.NewConn(ctx, stream, s)
				defer jconn.Close()
				<-jconn.DisconnectNotify()
				return nil
			})
		}
	}

	return g.Wait()
}

// Run binds to the provided address and concurrently serves Language Server
// Protocol requests.
func (s *Server) Run(ctx context.Context, addr string, stdio bool) error {
	log.Ctx(ctx).Info().
		Str("addr", addr).
		Msg("starting LSP server")

	if addr == "-" && !stdio {
		return fmt.Errorf("cannot use stdin with stdio disabled")
	}

	if addr == "-" || stdio {
		return s.listenStdin(ctx)
	}
	return s.listenTCP(ctx, addr)
}
