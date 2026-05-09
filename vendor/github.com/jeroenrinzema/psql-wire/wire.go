package wire

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jeroenrinzema/psql-wire/pkg/buffer"
	"github.com/jeroenrinzema/psql-wire/pkg/types"
)

type contextKey string

const sessionKey contextKey = "pgsession"

// GetSession retrieves the session from the context.
// The first return value is the session object, which can be used to access all session data.
// The second return value indicates whether the session was found in the context.
func GetSession(ctx context.Context) (*Session, bool) {
	session, ok := ctx.Value(sessionKey).(*Session)
	return session, ok
}

// GetAttribute retrieves a custom attribute from the session by key.
// The first return value is the attribute value, which will be nil if the attribute doesn't exist.
// The second return value indicates whether the attribute was found.
//
// Example:
//
//	tenantID, ok := wire.GetAttribute(ctx, "tenant_id")
//	if ok {
//	    // Use tenantID
//	}
func GetAttribute(ctx context.Context, key string) (interface{}, bool) {
	session, ok := GetSession(ctx)
	if !ok {
		return nil, false
	}

	value, exists := session.Attributes[key]
	return value, exists
}

// SetAttribute sets a custom attribute in the session.
// The key is the attribute name, and value can be any type.
// Returns true if the attribute was set successfully, false if the session wasn't found.
//
// Example:
//
//	wire.SetAttribute(ctx, "tenant_id", "tenant-123")
func SetAttribute(ctx context.Context, key string, value interface{}) bool {
	session, ok := GetSession(ctx)
	if !ok {
		return false
	}

	session.Attributes[key] = value
	return true
}

// ListenAndServe opens a new Postgres server using the given address and
// default configurations. The given handler function is used to handle simple
// queries. This method should be used to construct a simple Postgres server for
// testing purposes or simple use cases.
func ListenAndServe(address string, handler ParseFn) error {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	server, err := NewServer(handler, Logger(logger))
	if err != nil {
		return err
	}

	return server.ListenAndServe(address)
}

// NewServer constructs a new Postgres server using the given address and server options.
func NewServer(parse ParseFn, options ...OptionFn) (*Server, error) {
	srv := &Server{
		parse:           parse,
		logger:          slog.Default(),
		closer:          make(chan struct{}),
		ClientAuth:      tls.NoClientCert,
		Statements:      DefaultStatementCacheFn,
		Portals:         DefaultPortalCacheFn,
		Session:         func(ctx context.Context) (context.Context, error) { return ctx, nil },
		ShutdownTimeout: 1 * time.Second,
	}

	for _, option := range options {
		err := option(srv)
		if err != nil {
			return nil, fmt.Errorf("unexpected error while attempting to configure a new server: %w", err)
		}
	}

	return srv, nil
}

// Server contains options for listening to an address.
type Server struct {
	closing          atomic.Bool
	wg               sync.WaitGroup
	logger           *slog.Logger
	Auth             AuthStrategy
	BackendKeyData   BackendKeyDataFunc
	CancelRequest    CancelRequestFn
	BufferedMsgSize  int
	Parameters       Parameters
	TLSConfig        *tls.Config
	ClientAuth       tls.ClientAuthType
	parse            ParseFn
	Session          SessionHandler
	Statements       func() StatementCache
	Portals          func() PortalCache
	CloseConn        CloseFn
	TerminateConn    CloseFn
	FlushConn        FlushFn
	ParallelPipeline ParallelPipelineConfig
	Version          string
	ShutdownTimeout  time.Duration
	typeExtension    func(*pgtype.Map)
	closer           chan struct{}
}

// ListenAndServe opens a new Postgres server on the preconfigured address and
// starts accepting and serving incoming client connections.
func (srv *Server) ListenAndServe(address string) error {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}

	return srv.Serve(listener)
}

// Serve accepts and serves incoming Postgres client connections using the
// preconfigured configurations. The given listener will be closed once the
// server is gracefully closed.
func (srv *Server) Serve(listener net.Listener) error {
	// Early check to avoid logging and work if shutdown already started
	if srv.closing.Load() {
		return nil
	}

	srv.logger.Info("serving incoming connections", slog.String("addr", listener.Addr().String()))

	// Double-check: if shutdown started between the check and Add, we must undo
	if srv.closing.Load() {
		return nil
	}

	srv.wg.Add(1)
	go func() {
		defer srv.wg.Done()
		<-srv.closer

		srv.logger.Info("closing server")

		err := listener.Close()
		if err != nil {
			srv.logger.Error("unexpected error while attempting to close the net listener", "err", err)
		}
	}()

	for {
		conn, err := listener.Accept()
		if errors.Is(err, net.ErrClosed) {
			return nil
		}

		if err != nil {
			return err
		}

		// If shutdown has started, close the connection immediately
		if srv.closing.Load() {
			_ = conn.Close()
			continue
		}

		go func() {
			ctx := context.Background()
			err = srv.serve(ctx, conn)
			if err != nil {
				if srv.isNormalConnectionClosure(err) {
					srv.logger.Debug("client connection closed", "err", err)
				} else {
					srv.logger.Error("an unexpected error got returned while serving a client connection", "err", err)
				}
			}
		}()
	}
}

func (srv *Server) serve(ctx context.Context, conn net.Conn) error {
	// Create a per-connection pgx Map to avoid concurrent map writes
	// Each connection gets its own type map instance to prevent race conditions
	// when multiple goroutines access the same map concurrently during query execution
	connectionTypes := pgtype.NewMap()

	// Apply any type extension configured via ExtendTypes
	if srv.typeExtension != nil {
		srv.typeExtension(connectionTypes)
	}

	ctx = setTypeInfo(ctx, connectionTypes)
	ctx = setRemoteAddress(ctx, conn.RemoteAddr())
	defer conn.Close() //nolint:errcheck

	srv.logger.Debug("serving a new client connection")

	conn, version, reader, err := srv.Handshake(conn)
	if err != nil {
		return err
	}

	if version == types.VersionCancel {
		return conn.Close()
	}

	srv.logger.Debug("handshake successful, validating authentication")

	writer := buffer.NewWriter(srv.logger, conn)
	ctx, err = srv.readClientParameters(ctx, reader)
	if err != nil {
		return err
	}

	ctx, err = srv.handleAuth(ctx, reader, writer)
	if err != nil {
		return err
	}

	// Send BackendKeyData if a BackendKeyDataFunc is configured
	if srv.BackendKeyData != nil {
		srv.logger.Debug("sending backend key data")
		processID, secretKey := srv.BackendKeyData(ctx)
		err = writeBackendKeyData(writer, processID, secretKey)
		if err != nil {
			return err
		}
	}

	srv.logger.Debug("connection authenticated, writing server parameters")

	ctx, err = srv.writeParameters(ctx, writer, srv.Parameters)
	if err != nil {
		return err
	}

	ctx, err = srv.Session(ctx)
	if err != nil {
		return err
	}

	session := &Session{
		Server:           srv,
		Statements:       srv.Statements(),
		Portals:          srv.Portals(),
		Attributes:       make(map[string]interface{}),
		ParallelPipeline: srv.ParallelPipeline,
	}

	if srv.ParallelPipeline.Enabled {
		session.ResponseQueue = NewResponseQueue()
	}

	ctx = context.WithValue(ctx, sessionKey, session)

	return session.consumeCommands(ctx, conn, reader, writer)
}

// Close gracefully closes the underlaying Postgres server.
func (srv *Server) Close() error {
	if srv.closing.Load() {
		return nil
	}

	srv.closing.Store(true)
	close(srv.closer)
	srv.wg.Wait()
	return nil
}

// Shutdown gracefully shuts down the server with context and timeout support.
// It stops accepting new connections and waits for active connections to finish
// within the shorter of the context deadline or the server's configured ShutdownTimeout.
// If the context has no deadline, the server's ShutdownTimeout is used.
func (srv *Server) Shutdown(ctx context.Context) error {
	// Check if already shutting down or shut down
	if srv.closing.Load() {
		// If already closing, just wait for existing shutdown to complete
		srv.wg.Wait()
		return nil
	}

	// Use the shorter of context deadline or server timeout
	var shutdownCtx context.Context
	var cancel context.CancelFunc

	timeout := srv.ShutdownTimeout
	// Add our own timeout on top of the provided context. The earliest
	// deadline will win.
	if timeout == 0 {
		// Zero timeout means wait indefinitely
		shutdownCtx, cancel = context.WithCancel(ctx)
	} else {
		shutdownCtx, cancel = context.WithTimeout(ctx, timeout)
	}
	defer cancel()

	// Atomically check and set closing state
	if !srv.closing.CompareAndSwap(false, true) {
		// Another goroutine beat us to it, just wait for shutdown to complete
		srv.wg.Wait()
		return nil
	}

	srv.logger.Info("starting graceful shutdown")

	// Close the closer channel (we're the first/only one to get here)
	close(srv.closer)

	// Wait for active connections to finish or timeout
	done := make(chan struct{})
	go func() {
		srv.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		srv.logger.Info("graceful shutdown completed")
		return nil
	case <-shutdownCtx.Done():
		srv.logger.Warn("graceful shutdown timed out, some connections may be forcefully closed")
		return shutdownCtx.Err()
	}
}

// isNormalConnectionClosure checks if an error represents a normal client connection closure
// that shouldn't be logged as an error.
func (srv *Server) isNormalConnectionClosure(err error) bool {
	// Check for conn closed or conn termination normally
	if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
		return true
	}

	// Check for syscall errors that indicate normal connection closure
	var errno syscall.Errno
	if errors.As(err, &errno) {
		switch errno {
		case syscall.EPIPE, syscall.ECONNRESET:
			return true
		}
	}

	return false
}
