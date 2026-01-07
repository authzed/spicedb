package fdw

import (
	"context"
	"errors"
	"fmt"
	"sync"

	wire "github.com/jeroenrinzema/psql-wire"
	pg_query "github.com/pganalyze/pg_query_go/v6"

	"github.com/authzed/authzed-go/v1"

	log "github.com/authzed/spicedb/internal/logging"
)

// PgBackend implements a Postgres wire protocol server that translates SQL queries
// into SpiceDB API calls. It handles authentication, query parsing, and routing to
// appropriate table handlers.
type PgBackend struct {
	client   *authzed.Client
	server   *wire.Server // GUARDED_BY(mu)
	username string
	password string
	mu       sync.RWMutex
	wg       sync.WaitGroup
	closed   bool // GUARDED_BY(mu)
}

// NewPgBackend creates a new Postgres FDW backend server.
// The username and password are used for Postgres wire protocol authentication.
func NewPgBackend(client *authzed.Client, username, password string) *PgBackend {
	connHandler := &PgBackend{client: client, username: username, password: password}
	return connHandler
}

// Run starts the Postgres wire protocol server on the specified endpoint.
// It blocks until the context is cancelled or an error occurs.
func (p *PgBackend) Run(ctx context.Context, endpoint string) error {
	server, err := wire.NewServer(p.handler, wire.SessionMiddleware(sessionMiddleware))
	if err != nil {
		return err
	}

	server.Auth = wire.ClearTextPassword(p.validateAuth)

	// NOTE: uncomment to enable debug logging of the actual Postgres protocol.
	// slog.SetLogLoggerLevel(slog.LevelDebug)

	p.mu.Lock()
	p.server = server
	p.mu.Unlock()

	p.wg.Add(1)
	defer p.wg.Done()

	// Monitor context cancellation in a separate goroutine
	go func() {
		<-ctx.Done()
		// Don't call p.Close() here to avoid deadlock (Close waits on wg)
		// Just close the server directly, which will cause ListenAndServe to return
		// Use the closed flag to ensure we only close once
		p.mu.Lock()
		if !p.closed {
			p.closed = true
			srv := p.server
			p.mu.Unlock()
			if srv != nil {
				srv.Close()
			}
		} else {
			p.mu.Unlock()
		}
	}()

	return p.server.ListenAndServe(endpoint)
}

func (p *PgBackend) validateAuth(ctx context.Context, database, username, password string) (context.Context, bool, error) {
	if username != p.username {
		return ctx, false, fmt.Errorf("invalid username; expected %s", p.username)
	}

	if password != p.password {
		return ctx, false, errors.New("invalid password")
	}

	return ctx, true, nil
}

// Close gracefully shuts down the Postgres server.
func (p *PgBackend) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	server := p.server
	p.mu.Unlock()

	if server == nil {
		return nil
	}

	// Close the server (this will cause ListenAndServe to return)
	err := server.Close()

	// Wait for the Run goroutine to complete
	p.wg.Wait()

	return err
}

func (p *PgBackend) handler(ctx context.Context, query string) (wire.PreparedStatements, error) {
	parsed, err := pg_query.Parse(query)
	if err != nil {
		return nil, fmt.Errorf("failed to parse query: %w", err)
	}

	if len(parsed.Stmts) != 1 {
		return nil, errors.New("multiple statements not supported")
	}

	log.Trace().Str("query", query).Msg("pg backend query")

	stmt := parsed.Stmts[0].Stmt
	switch {
	// SET variable = value
	case stmt.GetVariableSetStmt() != nil:
		handle := func(ctx context.Context, writer wire.DataWriter, parameters []wire.Parameter) error {
			return writer.Complete("set ignored")
		}
		return wire.Prepared(wire.NewStatement(handle)), nil

	// EXPLAIN SELECT ...
	case stmt.GetExplainStmt() != nil:
		return p.handleExplainStmt(ctx, stmt.GetExplainStmt(), query)

	// DEALLOCATE c1
	// DEALLOCATE ALL
	case stmt.GetDeallocateStmt() != nil:
		return p.handleDeallocateStmt(ctx, stmt.GetDeallocateStmt(), query)

	// INSERT INTO ...
	case stmt.GetInsertStmt() != nil:
		return p.handleInsertStmt(ctx, stmt.GetInsertStmt(), query)

	// DELETE FROM ...
	case stmt.GetDeleteStmt() != nil:
		return p.handleDeleteStmt(ctx, stmt.GetDeleteStmt(), query)

	// DECLARE c1 CURSOR FOR SELECT ...
	case stmt.GetDeclareCursorStmt() != nil:
		return p.handleDeclareCursorStmt(ctx, stmt.GetDeclareCursorStmt(), query)

	// FETCH 100 FROM c1
	case stmt.GetFetchStmt() != nil:
		return p.handleFetchStmt(ctx, stmt.GetFetchStmt(), query)

	// CLOSE c1
	case stmt.GetClosePortalStmt() != nil:
		return p.handleClosePortalStmt(ctx, stmt.GetClosePortalStmt(), query)

	// START TRANSACTION
	// COMMIT
	// ABORT TRANSACTION
	case stmt.GetTransactionStmt() != nil:
		return p.handleTransactionStmt(ctx, stmt.GetTransactionStmt(), query)

	default:
		return nil, fmt.Errorf("not implemented: %v", stmt)
	}
}
