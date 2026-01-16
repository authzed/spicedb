package fdw

import (
	"context"
	"fmt"
	"sync"

	wire "github.com/jeroenrinzema/psql-wire"
	pg_query "github.com/pganalyze/pg_query_go/v6"

	"github.com/authzed/ctxkey"

	"github.com/authzed/spicedb/internal/fdw/common"
)

// session represents a PostgreSQL session state for the FDW.
// It tracks transaction state and maintains cursors for the current session.
// All methods are thread-safe.
type session struct {
	mu sync.Mutex

	// withinTransaction indicates whether the session is currently inside a transaction.
	withinTransaction bool // GUARDED_BY(mu)

	// cursors maps cursor names to their corresponding cursor objects.
	cursors map[string]*cursor // GUARDED_BY(mu)
}

// addCursor adds a cursor to the session's cursor map.
// This method is thread-safe.
func (s *session) addCursor(cursor *cursor) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cursors[cursor.name] = cursor
}

// deleteCursor removes a cursor from the session by name.
// This method is thread-safe.
func (s *session) deleteCursor(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.cursors, name)
}

// lookupCursor retrieves a cursor by name from the session.
// Returns an error if the cursor is not found.
// This method is thread-safe.
func (s *session) lookupCursor(name string) (*cursor, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cursor, ok := s.cursors[name]
	if !ok {
		return nil, common.NewSemanticsError(fmt.Errorf("cursor %q not found", name))
	}
	return cursor, nil
}

// sessionKey is the context key used to store and retrieve session state.
var sessionKey = ctxkey.New[*session]()

// sessionMiddleware is a middleware function that initializes a new session
// and stores it in the context. This should be called once per connection.
func sessionMiddleware(ctx context.Context) (context.Context, error) {
	return sessionKey.Set(ctx, &session{
		cursors: make(map[string]*cursor),
	}), nil
}

// mustSession retrieves the session from the context.
// Panics if the session is not present in the context.
func mustSession(ctx context.Context) *session {
	return sessionKey.MustValue(ctx)
}

// handleDeallocateStmt handles PostgreSQL DEALLOCATE statements.
// DEALLOCATE removes prepared statements and cursors by name.
// If the DEALLOCATE ALL form is used, all cursors are removed.
func (p *PgBackend) handleDeallocateStmt(_ context.Context, stmt *pg_query.DeallocateStmt, _ string) (wire.PreparedStatements, error) {
	handle := func(ctx context.Context, writer wire.DataWriter, parameters []wire.Parameter) error {
		session := mustSession(ctx)
		session.mu.Lock()
		defer session.mu.Unlock()

		if stmt.GetIsall() {
			// DEALLOCATE ALL - remove all cursors
			session.cursors = make(map[string]*cursor)
			return writer.Complete("deallocated all")
		}

		// DEALLOCATE <name> - remove specific cursor if it exists
		delete(session.cursors, stmt.GetName())

		return writer.Complete("deallocated cursor")
	}
	return wire.Prepared(wire.NewStatement(handle)), nil
}
