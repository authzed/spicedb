package fdw

import (
	"context"
	"errors"
	"fmt"

	wire "github.com/jeroenrinzema/psql-wire"
	pg_query "github.com/pganalyze/pg_query_go/v6"
)

func (p *PgBackend) handleTransactionStmt(_ context.Context, stmt *pg_query.TransactionStmt, _ string) (wire.PreparedStatements, error) {
	handle := func(ctx context.Context, writer wire.DataWriter, parameters []wire.Parameter) error {
		switch stmt.Kind {
		// START TRANSACTION
		case pg_query.TransactionStmtKind_TRANS_STMT_START:
			session := sessionKey.MustValue(ctx)
			session.mu.Lock()
			defer session.mu.Unlock()

			if session.withinTransaction {
				return errors.New("cannot start transaction within a transaction")
			}

			session.withinTransaction = true
			return writer.Complete("started")

		// ABORT TRANSACTION
		case pg_query.TransactionStmtKind_TRANS_STMT_ROLLBACK:
			fallthrough

		// COMMIT
		case pg_query.TransactionStmtKind_TRANS_STMT_COMMIT:
			session := sessionKey.MustValue(ctx)
			session.mu.Lock()
			defer session.mu.Unlock()

			if !session.withinTransaction {
				return errors.New("cannot commit/rollback transaction outside of a transaction")
			}

			session.withinTransaction = false
			return writer.Complete("completed")

		default:
			return fmt.Errorf("not implemented: %v", stmt)
		}
	}
	return wire.Prepared(wire.NewStatement(handle)), nil
}
