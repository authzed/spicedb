package common

import "context"

// TransactionBeginner a type which is able to begin a transaction on a relational database.
type TransactionBeginner interface {
	BeginTransaction(ctx context.Context, readOnly bool) (Transaction, error)
}

// Transaction a type which represents the common functionality on transactions of relational
// databases.  This closely models the functionality of the database/sql.Tx and other related types.
type Transaction interface {
	Query(ctx context.Context, query string, args ...interface{}) (Rows, error)
	Exec(ctx context.Context, stmt string, args ...interface{}) error
	Rollback(ctx context.Context) error
	Commit(ctx context.Context) error
}

// Rows a type which represents the common functionality of rows on sql results.
// This closely models the functionality of the database/sql.Rows and other related types.
type Rows interface {
	Close()
	Err() error
	Next() bool
	Scan(...interface{}) error
}
