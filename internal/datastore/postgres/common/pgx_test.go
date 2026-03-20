package common

import (
	"context"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

// multiStatementRows simulates pgx.Rows behavior for a two-statement simple-protocol query
// where the first statement (SELECT) returns 0 rows successfully, and the second statement
// (DO ASSERT) produces an error.
type multiStatementRows struct {
	closed          bool
	secondStmtError error
}

func (r *multiStatementRows) Next() bool                                   { return false }
func (r *multiStatementRows) Scan(...any) error                            { return fmt.Errorf("no rows") }
func (r *multiStatementRows) Values() ([]any, error)                       { return nil, nil }
func (r *multiStatementRows) RawValues() [][]byte                          { return nil }
func (r *multiStatementRows) Conn() *pgx.Conn                              { return nil }
func (r *multiStatementRows) CommandTag() pgconn.CommandTag                { return pgconn.NewCommandTag("SELECT 0") }
func (r *multiStatementRows) FieldDescriptions() []pgconn.FieldDescription { return nil }

func (r *multiStatementRows) Err() error {
	if r.closed {
		return r.secondStmtError
	}
	// Before Close(), first result set had no error
	return nil
}

func (r *multiStatementRows) Close() {
	r.closed = true
}

// fakeMultiStatementQuerier returns multiStatementRows from Query to simulate a
// two-statement batch where the SELECT succeeds and the ASSERT fails.
type fakeMultiStatementQuerier struct {
	secondStmtError error
}

func (q *fakeMultiStatementQuerier) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.NewCommandTag(""), nil
}

func (q *fakeMultiStatementQuerier) Query(_ context.Context, _ string, _ ...any) (pgx.Rows, error) {
	return &multiStatementRows{secondStmtError: q.secondStmtError}, nil
}

func (q *fakeMultiStatementQuerier) QueryRow(context.Context, string, ...any) pgx.Row {
	return nil
}

// TestQueryFuncSurfacesErrorFromSecondStatement verifies that QueryFunc returns
// an error produced by the second statement in a multi-statement batch (e.g. a
// DO ASSERT used by strict read mode). The second statement's error only becomes
// visible on pgx.Rows after Close() drains all result sets.
func TestQueryFuncSurfacesErrorFromSecondStatement(t *testing.T) {
	assertErr := &pgconn.PgError{Code: "P0004", Message: "replica missing revision"}
	querier := QuerierFuncsFor(&fakeMultiStatementQuerier{secondStmtError: assertErr})

	err := querier.QueryFunc(t.Context(), func(_ context.Context, rows pgx.Rows) error {
		// Simulate reading: no rows returned
		for rows.Next() {
			t.Fatal("expected no rows")
		}
		return rows.Err()
	}, "SELECT 1")

	require.ErrorIs(t, err, assertErr)
}
