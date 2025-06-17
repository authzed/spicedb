package postgres

import (
	"context"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/common"
)

type fakeQuerier struct {
	err error
}

func (mc fakeQuerier) QueryFunc(ctx context.Context, rowsFunc func(ctx context.Context, rows pgx.Rows) error, sql string, optionsAndArgs ...any) error {
	return mc.err
}

func (mc fakeQuerier) QueryRowFunc(ctx context.Context, rowFunc func(ctx context.Context, row pgx.Row) error, sql string, optionsAndArgs ...any) error {
	return mc.err
}

func (mc fakeQuerier) ExecFunc(_ context.Context, _ func(ctx context.Context, tag pgconn.CommandTag, err error) error, _ string, _ ...any) error {
	return mc.err
}

/*
NOTE: these tests aren't in a tabular format because we want to
assert require.ErrorsAs against different kinds of errors,
but you can't write a tabular test that takes different kinds of
errors in a struct because you lose type information.
*/

func TestStrictReaderDetectsLagErrors_NoErrors(t *testing.T) {
	mc := fakeQuerier{}
	reader := strictReaderQueryFuncs{
		wrapped: mc,
	}
	for _, err := range runQueries(t.Context(), reader) {
		require.NoError(t, err)
	}
}

func TestStrictReaderDetectsLagErrors_MissingRevisionInvalidArg(t *testing.T) {
	// missing revision on replica - invalid argument
	mc := fakeQuerier{
		err: &pgconn.PgError{Code: "22023", Message: "is in the future"},
	}
	reader := strictReaderQueryFuncs{
		wrapped: mc,
	}
	var expectedErr common.RevisionUnavailableError
	for _, err := range runQueries(t.Context(), reader) {
		require.ErrorAs(t, err, &expectedErr)
	}
}

func TestStrictReaderDetectsLagErrors_MissingRevisionOnReplica(t *testing.T) {
	// missing revision on replica
	mc := fakeQuerier{
		err: &pgconn.PgError{Message: "replica missing revision"},
	}
	reader := strictReaderQueryFuncs{
		wrapped: mc,
	}
	var expectedErr common.RevisionUnavailableError
	for _, err := range runQueries(t.Context(), reader) {
		require.ErrorAs(t, err, &expectedErr)
	}
}

func TestStrictReaderDetectsLagErrors_SerializationError(t *testing.T) {
	// serialization error due to conflicting WAL changes on replica
	mc := fakeQuerier{
		err: &pgconn.PgError{Code: "40001"},
	}
	reader := strictReaderQueryFuncs{
		wrapped: mc,
	}
	var expectedErr common.RevisionUnavailableError
	for _, err := range runQueries(t.Context(), reader) {
		require.ErrorAs(t, err, &expectedErr)
	}
}

func TestStrictReaderDetectsLagErrors_UnrelatedError(t *testing.T) {
	// bubbles up unrelated error
	mc := fakeQuerier{
		err: common.NewSerializationError(fmt.Errorf("augh")),
	}
	reader := strictReaderQueryFuncs{
		wrapped: mc,
	}
	var expectedErr common.SerializationError
	for _, err := range runQueries(t.Context(), reader) {
		require.ErrorAs(t, err, &expectedErr)
	}
}

func runQueries(ctx context.Context, reader strictReaderQueryFuncs) []error {
	return []error{
		reader.ExecFunc(ctx, nil, "SELECT 1"),
		reader.QueryFunc(ctx, nil, "SELECT 1"),
		reader.QueryRowFunc(ctx, nil, "SELECT 1"),
	}
}
