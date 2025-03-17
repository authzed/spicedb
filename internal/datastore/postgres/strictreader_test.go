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

func (mc fakeQuerier) ExecFunc(_ context.Context, _ func(ctx context.Context, tag pgconn.CommandTag, err error) error, _ string, _ ...interface{}) error {
	return mc.err
}

func TestStrictReaderDetectsLagErrors(t *testing.T) {
	mc := fakeQuerier{}
	reader := strictReaderQueryFuncs{
		wrapped: mc,
	}

	cases := []struct {
		name string
		in   error
		want error
	}{
		{
			name: "no error is bubbledtest",
		},
		{
			name: "missing revision on replica - invalid argument",
			in:   &pgconn.PgError{Code: "22023", Message: "is in the future"},
			want: common.RevisionUnavailableError{},
		},
		{
			name: "missing revision on replica",
			in:   &pgconn.PgError{Message: "replica missing revision"},
			want: common.RevisionUnavailableError{},
		},
		{
			name: "serialization error due to conflicting WAL changes on replica",
			in:   &pgconn.PgError{Code: "40001"},
			want: common.RevisionUnavailableError{},
		},
		{
			name: "bubbles up unrelated error",
			in:   fmt.Errorf("unrelated error"),
			want: fmt.Errorf("unrelated error"),
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			received := tt.in
			expected := tt.want

			mc.err = received
			reader.wrapped = mc
			err := reader.ExecFunc(context.Background(), nil, "SELECT 1")
			if expected != nil {
				require.ErrorAs(t, err, &expected)
			} else {
				require.NoError(t, err)
			}

			err = reader.QueryFunc(context.Background(), nil, "SELECT 1")
			if expected != nil {
				require.ErrorAs(t, err, &expected)
			} else {
				require.NoError(t, err)
			}

			err = reader.QueryRowFunc(context.Background(), nil, "SELECT 1")
			if expected != nil {
				require.ErrorAs(t, err, &expected)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
