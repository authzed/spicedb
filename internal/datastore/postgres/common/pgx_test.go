package common

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
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
func (r *multiStatementRows) TypeMap() *pgtype.Map                         { return pgtype.NewMap() }

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

// TestConfigurePgxSetsPingTimeout ensures pools get a bounded acquire-time Ping so
// a half-open connection cannot hang the acquiring caller forever. pgxpool leaves
// PingTimeout unset by default, which is what allowed the deadline-stripped
// optimized-revision Ping to block indefinitely.
func TestConfigurePgxSetsPingTimeout(t *testing.T) {
	req := require.New(t)

	cfg, err := pgxpool.ParseConfig("postgres://localhost:5432/db")
	req.NoError(err)
	req.Zero(cfg.PingTimeout, "precondition: pgxpool does not set a ping timeout by default")

	req.NoError(PoolOptions{}.ConfigurePgx(cfg, false))
	req.Equal(defaultPingTimeout, cfg.PingTimeout)
}

// TestConfigurePgxAppliesPingTimeoutOption ensures the configured ConnPingTimeout
// option overrides the default.
func TestConfigurePgxAppliesPingTimeoutOption(t *testing.T) {
	req := require.New(t)

	cfg, err := pgxpool.ParseConfig("postgres://localhost:5432/db")
	req.NoError(err)

	pingTimeout := 17 * time.Second
	req.NoError(PoolOptions{ConnPingTimeout: &pingTimeout}.ConfigurePgx(cfg, false))
	req.Equal(pingTimeout, cfg.PingTimeout)
}

// TestConfigurePgxKeepsPoolMaxedWhenAMaximumIsConfigured pins the production
// default: every path that serves real traffic passes both bounds, and a pool
// whose maximum is configured is kept full.
func TestConfigurePgxKeepsPoolMaxedWhenAMaximumIsConfigured(t *testing.T) {
	req := require.New(t)

	cfg, err := pgxpool.ParseConfig("postgres://localhost:5432/db")
	req.NoError(err)

	maxOpen := 20
	req.NoError(PoolOptions{MaxOpenConns: &maxOpen}.ConfigurePgx(cfg, false))
	req.Equal(int32(20), cfg.MaxConns)
	req.Equal(int32(20), cfg.MinConns)
}

// TestConfigurePgxKeepsOneConnectionWhenNoBoundsAreConfigured covers the callers
// that build a datastore directly with no pool options: the test harness and
// library embedders. Defaulting their minimum to pgx's own MaxConns default --
// which is max(4, NumCPU) -- would make every such datastore establish a
// core-count's worth of connections synchronously before it could be used, for
// no benefit.
func TestConfigurePgxKeepsOneConnectionWhenNoBoundsAreConfigured(t *testing.T) {
	req := require.New(t)

	cfg, err := pgxpool.ParseConfig("postgres://localhost:5432/db")
	req.NoError(err)
	req.Positive(cfg.MaxConns, "precondition: pgxpool picks its own maximum")

	req.NoError(PoolOptions{}.ConfigurePgx(cfg, false))
	req.Equal(int32(1), cfg.MinConns)
	req.Greater(cfg.MaxConns, cfg.MinConns, "the maximum should still be pgx's own default")
}

// TestConfigurePgxAppliesExplicitMinimum ensures an explicitly configured
// minimum wins over either default.
func TestConfigurePgxAppliesExplicitMinimum(t *testing.T) {
	maxOpen := 20

	for _, tc := range []struct {
		name    string
		maxOpen *int
	}{
		{name: "with a maximum", maxOpen: &maxOpen},
		{name: "without a maximum"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := require.New(t)

			cfg, err := pgxpool.ParseConfig("postgres://localhost:5432/db")
			req.NoError(err)

			minOpen := 7
			req.NoError(PoolOptions{MinOpenConns: &minOpen, MaxOpenConns: tc.maxOpen}.ConfigurePgx(cfg, false))
			req.Equal(int32(7), cfg.MinConns)
		})
	}
}
