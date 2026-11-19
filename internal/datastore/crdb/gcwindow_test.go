package crdb

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

func TestParseGCTTL(t *testing.T) {
	t.Parallel()

	tcs := []struct {
		name      string
		configSQL string
		expected  time.Duration
		expectErr bool
	}{
		{
			name: "range default",
			configSQL: `ALTER RANGE default CONFIGURE ZONE USING
	range_min_bytes = 134217728,
	range_max_bytes = 536870912,
	gc.ttlseconds = 14400,
	num_replicas = 3,
	constraints = '[]',
	lease_preferences = '[]'`,
			expected: 4 * time.Hour,
		},
		{
			name: "multi-region regional by row table",
			configSQL: `ALTER TABLE relation_tuple CONFIGURE ZONE USING
	range_min_bytes = 134217728,
	range_max_bytes = 536870912,
	gc.ttlseconds = 86400,
	num_replicas = 5,
	num_voters = 3,
	constraints = '{+region=us-east1: 1, +region=us-west1: 1}',
	voter_constraints = '[+region=us-east1]',
	lease_preferences = '[[+region=us-east1]]'`,
			expected: 24 * time.Hour,
		},
		{
			name:      "single digit",
			configSQL: "ALTER TABLE u CONFIGURE ZONE USING gc.ttlseconds = 7",
			expected:  7 * time.Second,
		},
		{
			name:      "no spaces",
			configSQL: "ALTER DATABASE d CONFIGURE ZONE USING gc.ttlseconds=600",
			expected:  10 * time.Minute,
		},
		{
			name:      "zero",
			configSQL: "ALTER RANGE default CONFIGURE ZONE USING gc.ttlseconds = 0",
			expectErr: true,
		},
		{
			name:      "missing",
			configSQL: "ALTER RANGE default CONFIGURE ZONE USING num_replicas = 3",
			expectErr: true,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ttl, err := parseGCTTL(tc.configSQL)
			if tc.expectErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expected, ttl)
		})
	}
}

type fakeZoneQuerier struct {
	zones   map[string]zoneResult
	queried []string
}

type zoneResult struct {
	target    string
	configSQL string
	err       error
}

func (f *fakeZoneQuerier) ExecFunc(context.Context, func(context.Context, pgconn.CommandTag, error) error, string, ...any) error {
	return errors.New("not implemented")
}

func (f *fakeZoneQuerier) QueryFunc(context.Context, func(context.Context, pgx.Rows) error, string, ...any) error {
	return errors.New("not implemented")
}

func (f *fakeZoneQuerier) QueryRowFunc(ctx context.Context, rowFunc func(context.Context, pgx.Row) error, sql string, _ ...any) error {
	f.queried = append(f.queried, sql)
	res, ok := f.zones[sql]
	if !ok {
		return &pgconn.PgError{Code: "42P01", Message: "relation does not exist"}
	}
	if res.err != nil {
		return res.err
	}
	return rowFunc(ctx, fakeZoneRow(res))
}

type fakeZoneRow zoneResult

func (r fakeZoneRow) Scan(dest ...any) error {
	*dest[0].(*string) = r.target
	*dest[1].(*string) = r.configSQL
	return nil
}

func TestResolveGCWindow(t *testing.T) {
	t.Parallel()

	const (
		tableQuery          = "SHOW ZONE CONFIGURATION FROM TABLE relation_tuple;"
		integrityTableQuery = "SHOW ZONE CONFIGURATION FROM TABLE relation_tuple_with_integrity;"
		rangeQuery          = queryShowZoneConfig
	)
	zone := func(target string, seconds string) zoneResult {
		return zoneResult{target: target, configSQL: "ALTER ... CONFIGURE ZONE USING gc.ttlseconds = " + seconds}
	}

	tcs := []struct {
		name              string
		withIntegrity     bool
		zones             map[string]zoneResult
		expectedWindow    time.Duration
		expectedQueries   []string
		expectedErrString string
	}{
		{
			name:            "database zone above range default",
			zones:           map[string]zoneResult{tableQuery: zone("DATABASE spicedb", "86400"), rangeQuery: zone("RANGE default", "14400")},
			expectedWindow:  24 * time.Hour,
			expectedQueries: []string{tableQuery},
		},
		{
			name:            "data zone above configured is capped",
			zones:           map[string]zoneResult{tableQuery: zone("DATABASE spicedb", "172800")},
			expectedWindow:  24 * time.Hour,
			expectedQueries: []string{tableQuery},
		},
		{
			name:            "database zone below range default",
			zones:           map[string]zoneResult{tableQuery: zone("DATABASE spicedb", "14400"), rangeQuery: zone("RANGE default", "90000")},
			expectedWindow:  4 * time.Hour,
			expectedQueries: []string{tableQuery},
		},
		{
			name:            "table override",
			zones:           map[string]zoneResult{tableQuery: zone("TABLE spicedb.public.relation_tuple", "21600")},
			expectedWindow:  6 * time.Hour,
			expectedQueries: []string{tableQuery},
		},
		{
			name:            "inherits range default",
			zones:           map[string]zoneResult{tableQuery: zone("RANGE default", "14400")},
			expectedWindow:  4 * time.Hour,
			expectedQueries: []string{tableQuery},
		},
		{
			name:            "table missing falls back to range default",
			zones:           map[string]zoneResult{rangeQuery: zone("RANGE default", "14400")},
			expectedWindow:  4 * time.Hour,
			expectedQueries: []string{tableQuery, rangeQuery},
		},
		{
			name:            "table query error falls back to range default",
			zones:           map[string]zoneResult{tableQuery: {err: errors.New("permission denied")}, rangeQuery: zone("RANGE default", "14400")},
			expectedWindow:  4 * time.Hour,
			expectedQueries: []string{tableQuery, rangeQuery},
		},
		{
			name:            "unparseable table zone falls back to range default",
			zones:           map[string]zoneResult{tableQuery: {target: "DATABASE spicedb", configSQL: "num_replicas = 3"}, rangeQuery: zone("RANGE default", "90000")},
			expectedWindow:  24 * time.Hour,
			expectedQueries: []string{tableQuery, rangeQuery},
		},
		{
			name:              "fallback range default error",
			zones:             map[string]zoneResult{tableQuery: {err: errors.New("permission denied")}, rangeQuery: {err: errors.New("boom")}},
			expectedErrString: "boom",
		},
		{
			name:            "integrity table",
			withIntegrity:   true,
			zones:           map[string]zoneResult{integrityTableQuery: zone("DATABASE spicedb", "21600"), tableQuery: zone("DATABASE spicedb", "86400")},
			expectedWindow:  6 * time.Hour,
			expectedQueries: []string{integrityTableQuery},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			querier := &fakeZoneQuerier{zones: tc.zones}
			window, err := resolveGCWindow(t.Context(), querier, 24*time.Hour, tc.withIntegrity)
			if tc.expectedErrString != "" {
				require.ErrorContains(t, err, tc.expectedErrString)
				require.Equal(t, []string{tableQuery, rangeQuery}, querier.queried)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expectedWindow, window)
			require.Equal(t, tc.expectedQueries, querier.queried)
		})
	}
}
