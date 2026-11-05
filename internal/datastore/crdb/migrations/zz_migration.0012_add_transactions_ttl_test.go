package migrations

import (
	"testing"

	"github.com/Masterminds/semver"
	"github.com/stretchr/testify/require"
)

func TestCRDBMigrationHeadIsTransactionsTTL(t *testing.T) {
	head, err := CRDBMigrations.HeadRevision()
	require.NoError(t, err)
	require.Equal(t, "add-transactions-ttl", head)

	names, err := CRDBMigrations.MigrationNames()
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(names), 2)
	require.Equal(t, "add-transactions-ttl", names[len(names)-1])
	require.Equal(t, "populate-schema-tables", names[len(names)-2])
}

func TestTransactionsTTLAlterSQL(t *testing.T) {
	mustVersion := func(t *testing.T, s string) *semver.Version {
		t.Helper()
		v, err := semver.NewVersion(s)
		require.NoError(t, err)
		return v
	}

	tests := []struct {
		name        string
		version     string
		wantSQL     string
		wantErr     bool
		wantContain []string
	}{
		{
			name:    "unsupported pre-22",
			version: "21.2.0",
			wantErr: true,
		},
		{
			name:        "v22.1 uses ttl_expire_after only",
			version:     "22.1.17",
			wantSQL:     addTransactionsTTLQueryBasic,
			wantContain: []string{"ttl_expire_after = '24 hours'"},
		},
		{
			name:    "v22.2 adds ttl_job_cron",
			version: "22.2.0",
			wantSQL: addTransactionsTTLQuery,
			wantContain: []string{
				"ttl_expire_after = '24 hours'",
				"ttl_job_cron = '@hourly'",
			},
		},
		{
			name:    "v23 uses ttl_job_cron without changefeed suppression",
			version: "23.1.0",
			wantSQL: addTransactionsTTLQuery,
			wantContain: []string{
				"ttl_expire_after = '24 hours'",
				"ttl_job_cron = '@hourly'",
			},
		},
		{
			name:    "v24 suppresses TTL deletes from changefeeds",
			version: "24.1.0",
			wantSQL: addTransactionsTTLQueryWithTTLIgnore,
			wantContain: []string{
				"ttl_expire_after = '24 hours'",
				"ttl_job_cron = '@hourly'",
				"ttl_disable_changefeed_replication = 'true'",
			},
		},
		{
			name:    "latest tested major uses changefeed suppression",
			version: "26.2.5",
			wantSQL: addTransactionsTTLQueryWithTTLIgnore,
			wantContain: []string{
				"ttl_expire_after = '24 hours'",
				"ttl_disable_changefeed_replication = 'true'",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sql, err := transactionsTTLAlterSQL(mustVersion(t, tt.version))
			if tt.wantErr {
				require.Error(t, err)
				require.Empty(t, sql)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantSQL, sql)
			for _, fragment := range tt.wantContain {
				require.Contains(t, sql, fragment)
			}
			require.Contains(t, sql, "ALTER TABLE transactions SET")
		})
	}
}
