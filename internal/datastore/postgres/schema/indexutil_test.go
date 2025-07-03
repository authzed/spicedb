package schema

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/common"
)

// fakeConn implements execer for testing
type fakeConn struct {
	execFunc func(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
}

func (m *fakeConn) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	if m.execFunc != nil {
		return m.execFunc(ctx, sql, arguments...)
	}
	return pgconn.CommandTag{}, nil
}

// mockCommandTag creates a mock command tag
func mockCommandTag() pgconn.CommandTag {
	return pgconn.CommandTag{}
}

// createQueryCanceledError creates a mock PgError with query canceled code
func createQueryCanceledError() error {
	return &pgconn.PgError{
		Code: "57014", // pgQueryCanceled
	}
}

// createGenericError creates a generic database error
func createGenericError(message string) error {
	return &pgconn.PgError{
		Code:    "42000", // generic SQL error
		Message: message,
	}
}

func TestCreateIndexConcurrently_DropIndexTimeoutError(t *testing.T) {
	indexDef := common.IndexDefinition{
		Name:       "test_index",
		ColumnsSQL: "test_table (column1, column2)",
	}

	mock := &fakeConn{
		execFunc: func(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
			if strings.Contains(sql, "DROP INDEX") {
				return pgconn.CommandTag{}, createQueryCanceledError()
			}
			return mockCommandTag(), nil
		},
	}

	err := CreateIndexConcurrently(t.Context(), mock, indexDef)

	require.Error(t, err)
	require.Contains(t, err.Error(), "timed out while trying to drop index test_index")
	require.Contains(t, err.Error(), timeoutMessage)
}

func TestCreateIndexConcurrently_DropIndexGenericError(t *testing.T) {
	indexDef := common.IndexDefinition{
		Name:       "test_index",
		ColumnsSQL: "test_table (column1, column2)",
	}

	mock := &fakeConn{
		execFunc: func(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
			if strings.Contains(sql, "DROP INDEX") {
				return pgconn.CommandTag{}, createGenericError("permission denied")
			}
			return mockCommandTag(), nil
		},
	}

	err := CreateIndexConcurrently(t.Context(), mock, indexDef)

	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to drop index test_index before creating it")
	require.Contains(t, err.Error(), "permission denied")
	require.NotContains(t, err.Error(), timeoutMessage)
}

func TestCreateIndexConcurrently_CreateIndexTimeoutError(t *testing.T) {
	indexDef := common.IndexDefinition{
		Name:       "test_index",
		ColumnsSQL: "test_table (column1, column2)",
	}

	mock := &fakeConn{
		execFunc: func(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
			if strings.Contains(sql, "DROP INDEX") {
				return mockCommandTag(), nil
			}
			if strings.Contains(sql, "CREATE INDEX") {
				return pgconn.CommandTag{}, createQueryCanceledError()
			}
			return mockCommandTag(), nil
		},
	}

	err := CreateIndexConcurrently(t.Context(), mock, indexDef)

	require.Error(t, err)
	require.Contains(t, err.Error(), "timed out while trying to create index test_index")
	require.Contains(t, err.Error(), timeoutMessage)
}

func TestCreateIndexConcurrently_CreateIndexGenericError(t *testing.T) {
	indexDef := common.IndexDefinition{
		Name:       "test_index",
		ColumnsSQL: "test_table (column1, column2)",
	}

	mock := &fakeConn{
		execFunc: func(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
			if strings.Contains(sql, "DROP INDEX") {
				return mockCommandTag(), nil
			}
			if strings.Contains(sql, "CREATE INDEX") {
				return pgconn.CommandTag{}, createGenericError("invalid syntax")
			}
			return mockCommandTag(), nil
		},
	}

	err := CreateIndexConcurrently(t.Context(), mock, indexDef)

	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to create index test_index")
	require.Contains(t, err.Error(), "invalid syntax")
	require.NotContains(t, err.Error(), timeoutMessage)
}

func TestCreateIndexConcurrently_Success(t *testing.T) {
	indexDef := common.IndexDefinition{
		Name:       "test_index",
		ColumnsSQL: "test_table (column1, column2)",
	}

	execCalls := []string{}
	mock := &fakeConn{
		execFunc: func(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
			execCalls = append(execCalls, sql)
			return mockCommandTag(), nil
		},
	}

	err := CreateIndexConcurrently(t.Context(), mock, indexDef)

	require.NoError(t, err)
	require.Len(t, execCalls, 2)

	// Verify DROP INDEX was called first
	require.Contains(t, execCalls[0], "DROP INDEX CONCURRENTLY IF EXISTS")
	require.Contains(t, execCalls[0], "test_index")

	// Verify CREATE INDEX was called second
	require.Contains(t, execCalls[1], "CREATE INDEX CONCURRENTLY")
	require.Contains(t, execCalls[1], "test_index")
	require.Contains(t, execCalls[1], "test_table (column1, column2)")
}

func TestCreateIndexConcurrently_SQLTemplates(t *testing.T) {
	indexDef := common.IndexDefinition{
		Name:       "idx_relation_tuple_living_xid",
		ColumnsSQL: "relation_tuple (namespace, object_id, relation) WHERE deleted_xid = '9223372036854775807'::xid8",
	}

	executedQueries := []string{}
	mock := &fakeConn{
		execFunc: func(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
			executedQueries = append(executedQueries, sql)
			return mockCommandTag(), nil
		},
	}

	err := CreateIndexConcurrently(t.Context(), mock, indexDef)

	require.NoError(t, err)
	require.Len(t, executedQueries, 2)

	// Verify the exact SQL templates are used correctly
	expectedDropSQL := fmt.Sprintf(dropIndexTemplate, indexDef.Name)
	expectedCreateSQL := fmt.Sprintf(createIndexTemplate, indexDef.Name, indexDef.ColumnsSQL)

	require.Equal(t, strings.TrimSpace(expectedDropSQL), strings.TrimSpace(executedQueries[0]))
	require.Equal(t, strings.TrimSpace(expectedCreateSQL), strings.TrimSpace(executedQueries[1]))
}

func TestCreateIndexConcurrently_ContextCancellation(t *testing.T) {
	indexDef := common.IndexDefinition{
		Name:       "test_index",
		ColumnsSQL: "test_table (column1)",
	}

	ctx, cancel := context.WithCancel(t.Context())

	mock := &fakeConn{
		execFunc: func(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
			if strings.Contains(sql, "DROP INDEX") {
				return mockCommandTag(), nil
			}
			// Cancel context during CREATE INDEX
			cancel()
			return pgconn.CommandTag{}, context.Canceled
		},
	}

	err := CreateIndexConcurrently(ctx, mock, indexDef)

	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to create index test_index")
}

// Test that verifies the exact error message formatting
func TestCreateIndexConcurrently_ErrorMessageFormatting(t *testing.T) {
	testCases := []struct {
		name           string
		indexName      string
		phase          string // "drop" or "create"
		isTimeout      bool
		expectedSubstr []string
	}{
		{
			name:      "drop timeout with special characters in index name",
			indexName: "idx_test-name_with.special$chars",
			phase:     "drop",
			isTimeout: true,
			expectedSubstr: []string{
				"timed out while trying to drop index idx_test-name_with.special$chars",
				timeoutMessage,
			},
		},
		{
			name:      "create timeout with special characters in index name",
			indexName: "idx_test-name_with.special$chars",
			phase:     "create",
			isTimeout: true,
			expectedSubstr: []string{
				"timed out while trying to create index idx_test-name_with.special$chars",
				timeoutMessage,
			},
		},
		{
			name:      "drop generic error",
			indexName: "simple_index",
			phase:     "drop",
			isTimeout: false,
			expectedSubstr: []string{
				"failed to drop index simple_index before creating it",
			},
		},
		{
			name:      "create generic error",
			indexName: "simple_index",
			phase:     "create",
			isTimeout: false,
			expectedSubstr: []string{
				"failed to create index simple_index",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			indexDef := common.IndexDefinition{
				Name:       tc.indexName,
				ColumnsSQL: "test_table (column1)",
			}

			mock := &fakeConn{
				execFunc: func(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
					if tc.phase == "drop" && strings.Contains(sql, "DROP INDEX") {
						if tc.isTimeout {
							return pgconn.CommandTag{}, createQueryCanceledError()
						}
						return pgconn.CommandTag{}, createGenericError("test error")
					}
					if tc.phase == "create" && strings.Contains(sql, "CREATE INDEX") {
						if tc.isTimeout {
							return pgconn.CommandTag{}, createQueryCanceledError()
						}
						return pgconn.CommandTag{}, createGenericError("test error")
					}
					return mockCommandTag(), nil
				},
			}

			err := CreateIndexConcurrently(t.Context(), mock, indexDef)

			require.Error(t, err)
			for _, substr := range tc.expectedSubstr {
				require.Contains(t, err.Error(), substr)
			}
		})
	}
}
