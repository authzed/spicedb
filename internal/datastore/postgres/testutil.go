//go:build ci && docker
// +build ci,docker

package postgres

import (
	"context"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	pgxcommon "github.com/authzed/spicedb/internal/datastore/postgres/common"
)

func getExplanation(ctx context.Context, querier pgxcommon.Querier, sql string, args []any) (string, error) {
	// Make sure Postgres stats are fully up-to-date so it selects the correct index.
	_, err := querier.Exec(ctx, "ANALYZE;")
	if err != nil {
		return "", err
	}

	explainRows, err := querier.Query(ctx, "EXPLAIN ANALYZE "+sql, args...)
	if err != nil {
		return "", err
	}

	explanation := ""
	for explainRows.Next() {
		explanation += string(explainRows.RawValues()[0]) + "\n"
	}
	explainRows.Close()
	if err := explainRows.Err(); err != nil {
		return "", err
	}
	return explanation, nil
}

type withQueryInterceptor struct {
	explanations map[string]string
}

func (ql *withQueryInterceptor) InterceptExec(ctx context.Context, querier pgxcommon.Querier, sql string, args ...interface{}) (pgconn.CommandTag, error) {
	if strings.HasPrefix(sql, "WITH") {
		// Note, we disable seqscan here to ensure we get an index scan for testing.
		_, err := querier.Exec(ctx, "set enable_seqscan = off;")
		if err != nil {
			return pgconn.CommandTag{}, err
		}

		explanation, err := getExplanation(ctx, querier, sql, args)
		if err != nil {
			return pgconn.CommandTag{}, err
		}

		ql.explanations[sql] = explanation
	}

	return querier.Exec(ctx, sql, args...)
}

func (ql *withQueryInterceptor) InterceptQueryRow(ctx context.Context, querier pgxcommon.Querier, sql string, optionsAndArgs ...interface{}) pgx.Row {
	return querier.QueryRow(ctx, sql, optionsAndArgs...)
}

func (ql *withQueryInterceptor) InterceptQuery(ctx context.Context, querier pgxcommon.Querier, sql string, args ...interface{}) (pgx.Rows, error) {
	return querier.Query(ctx, sql, args...)
}
