//go:build datastore

package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"testing"

	_ "github.com/go-sql-driver/mysql"

	"github.com/authzed/spicedb/pkg/datastore/test"
)

func init() {
	test.RegisterSchemaSnapshotter(Engine, snapshotMySQLSchema)
}

// mysqlAutoIncrementCounter matches the AUTO_INCREMENT table option that
// `SHOW CREATE TABLE` reports. It is the next value the counter will hand out,
// not part of the table's definition, so it moves whenever a row is inserted -
// including by the datastore recording its first transaction at startup. It is
// stripped so that writing data does not read as a schema change.
var mysqlAutoIncrementCounter = regexp.MustCompile(` AUTO_INCREMENT=\d+`)

// snapshotMySQLSchema captures the schema of a MySQL database as the CREATE
// statements the server itself reports, which carry the columns, indexes,
// constraints and table options of every table.
func snapshotMySQLSchema(ctx context.Context, _ testing.TB, uri string) (string, error) {
	db, err := sql.Open("mysql", uri)
	if err != nil {
		return "", fmt.Errorf("failed to connect to the mysql test database: %w", err)
	}
	defer func() { _ = db.Close() }()

	rows, err := db.QueryContext(ctx, "SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE()")
	if err != nil {
		return "", fmt.Errorf("failed to list the mysql tables: %w", err)
	}

	var tableNames []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			_ = rows.Close()
			return "", fmt.Errorf("failed to scan a table name: %w", err)
		}
		tableNames = append(tableNames, tableName)
	}
	_ = rows.Close()
	if err := rows.Err(); err != nil {
		return "", fmt.Errorf("failed to list the mysql tables: %w", err)
	}

	slices.Sort(tableNames)

	statements := make([]string, 0, len(tableNames))
	for _, tableName := range tableNames {
		var name, createStatement string
		// The table name is an identifier read back from information_schema, so
		// it cannot be a bind parameter here.
		if err := db.QueryRowContext(ctx, "SHOW CREATE TABLE `"+tableName+"`").Scan(&name, &createStatement); err != nil {
			return "", fmt.Errorf("failed to read the definition of table %q: %w", tableName, err)
		}
		statements = append(statements, mysqlAutoIncrementCounter.ReplaceAllString(createStatement, ""))
	}

	return strings.Join(statements, "\n"), nil
}
