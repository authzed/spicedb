package mysql

import "database/sql"

type mysqlDbRows struct {
	dbRows *sql.Rows
}

func (rows *mysqlDbRows) Close() error {
	return rows.dbRows.Close()
}

func (rows *mysqlDbRows) Columns() ([]string, error) {
	return rows.dbRows.Columns()
}

func (rows *mysqlDbRows) Err() error {
	return rows.dbRows.Err()
}

func (rows *mysqlDbRows) Next() bool {
	return rows.dbRows.Next()
}

func (rows *mysqlDbRows) Scan(...interface{}) error {
	return rows.dbRows.Scan()
}
