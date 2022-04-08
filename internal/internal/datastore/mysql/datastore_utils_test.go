package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/datastore/mysql/migrations"
	"github.com/authzed/spicedb/pkg/migrate"
	"github.com/authzed/spicedb/pkg/secrets"
	"github.com/stretchr/testify/require"
)

const (
	chunkRelationshipCount = 2000
	mysqlPort              = 3306
	testDBPrefix           = "spicedb_test"
	creds                  = "root:secret"
)

var containerPort string

type sqlTest struct {
	tablePrefix string
}

func newTester() *sqlTest {
	return &sqlTest{}
}

func newPrefixTester(tablePrefix string) *sqlTest {
	return &sqlTest{tablePrefix: tablePrefix}
}

func (st *sqlTest) New(revisionFuzzingTimedelta, gcWindow time.Duration, _ uint16) (datastore.Datastore, error) {
	connectStr := setupDatabase()

	migrateDatabaseWithPrefix(connectStr, st.tablePrefix)

	ds, err := NewMysqlDatastore(connectStr,
		RevisionFuzzingTimedelta(revisionFuzzingTimedelta),
		GCWindow(gcWindow),
		GCInterval(0*time.Second), // Disable auto GC
		TablePrefix(st.tablePrefix),
	)
	if err != nil {
		return nil, err
	}

	// seed the base datastore revision
	if _, err := ds.IsReady(context.Background()); err != nil {
		return nil, err
	}
	return ds, nil
}

func createMigrationDriver(connectStr string) (*migrations.MysqlDriver, error) {
	return createMigrationDriverWithPrefix(connectStr, "")
}

func createMigrationDriverWithPrefix(connectStr string, prefix string) (*migrations.MysqlDriver, error) {
	migrationDriver, err := migrations.NewMysqlDriverFromDSN(connectStr, prefix)
	if err != nil {
		return nil, fmt.Errorf("unable to initialize migration engine: %w", err)
	}

	return migrationDriver, nil
}

func setupDatabase() string {
	var db *sql.DB
	connectStr := fmt.Sprintf("%s@(localhost:%s)/mysql", creds, containerPort)
	db, err := sql.Open("mysql", connectStr)
	if err != nil {
		log.Fatalf("couldn't open DB: %s", err)
	}
	defer func() {
		err := db.Close() // we do not want this connection to stay open
		if err != nil {
			log.Fatalf("failed to close db: %s", err)
		}
	}()

	uniquePortion, err := secrets.TokenHex(4)
	if err != nil {
		log.Fatalf("Could not generate unique portion of db name: %s", err)
	}
	dbName := testDBPrefix + uniquePortion

	tx, err := db.Begin()
	_, err = tx.Exec(fmt.Sprintf("CREATE DATABASE %s;", dbName))
	if err != nil {
		log.Fatalf("failed to create database: %s: %s", dbName, err)
	}

	err = tx.Commit()
	if err != nil {
		log.Fatalf("failed to commit: %s", err)
	}

	return fmt.Sprintf("%s@(localhost:%s)/%s?parseTime=true", creds, containerPort, dbName)
}

func migrateDatabase(connectStr string) {
	migrateDatabaseWithPrefix(connectStr, "")
}

func migrateDatabaseWithPrefix(connectStr, tablePrefix string) {
	migrationDriver, err := createMigrationDriverWithPrefix(connectStr, tablePrefix)
	if err != nil {
		log.Fatalf("failed to create prefixed migration driver: %s", err)
	}

	err = migrations.Manager.Run(migrationDriver, migrate.Head, migrate.LiveRun)
	if err != nil {
		log.Fatalf("failed to run migration: %s", err)
	}
}

func FailOnError(t *testing.T, f func() error) {
	require.NoError(t, f())
}
