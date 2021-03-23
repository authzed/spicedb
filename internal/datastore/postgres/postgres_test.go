// +build ci

package postgres

import (
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/jmoiron/sqlx"
	"github.com/ory/dockertest"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/datastore/test"
	"github.com/authzed/spicedb/pkg/secrets"
)

type postgresTest struct {
	db   *sqlx.DB
	port string
}

func (pgt postgresTest) New(revisionFuzzingTimedelta time.Duration) (datastore.Datastore, error) {
	uniquePortion, err := secrets.TokenHex(4)
	if err != nil {
		return nil, err
	}

	newDBName := "db" + uniquePortion

	_, err = pgt.db.Exec("CREATE DATABASE " + newDBName)
	if err != nil {
		return nil, fmt.Errorf("unable to create database: %w", err)
	}

	connectStr := fmt.Sprintf(
		"postgres://postgres:secret@localhost:%s/%s?sslmode=disable",
		pgt.port,
		newDBName,
	)

	m, err := migrate.New("file://migrations", connectStr)
	if err != nil {
		return nil, fmt.Errorf("unable to initialize migration engine: %w", err)
	}

	err = m.Up()
	if err != nil {
		return nil, fmt.Errorf("unable to migrate database: %w", err)
	}

	return NewPostgresDatastore(connectStr, 0, revisionFuzzingTimedelta)
}

func TestPostgresDatastore(t *testing.T) {
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	resource, err := pool.Run("postgres", "9.6", []string{"POSTGRES_PASSWORD=secret", "POSTGRES_DB=postgres"})
	if err != nil {
		log.Fatalf("Could not start resource: %s", err)
	}

	defer func() {
		// When you're done, kill and remove the container
		if err = pool.Purge(resource); err != nil {
			log.Fatalf("Could not purge resource: %s", err)
		}
	}()

	var db *sqlx.DB
	port := resource.GetPort("5432/tcp")
	if err = pool.Retry(func() error {
		var err error
		db, err = sqlx.Connect("postgres", fmt.Sprintf("postgres://postgres:secret@localhost:%s/postgres?sslmode=disable", port))
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	tester := &postgresTest{db: db, port: port}

	test.TestAll(t, tester)
}
